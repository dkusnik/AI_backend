package pl.gov.nac.warc.utils.gzip;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Intel ISA-L based GZIP decompressor using Java FFM API.
 * Provides SIMD-accelerated decompression on Intel CPUs.
 *
 * <p>
 * Falls back to JDK implementation if ISA-L is not available.
 */
public final class IsalDecompressor implements GzipDecompressorFactory {

  public static final IsalDecompressor INSTANCE = new IsalDecompressor();

  private static final Logger log = LogManager.getLogger(IsalDecompressor.class);
  private static final boolean AVAILABLE;
  private static volatile boolean enabled = true;
  private static final MethodHandle INFLATE_INIT;
  private static final MethodHandle INFLATE;

  // inflate_state struct size — verified via offsetof on libisal-dev 2.31.0:
  // sizeof(inflate_state) = 87368 bytes on x86_64 Linux
  private static final int STATE_SIZE = 90000;

  // Offsets in inflate_state struct — verified via offsetof(struct inflate_state, field):
  private static final long NEXT_OUT_OFFSET = 0;    // uint8_t *next_out
  private static final long AVAIL_OUT_OFFSET = 8;   // uint32_t avail_out
  private static final long TOTAL_OUT_OFFSET = 12;  // uint32_t total_out
  private static final long NEXT_IN_OFFSET = 16;    // uint8_t *next_in
  private static final long AVAIL_IN_OFFSET = 32;   // uint32_t avail_in (after read_in uint64)
  private static final long CRC_FLAG_OFFSET = 21172; // uint32_t crc_flag
  private static final long BLOCK_STATE_OFFSET = 21160; // enum isal_block_state block_state

  // ISA-L constants
  private static final int IGZIP_GZIP = 1;       // crc_flag value: handle full gzip framing
  private static final int ISAL_BLOCK_FINISH = 5; // block_state: member fully decompressed and flushed

  static {
    boolean available = false;
    MethodHandle initHandle = null;
    MethodHandle inflateHandle = null;

    AcceleratorDetector.IsaLLibraryResolution resolution = AcceleratorDetector.resolveIsaLLibrary();
    if (resolution.available()) {
      try {
        Linker linker = Linker.nativeLinker();
        SymbolLookup lookup = SymbolLookup.libraryLookup(
            resolution.resolvedPath(), Arena.global());

        // void isal_inflate_init(struct inflate_state *state)
        initHandle = linker.downcallHandle(
            lookup.find("isal_inflate_init").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

        // int isal_inflate(struct inflate_state *state)
        inflateHandle = linker.downcallHandle(
            lookup.find("isal_inflate").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

        available = true;
        log.info("ISA-L FFM wrapper initialized successfully via {}: {}",
            resolution.resolutionMode(), resolution.resolvedPath());

      } catch (Exception e) {
        log.error("Failed to initialize ISA-L FFM wrapper via {} ({}): {}",
            resolution.resolutionMode(), resolution.resolvedPath(), e.getMessage());
      }
    } else {
      log.info("ISA-L unavailable: {}", resolution.detail());
    }

    AVAILABLE = available;
    INFLATE_INIT = initHandle;
    INFLATE = inflateHandle;
  }

  private IsalDecompressor() {
  }

  @Override
  public String name() {
    return "isal";
  }

  public static void setEnabled(boolean value) {
    enabled = value;
    log.info("ISA-L enabled: {}", value);
  }

  @Override
  public boolean isAvailable() {
    return AVAILABLE && enabled;
  }

  @Override
  public InputStream wrap(InputStream compressed) throws IOException {
    if (!isAvailable()) {
      throw new UnsupportedOperationException("ISA-L not available");
    }
    return new IsalInflateInputStream(compressed);
  }

  /**
   * InputStream wrapper that uses ISA-L for decompression.
   */
  private static class IsalInflateInputStream extends InputStream {

    private final InputStream source;
    private final Arena arena;
    private final MemorySegment state;
    private final MemorySegment inputBuffer;
    private final MemorySegment outputBuffer;

    private static final int INPUT_BUFFER_SIZE = 131072; // 128KB
    private static final int OUTPUT_BUFFER_SIZE = 262144; // 256KB

    private final byte[] inputStagingBuf = new byte[INPUT_BUFFER_SIZE];
    private final byte[] leftoverBuf = new byte[INPUT_BUFFER_SIZE];
    private int outputPos = 0;
    private int outputLimit = 0;
    private boolean finished = false;
    private boolean memberInProgress = false;
    private boolean memberFinished = false;
    private boolean closed = false;

    IsalInflateInputStream(InputStream source) throws IOException {
      this.source = source;
      // ofShared() allows cross-thread access (needed when virtual threads migrate
      // between carrier threads on blocking I/O). Supports explicit close().
      this.arena = Arena.ofShared();

      // Allocate native buffers
      this.state = arena.allocate(STATE_SIZE);
      this.inputBuffer = arena.allocate(INPUT_BUFFER_SIZE);
      this.outputBuffer = arena.allocate(OUTPUT_BUFFER_SIZE);

      // Initialize inflate state and enable full gzip framing (header + CRC check)
      try {
        INFLATE_INIT.invokeExact(state);
        // IGZIP_GZIP = 1: ISA-L handles gzip header, CRC32, and member boundaries
        state.set(ValueLayout.JAVA_INT, CRC_FLAG_OFFSET, IGZIP_GZIP);
      } catch (Throwable t) {
        throw new IOException("Failed to initialize ISA-L inflate state", t);
      }
    }

    @Override
    public int read() throws IOException {
      byte[] b = new byte[1];
      int n = read(b, 0, 1);
      return n == -1 ? -1 : (b[0] & 0xFF);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (finished && outputPos >= outputLimit) {
        return -1;
      }

      int totalRead = 0;

      while (len > 0) {
        // First, drain any output we have
        if (outputPos < outputLimit) {
          int available = outputLimit - outputPos;
          int toCopy = Math.min(available, len);
          MemorySegment.copy(outputBuffer, outputPos,
              MemorySegment.ofArray(b), off, toCopy);
          outputPos += toCopy;
          off += toCopy;
          len -= toCopy;
          totalRead += toCopy;
          continue;
        }

        if (finished) {
          break;
        }

        // Refill output buffer by decompressing more input
        refillOutput();
      }

      return totalRead == 0 && finished ? -1 : totalRead;
    }

    private void refillOutput() throws IOException {
      outputPos = 0;
      outputLimit = 0;

      try {
        while (outputLimit == 0 && !finished) {
          // Check if we have leftover input from previous iteration
          int availIn = state.get(ValueLayout.JAVA_INT, AVAIL_IN_OFFSET);

          if (availIn == 0) {
            // Read more input
            int bytesRead = source.read(inputStagingBuf);

            if (bytesRead <= 0) {
              if (memberInProgress && !memberFinished) {
                throw new EOFException("Truncated gzip member: unexpected EOF before member completed");
              }
              finished = true;
              return;
            }

            // Copy to native buffer and point ISA-L at it
            MemorySegment.copy(MemorySegment.ofArray(inputStagingBuf), 0,
                inputBuffer, 0, bytesRead);
            state.set(ValueLayout.ADDRESS, NEXT_IN_OFFSET, inputBuffer);
            state.set(ValueLayout.JAVA_INT, AVAIL_IN_OFFSET, bytesRead);
          }

          if (!memberInProgress) {
            memberInProgress = true;
            memberFinished = false;
          }

          // Set up output buffer
          state.set(ValueLayout.ADDRESS, NEXT_OUT_OFFSET, outputBuffer);
          state.set(ValueLayout.JAVA_INT, AVAIL_OUT_OFFSET, OUTPUT_BUFFER_SIZE);

          // Decompress
          int result = (int) INFLATE.invokeExact(state);
          if (result != 0) {
            throw new IOException("ISA-L inflate error code: " + result);
          }

          // Calculate how much was written to output this call
          int availOut = state.get(ValueLayout.JAVA_INT, AVAIL_OUT_OFFSET);
          outputLimit = OUTPUT_BUFFER_SIZE - availOut;

          // Detect end-of-gzip-member: ISA-L sets block_state = ISAL_BLOCK_FINISH (5)
          // when the member's deflate data + gzip trailer have been fully processed.
          int blockState = state.get(ValueLayout.JAVA_INT, BLOCK_STATE_OFFSET);
          if (blockState == ISAL_BLOCK_FINISH) {
            memberFinished = true;
            memberInProgress = false;
            // ISA-L advanced next_in past the gzip trailer; save remaining bytes.
            int remainingAvailIn = state.get(ValueLayout.JAVA_INT, AVAIL_IN_OFFSET);
            int leftoverLen = 0;
            if (remainingAvailIn > 0) {
              // Compute offset of remaining bytes inside inputBuffer
              long nextInAddr = state.get(ValueLayout.ADDRESS, NEXT_IN_OFFSET).address();
              long startOffset = nextInAddr - inputBuffer.address();
              leftoverLen = remainingAvailIn;
              MemorySegment.copy(inputBuffer, ValueLayout.JAVA_BYTE, startOffset,
                  leftoverBuf, 0, leftoverLen);
            }
            // Re-initialize state for the next gzip member
            INFLATE_INIT.invokeExact(state);
            state.set(ValueLayout.JAVA_INT, CRC_FLAG_OFFSET, IGZIP_GZIP);
            memberFinished = false;
            // Restore leftover bytes (first bytes of the next member, if any)
            if (leftoverLen > 0) {
              MemorySegment.copy(MemorySegment.ofArray(leftoverBuf), 0,
                  inputBuffer, 0, leftoverLen);
              state.set(ValueLayout.ADDRESS, NEXT_IN_OFFSET, inputBuffer);
              state.set(ValueLayout.JAVA_INT, AVAIL_IN_OFFSET, leftoverLen);
            }
            // If leftover is empty, the next loop iteration will read more from source
          }
        }
      } catch (IOException e) {
        throw e;
      } catch (Throwable t) {
        throw new IOException("ISA-L decompression failed", t);
      }
    }

    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }
      closed = true;
      try {
        source.close();
      } finally {
        arena.close();
      }
    }
  }
}
