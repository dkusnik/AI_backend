package pl.gov.nac.warc.producers;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.netpreserve.jwarc.WarcReader;
import org.netpreserve.jwarc.WarcRecord;

import pl.gov.nac.warc.reactive.Metrics;
import pl.gov.nac.warc.reactive.ReactiveInterfaces;
import pl.gov.nac.warc.records.Record;
import pl.gov.nac.warc.records.file.RecordFileWarc;
import pl.gov.nac.warc.records.warc.RecordWarcInFile;
import pl.gov.nac.warc.records.warc.RecordWarcJwarc;
import pl.gov.nac.warc.records.warc.RecordWarcRawBytes;
import pl.gov.nac.warc.records.warc.RecordWarcUniversal;
import pl.gov.nac.warc.utils.BufferPool;
import pl.gov.nac.warc.utils.PooledBuffer;
import pl.gov.nac.warc.utils.WarcCodec;
import pl.gov.nac.warc.utils.WarcIO;

/**
 * WARC/WACZ producer using jwarc library.
 */
public final class ArchiveExtractorJwarc implements ReactiveInterfaces.ReactiveProducer<Object> {

  private static final Logger log = LogManager.getLogger(ArchiveExtractorJwarc.class);
  private static final String METRIC_PRODUCER = "producer";

  private Flow.Subscriber<? super Object> subscriber;
  private List<String> inputFiles = new ArrayList<>();
  private Class<?> negotiatedOutputType = RecordWarcUniversal.class;
  // When used with VirtualThreadEngine, demand-based backpressure
  // here stacks on top of the engine's own queue capacity.  The engine calls
  // request(Long.MAX_VALUE), so this semaphore is effectively unbounded in that
  // configuration and the real backpressure is the engine's in-flight limits.
  // (Finite request(n) demand came from the removed ReactiveEngine; the
  // semaphore is kept for Reactive Streams compliance with bounded subscribers.)
  private final java.util.concurrent.Semaphore demand = new java.util.concurrent.Semaphore(0);
  private volatile boolean shuttingDown = false;

  // P2-04: Reusable header map to reduce allocations
  private final java.util.Map<String, String> reusableHeaderMap = new java.util.LinkedHashMap<>();

  // Default priority: file/raw first for efficiency
  private List<Class<? extends Record>> outputTypes = new ArrayList<>(List.of(
      RecordFileWarc.class,
      RecordWarcRawBytes.class, RecordWarcJwarc.class,
      RecordWarcUniversal.class, RecordWarcInFile.class));

  @Override
  public List<Class<? extends Record>> emittedOutputTypes() {
    return outputTypes;
  }

  @Override
  public void configure(Map<String, Object> cfg) {
    Metrics.setModuleHeader(METRIC_PRODUCER, "jwarc Archive reader (WARC/WACZ)");

    // Resolve input files
    this.inputFiles = WarcCodec.getFiles(cfg);

    // Handle output format preference
    if (cfg.get("output") instanceof String fmt) {
      if ("universal".equalsIgnoreCase(fmt)) {
        // Force parsing to universal record
        outputTypes = List.of(RecordWarcUniversal.class);
      } else if ("bytes".equalsIgnoreCase(fmt)) {
        // Prefer raw bytes (parsing but keeping payload raw)
        outputTypes = List.of(RecordWarcRawBytes.class, RecordWarcUniversal.class);
      } else if ("native".equalsIgnoreCase(fmt)) {
        // Prefer native jwarc record
        outputTypes = List.of(RecordWarcJwarc.class, RecordWarcUniversal.class);
      }
    }

    log.info("Files: {}", inputFiles);
  }

  @Override
  public boolean beforeCheck(Map<String, Object> cfg) {
    if (inputFiles == null || inputFiles.isEmpty()) {
      log.error("No input files configured");
      return false;
    }

    for (String path : inputFiles) {
      Path filePath = Path.of(path);
      if (!Files.exists(filePath) || !Files.isRegularFile(filePath)) {
        log.error("File not found or not regular file: {}", path);
        return false;
      }
      if (!Files.isReadable(filePath)) {
        log.error("File not readable: {}", path);
        return false;
      }
    }
    return true;
  }

  @Override
  public int afterCheck(Map<String, Object> cfg) {
    return 0;
  }

  @Override
  public void onNegotiatedOutputType(Class<?> type) {
    if (type != null) {
      this.negotiatedOutputType = type;
    }
  }

  @Override
  public void startProducing() {
    log.info("Starting production");
    shuttingDown = false;
    Throwable estimateFailure = estimateTotals();
    if (estimateFailure != null) {
      failProduction(new RuntimeException("Failed to estimate archive totals", estimateFailure));
      return;
    }

    if (negotiatedOutputType != null
        && pl.gov.nac.warc.records.file.RecordFile.class.isAssignableFrom(negotiatedOutputType)) {
      log.info("Using optimized file pass-through mode");
      int failedInputs = 0;
      Throwable firstFailure = null;
      for (String path : inputFiles) {
        WarcCodec.ArchiveType type = WarcCodec.detectType(path);
        if (type == WarcCodec.ArchiveType.WARC) {
          emit(new RecordFileWarc(Path.of(path)));
          Metrics.inc(METRIC_PRODUCER, "recordsOut");
        } else {
          RuntimeException failure = new RuntimeException("Unsupported archive type for file pass-through: " + path);
          log.error(failure.getMessage());
          failedInputs++;
          if (firstFailure == null) {
            firstFailure = failure;
          }
        }
      }
      if (failedInputs > 0) {
        Metrics.add(METRIC_PRODUCER, "failed-inputs", failedInputs);
        failProduction(new RuntimeException("Archive production failed for " + failedInputs + " input(s)",
            firstFailure));
        return;
      }
      complete();
      return;
    }

    int failedInputs = 0;
    Throwable firstFailure = null;
    for (String path : inputFiles) {
      WarcCodec.ArchiveType type = WarcCodec.detectType(path);
      log.info("Processing {} as {}", path, type);

      try {
        switch (type) {
          case WARC, GZIP, ZSTD, LZ4, XZ -> processWarc(path);
          case WACZ -> processWacz(path);
          case UNKNOWN -> throw new IOException("Unknown archive type: " + path);
        }
      } catch (Exception e) {
        log.error("Error processing " + path, e);
        failedInputs++;
        if (firstFailure == null) {
          firstFailure = e;
        }
      }
    }

    if (failedInputs > 0) {
      Metrics.add(METRIC_PRODUCER, "failed-inputs", failedInputs);
      failProduction(new RuntimeException("Archive production failed for " + failedInputs + " input(s)",
          firstFailure));
      return;
    }

    complete();
  }

  private void processWarc(String path) throws java.io.IOException {
    Path p = Path.of(path);
    String filename = p.getFileName().toString();
    CountingInputStream counter = new CountingInputStream(new FileInputStream(path));
    try (InputStream is = WarcCodec.decompressIfNeeded(path, counter);
        WarcReader reader = new WarcReader(is)) {
      // prev must be captured BEFORE next() reads compressed bytes for the record
      long prev = 0;
      for (WarcRecord rec : reader) {
        emitRecord(rec, filename);
        long cur = counter.count;
        Metrics.add(METRIC_PRODUCER, "inputBytesRead", cur - prev);
        prev = cur;
      }
    }
  }

  private void processWacz(String path) throws java.io.IOException {
    try (ZipFile zip = new ZipFile(path)) {
      List<ZipEntry> warcEntries = new ArrayList<>();
      int failedSegments = 0;
      Throwable firstFailure = null;

      Enumeration<? extends ZipEntry> entries = zip.entries();
      while (entries.hasMoreElements()) {
        ZipEntry e = entries.nextElement();
        if (!e.isDirectory() && e.getName().startsWith("archive/") &&
            (e.getName().endsWith(".warc.gz") || e.getName().endsWith(".warc") ||
                e.getName().endsWith(".warc.zst") || e.getName().endsWith(".warc.lz4"))) {
          warcEntries.add(e);
        }
      }

      log.info("Found {} segments in WACZ", warcEntries.size());

      for (ZipEntry entry : warcEntries) {
        String segmentName = entry.getName();
        try (InputStream is = WarcCodec.decompressIfNeeded(segmentName, zip.getInputStream(entry));
            WarcReader reader = new WarcReader(is)) {

          for (WarcRecord rec : reader) {
            emitRecord(rec, segmentName);
          }
        } catch (Exception e) {
          log.error("Error reading segment " + segmentName, e);
          Metrics.inc(METRIC_PRODUCER, "errors");
          failedSegments++;
          if (firstFailure == null) {
            firstFailure = e;
          }
        }
      }
      if (failedSegments > 0) {
        throw new IOException("WACZ processing failed for " + failedSegments + " segment(s)", firstFailure);
      }
    }
  }

  @Override
  public void subscribe(Flow.Subscriber<? super Object> subscriber) {
    this.subscriber = subscriber;
    subscriber.onSubscribe(new Flow.Subscription() {
      @Override
      public void request(long n) {
        if (n > 0) {
          demand.release(n > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) n);
        }
      }

      @Override
      public void cancel() {
        shuttingDown = true;
        demand.release(1000); // Wake up producer
      }
    });
  }

  private void emit(Object item) {
    if (subscriber != null) {
      subscriber.onNext(item);
    }
  }

  private void complete() {
    if (subscriber != null) {
      subscriber.onComplete();
    }
  }

  private void failProduction(Throwable failure) {
    Metrics.inc(METRIC_PRODUCER, "errors");
    if (subscriber != null) {
      subscriber.onError(failure);
    }
  }

  private void emitRecord(WarcRecord rec, String sourceFile) throws java.io.IOException {
    PooledBuffer pooled = BufferPool.INSTANCE.borrow();
    WarcIO.serialize(rec, pooled);

    Metrics.inc(METRIC_PRODUCER, "recordsOut");
    Metrics.add(METRIC_PRODUCER, "bytesOut", pooled.length);

    if (subscriber == null) {
      pooled.release();
      return;
    }

    if (shuttingDown) {
      pooled.release();
      return;
    }

    try {
      demand.acquire();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      shuttingDown = true;
      pooled.release();
      return;
    }

    Object out;
    if (negotiatedOutputType == RecordWarcUniversal.class) {
      byte[] bytes = java.util.Arrays.copyOf(pooled.array, pooled.length);

      // P2-04: Reuse map
      reusableHeaderMap.clear();
      rec.headers().map().forEach((k, v) -> {
        if (!v.isEmpty())
          reusableHeaderMap.put(k, v.get(0));
      });

      // We still need to create a new map for the record as it is
      // immutable/referenced elsewhere
      // But we can construct it cleaner or clone?
      // RecordWarcUniversal currently takes Map.
      // Optimization: If RecordWarcUniversal copies the map, we can pass reusable.
      // If it stores reference, we must copy.
      // Checking RecordWarcUniversal: it likely stores it.
      // So we must copy. BUT, LinkedHashMap copy constructor is faster than per-entry
      // put?
      // Actually, let's keep it safe: new LinkedHashMap(reusableHeaderMap)

      java.util.Map<String, String> headerMap = new java.util.LinkedHashMap<>(reusableHeaderMap);

      RecordWarcUniversal rwu = new RecordWarcUniversal(rec.type(), headerMap, bytes);
      if (sourceFile != null) {
        rwu.headers().put("X-Source-Warc", sourceFile);
      }
      out = rwu;
      pooled.release();
    } else if (RecordWarcJwarc.class.isAssignableFrom(negotiatedOutputType) ||
        org.netpreserve.jwarc.WarcRecord.class.isAssignableFrom(negotiatedOutputType)) {
      byte[] copy = java.util.Arrays.copyOf(pooled.array, pooled.length);
      // Must wrap native record
      out = new RecordWarcJwarc(rec, copy);
      pooled.release();
    } else {
      // Default: wrap in RecordWarcRawBytes if expecting Record, otherwise raw bytes
      if (Record.class.isAssignableFrom(negotiatedOutputType)) {
        // We need to copy because pooled buffer will be released
        byte[] copy = java.util.Arrays.copyOf(pooled.array, pooled.length);
        out = new pl.gov.nac.warc.records.warc.RecordWarcRawBytes(copy);
        pooled.release();
      } else {
        // If they really want PooledBuffer (unlikely given hierarchy rule)
        out = pooled;
      }
    }

    emit(out);
  }

  private Throwable estimateTotals() {
    long totalSize = 0;
    long totalEstimatedRecords = 0;
    Throwable firstFailure = null;

    for (String inputPath : inputFiles) {
      try {
        Path path = Path.of(inputPath);
        long size = Files.size(path);
        totalSize += size;

        if (size == 0)
          continue;

        // For WACZ, we don't easily sample segments without opening ZIP.
        // Just use file size for now.
        if (WarcCodec.detectType(inputPath) == WarcCodec.ArchiveType.WACZ) {
          continue;
        }

        String cdxPath = WarcCodec.findCdxSidecar(inputPath);
        if (cdxPath != null) {
          long cdxRecordCount = WarcCodec.countRecordsInCdxj(cdxPath);
          if (cdxRecordCount > 0) {
            totalEstimatedRecords += cdxRecordCount;
            log.info("Counted {} records for {} via CDXJ", cdxRecordCount, inputPath);
            continue;
          }
        }

        // Fallback: full file scan (for accurate count)
        log.info("No CDXJ found, scanning entire file for record count: {}", inputPath);
        long recordCount = WarcCodec.countRecordsByFullScan(inputPath);
        if (recordCount > 0) {
          totalEstimatedRecords += recordCount;
          log.info("Counted {} records in {}", recordCount, inputPath);
        } else {
          // Ultimate fallback: estimate based on sampling
          long totalRecordsScanned = 0;
          long totalBytesScanned = 0;
          int sampleChunks = 5;
          int sampleBufferSize = 256 * 1024;

          try (FileChannel ch = FileChannel.open(path)) {
            ByteBuffer buf = ByteBuffer.allocate(sampleBufferSize);

            for (int i = 0; i < sampleChunks; i++) {
              long offset = (size * i) / sampleChunks;
              if (offset + sampleBufferSize > size)
                offset = Math.max(0, size - sampleBufferSize);

              ch.position(offset);
              buf.clear();
              int read = ch.read(buf);
              if (read <= 0)
                continue;

              buf.flip();
              byte[] data = new byte[read];
              buf.get(data);

              int count = countSignatures(data);
              if (count > 0) {
                totalRecordsScanned += count;
                totalBytesScanned += read;
              }
            }
          }

          if (totalRecordsScanned > 0 && totalBytesScanned > 0) {
            double recordsPerByte = (double) totalRecordsScanned / totalBytesScanned;
            totalEstimatedRecords += (long) (size * recordsPerByte);
            log.info("Estimated {} records for {} via sampling", (long) (size * recordsPerByte), inputPath);
          }
        }
      } catch (Exception e) {
        log.error("Failed to estimate totals for " + inputPath, e);
        Metrics.inc(METRIC_PRODUCER, "failed-inputs");
        if (firstFailure == null) {
          firstFailure = e;
        }
      }
    }

    Metrics.set("engine", "totalBytes", totalSize);
    Metrics.set("engine", "totalRecords", totalEstimatedRecords);
    log.info("Estimated total bytes: {}, records: {}", totalSize, totalEstimatedRecords);
    return firstFailure;
  }

  private int countSignatures(byte[] data) {
    int count = 0;
    // Search for GZIP ID1 ID2 (0x1f 0x8b) OR "WARC/"
    int i = 0;
    while (i < data.length - 10) {
      // Check for GZIP: 1F 8B 08
      boolean isGzip = (data[i] == (byte) 0x1f) && (data[i + 1] == (byte) 0x8b) && (data[i + 2] == (byte) 0x08);

      // Check for Uncompressed WARC: "WARC/" -> 57 41 52 43 2F
      boolean isWarc = (data[i] == 0x57) && (data[i + 1] == 0x41) && (data[i + 2] == 0x52)
          && (data[i + 3] == 0x43) && (data[i + 4] == 0x2F);

      if (isGzip || isWarc) {
        count++;
        i += 21;
      } else {
        i++;
      }
    }
    return count;
  }

  /** Counts raw bytes read from the underlying stream (compressed file bytes). */
  private static final class CountingInputStream extends InputStream {
    private final InputStream delegate;
    volatile long count = 0;

    CountingInputStream(InputStream delegate) {
      this.delegate = delegate;
    }

    @Override
    public int read() throws java.io.IOException {
      int b = delegate.read();
      if (b != -1)
        count++;
      return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws java.io.IOException {
      int n = delegate.read(b, off, len);
      if (n > 0)
        count += n;
      return n;
    }

    @Override
    public void close() throws java.io.IOException {
      delegate.close();
    }
  }
}
