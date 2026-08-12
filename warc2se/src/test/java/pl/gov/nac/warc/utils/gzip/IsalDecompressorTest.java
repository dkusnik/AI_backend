package pl.gov.nac.warc.utils.gzip;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import pl.gov.nac.warc.utils.WarcCodec;

class IsalDecompressorTest {

  @BeforeEach
  void enableIsalForCurrentTest() {
    IsalDecompressor.setEnabled(true);
  }

  @Test
  void acceleratorDetectionMatchesUsableIsalWrapperOnThisHost() {
    boolean detectorSaysAvailable = AcceleratorDetector.hasIsaL();
    boolean wrapperSaysAvailable = IsalDecompressor.INSTANCE.isAvailable();

    assertEquals(detectorSaysAvailable, wrapperSaysAvailable,
        "ISA-L detection and wrapper availability should agree on the current host");
  }

  @Test
  void roundTripsSingleMemberGzip() throws IOException {
    Assumptions.assumeTrue(IsalDecompressor.INSTANCE.isAvailable(), "ISA-L not available on this host");

    byte[] gzipped = gzipMember("single-member-payload");

    byte[] expected = decompressWithCommons(gzipped);
    byte[] actual = readAll(IsalDecompressor.INSTANCE.wrap(new ByteArrayInputStream(gzipped)));

    assertArrayEquals(expected, actual);
  }

  @Test
  void roundTripsConcatenatedMultiMemberGzip() throws IOException {
    Assumptions.assumeTrue(IsalDecompressor.INSTANCE.isAvailable(), "ISA-L not available on this host");

    byte[] gzipped = concat(
        gzipMember("member-one\n"),
        gzipMember("member-two\n"),
        gzipMember("member-three\n"));

    byte[] expected = decompressWithCommons(gzipped);
    byte[] actual = readAll(IsalDecompressor.INSTANCE.wrap(new ByteArrayInputStream(gzipped)));

    assertArrayEquals(expected, actual);
    assertEquals("member-one\nmember-two\nmember-three\n", new String(actual, StandardCharsets.UTF_8));
  }

  @Test
  void rejectsTruncatedSingleMemberGzip() throws IOException {
    Assumptions.assumeTrue(IsalDecompressor.INSTANCE.isAvailable(), "ISA-L not available on this host");

    byte[] gzipped = truncateTail(gzipMember("truncate-me"), 4);

    assertThrows(IOException.class,
        () -> readAll(IsalDecompressor.INSTANCE.wrap(new ByteArrayInputStream(gzipped))));
  }

  @Test
  void rejectsTruncatedFinalMemberInConcatenatedGzip() throws IOException {
    Assumptions.assumeTrue(IsalDecompressor.INSTANCE.isAvailable(), "ISA-L not available on this host");

    byte[] gzipped = concat(
        gzipMember("first-member"),
        truncateTail(gzipMember("second-member"), 6));

    assertThrows(IOException.class,
        () -> readAll(IsalDecompressor.INSTANCE.wrap(new ByteArrayInputStream(gzipped))));
  }

  @Test
  void warcCodecDecompressIfNeededSurfacesTruncationWhenIsalIsActive() throws IOException {
    Assumptions.assumeTrue(IsalDecompressor.INSTANCE.isAvailable(), "ISA-L not available on this host");

    byte[] truncated = truncateTail(gzipMember("WARC/1.0\r\n\r\npayload\r\n\r\n"), 5);

    try (InputStream decompressed = WarcCodec.decompressIfNeeded(
        "synthetic-truncated.warc.gz", new ByteArrayInputStream(truncated))) {
      assertThrows(IOException.class, decompressed::readAllBytes);
    }
  }

  private static byte[] readAll(InputStream in) throws IOException {
    try (InputStream input = in) {
      return input.readAllBytes();
    }
  }

  private static byte[] decompressWithCommons(byte[] gzipped) throws IOException {
    try (GzipCompressorInputStream in = GzipCompressorInputStream.builder()
        .setInputStream(new ByteArrayInputStream(gzipped))
        .setDecompressConcatenated(true)
        .get()) {
      return in.readAllBytes();
    }
  }

  private static byte[] gzipMember(String payload) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (GZIPOutputStream gzip = new GZIPOutputStream(new NonClosingOutputStream(out))) {
      gzip.write(payload.getBytes(StandardCharsets.UTF_8));
    }
    return out.toByteArray();
  }

  private static byte[] concat(byte[]... parts) {
    int total = 0;
    for (byte[] part : parts) {
      total += part.length;
    }

    byte[] merged = new byte[total];
    int offset = 0;
    for (byte[] part : parts) {
      System.arraycopy(part, 0, merged, offset, part.length);
      offset += part.length;
    }
    return merged;
  }

  private static byte[] truncateTail(byte[] input, int bytesToRemove) {
    assertTrue(input.length > bytesToRemove, "Test fixture must stay non-empty after truncation");
    return Arrays.copyOf(input, input.length - bytesToRemove);
  }

  private static final class NonClosingOutputStream extends FilterOutputStream {

    private NonClosingOutputStream(ByteArrayOutputStream out) {
      super(out);
    }

    @Override
    public void close() throws IOException {
      flush();
    }
  }
}
