package pl.gov.nac.warc.utils.gzip;

import java.io.InputStream;

import io.airlift.compress.gzip.JdkGzipHadoopStreams;

/**
 * Aircompressor-based GZIP decompressor.
 * Uses io.airlift aircompressor library for faster decompression.
 */
public final class AircompressorDecompressor implements GzipDecompressorFactory {

  public static final AircompressorDecompressor INSTANCE = new AircompressorDecompressor();

  private static final boolean AVAILABLE;

  static {
    boolean available = false;
    try {
      Class.forName("io.airlift.compress.gzip.JdkGzipHadoopStreams");
      available = true;
    } catch (ClassNotFoundException e) {
      available = false;
    }
    AVAILABLE = available;
  }

  private AircompressorDecompressor() {
  }

  @Override
  public String name() {
    return "aircompressor";
  }

  @Override
  public boolean isAvailable() {
    return AVAILABLE;
  }

  @Override
  public InputStream wrap(InputStream compressed) throws java.io.IOException {
    if (!AVAILABLE) {
      throw new UnsupportedOperationException("aircompressor not available");
    }
    // Use JdkGzipHadoopStreams for decompression - it provides GzipInputStream
    return new JdkGzipHadoopStreams().createInputStream(compressed);
  }
}
