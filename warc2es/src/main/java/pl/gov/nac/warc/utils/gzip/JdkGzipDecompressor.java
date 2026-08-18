package pl.gov.nac.warc.utils.gzip;

import java.io.InputStream;
import java.util.zip.GZIPInputStream;

/**
 * JDK-based GZIP decompressor using java.util.zip.GZIPInputStream.
 * This is the baseline implementation, always available.
 */
public final class JdkGzipDecompressor implements GzipDecompressorFactory {

  public static final JdkGzipDecompressor INSTANCE = new JdkGzipDecompressor();

  private JdkGzipDecompressor() {
  }

  @Override
  public String name() {
    return "jdk";
  }

  @Override
  public boolean isAvailable() {
    return true; // Always available
  }

  @Override
  public InputStream wrap(InputStream compressed) throws java.io.IOException {
    return new GZIPInputStream(compressed, 131072); // 128KB buffer
  }
}
