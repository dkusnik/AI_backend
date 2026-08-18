package pl.gov.nac.warc.utils.gzip;

import java.io.InputStream;

/**
 * Factory interface for pluggable GZIP decompression strategies.
 *
 * <p>
 * Implementations provide different decompression algorithms:
 * <ul>
 * <li>{@code jdk} - Standard java.util.zip.GZIPInputStream</li>
 * <li>{@code aircompressor} - io.airlift aircompressor (faster)</li>
 * <li>{@code isal} - Intel ISA-L via FFM (fastest, Linux only)</li>
 * </ul>
 */
public interface GzipDecompressorFactory {

  /**
   * Factory name for logging and configuration.
   */
  String name();

  /**
   * Check if this decompressor is available on current platform.
   */
  boolean isAvailable();

  /**
   * Wrap a compressed InputStream with decompression.
   */
  InputStream wrap(InputStream compressed) throws java.io.IOException;
}
