package pl.gov.nac.warc.utils.gzip;

import java.io.File;
import java.util.function.Predicate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Detects availability of hardware/native acceleration features.
 */
public final class AcceleratorDetector {

  private static final Logger log = LogManager.getLogger(AcceleratorDetector.class);
  private static final String ISA_L_PROPERTY = "warc.isal.library";
  private static final String ISA_L_ENV = "WARC_ISAL_LIBRARY";
  private static final String[] ISA_L_CANDIDATES = {
      "/usr/lib/x86_64-linux-gnu/libisal.so.2",
      "/usr/lib/libisal.so.2",
      "/usr/local/lib/libisal.so.2",
      "/lib/x86_64-linux-gnu/libisal.so.2"
  };

  private static IsaLLibraryResolution isaLResolution;
  private static Boolean ioUringAvailable;
  private static Boolean qatAvailable;

  private AcceleratorDetector() {
  }

  /**
   * Check if Intel ISA-L library is available.
   * Detection and FFM loading must use the same resolved path.
   */
  public static boolean hasIsaL() {
    return resolveIsaLLibrary().available();
  }

  public static IsaLLibraryResolution resolveIsaLLibrary() {
    if (isaLResolution == null) {
      isaLResolution = resolveIsaLLibrary(
          System.getProperty(ISA_L_PROPERTY),
          System.getenv(ISA_L_ENV),
          path -> new File(path).isFile());
    }
    return isaLResolution;
  }

  /**
   * Check if io_uring is available (Linux 5.1+).
   */
  public static boolean hasIoUring() {
    if (ioUringAvailable == null) {
      ioUringAvailable = checkKernelVersion(5, 1);
    }
    return ioUringAvailable;
  }

  /**
   * Check if Intel QAT devices are present.
   * NOTE: Detection only - implementation not yet available.
   */
  public static boolean hasQat() {
    if (qatAvailable == null) {
      qatAvailable = new File("/dev/qat_adf_ctl").exists() ||
          new File("/dev/qat").exists() ||
          new File("/sys/class/qat").exists();
      if (qatAvailable) {
        log.info("QAT hardware detected but not yet implemented");
      }
    }
    return qatAvailable;
  }

  /**
   * Log all available accelerators for diagnostics.
   */
  public static void logAvailability() {
    IsaLLibraryResolution isaL = resolveIsaLLibrary();
    log.info("Hardware Acceleration Availability:");
    if (isaL.available()) {
      log.info("  - ISA-L: true ({}, {})", isaL.resolutionMode(), isaL.resolvedPath());
    } else {
      log.info("  - ISA-L: false ({})", isaL.detail());
    }
    log.info("  - io_uring: {}", hasIoUring());
    log.info("  - Intel QAT: {} (not implemented)", hasQat());
  }

  static IsaLLibraryResolution resolveIsaLLibrary(
      String propertyPath,
      String envPath,
      Predicate<String> pathExists) {
    if (propertyPath != null && !propertyPath.isBlank()) {
      return resolveConfiguredPath(propertyPath, "system-property", pathExists);
    }
    if (envPath != null && !envPath.isBlank()) {
      return resolveConfiguredPath(envPath, "environment", pathExists);
    }

    for (String candidate : ISA_L_CANDIDATES) {
      if (pathExists.test(candidate)) {
        return new IsaLLibraryResolution(true, candidate, "well-known-path", "found");
      }
    }

    return new IsaLLibraryResolution(false, null, "not-found",
        "No ISA-L library found in " + String.join(", ", ISA_L_CANDIDATES));
  }

  private static IsaLLibraryResolution resolveConfiguredPath(
      String configuredPath,
      String mode,
      Predicate<String> pathExists) {
    if (pathExists.test(configuredPath)) {
      return new IsaLLibraryResolution(true, configuredPath, mode, "configured path");
    }
    return new IsaLLibraryResolution(false, null, mode,
        "Configured ISA-L library path not found: " + configuredPath);
  }

  private static boolean checkKernelVersion(int major, int minor) {
    try {
      String version = System.getProperty("os.version", "");
      String[] parts = version.split("[.-]");
      if (parts.length >= 2) {
        int maj = Integer.parseInt(parts[0]);
        int min = Integer.parseInt(parts[1]);
        return maj > major || (maj == major && min >= minor);
      }
    } catch (NumberFormatException e) {
      // Ignore
    }
    return false;
  }

  public record IsaLLibraryResolution(
      boolean available,
      String resolvedPath,
      String resolutionMode,
      String detail) {
  }
}
