package pl.gov.nac.warc.checkers;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.gov.nac.warc.reactive.Metrics;
import pl.gov.nac.warc.reactive.ReactiveInterfaces;

/**
 * Validates archive magic bytes and basic compression signatures.
 */
public final class CheckerWarcMagic implements ReactiveInterfaces.ReactiveModule {

  private static final Logger log = LogManager.getLogger(CheckerWarcMagic.class);
  private static final String METRIC_KEY = "checker";
  private final List<String> filesToCheck = new ArrayList<>();

  @Override
  public void configure(Map<String, Object> cfg) {
    Metrics.setModuleHeader(METRIC_KEY, "WARC magic validator");
    if (cfg == null) {
      return;
    }

    filesToCheck.clear();

    Object filesObj = cfg.get("files");
    if (filesObj instanceof List<?> list && !list.isEmpty()) {
      for (Object o : list) {
        if (o == null) {
          continue;
        }
        String path = o.toString();
        if (!path.isBlank()) {
          filesToCheck.add(path);
        }
      }
    }
    Object fileObj = cfg.get("file");
    if (fileObj instanceof String s && !s.isBlank()) {
      filesToCheck.add(s);
    }
    Object pathObj = cfg.get("path");
    if (pathObj instanceof String s && !s.isBlank()) {
      filesToCheck.add(s);
    }
  }

  @Override
  public boolean beforeCheck(Map<String, Object> cfg) {
    return true;
  }

  @Override
  public int afterCheck(Map<String, Object> cfg) {
    if (filesToCheck.isEmpty()) {
      log.error("Cannot validate magic bytes: no file configured");
      return 1;
    }

    int failures = 0;
    for (String fileToCheck : filesToCheck) {
      int rc = validateSingle(fileToCheck);
      if (rc != 0) {
        failures++;
      }
    }
    return failures == 0 ? 0 : 1;
  }

  private int validateSingle(String fileToCheck) {
    Path p = Path.of(fileToCheck);
    if (!Files.exists(p) || !Files.isRegularFile(p)) {
      log.error("Validation failed: file not found or not regular file: {}", fileToCheck);
      return 1;
    }

    try (InputStream is = Files.newInputStream(p)) {
      byte[] magic = new byte[8];
      int read = is.read(magic);
      if (read < 4) {
        log.error("Validation failed: file too short to contain magic header");
        Metrics.inc(METRIC_KEY, "invalidMagic");
        return 1;
      }

      String detected = detectMagic(magic, read);
      if (detected == null) {
        log.error("Validation failed: unsupported or unknown magic bytes");
        Metrics.inc(METRIC_KEY, "invalidMagic");
        return 1;
      }

      log.info("Magic bytes OK: {}", detected);
      Metrics.inc(METRIC_KEY, "magicOk");
      return 0;
    } catch (IOException e) {
      log.error("Validation failed while reading magic bytes: {}", e.getMessage());
      Metrics.inc(METRIC_KEY, "magicReadError");
      return 1;
    }
  }

  private static String detectMagic(byte[] magic, int read) {
    if (magic[0] == 0x1f && magic[1] == (byte) 0x8b) {
      return "gzip";
    }
    if (magic[0] == 0x28 && magic[1] == (byte) 0xb5 && magic[2] == 0x2f && magic[3] == (byte) 0xfd) {
      return "zstd";
    }
    if (magic[0] == 0x04 && magic[1] == 0x22 && magic[2] == 0x4d && magic[3] == 0x18) {
      return "lz4";
    }
    if (read >= 6 && magic[0] == (byte) 0xfd && magic[1] == 0x37 && magic[2] == 0x7a
        && magic[3] == 0x58 && magic[4] == 0x5a && magic[5] == 0x00) {
      return "xz";
    }
    if (magic[0] == 0x50 && magic[1] == 0x4b && magic[2] == 0x03 && magic[3] == 0x04) {
      return "zip";
    }
    if (magic[0] == 'W' && magic[1] == 'A' && magic[2] == 'R' && magic[3] == 'C') {
      return "plain-warc";
    }
    return null;
  }
}
