package pl.gov.nac.warc.checkers;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.gov.nac.warc.reactive.Metrics;
import pl.gov.nac.warc.reactive.ReactiveInterfaces;
import pl.gov.nac.warc.utils.WarcCodec;

/**
 * Validates record-level WARC header compliance:
 * - supported WARC versions (1.0 / 1.1)
 * - required WARC headers
 * - presence of X-NAC-* headers
 */
public final class CheckerWarcHeaders implements ReactiveInterfaces.ReactiveModule {

  private static final Logger log = LogManager.getLogger(CheckerWarcHeaders.class);
  private static final String METRIC_KEY = "checker";

  private final List<String> filesToCheck = new ArrayList<>();
  private boolean requireNacHeaders = true;

  @Override
  public void configure(Map<String, Object> cfg) {
    Metrics.setModuleHeader(METRIC_KEY, "WARC header validator");
    if (cfg == null) {
      return;
    }

    Object reqNac = cfg.get("requireNacHeaders");
    if (reqNac != null) {
      requireNacHeaders = Boolean.parseBoolean(reqNac.toString());
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
      log.error("Cannot validate headers: no file configured");
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

    int count = 0;
    int invalid = 0;
    Iterator<WarcCodec.ParsedRecord> it = null;

    try {
      WarcCodec.ArchiveType type = WarcCodec.detectType(fileToCheck);
      it = switch (type) {
        case WARC, GZIP -> WarcCodec.openWarc(fileToCheck);
        case WACZ -> WarcCodec.openWacz(fileToCheck);
        default -> throw new IllegalArgumentException("Unsupported archive type for header validation: " + type);
      };

      while (it.hasNext()) {
        WarcCodec.ParsedRecord rec = it.next();
        count++;

        String version = rec.getVersion();
        if (!"WARC/1.0".equals(version) && !"WARC/1.1".equals(version)) {
          log.error("Record {} invalid version: {}", count, version);
          invalid++;
          Metrics.inc(METRIC_KEY, "invalidVersion");
          continue;
        }

        Map<String, String> headers = rec.getHeaders();
        if (!hasRequiredHeaders(headers)) {
          log.error("Record {} missing required WARC headers", count);
          invalid++;
          Metrics.inc(METRIC_KEY, "missingRequiredHeaders");
          continue;
        }

        if (requireNacHeaders && !hasNacHeaders(headers)) {
          log.error("Record {} missing X-NAC-* headers", count);
          invalid++;
          Metrics.inc(METRIC_KEY, "missingNacHeaders");
          continue;
        }

        Metrics.inc(METRIC_KEY, "recordsValidated");
      }
    } catch (Exception e) {
      log.error("Header validation failed: {}", e.getMessage());
      return 1;
    } finally {
      if (it instanceof AutoCloseable ac) {
        try {
          ac.close();
        } catch (Exception e) {
          log.warn("Failed closing iterator for {}: {}", fileToCheck, e.getMessage());
        }
      }
    }

    if (invalid > 0) {
      log.error("Header validation failed: {} invalid out of {} records", invalid, count);
      return 1;
    }

    log.info("Header validation OK: {} records", count);
    return 0;
  }

  private static boolean hasRequiredHeaders(Map<String, String> headers) {
    return hasHeader(headers, "warc-type")
        && hasHeader(headers, "warc-record-id")
        && hasHeader(headers, "warc-date");
  }

  private static boolean hasNacHeaders(Map<String, String> headers) {
    for (String key : headers.keySet()) {
      if (key.toUpperCase(Locale.ROOT).startsWith("X-NAC-")) {
        return true;
      }
    }
    return false;
  }

  private static boolean hasHeader(Map<String, String> headers, String key) {
    String val = headers.get(key);
    return val != null && !val.isBlank();
  }
}
