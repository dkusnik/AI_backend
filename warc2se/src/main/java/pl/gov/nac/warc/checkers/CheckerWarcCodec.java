package pl.gov.nac.warc.checkers;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.gov.nac.warc.reactive.Metrics;
import pl.gov.nac.warc.reactive.ReactiveInterfaces;
import pl.gov.nac.warc.utils.WarcCodec;

/**
 * High-performance WARC validator using custom WarcCodec.
 * Uses DirectBuffer optimization for maximum throughput.
 */
public final class CheckerWarcCodec implements ReactiveInterfaces.ReactiveModule {

  private static final Logger log = LogManager.getLogger(CheckerWarcCodec.class);
  private static final String METRIC_KEY = "checker";
  private final List<String> filesToCheck = new ArrayList<>();

  @Override
  public void configure(Map<String, Object> cfg) {
    Metrics.setModuleHeader(METRIC_KEY, "Custom Codec WARC validator");

    if (cfg == null) {
      log.error("Configuration missing");
      return;
    }

    filesToCheck.clear();

    // files: [ ... ]
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

    // file: "..."
    Object fileObj = cfg.get("file");
    if (fileObj instanceof String s && !s.isBlank()) {
      filesToCheck.add(s);
    }

    // path: "..."
    Object pathObj = cfg.get("path");
    if (pathObj instanceof String s && !s.isBlank()) {
      filesToCheck.add(s);
    }

    if (filesToCheck.isEmpty()) {
      log.warn("No file specified in config.");
      return;
    }

    filesToCheck.forEach(path -> log.info("Will validate file: {}", path));
  }

  @Override
  public boolean beforeCheck(Map<String, Object> cfg) {
    log.info("beforeCheck");
    return true;
  }

  @Override
  public int afterCheck(Map<String, Object> cfg) {
    log.info("afterCheck");

    if (filesToCheck.isEmpty()) {
      log.error("Cannot validate: no file configured");
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

    Iterator<WarcCodec.ParsedRecord> it = null;
    log.info("Validating WARC: {}", fileToCheck);

    try {
      WarcCodec.ArchiveType type = WarcCodec.detectType(fileToCheck);

      it = switch (type) {
        case WARC, GZIP -> WarcCodec.openWarc(fileToCheck);
        case WACZ -> WarcCodec.openWacz(fileToCheck);
        default -> throw new IllegalArgumentException("Unknown archive type: " + fileToCheck);
      };

      int count = 0;
      while (it.hasNext()) {
        WarcCodec.ParsedRecord rec = it.next();
        // Validate record structure
        if (rec.type() == null || rec.type().isEmpty()) {
          throw new IllegalStateException("Record missing WARC-Type at index " + count);
        }
        Metrics.inc(METRIC_KEY, "recordsValidated");
        count++;
      }

      log.info("Validation OK: {} records validated", count);
      return 0;

    } catch (Exception e) {
      log.error("INVALID WARC: {}", e.getMessage());
      Metrics.inc(METRIC_KEY, "invalid");
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
  }
}
