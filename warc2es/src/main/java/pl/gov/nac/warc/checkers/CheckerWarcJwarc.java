package pl.gov.nac.warc.checkers;

import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.netpreserve.jwarc.WarcReader;

import pl.gov.nac.warc.reactive.Metrics;
import pl.gov.nac.warc.reactive.ReactiveInterfaces;

public final class CheckerWarcJwarc implements ReactiveInterfaces.ReactiveModule {

  private static final Logger log = LogManager.getLogger(CheckerWarcJwarc.class);
  private static final String METRIC_KEY = "checker";
  private static final String LOG_WILL_VALIDATE = "Will validate file: {}";

  private final List<String> filesToCheck = new ArrayList<>();

  @Override
  public void configure(Map<String, Object> cfg) {
    Metrics.setModuleHeader(METRIC_KEY, "jwarc WARC validator");

    if (cfg == null) {
      log.warn("Configuration missing: expected a config block with 'file', 'path', 'output', or 'files'");
      return;
    }

    filesToCheck.clear();

    // 1. files: [ ... ]
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

    // 2. file: "..."
    Object fileObj = cfg.get("file");
    if (fileObj instanceof String s && !s.isBlank()) {
      filesToCheck.add(s);
    }

    // 3. path: "..."
    Object pathObj = cfg.get("path");
    if (pathObj instanceof String s && !s.isBlank()) {
      filesToCheck.add(s);
    }

    // 4. output: "..."
    Object outObj = cfg.get("output");
    if (outObj instanceof String s && !s.isBlank()) {
      filesToCheck.add(s);
    }

    if (!filesToCheck.isEmpty()) {
      filesToCheck.forEach(path -> log.info(LOG_WILL_VALIDATE, path));
      return;
    }

    log.warn("Configuration incomplete: expected one of 'file', 'path', 'output', or 'files'. Checker will not run.");
  }

  @Override
  public boolean beforeCheck(Map<String, Object> cfg) {
    log.info("beforeCheck");
    return true; // always allowed to run
  }

  @Override
  public int afterCheck(Map<String, Object> cfg) {
    log.info("afterCheck");

    if (filesToCheck.isEmpty()) {
      log.error(
          "Cannot validate: no file configured. Please specify 'file', 'path', 'output', or 'files' in the YAML config.");
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
      Metrics.inc(METRIC_KEY, "missingFile");
      return 1;
    }

    if (!Files.isReadable(p)) {
      log.error("Validation failed: file not readable: {}", fileToCheck);
      Metrics.inc(METRIC_KEY, "unreadableFile");
      return 1;
    }

    log.info("Validating WARC: {}", fileToCheck);

    try (WarcReader reader = new WarcReader(new FileInputStream(fileToCheck))) {
      int count = 0;

      for (var _ : reader) {
        Metrics.inc(METRIC_KEY, "recordsValidated");
        count++;
      }

      log.info("Validation OK: {} records validated", count);
      return 0;

    } catch (Exception e) {
      log.error("INVALID WARC: {}", e.getMessage());
      Metrics.inc(METRIC_KEY, "invalid");
      return 1;
    }
  }
}
