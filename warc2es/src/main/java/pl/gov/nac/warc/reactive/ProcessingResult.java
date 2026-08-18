package pl.gov.nac.warc.reactive;

import java.io.PrintStream;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Stable, machine-readable result of one Java pipeline invocation. */
@JsonPropertyOrder({
    "schema", "status", "exit_code", "records_in", "records_out",
    "records_indexed", "records_skipped", "errors", "elapsed_ms", "error", "metrics"
})
public record ProcessingResult(
    String schema,
    String status,
    @JsonProperty("exit_code") int exitCode,
    @JsonProperty("records_in") Long recordsIn,
    @JsonProperty("records_out") Long recordsOut,
    @JsonProperty("records_indexed") Long recordsIndexed,
    @JsonProperty("records_skipped") Long recordsSkipped,
    int errors,
    @JsonProperty("elapsed_ms") long elapsedMs,
    ErrorDetail error,
    MetricsDetail metrics) {

  public static final String SCHEMA = "warc2es.processing/v1";
  public static final String METRICS_SCHEMA = "warc2es.metrics/v1";

  public static final String INVALID_ARGUMENTS = "invalid_arguments";
  public static final String UNSUPPORTED_ENGINE = "unsupported_engine";
  public static final String PIPELINE_NEGOTIATION_FAILED = "pipeline_negotiation_failed";
  public static final String MODULE_NOT_FOUND = "module_not_found";
  public static final String CONFIGURATION_ERROR = "configuration_error";
  public static final String BEFORE_CHECK_FAILED = "before_check_failed";
  public static final String PROCESSING_FAILED = "processing_failed";
  public static final String AFTER_CHECK_FAILED = "after_check_failed";
  public static final String INTERRUPTED = "interrupted";
  public static final String TIMED_OUT = "timed_out";
  public static final String INTERNAL_ERROR = "internal_error";

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Pattern URL_CREDENTIALS = Pattern.compile(
      "(?i)(https?://)([^\\s/@:]+):([^\\s/@]+)@");
  private static final Pattern SECRET_ASSIGNMENT = Pattern.compile(
      "(?i)\\b(password|passwd|token|es-pass)(\\s*[=:]\\s*)(\"[^\"]*\"|'[^']*'|[^\\s,;}]+)");
  private static final Pattern AUTHORIZATION = Pattern.compile(
      "(?i)\\bauthorization\\s*[=:]\\s*[^\\r\\n,;}]+");
  private static final Pattern STACK_FRAME = Pattern.compile(
      "(?m)\\R\\s*(?:at\\s+|Caused by:)[^\\r\\n]+");
  private static final int MAX_ERROR_MESSAGE_LENGTH = 1024;

  @JsonPropertyOrder({ "code", "message" })
  public record ErrorDetail(String code, String message) {
  }

  @JsonPropertyOrder({ "schema", "counters" })
  public record MetricsDetail(String schema, Map<String, Map<String, Long>> counters) {
  }

  /**
   * Build a result at the Pipeline/engine boundary. Before the engine starts,
   * record fields and metrics are deliberately null rather than invented zeros.
   */
  public static ProcessingResult create(
      int exitCode,
      boolean dryRun,
      boolean metricsAvailable,
      boolean elasticsearchActive,
      long elapsedMs,
      String errorCode,
      String errorMessage) {

    boolean success = exitCode == 0;
    String status = success ? (dryRun ? "dry_run" : "ok") : "error";
    Long recordsIn = null;
    Long recordsOut = null;
    Long recordsSkipped = null;
    Long recordsIndexed = null;
    MetricsDetail metrics = null;

    if (metricsAvailable) {
      if (dryRun && success) {
        recordsIn = 0L;
        recordsOut = 0L;
        recordsSkipped = 0L;
        recordsIndexed = elasticsearchActive ? 0L : null;
      } else {
        recordsIn = nonnegative(Metrics.get("engine", "recordsIn"));
        recordsOut = nonnegative(Metrics.get("engine", "recordsOut"));
        recordsSkipped = Math.max(recordsIn - recordsOut, 0L);
        recordsIndexed = elasticsearchActive
            ? nonnegative(Metrics.get("es-exporter-vt", "indexed"))
            : null;
      }
      metrics = snapshotMetrics();
    }

    ErrorDetail error = success
        ? null
        : new ErrorDetail(
            errorCode == null || errorCode.isBlank() ? INTERNAL_ERROR : errorCode,
            sanitizeMessage(errorMessage));

    return new ProcessingResult(
        SCHEMA,
        status,
        exitCode,
        recordsIn,
        recordsOut,
        recordsIndexed,
        recordsSkipped,
        success ? 0 : 1,
        Math.max(elapsedMs, 0L),
        error,
        metrics);
  }

  public String toJson() throws JsonProcessingException {
    return MAPPER.writeValueAsString(this);
  }

  /** Writes exactly one compact object and one newline. */
  public boolean writeTo(PrintStream out) {
    try {
      out.println(toJson());
      return true;
    } catch (JsonProcessingException e) {
      out.printf(
          "{\"schema\":\"%s\",\"status\":\"error\",\"exit_code\":1,"
              + "\"records_in\":null,\"records_out\":null,\"records_indexed\":null,"
              + "\"records_skipped\":null,\"errors\":1,\"elapsed_ms\":%d,"
              + "\"error\":{\"code\":\"%s\",\"message\":\"result serialization failed\"},"
              + "\"metrics\":null}%n",
          SCHEMA, Math.max(elapsedMs, 0L), INTERNAL_ERROR);
      return false;
    }
  }

  public static String sanitizeMessage(String message) {
    String sanitized = message == null || message.isBlank() ? "unspecified failure" : message;
    sanitized = URL_CREDENTIALS.matcher(sanitized).replaceAll("$1***:***@");
    sanitized = SECRET_ASSIGNMENT.matcher(sanitized).replaceAll("$1$2***");
    sanitized = AUTHORIZATION.matcher(sanitized).replaceAll("authorization=***");
    sanitized = STACK_FRAME.matcher(sanitized).replaceAll("");
    sanitized = sanitized.replace('\r', ' ').replace('\n', ' ').replace('\t', ' ')
        .replaceAll(" +", " ").trim();
    if (sanitized.length() > MAX_ERROR_MESSAGE_LENGTH) {
      sanitized = sanitized.substring(0, MAX_ERROR_MESSAGE_LENGTH);
    }
    return sanitized;
  }

  private static MetricsDetail snapshotMetrics() {
    Map<String, Map<String, Long>> counters = new TreeMap<>();
    for (Map.Entry<Metrics.Key, LongAdder> entry : Metrics.snapshot().entrySet()) {
      String namespace = snakeCase(entry.getKey().namespace);
      String counter = snakeCase(entry.getKey().name);
      counters.computeIfAbsent(namespace, ignored -> new TreeMap<>())
          .put(counter, nonnegative(entry.getValue().sum()));
    }
    return new MetricsDetail(METRICS_SCHEMA, counters);
  }

  private static String snakeCase(String value) {
    return value
        .replaceAll("([a-z0-9])([A-Z])", "$1_$2")
        .replaceAll("[^A-Za-z0-9]+", "_")
        .replaceAll("^_+|_+$", "")
        .toLowerCase(java.util.Locale.ROOT);
  }

  private static long nonnegative(long value) {
    return Math.max(value, 0L);
  }
}
