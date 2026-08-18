package pl.gov.nac.warc.utils;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Lightweight Elasticsearch HTTP client using java.net.http.HttpClient.
 * Fully compatible with virtual threads (no JNI pinning).
 *
 * Supports:
 * - Bulk indexing with NDJSON format
 * - Configurable retries with exponential backoff
 * - Connection health checks
 */
public final class ElasticsearchHttpClient implements AutoCloseable {

  private static final Logger log = LogManager.getLogger(ElasticsearchHttpClient.class);
  private static final ObjectMapper JSON = new ObjectMapper();

  private static final String CONTENT_TYPE = "Content-Type";
  private static final String APP_JSON = "application/json";
  private static final String APP_NDJSON = "application/x-ndjson";
  private static final String DOC_ENDPOINT = "/_doc/";
  private static final String REASON = "reason";
  private static final Duration MAX_RETRY_BACKOFF = Duration.ofSeconds(30);

  private static final String AUTHORIZATION = "Authorization";

  private final HttpClient http;
  private final URI baseUri;
  private final String authHeader; // null when no credentials

  // Retry configuration
  private final int maxRetries;
  private final Duration initialBackoff;
  private final double backoffMultiplier;

  /**
   * Create client with default retry settings, no auth.
   */
  public ElasticsearchHttpClient(String esUrl) {
    this(esUrl, null, null, 3, Duration.ofMillis(500), 2.0);
  }

  /**
   * Create client with custom retry settings, no auth.
   */
  public ElasticsearchHttpClient(String esUrl, int maxRetries, Duration initialBackoff, double backoffMultiplier) {
    this(esUrl, null, null, maxRetries, initialBackoff, backoffMultiplier);
  }

  /**
   * Create client with Basic auth and custom retry settings.
   *
   * @param esUrl             Elasticsearch base URL (e.g., "http://localhost:9200")
   * @param esUser            Username (nullable — skips auth if null/blank)
   * @param esPass            Password (nullable)
   * @param maxRetries        Maximum retry attempts (0 = no retries)
   * @param initialBackoff    Initial wait time before first retry
   * @param backoffMultiplier Multiplier for subsequent retries (e.g., 2.0 for exponential)
   */
  public ElasticsearchHttpClient(String esUrl, String esUser, String esPass,
      int maxRetries, Duration initialBackoff, double backoffMultiplier) {
    this.baseUri = URI.create(esUrl.endsWith("/") ? esUrl.substring(0, esUrl.length() - 1) : esUrl);
    this.maxRetries = maxRetries;
    this.initialBackoff = initialBackoff;
    this.backoffMultiplier = backoffMultiplier;

    if (esUser != null && !esUser.isBlank()) {
      String credentials = esUser + ":" + (esPass != null ? esPass : "");
      this.authHeader = "Basic " + Base64.getEncoder().encodeToString(
          credentials.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    } else {
      this.authHeader = null;
    }

    this.http = HttpClient.newBuilder()
        .executor(Executors.newVirtualThreadPerTaskExecutor())
        .connectTimeout(Duration.ofSeconds(10))
        .build();
  }

  private HttpRequest.Builder baseRequest(URI uri) {
    HttpRequest.Builder b = HttpRequest.newBuilder().uri(uri);
    if (authHeader != null) {
      b.header(AUTHORIZATION, authHeader);
    }
    return b;
  }

  // =========================================================================
  // Public API
  // =========================================================================

  /**
   * Check cluster health (synchronous).
   */
  public boolean isHealthy() {
    try {
      HttpRequest req = baseRequest(baseUri.resolve("/_cluster/health"))
          .GET()
          .timeout(Duration.ofSeconds(5))
          .build();

      HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
      return resp.statusCode() == 200;
    } catch (Exception e) {
      log.error("Health check failed", e);
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      return false;
    }
  }

  /**
   * Bulk index documents (synchronous with retries).
   */
  public BulkResult bulk(String index, List<Document> docs) {
    return bulk(index, docs, false);
  }

  /**
   * Bulk index documents (synchronous with retries).
   * @param isDataStream true → use op_type=create (required for ES data streams);
   *                     version_conflict responses remain bulk errors.
   */
  public BulkResult bulk(String index, List<Document> docs, boolean isDataStream) {
    return bulkWithRetry(index, docs, isDataStream, 0);
  }

  /**
   * Bulk index documents (async).
   */
  public CompletableFuture<BulkResult> bulkAsync(String index, List<Document> docs) {
    return CompletableFuture.supplyAsync(() -> bulk(index, docs, false), http.executor().orElseThrow());
  }

  // =========================================================================
  // Retry Logic
  // =========================================================================

  // Iterative loop rather than recursion: eliminates stack growth proportional
  // to maxRetries, which is user-configurable with no documented upper bound.
  private BulkResult bulkWithRetry(String index, List<Document> docs, boolean isDataStream, int attempt) {
    int currentAttempt = attempt;
    while (true) {
      try {
        return executeBulk(index, docs, isDataStream);
      } catch (Exception e) {
        if (currentAttempt < maxRetries && isRetryable(e)) {
          long waitMs = retryDelayMillis(currentAttempt);
          log.info("Retry {}/{} after {}ms: {}",
              currentAttempt + 1, maxRetries, waitMs, e.getMessage());

          try {
            Thread.sleep(waitMs);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return BulkResult.failure("Interrupted during retry backoff");
          }

          currentAttempt++;
        } else {
          log.error("Bulk request failed after {} retries: {}", currentAttempt, e.getMessage());
          return BulkResult.failure(e.getMessage());
        }
      }
    }
  }

  private boolean isRetryable(Exception e) {
    if (e instanceof InterruptedException)
      return false;

    if (e instanceof HttpStatusException statusException) {
      int status = statusException.statusCode();
      return status == 429 || status >= 500 && status < 600;
    }

    return e instanceof IOException;
  }

  private long retryDelayMillis(int attempt) {
    long maximum = MAX_RETRY_BACKOFF.toMillis();
    double exponential = initialBackoff.toMillis() * Math.pow(backoffMultiplier, attempt);
    long capped = !Double.isFinite(exponential) || exponential >= maximum
        ? maximum
        : Math.max(0L, (long) exponential);
    return capped == 0 ? 0 : ThreadLocalRandom.current().nextLong(capped + 1);
  }

  // =========================================================================
  // Bulk Execution
  // =========================================================================

  private BulkResult executeBulk(String index, List<Document> docs, boolean isDataStream) throws IOException, InterruptedException {
    NdjsonBuildResult ndjsonResult = buildNDJSON(index, docs, isDataStream);
    if (ndjsonResult.serializedDocs() == 0) {
      if (ndjsonResult.skippedDocs() > 0) {
        return new BulkResult(0, ndjsonResult.skippedDocs(), ndjsonResult.errorMessages());
      }
      return new BulkResult(0, 0, List.of());
    }

    HttpRequest req = baseRequest(baseUri.resolve("/_bulk"))
        .header(CONTENT_TYPE, APP_NDJSON)
        .POST(HttpRequest.BodyPublishers.ofString(ndjsonResult.ndjson()))
        .timeout(Duration.ofSeconds(60))
        .build();

    HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());

    if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
      throw new HttpStatusException(resp.statusCode());
    }

    BulkResult bulkResult = parseBulkResponse(resp.body(), ndjsonResult.serializedDocs());
    if (ndjsonResult.skippedDocs() == 0) {
      return bulkResult;
    }

    List<String> mergedMessages = new ArrayList<>(bulkResult.errorMessages());
    for (String msg : ndjsonResult.errorMessages()) {
      if (mergedMessages.size() >= 5) {
        break;
      }
      mergedMessages.add(msg);
    }
    return new BulkResult(
        bulkResult.indexed(),
        bulkResult.errors() + ndjsonResult.skippedDocs(),
        mergedMessages);
  }

  private NdjsonBuildResult buildNDJSON(String index, List<Document> docs, boolean isDataStream) {
    StringBuilder sb = new StringBuilder(docs.size() * 512);
    List<String> errorMessages = new ArrayList<>();
    int serializedDocs = 0;
    int skippedDocs = 0;

    // Data streams require op_type=create (no upsert). Duplicate _ids produce
    // version_conflict errors.
    // Regular indices use op_type=index which upserts by _id.
    String opType = isDataStream ? "create" : "index";

    for (Document doc : docs) {
      try {
        String serializedSource = JSON.writeValueAsString(doc.source());
        sb.append("{\"").append(opType).append("\":{\"_index\":\"").append(escapeJson(index)).append("\"");
        if (doc.id() != null && !doc.id().isEmpty()) {
          sb.append(",\"_id\":\"").append(escapeJson(doc.id())).append("\"");
        }
        sb.append("}}\n");
        sb.append(serializedSource).append("\n");
        serializedDocs++;
      } catch (Exception e) {
        skippedDocs++;
        String docId = (doc.id() == null || doc.id().isEmpty()) ? "<auto>" : doc.id();
        log.error("Failed to serialize document id={}", docId, e);
        if (errorMessages.size() < 5) {
          errorMessages.add("serialize-failed id=" + docId + ": " + e.getMessage());
        }
      }
    }

    return new NdjsonBuildResult(sb.toString(), serializedDocs, skippedDocs, errorMessages);
  }

  private BulkResult parseBulkResponse(String body, int totalDocs) {
    try {
      JsonNode root = JSON.readTree(body);
      boolean hasErrors = root.path("errors").asBoolean(false);

      if (!hasErrors) {
        return new BulkResult(totalDocs, 0, List.of());
      }

      // Parse individual item errors
      List<String> errorMessages = new ArrayList<>();
      int errorCount = 0;

      JsonNode items = root.path("items");
      if (items.isArray()) {
        for (JsonNode item : items) {
          // Response key matches the request op_type ("index" or "create")
          JsonNode opResult = item.path("index").isMissingNode() ? item.path("create") : item.path("index");
          JsonNode error = opResult.path("error");
          if (!error.isMissingNode()) {
            errorCount++;
            String reason = error.has(REASON) ? error.get(REASON).asText() : "unknown";
            if (errorMessages.size() < 5) { // Limit stored errors
              errorMessages.add(reason);
            }
          }
        }
      }

      return new BulkResult(totalDocs - errorCount, errorCount, errorMessages);

    } catch (Exception e) {
      log.error("Failed to parse bulk response", e);
      return new BulkResult(0, totalDocs, List.of("Parse error: " + e.getMessage()));
    }
  }

  private static String escapeJson(String s) {
    if (s == null)
      return "";
    StringBuilder sb = new StringBuilder(s.length() + 16);
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '"'  -> sb.append("\\\"");
        case '\\' -> sb.append("\\\\");
        case '\n' -> sb.append("\\n");
        case '\r' -> sb.append("\\r");
        case '\t' -> sb.append("\\t");
        case '\b' -> sb.append("\\b");
        case '\f' -> sb.append("\\f");
        default   -> {
          if (c < 0x20) {
            sb.append(String.format("\\u%04x", (int) c));
          } else {
            sb.append(c);
          }
        }
      }
    }
    return sb.toString();
  }

  // =========================================================================
  // CRUD Operations
  // =========================================================================

  /**
   * Get a document by ID.
   *
   * @param index Index name
   * @param id    Document ID
   * @return Document if found, empty Optional otherwise
   */
  public java.util.Optional<Document> get(String index, String id) {
    try {
      HttpRequest req = baseRequest(baseUri.resolve("/" + escapeUri(index) + DOC_ENDPOINT + escapeUri(id)))
          .GET()
          .timeout(Duration.ofSeconds(10))
          .build();

      HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());

      if (resp.statusCode() == 404) {
        return java.util.Optional.empty();
      }
      if (resp.statusCode() >= 400) {
        log.error("GET failed: {}", resp.statusCode());
        return java.util.Optional.empty();
      }

      JsonNode root = JSON.readTree(resp.body());
      if (!root.path("found").asBoolean(false)) {
        return java.util.Optional.empty();
      }

      @SuppressWarnings("unchecked")
      Map<String, Object> source = JSON.convertValue(root.path("_source"), Map.class);
      return java.util.Optional.of(Document.of(root.path("_id").asText(), source));

    } catch (Exception e) {
      log.error("GET request failed", e);
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      return java.util.Optional.empty();
    }
  }

  /**
   * Check if document exists.
   */
  public boolean exists(String index, String id) {
    try {
      HttpRequest req = baseRequest(baseUri.resolve("/" + escapeUri(index) + DOC_ENDPOINT + escapeUri(id)))
          .method("HEAD", HttpRequest.BodyPublishers.noBody())
          .timeout(Duration.ofSeconds(5))
          .build();

      HttpResponse<Void> resp = http.send(req, HttpResponse.BodyHandlers.discarding());
      return resp.statusCode() == 200;

    } catch (Exception e) {
      log.error("EXISTS check failed", e);
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      return false;
    }
  }

  /**
   * Delete a single document by ID.
   *
   * @return true if deleted, false if not found or error
   */
  public boolean delete(String index, String id) {
    try {
      HttpRequest req = baseRequest(baseUri.resolve("/" + escapeUri(index) + DOC_ENDPOINT + escapeUri(id)))
          .DELETE()
          .timeout(Duration.ofSeconds(10))
          .build();

      HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());

      if (resp.statusCode() == 404) {
        return false;
      }
      if (resp.statusCode() >= 400) {
        log.error("DELETE failed: {}", resp.statusCode());
        return false;
      }

      JsonNode root = JSON.readTree(resp.body());
      return "deleted".equals(root.path("result").asText());

    } catch (Exception e) {
      log.error("DELETE request failed", e);
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      return false;
    }
  }

  /**
   * Delete documents matching a query.
   *
   * @param index     Index name
   * @param queryJson Elasticsearch query DSL JSON (the "query" object content)
   * @return DeleteResult with deletion count
   */
  public DeleteResult deleteByQuery(String index, String queryJson) {
    try {
      String body = "{\"query\":" + queryJson + "}";
      HttpRequest req = baseRequest(baseUri.resolve("/" + escapeUri(index) + "/_delete_by_query"))
          .header(CONTENT_TYPE, APP_JSON)
          .POST(HttpRequest.BodyPublishers.ofString(body))
          .timeout(Duration.ofMinutes(5))
          .build();

      HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());

      if (resp.statusCode() >= 400) {
        return new DeleteResult(0, 1, List.of("HTTP " + resp.statusCode()));
      }

      JsonNode root = JSON.readTree(resp.body());
      long deleted = root.path("deleted").asLong(0);
      long failures = root.path("failures").size();
      List<String> errors = new ArrayList<>();
      for (JsonNode failure : root.path("failures")) {
        if (errors.size() < 5) {
          errors.add(failure.path("cause").path(REASON).asText("unknown"));
        }
      }
      return new DeleteResult(deleted, failures, errors);

    } catch (Exception e) {
      log.error("DELETE_BY_QUERY failed", e);
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      return new DeleteResult(0, 1, List.of(e.getMessage()));
    }
  }

  /**
   * List documents with pagination (match_all query).
   */
  public SearchResult list(String index, int from, int size) {
    return searchRaw(index, "{\"match_all\":{}}", from, size);
  }

  /**
   * Search with raw Elasticsearch query DSL.
   *
   * @param index     Index name
   * @param queryJson Query DSL JSON (the "query" object content)
   * @param from      Pagination offset
   * @param size      Results per page
   */
  public SearchResult searchRaw(String index, String queryJson, int from, int size) {
    try {
      String body = String.format("{\"query\":%s,\"from\":%d,\"size\":%d}", queryJson, from, size);
      HttpRequest req = baseRequest(baseUri.resolve("/" + escapeUri(index) + "/_search"))
          .header(CONTENT_TYPE, APP_JSON)
          .POST(HttpRequest.BodyPublishers.ofString(body))
          .timeout(Duration.ofSeconds(30))
          .build();

      HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());

      if (resp.statusCode() >= 400) {
        log.error("SEARCH failed: {}", resp.statusCode());
        return new SearchResult(0, List.of(), null);
      }

      return parseSearchResponse(resp.body());

    } catch (Exception e) {
      log.error("SEARCH request failed", e);
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      return new SearchResult(0, List.of(), null);
    }
  }

  /**
   * Search using Google-like query syntax.
   * Requires GoogleQueryParser class.
   */
  public SearchResult search(String index, String googleQuery, int from, int size) {
    String esQuery = GoogleQueryParser.toElasticsearchQuery(googleQuery);
    return searchRaw(index, esQuery, from, size);
  }

  /**
   * Count documents matching a query.
   *
   * @param queryJson Query DSL JSON (the "query" object content), or null for
   *                  count all
   */
  public long count(String index, String queryJson) {
    try {
      String body = queryJson != null ? "{\"query\":" + queryJson + "}" : "{}";
      HttpRequest req = baseRequest(baseUri.resolve("/" + escapeUri(index) + "/_count"))
          .header(CONTENT_TYPE, APP_JSON)
          .POST(HttpRequest.BodyPublishers.ofString(body))
          .timeout(Duration.ofSeconds(10))
          .build();

      HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());

      if (resp.statusCode() >= 400) {
        log.error("COUNT failed: {}", resp.statusCode());
        return -1;
      }

      JsonNode root = JSON.readTree(resp.body());
      return root.path("count").asLong(0);

    } catch (Exception e) {
      log.error("COUNT request failed", e);
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      return -1;
    }
  }

  private SearchResult parseSearchResponse(String body) {
    try {
      JsonNode root = JSON.readTree(body);
      JsonNode hitsNode = root.path("hits");
      long totalHits = hitsNode.path("total").path("value").asLong(0);

      List<SearchHit> hits = new ArrayList<>();
      for (JsonNode hit : hitsNode.path("hits")) {
        String id = hit.path("_id").asText();
        double score = hit.path("_score").asDouble(0);

        @SuppressWarnings("unchecked")
        Map<String, Object> source = JSON.convertValue(hit.path("_source"), Map.class);

        Map<String, List<String>> highlights = new java.util.HashMap<>();
        JsonNode highlightNode = hit.path("highlight");
        if (!highlightNode.isMissingNode()) {
          highlightNode.fields().forEachRemaining(entry -> {
            List<String> fragments = new ArrayList<>();
            for (JsonNode frag : entry.getValue()) {
              fragments.add(frag.asText());
            }
            highlights.put(entry.getKey(), fragments);
          });
        }

        hits.add(new SearchHit(id, score, source, highlights));
      }

      return new SearchResult(totalHits, hits, null);

    } catch (Exception e) {
      log.error("Failed to parse search response", e);
      return new SearchResult(0, List.of(), null);
    }
  }

  private static String escapeUri(String s) {
    // URLEncoder produces form-encoding (spaces → '+'); use URI raw-path encoding
    // instead so spaces and special chars become %XX as required by the ES REST API.
    try {
      return new java.net.URI(null, null, s, null).toASCIIString();
    } catch (java.net.URISyntaxException e) {
      // Fallback: percent-encode manually via charset encoder
      return java.net.URLEncoder.encode(s, java.nio.charset.StandardCharsets.UTF_8)
          .replace("+", "%20");
    }
  }

  private record NdjsonBuildResult(String ndjson, int serializedDocs, int skippedDocs, List<String> errorMessages) {
  }

  private static final class HttpStatusException extends IOException {
    private final int statusCode;

    private HttpStatusException(int statusCode) {
      super("HTTP " + statusCode);
      this.statusCode = statusCode;
    }

    private int statusCode() {
      return statusCode;
    }
  }

  @Override
  public void close() {
    // HttpClient.close() shuts down the internally-owned virtual-thread executor
    // (Java 21+). Without this the executor leaks on client close.
    http.close();
  }

  // =========================================================================
  // Data Types
  // =========================================================================

  /**
   * Document to be indexed.
   */
  public record Document(String id, Map<String, Object> source) {
    public static Document of(Map<String, Object> source) {
      return new Document(null, source);
    }

    public static Document of(String id, Map<String, Object> source) {
      return new Document(id, source);
    }
  }

  /**
   * Result of a bulk operation.
   */
  public record BulkResult(int indexed, int errors, List<String> errorMessages) {
    public boolean hasErrors() {
      return errors > 0;
    }

    public static BulkResult failure(String message) {
      return new BulkResult(0, 1, List.of(message));
    }
  }

  /**
   * Result of a delete-by-query operation.
   */
  public record DeleteResult(long deleted, long failures, List<String> errors) {
    public boolean hasErrors() {
      return failures > 0;
    }
  }

  /**
   * Result of a search operation.
   */
  public record SearchResult(long totalHits, List<SearchHit> hits, String scrollId) {
    public boolean isEmpty() {
      return hits.isEmpty();
    }
  }

  /**
   * Single search hit.
   */
  public record SearchHit(String id, double score, Map<String, Object> source,
      Map<String, List<String>> highlights) {
  }
}
