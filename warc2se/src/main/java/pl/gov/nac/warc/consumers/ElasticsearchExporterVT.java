package pl.gov.nac.warc.consumers;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.gov.nac.warc.reactive.Metrics;
import pl.gov.nac.warc.reactive.ReactiveInterfaces;
import pl.gov.nac.warc.records.Record;
import pl.gov.nac.warc.records.RecordInMemory;
import pl.gov.nac.warc.records.warc.RecordWarc;
import pl.gov.nac.warc.records.warc.RecordWarcInFile;
import pl.gov.nac.warc.records.warc.RecordWarcUniversal;
import pl.gov.nac.warc.records.warc.RecordWet;
import pl.gov.nac.warc.utils.ElasticsearchHttpClient;
import pl.gov.nac.warc.utils.ElasticsearchHttpClient.BulkResult;
import pl.gov.nac.warc.utils.ElasticsearchHttpClient.Document;

/**
 * Virtual-thread-based Elasticsearch consumer using java.net.http.HttpClient.
 *
 * Features:
 * - Full virtual thread compatibility (no JNI pinning)
 * - Parallel batch processing with backpressure
 * - Configurable retry with exponential backoff
 * - VirtualThreadSchedulerMXBean monitoring
 */
public final class ElasticsearchExporterVT implements ReactiveInterfaces.ReactiveConsumer<RecordInMemory> {

  private static final Logger log = LogManager.getLogger(ElasticsearchExporterVT.class);
  private static final String METRIC_KEY = "es-exporter-vt";
  private static final Pattern PROVENANCE_IDENTIFIER = Pattern.compile("[A-Za-z0-9._-]{1,128}");

  // Configuration
  private String index;
  private boolean isDataStream;
  private int batchSize;
  private int maxConcurrentBatches;

  // Client and execution
  private ElasticsearchHttpClient client;
  private ExecutorService vtExecutor;
  private Semaphore inFlight;

  // Ingest timestamp: captured once at pipeline start, same for ALL records
  private String ingestTimestamp;

  // Ingest-time metadata supplied via CLI
  private String ingestUrlId;    // --url-id
  private String ingestCrawlId;   // --crawl-id (explicit override when provided)
  private String ingestStartDate; // --start-date metadata; never part of document identity
  private final Map<String, Map<String, Object>> seenDocuments = new ConcurrentHashMap<>();

  // Batch accumulation (synchronized access)
  private final List<Document> batch = new ArrayList<>();
  private final AtomicInteger pendingBatches = new AtomicInteger(0);
  private final Object batchCompletionMonitor = new Object();

  @Override
  public void configure(Map<String, Object> cfg) {
    Metrics.setModuleHeader(METRIC_KEY, "ES Exporter VT");

    // Connection config
    String esUrl = getString(cfg, "es-url", "http://localhost:9200");
    String esUser = getString(cfg, "es-user", "");
    String esPass = getString(cfg, "es-pass", "");
    String configIndex = getString(cfg, "index", "docs-test");
    String configStream = getString(cfg, "stream", "");

    // If stream is set, use it as target (data stream mode)
    // Otherwise, use index (plain index mode)
    index = (configStream != null && !configStream.isBlank()) ? configStream : configIndex;
    isDataStream = configStream != null && !configStream.isBlank();

    batchSize = getInt(cfg, "batch-size", 100);
    maxConcurrentBatches = getInt(cfg, "max-concurrent-batches", 10);

    // Retry config
    int retryCount = getInt(cfg, "retry-count", 3);
    long retryBackoffMs = getLong(cfg, "retry-backoff-ms", 500);
    double retryMultiplier = getDouble(cfg, "retry-backoff-multiplier", 2.0);

    // Initialize client with retry settings and optional Basic auth
    client = new ElasticsearchHttpClient(
        esUrl,
        esUser.isBlank() ? null : esUser,
        esPass.isBlank() ? null : esPass,
        retryCount,
        Duration.ofMillis(retryBackoffMs),
        retryMultiplier);

    // Shut down any previous executor before replacing it so that in-progress
    // virtual threads are interrupted and the old executor is not leaked when
    // configure() is called more than once.
    if (vtExecutor != null) {
      vtExecutor.shutdownNow();
    }

    // Initialize execution infrastructure
    vtExecutor = Executors.newVirtualThreadPerTaskExecutor();
    inFlight = new Semaphore(maxConcurrentBatches);

    // Capture ingest timestamp once for entire pipeline run
    ingestTimestamp = Instant.now().toString();

    // Ingest-time metadata supplied via CLI.
    String rawUrlId = getString(cfg, "url-id", "");
    ingestUrlId = validateExpectedIdentifier(rawUrlId, "url-id");
    String rawCrawlId = getString(cfg, "crawl-id", "");
    ingestCrawlId = validateExpectedIdentifier(rawCrawlId, "crawl-id");
    String rawStartDate = getString(cfg, "start-date", "");
    ingestStartDate = (rawStartDate != null && !rawStartDate.isBlank()) ? rawStartDate : null;
    seenDocuments.clear();

    log.info("Configured: url={}, target={} ({}), batch={}, concurrent={}, retries={}, urlId={}, crawlId={}, startDate={}, ingestTs={}",
        esUrl, index, isDataStream ? "data-stream" : "index",
        batchSize, maxConcurrentBatches, retryCount, ingestUrlId, ingestCrawlId, ingestStartDate, ingestTimestamp);

    logVTSchedulerStats();
  }

  @Override
  public List<Class<? extends Record>> acceptedInputTypes() {
    // RecordWarcInFile excluded: extractMetadata() returns null for it, causing
    // silent record drops. Omitting it lets the type negotiator catch misconfigured
    // pipelines at startup with a clear error.
    return List.of(RecordWet.class, RecordWarc.class);
  }

  @Override
  public boolean beforeCheck(Map<String, Object> cfg) {
    if (client == null) {
      log.error("Client not initialized");
      return false;
    }

    if (ingestUrlId == null || ingestCrawlId == null) {
      log.error("Both url-id and crawl-id are required expected provenance values");
      return false;
    }

    if (!client.isHealthy()) {
      log.error("Elasticsearch cluster not healthy");
      return false;
    }

    log.info("Elasticsearch health check passed");
    return true;
  }

  @Override
  public int afterCheck(Map<String, Object> cfg) {
    long batchFailures = Metrics.get(METRIC_KEY, "batch-failures");
    long batchSubmitFailures = Metrics.get(METRIC_KEY, "batch-submit-failures");
    long bulkErrors = Metrics.get(METRIC_KEY, "bulk-errors");
    long indexed = Metrics.get(METRIC_KEY, "indexed");
    long recordsIn = Metrics.get(METRIC_KEY, "recordsIn");
    long empty = Metrics.get(METRIC_KEY, "empty");
    long expectedIndexed = Math.max(0, recordsIn - empty);

    if (batchFailures > 0 || batchSubmitFailures > 0 || bulkErrors > 0) {
      log.error("Elasticsearch export failed: batchFailures={}, batchSubmitFailures={}, bulkErrors={}",
          batchFailures, batchSubmitFailures, bulkErrors);
      return 1;
    }
    if (indexed < expectedIndexed) {
      log.error("Elasticsearch export incomplete: indexed={} expected={}", indexed, expectedIndexed);
      return 1;
    }
    return 0;
  }

  @Override
  public void onSubscribe(Flow.Subscription subscription) {
    // Request unbounded demand: back-pressure for this consumer is enforced by the
    // internal `inFlight` semaphore (maxConcurrentBatches permits) rather than by
    // reactive demand signals. Issuing Long.MAX_VALUE here satisfies RS §3.17 and
    // avoids stalling the upstream producer; the semaphore in `onNext` provides the
    // actual throttle that prevents unbounded memory growth.
    subscription.request(Long.MAX_VALUE);
  }

  @Override
  public void onNext(RecordInMemory item) {
    Metrics.inc(METRIC_KEY, "recordsIn");

    try {
      Document doc = convertToDocument(item);
      if (doc == null) {
        Metrics.inc(METRIC_KEY, "empty");
        return;
      }

      boolean shouldFlush;
      synchronized (batch) {
        batch.add(doc);
        shouldFlush = batch.size() >= batchSize;
      }
      if (shouldFlush) {
        flushBatch();
      }
    } catch (Exception e) {
      log.error("Error processing record", e);
      Metrics.inc(METRIC_KEY, "errors");
    } finally {
      releasePooledBuffer(item);
    }
  }

  @Override
  public void onError(Throwable throwable) {
    log.error("Pipeline error: {}", throwable.getMessage(), throwable);
    shutdown();
  }

  @Override
  public void onComplete() {
    log.info("Pipeline complete. Flushing remaining records.");

    // Flush any remaining batch
    flushBatch();

    // Wait for all in-flight batches to complete
    waitForPendingBatches();

    shutdown();
    logVTSchedulerStats();

    log.info("Consumer shutdown complete");
  }

  // =========================================================================
  // Batch Processing (Virtual Threads)
  // =========================================================================

  private void flushBatch() {
    List<Document> toFlush;
    synchronized (batch) {
      if (batch.isEmpty())
        return;

      toFlush = new ArrayList<>(batch);
      batch.clear();
    }

    try {
      inFlight.acquire();
    } catch (InterruptedException e) {
      synchronized (batch) {
        batch.addAll(0, toFlush);
      }
      Thread.currentThread().interrupt();
      Metrics.inc(METRIC_KEY, "batch-submit-failures");
      log.error("Interrupted while waiting for Elasticsearch batch permit", e);
      return;
    }

    pendingBatches.incrementAndGet();

    // Submit batch to virtual thread executor
    try {
      vtExecutor.submit(() -> processBatch(toFlush));
    } catch (RejectedExecutionException e) {
      log.error("Executor rejected batch submission; processing batch inline", e);
      processBatch(toFlush);
    }
  }

  private void processBatch(List<Document> docs) {
    try {
      BulkResult result = client.bulk(index, docs, isDataStream);

      Metrics.add(METRIC_KEY, "indexed", result.indexed());
      if (result.hasErrors()) {
        Metrics.add(METRIC_KEY, "bulk-errors", result.errors());
        log.error("Bulk indexing failed: {}/{} errors", result.errors(), docs.size());
        for (String msg : result.errorMessages()) {
          log.error("  → {}", msg);
        }
      }
    } catch (Exception e) {
      log.error("Batch processing failed", e);
      Metrics.inc(METRIC_KEY, "batch-failures");
    } finally {
      inFlight.release();
      if (pendingBatches.decrementAndGet() == 0) {
        synchronized (batchCompletionMonitor) {
          batchCompletionMonitor.notifyAll();
        }
      }
    }
  }

  private void waitForPendingBatches() {
    synchronized (batchCompletionMonitor) {
      while (pendingBatches.get() > 0) {
        try {
          batchCompletionMonitor.wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          log.warn("Interrupted while waiting for pending batches");
          if (pendingBatches.get() > 0) {
            log.error("Interrupted with {} batches still pending", pendingBatches.get());
          }
          return;
        }
      }
    }
  }

  // =========================================================================
  // Document Conversion
  // =========================================================================

  Document convertToDocument(RecordInMemory item) {
    RecordMetadata meta = extractMetadata(item);
    if (meta == null) {
      return null;
    }

    if (meta.warcinfo) {
      validateProvenance("url-id", ingestUrlId, meta.urlId);
      validateProvenance("crawl-id", ingestCrawlId, meta.crawlId);
      return null;
    }

    if (meta.content == null || meta.content.isBlank()) {
      return null;
    }

    validateProvenance("url-id", ingestUrlId, meta.urlId);
    validateProvenance("crawl-id", ingestCrawlId, meta.crawlId);

    String effectiveFirstSeen = resolveFirstSeen(item, meta);
    String nacLastSeen = resolveLastSeen(item, meta);

    // Build document with @timestamp for data stream compatibility
    Map<String, Object> source = new HashMap<>();
    source.put("@timestamp", ingestTimestamp); // Same for all records in batch
    source.put("warc-uri", meta.uri);
    source.put("warc-date", meta.date);
    source.put("warc-digest", meta.digest);
    source.put("wet-lang", parseLang(meta.lang));
    source.put("nac-url-id", meta.urlId);
    source.put("nac-crawl-id", meta.crawlId);
    source.put("content", meta.content);

    // Add temporal lifecycle fields (Phase 2)
    Integer nacMissingCount = extractMissingCount(item);
    String nacStatus = extractStatus(item);

    if (effectiveFirstSeen != null) {
      source.put("nac-first-seen", effectiveFirstSeen);
    }
    if (nacLastSeen != null) {
      source.put("nac-last-seen", nacLastSeen);
    }
    if (nacMissingCount != null) {
      source.put("nac-missing-count", nacMissingCount);
    }
    if (nacStatus != null) {
      source.put("nac-status", nacStatus);
    }

    // Add merge provenance fields (Task #50)
    String mergeResult = extractMergeResult(item);
    Integer revisitCount = extractRevisitCount(item);
    String deduplicatedScope = extractDeduplicatedScope(item);
    String primaryUri = extractPrimaryUri(item);
    String previousUri = extractPreviousUri(item);
    Integer chainLength = extractChainLength(item);

    if (mergeResult != null) {
      source.put("nac-merge-result", mergeResult);
      source.put("merge-provenance", mergeResult); // Alias for queries
    }
    if (revisitCount != null) {
      source.put("nac-revisit-count", revisitCount);
    }
    if (deduplicatedScope != null) {
      source.put("nac-deduplicated", deduplicatedScope);
    }
    if (primaryUri != null) {
      source.put("nac-primary-uri", primaryUri);
    }
    if (previousUri != null) {
      source.put("nac-previous-uri", previousUri);
    }
    if (chainLength != null) {
      source.put("nac-chain-length", chainLength);
    }

    // Use URI plus effective first-seen as the stable WET document identity.
    // Invocation start-date and provenance are deliberately not identity components.
    String docId = null;

    // Delimiter: U+001E (ASCII Record Separator) is invalid in both RFC-3986 URIs
    // and ISO-8601 dates, so it can never appear in either field and creates
    // an unambiguous composite key.
    final String SEP = "\u001E";
    if (meta.uri != null && effectiveFirstSeen != null) {
      docId = meta.uri + SEP + effectiveFirstSeen;
    } else if (meta.digest != null) {
      // Last resort: digest only (no URI available)
      docId = meta.digest.replace("sha256:", "");
    }

    if (docId == null) {
      throw new IllegalArgumentException("integrity error: record has neither URI identity nor digest fallback");
    }

    Map<String, Object> prior = seenDocuments.putIfAbsent(docId, new HashMap<>(source));
    if (prior != null) {
      if (prior.equals(source)) {
        return null;
      }
      throw new IllegalArgumentException("integrity error: conflicting content or provenance for document id " + docId);
    }

    return Document.of(docId, source);
  }

  private static class RecordMetadata {
    String digest, uri, date, lang, content;
    String urlId, crawlId;
    boolean warcinfo;
  }

  private RecordMetadata extractMetadata(RecordInMemory item) {
    RecordMetadata meta = new RecordMetadata();
    try {
      if (item instanceof RecordWet wet) {
        meta.digest = wet.digest();
        meta.uri = wet.targetUri();
        meta.date = wet.date();
        meta.lang = wet.language();
        if (meta.lang == null) {
          meta.lang = wet.headers().get("WARC-Identified-Content-Language");
        }
        meta.content = wet.text();
        Map<String, String> h = wet.headers();
        meta.urlId = getHeader(h, "X-NAC-URL-ID");
        meta.crawlId = getHeader(h, "X-NAC-Crawl-ID");
      } else if (item instanceof RecordWarcUniversal universal) {
        meta.digest = universal.digest();
        meta.uri = universal.targetUri();
        meta.date = universal.warcDate();
        meta.lang = universal.headers().get("WARC-Identified-Content-Language");
        Map<String, String> h = universal.headers();
        meta.warcinfo = "warcinfo".equalsIgnoreCase(universal.warcType());

        if (universal.bodyBytes() != null) {
          meta.content = new String(universal.bodyBytes(), StandardCharsets.UTF_8).trim();
        }
        if (meta.warcinfo) {
          meta.urlId = getWarcinfoField(meta.content, "X-NAC-URL-ID");
          meta.crawlId = getWarcinfoField(meta.content, "X-NAC-Crawl-ID");
        } else {
          meta.urlId = getHeader(h, "X-NAC-URL-ID");
          meta.crawlId = getHeader(h, "X-NAC-Crawl-ID");
        }
      }
    } catch (Exception e) {
      log.error("Failed to extract metadata", e);
      return null;
    }
    return meta;
  }

  private String getWarcinfoField(String content, String key) {
    if (content == null) {
      return null;
    }
    for (String line : content.split("\\R")) {
      int separator = line.indexOf(':');
      if (separator > 0 && key.equalsIgnoreCase(line.substring(0, separator).trim())) {
        return line.substring(separator + 1).trim();
      }
    }
    return null;
  }

  private String getHeader(Map<String, String> headers, String... keys) {
    for (String k : keys) {
      String v = headers.get(k);
      if (v != null)
        return v;
    }
    return null;
  }

  private String parseLang(String langHeader) {
    if (langHeader == null)
      return null;
    if (langHeader.startsWith("lang=")) {
      int semi = langHeader.indexOf(';');
      if (semi > 0) {
        return langHeader.substring(5, semi);
      }
      return langHeader.substring(5);
    }
    return langHeader;
  }

  private String resolveFirstSeen(RecordInMemory item, RecordMetadata meta) {
    String firstSeen = extractFirstSeen(item);
    if (firstSeen == null) {
      firstSeen = meta.date;
    }
    return firstSeen;
  }

  private static String validateExpectedIdentifier(String value, String name) {
    if (value == null || value.isBlank()) {
      return null;
    }
    if (!PROVENANCE_IDENTIFIER.matcher(value).matches()) {
      throw new IllegalArgumentException(name + " must match [A-Za-z0-9._-]{1,128}");
    }
    return value;
  }

  private static void validateProvenance(String name, String expected, String actual) {
    if (expected == null) {
      return;
    }
    if (!expected.equals(actual)) {
      throw new IllegalArgumentException(
          "integrity error: provenance mismatch for " + name + ": expected " + expected + ", record has " + actual);
    }
  }

  private String resolveLastSeen(RecordInMemory item, RecordMetadata meta) {
    String lastSeen = extractLastSeen(item);
    if (lastSeen == null) {
      lastSeen = meta.date;
    }
    return lastSeen;
  }

  // =========================================================================
  // Temporal Lifecycle Field Extraction
  // =========================================================================

  /**
   * Extract X-NAC-First-Seen header from item (case-insensitive).
   */
  private String extractFirstSeen(RecordInMemory item) {
    Map<String, String> headers = null;
    if (item instanceof RecordWet wet)
      headers = wet.headers();
    else if (item instanceof RecordWarcUniversal universal)
      headers = universal.headers();

    if (headers == null)
      return null;

    String firstSeen = headers.get("X-NAC-First-Seen");
    if (firstSeen == null)
      firstSeen = headers.get("x-nac-first-seen");
    return firstSeen;
  }

  private String extractLastSeen(RecordInMemory item) {
    Map<String, String> headers = null;
    if (item instanceof RecordWet wet)
      headers = wet.headers();
    else if (item instanceof RecordWarcUniversal universal)
      headers = universal.headers();

    if (headers == null)
      return null;

    String lastSeen = headers.get("X-NAC-Last-Seen");
    if (lastSeen == null)
      lastSeen = headers.get("x-nac-last-seen");
    return lastSeen;
  }

  private Integer extractMissingCount(RecordInMemory item) {
    try {
      Map<String, String> headers = null;
      if (item instanceof RecordWet wet)
        headers = wet.headers();
      else if (item instanceof RecordWarcUniversal universal)
        headers = universal.headers();

      if (headers == null)
        return null;

      String missingCountStr = headers.get("X-NAC-Missing-Count");
      if (missingCountStr == null)
        missingCountStr = headers.get("x-nac-missing-count");
      if (missingCountStr != null) {
        return Integer.parseInt(missingCountStr);
      }
    } catch (Exception e) {
      log.warn("Failed to extract missing_count: {}", e.getMessage());
    }
    return null;
  }

  private String extractStatus(RecordInMemory item) {
    Map<String, String> headers = null;
    if (item instanceof RecordWet wet)
      headers = wet.headers();
    else if (item instanceof RecordWarcUniversal universal)
      headers = universal.headers();

    if (headers == null)
      return null;

    String status = headers.get("X-NAC-Status");
    if (status == null)
      status = headers.get("x-nac-status");
    return status;
  }

  // =========================================================================
  // Merge Provenance Field Extraction (Task #50)
  // =========================================================================

  /**
   * Extract NAC-Merge-Result header (base-only, merged, new, uri-changed).
   */
  private String extractMergeResult(RecordInMemory item) {
    Map<String, String> headers = getHeaders(item);
    if (headers == null)
      return null;

    return getHeader(headers, "NAC-Merge-Result", "nac-merge-result");
  }

  /**
   * Extract X-NAC-Record-Revisit-Count header (number of times content revisited).
   */
  private Integer extractRevisitCount(RecordInMemory item) {
    try {
      Map<String, String> headers = getHeaders(item);
      if (headers == null)
        return null;

      String countStr = getHeader(headers, "X-NAC-Record-Revisit-Count", "x-nac-record-revisit-count");
      if (countStr != null) {
        return Integer.parseInt(countStr);
      }
    } catch (NumberFormatException e) {
      log.warn("Failed to parse revisit count: {}", e.getMessage());
    }
    return null;
  }

  /**
   * Extract NAC-Deduplicated header (global, url, none).
   */
  private String extractDeduplicatedScope(RecordInMemory item) {
    Map<String, String> headers = getHeaders(item);
    if (headers == null)
      return null;

    return getHeader(headers, "NAC-Deduplicated", "nac-deduplicated");
  }

  /**
   * Extract X-NAC-Primary-URI header (original URI for uri-changed records).
   */
  private String extractPrimaryUri(RecordInMemory item) {
    Map<String, String> headers = getHeaders(item);
    if (headers == null)
      return null;

    return getHeader(headers, "X-NAC-Primary-URI", "x-nac-primary-uri");
  }

  /**
   * Extract X-NAC-Previous-URI header (previous URI in chain).
   */
  private String extractPreviousUri(RecordInMemory item) {
    Map<String, String> headers = getHeaders(item);
    if (headers == null)
      return null;

    return getHeader(headers, "X-NAC-Previous-URI", "x-nac-previous-uri");
  }

  /**
   * Extract X-NAC-Chain-Length header (URI relocation chain length).
   */
  private Integer extractChainLength(RecordInMemory item) {
    try {
      Map<String, String> headers = getHeaders(item);
      if (headers == null)
        return null;

      String lengthStr = getHeader(headers, "X-NAC-Chain-Length", "x-nac-chain-length");
      if (lengthStr != null) {
        return Integer.parseInt(lengthStr);
      }
    } catch (NumberFormatException e) {
      log.warn("Failed to parse chain length: {}", e.getMessage());
    }
    return null;
  }

  /**
   * Helper to extract headers from different record types.
   */
  private Map<String, String> getHeaders(RecordInMemory item) {
    if (item instanceof RecordWet wet)
      return wet.headers();
    else if (item instanceof RecordWarcUniversal universal)
      return universal.headers();
    return null;
  }

  private void releasePooledBuffer(RecordInMemory item) {
    // Pipeline handles release
  }

  // =========================================================================
  // Monitoring
  // =========================================================================

  private void logVTSchedulerStats() {
    try {
      var threadMXBean = ManagementFactory.getThreadMXBean();

      // Try to get VT-specific stats via MBeanServer (avoids type issues)
      try {
        var mbs = ManagementFactory.getPlatformMBeanServer();
        var vtBeanName = new javax.management.ObjectName(
            "java.lang:type=VirtualThreadScheduler");

        if (mbs.isRegistered(vtBeanName)) {
          var parallelism = mbs.getAttribute(vtBeanName, "Parallelism");
          var poolSize = mbs.getAttribute(vtBeanName, "PoolSize");
          var queued = mbs.getAttribute(vtBeanName, "QueuedVirtualThreadCount");
          log.info("VT Scheduler: parallelism={}, poolSize={}, queued={}",
              parallelism, poolSize, queued);
          return;
        }
      } catch (Exception _) {
        // VT MXBean not available
      }

      // Fallback: log general thread stats
      log.info("Thread count: {} (daemon: {})",
          threadMXBean.getThreadCount(), threadMXBean.getDaemonThreadCount());
    } catch (Exception _) {
      // Ignore monitoring errors
    }
  }

  // =========================================================================
  // Lifecycle
  // =========================================================================

  private void shutdown() {
    if (vtExecutor != null) {
      vtExecutor.shutdown();
      try {
        if (!vtExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
          vtExecutor.shutdownNow();
        }
      } catch (InterruptedException _) {
        vtExecutor.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
    if (client != null) {
      client.close();
    }
  }

  // =========================================================================
  // Config Helpers
  // =========================================================================

  private static String getString(Map<String, Object> cfg, String key, String defaultValue) {
    Object v = cfg.get(key);
    if (v == null) {
      return defaultValue;
    }
    return v instanceof String s ? s : String.valueOf(v);
  }

  private static int getInt(Map<String, Object> cfg, String key, int defaultValue) {
    Object v = cfg.get(key);
    if (v instanceof Number n)
      return n.intValue();
    if (v instanceof String s) {
      try {
        return Integer.parseInt(s);
      } catch (NumberFormatException e) {
        /* ignore */ }
    }
    return defaultValue;
  }

  private static long getLong(Map<String, Object> cfg, String key, long defaultValue) {
    Object v = cfg.get(key);
    if (v instanceof Number n)
      return n.longValue();
    if (v instanceof String s) {
      try {
        return Long.parseLong(s);
      } catch (NumberFormatException e) {
        /* ignore */ }
    }
    return defaultValue;
  }

  private static double getDouble(Map<String, Object> cfg, String key, double defaultValue) {
    Object v = cfg.get(key);
    if (v instanceof Number n)
      return n.doubleValue();
    if (v instanceof String s) {
      try {
        return Double.parseDouble(s);
      } catch (NumberFormatException e) {
        /* ignore */ }
    }
    return defaultValue;
  }
}
