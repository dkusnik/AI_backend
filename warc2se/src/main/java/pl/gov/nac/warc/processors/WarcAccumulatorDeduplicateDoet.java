package pl.gov.nac.warc.processors;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.netpreserve.jwarc.WarcReader;
import org.netpreserve.jwarc.WarcRecord;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import pl.gov.nac.warc.reactive.Metrics;
import pl.gov.nac.warc.reactive.ReactiveInterfaces;
import pl.gov.nac.warc.records.Record;
import pl.gov.nac.warc.records.RecordBatch;
import pl.gov.nac.warc.records.warc.RecordWarc;
import pl.gov.nac.warc.records.warc.RecordWarcInFile;
import pl.gov.nac.warc.records.warc.RecordWarcUniversal;
import pl.gov.nac.warc.records.warc.RecordWet;
import pl.gov.nac.warc.utils.WarcIO;

/**
 * Accumulates WET records for DOET (Digest Ordered Extracted Text) output.
 * Uses RocksDB to store content with collision chain support.
 *
 * Storage schema:
 * - "digest" → conflict count (int, serialized)
 * - "digest;N" → DoetEntry (content + metadata)
 *
 * On completion, emits records sorted by (digest, content-length, bytewise
 * payload).
 */
public class WarcAccumulatorDeduplicateDoet implements ReactiveInterfaces.ReactiveProcessor<Object, Object> {

  private static final Logger log = LogManager.getLogger(WarcAccumulatorDeduplicateDoet.class);
  private static final String METRIC_KEY = "doet-deduplicator";
  private static final String HEADER_DIGEST = "WARC-Payload-Digest";
  private static final Pattern DATE_KEY_PATTERN = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");

  static {
    RocksDB.loadLibrary();
  }

  private Flow.Subscriber<? super Object> downstream;
  private RocksDB db;
  private Options options;
  private java.nio.file.Path dbPathObj;
  private boolean doetMerge = false;
  private boolean sortOnlyMode = false;
  private String bucketPrefix = "extract";
  private boolean perDayBuckets = false;
  private String lastSeenDigest = "";
  // ConcurrentHashMap: digestRegistry is written from concurrent virtual threads
  // via onNext(). HashMap would produce ConcurrentModificationException or silent
  // structural corruption under concurrent access.
  private final java.util.Map<String, DigestState> digestRegistry =
      new java.util.concurrent.ConcurrentHashMap<>();
  private final java.util.concurrent.atomic.AtomicLong sortOnlySeq = new java.util.concurrent.atomic.AtomicLong(0);
  // Guard against double-complete / reentrant onComplete calls.
  // Ensures downstream.onNext() is never called after downstream.onComplete().
  private final java.util.concurrent.atomic.AtomicBoolean completionStarted =
      new java.util.concurrent.atomic.AtomicBoolean(false);
  private boolean trackBaselineDate = false;
  private int missingThreshold = 3; // Default: 3 consecutive absences
  private String currentCrawlId = "unknown"; // Current crawl identifier
  private String deduplicateScope = "global"; // "global" (default) or "url"
  private static final ThreadLocal<java.security.MessageDigest> SHA256 = ThreadLocal.withInitial(() -> {
    try {
      return java.security.MessageDigest.getInstance("SHA-256");
    } catch (java.security.NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
  });

  /**
   * Tracks URI chain metadata for merge operations.
   * Stores the primary URI (first seen), current URI, chain length, and dates.
   *
   * Phase 2 additions: Temporal lifecycle tracking (first/last seen, missing
   * detection, status)
   */
  private static class DigestState {
    // Identity
    String digest;

    // URI Chain Tracking (Phase 1)
    String primaryUri;
    String currentUri;
    String previousUri;
    int chainLength;
    long contentLength;
    String baselineDate;

    // Temporal Lifecycle (Phase 2)
    java.time.Instant firstSeen; // First observation timestamp
    java.time.Instant lastSeen; // Most recent observation
    String lastSeenDate; // Keep for backward compat (WARC-Date string)
    int missingCount; // Consecutive missing crawls
    int revisitCount; // Number of times this content has been seen (starts at 1)
    boolean seenInCurrentBatch; // Batch processing flag
    String lastCrawlId; // Crawl identifier tracking
    String status; // "active" | "missing" | "deleted"

    // Merge support
    Object deferredRecord; // Record deferred from primary file (for base-only emission)
    Object recordToEmit; // Record to emit (with provenance already set)
    String provenance; // Provenance for this record

    // Source-tracking for robust URL-scope merge across split batches
    boolean seenPrimarySource;
    boolean seenSecondarySource;
    RecordWarcUniversal primaryRepresentative;
  }

  @SuppressWarnings("resource") // Options is closed in closeDb()
  @Override
  public void configure(Map<String, Object> cfg) {
    Metrics.setModuleHeader(METRIC_KEY, "DOET Deduplicator");

    String dbPath = (String) cfg.getOrDefault("rocksdb-path", "./tmp/db/dedup");
    this.primaryFilePattern = (String) cfg.get("primary-file");
    if (primaryFilePattern != null) {
      log.info("Primary file pattern: {}", primaryFilePattern);
    }

    if (cfg.containsKey("deduplicate-scope")) {
      this.deduplicateScope = cfg.get("deduplicate-scope").toString().toLowerCase();
      if (!deduplicateScope.equals("global") && !deduplicateScope.equals("url")
          && !deduplicateScope.equals("none") && !deduplicateScope.equals("sort-only")) {
        throw new IllegalArgumentException(
            "deduplicate-scope must be 'global', 'url', 'none', or 'sort-only', got: " + deduplicateScope);
      }
      this.sortOnlyMode = "sort-only".equals(deduplicateScope);
      if ("none".equals(deduplicateScope)) {
        log.info("Deduplicate scope: none (processor will be skipped in pipeline)");
        return; // Skip further initialization if disabled
      }
      log.info("Deduplicate scope: {}", deduplicateScope);
    }

    if (cfg.containsKey("bucket-prefix")) {
      String p = cfg.get("bucket-prefix").toString().trim();
      if (!p.isBlank()) {
        this.bucketPrefix = p;
      }
    }

    if (cfg.containsKey("per-day")) {
      this.perDayBuckets = Boolean.parseBoolean(cfg.get("per-day").toString());
    }

    if (cfg.containsKey("doet-merge") || cfg.containsKey("doetMerge")) {
      this.doetMerge = Boolean.parseBoolean(cfg.getOrDefault("doet-merge", cfg.get("doetMerge")).toString());
    }

    if (cfg.containsKey("track-baseline-date")) {
      this.trackBaselineDate = Boolean.parseBoolean(cfg.get("track-baseline-date").toString());
      log.info("Track baseline date: {}", trackBaselineDate);
    }

    // Temporal lifecycle configuration
    if (cfg.containsKey("missing-threshold")) {
      this.missingThreshold = Integer.parseInt(cfg.get("missing-threshold").toString());
    }

    if (cfg.containsKey("crawl-id")) {
      String crawlIdCfg = cfg.get("crawl-id").toString();
      if ("auto".equalsIgnoreCase(crawlIdCfg) || crawlIdCfg.isBlank()) {
        // Auto-generate from timestamp
        this.currentCrawlId = "crawl-" + java.time.Instant.now().toString();
      } else {
        this.currentCrawlId = crawlIdCfg;
      }
    } else {
      // Auto-generate if not specified
      this.currentCrawlId = "crawl-" + java.time.Instant.now().toString();
    }

    if (doetMerge) {
      log.info("Streaming Deduplication Mode Enabled (No RocksDB). Temporal tracking: threshold={}, crawl-id={}",
          missingThreshold, currentCrawlId);
      return;
    } else {
      log.info("Temporal tracking: threshold={}, crawl-id={}",
          missingThreshold, currentCrawlId);
    }

    try {
      dbPathObj = java.nio.file.Path.of(dbPath);

      // Always reset database for clean deduplication each run
      if (java.nio.file.Files.exists(dbPathObj)) {
        ensureSafeToClear(dbPathObj);
        log.info("Clearing existing RocksDB database: {}", dbPath);
        deleteDirectory(dbPathObj.toFile());
      }

      // Ensure directory exists
      java.nio.file.Files.createDirectories(dbPathObj);

      options = new Options().setCreateIfMissing(true);
      try {
        db = RocksDB.open(options, dbPath);
        log.info("Opened RocksDB at: {}", dbPath);
      } catch (Exception e) {
        options.close();
        throw e;
      }
    } catch (Exception e) {
      throw new IllegalStateException("Failed to initialize RocksDB: " + e, e);
    }
  }

  private void deleteDirectory(java.io.File directory) throws java.io.IOException {
    Path root = directory.toPath();
    if (!Files.exists(root)) {
      return;
    }
    try (var paths = Files.walk(root)) {
      List<Path> deleteOrder = paths.sorted(Comparator.reverseOrder()).toList();
      for (Path path : deleteOrder) {
        Files.deleteIfExists(path);
      }
    }
  }

  /**
   * Guard for the reset-at-open delete: only clear a directory that is empty
   * or carries RocksDB marker files. A mistyped rocksdb-path must not destroy
   * unrelated data.
   */
  private static void ensureSafeToClear(Path root) throws java.io.IOException {
    if (!Files.isDirectory(root)) {
      throw new java.io.IOException("rocksdb-path exists and is not a directory: " + root);
    }
    if (Files.exists(root.resolve("CURRENT")) || Files.exists(root.resolve("IDENTITY"))) {
      return;
    }
    try (var entries = Files.list(root)) {
      if (entries.findAny().isEmpty()) {
        return;
      }
    }
    throw new java.io.IOException(
        "Refusing to clear rocksdb-path " + root
            + ": directory is not empty and has no RocksDB CURRENT/IDENTITY marker;"
            + " use an empty or dedicated directory");
  }

  @Override
  public boolean isEnabled(Map<String, Object> cfg) {
    // Processor is always enabled, but behavior changes based on scope
    // When scope is "none", it acts as a passthrough
    return true;
  }

  @Override
  public List<Class<? extends Record>> acceptedInputTypes() {
    if (doetMerge) {
      return List.of(RecordBatch.class, RecordWet.class, RecordWarc.class, RecordWarcInFile.class,
          RecordWarcUniversal.class);
    }
    return List.of(RecordWet.class, RecordWarc.class, RecordWarcInFile.class,
        RecordWarcUniversal.class);
  }

  @Override
  public void subscribe(Flow.Subscriber<? super Object> subscriber) {
    this.downstream = subscriber;
  }

  @Override
  public void onSubscribe(Flow.Subscription subscription) {
    downstream.onSubscribe(subscription);
  }

  @Override
  public void onComplete() {
    if (!completionStarted.compareAndSet(false, true)) {
      return; // guard against reentrant / double-complete (RS §1.8)
    }

    try {
      // Passthrough mode - just complete downstream
      if ("none".equals(deduplicateScope)) {
        downstream.onComplete();
        return;
      }

      if (doetMerge) {
        // DOET ORDERING FIX: Emit all buffered records in sorted digest order
        emitSortedMergeRecords();

        // Detect missing content before completing
        detectMissingContent();
        downstream.onComplete();
        return;
      }
      if (sortOnlyMode) {
        emitSortOnlyRecords();
        return;
      }
      try {
        emitCrawlSeriesDeduplicatedRecords();
      } catch (Exception e) {
        log.error("Error during emission: {}", e.getMessage(), e);
        Metrics.inc(METRIC_KEY, "emission-errors");
      } finally {
        downstream.onComplete();
      }
    } finally {
      closeDb();
    }
  }

  @Override
  public void onNext(Object item) {
    try {
      // Handle batched records (merge mode with type negotiation)
      if (item instanceof RecordBatch batch) {
        handleRecordBatch(batch);
        return;
      }

      // In merge mode, ONLY accept RecordBatch
      if (doetMerge) {
        log.error("Merge mode requires RecordBatch input, got: {}. Check type negotiation.",
            item.getClass().getSimpleName());
        Metrics.inc(METRIC_KEY, "type-mismatch-error");
        return;
      }

      // Passthrough mode when deduplication is disabled
      // Records are emitted in order of occurrence (ascending)
      if (sortOnlyMode) {
        handleSortOnly(item);
        return;
      }

      if ("none".equals(deduplicateScope)) {
        // Add nac-deduplicated header to indicate no deduplication
        if (item instanceof RecordWarcUniversal rwu) {
          rwu.headers().put("nac-deduplicated", "none");
        }
        downstream.onNext(item);
        Metrics.inc(METRIC_KEY, "passthrough");
        return;
      }

      if (item instanceof RecordWarcUniversal rwu) {
        String warcType = rwu.warcType();
        if ("warcinfo".equalsIgnoreCase(warcType) || "metadata".equalsIgnoreCase(warcType)) {
          Metrics.inc(METRIC_KEY, "dropped-metadata");
          return;
        }
      }

      RecordInfo info = extractInfo(item);
      if (info == null || info.payload == null || info.payload.length == 0) {
        Metrics.inc(METRIC_KEY, "empty-dropped");
        return;
      }

      // Normal deduplication mode (non-merge)
      // 1. Get collision count for this digest
      String scopedDigestKey = scopedDigestKey(info);
      byte[] countKey = scopedDigestKey.getBytes(StandardCharsets.UTF_8);
      byte[] countVal = db.get(countKey);
      int collisionCount = countVal == null ? 0 : java.nio.ByteBuffer.wrap(countVal).getInt();

      // 2. Check each content in the chain for exact match
      byte[] matchedKey = findMatch(info, collisionCount);
      // In simple-dedup mode (no primary-file pattern) every first-seen record is
      // treated as "primary" so it gets flag 0x01 and is emitted as NAC-Merge-Result:
      // primary.  File distinction only applies when a primary-file pattern is set.
      boolean fromPrimary = (primaryFilePattern == null || primaryFilePattern.isBlank())
          || isFromPrimary(item);

      if (matchedKey != null) {
        // Content exists - update provenance if needed
        byte[] existingRaw = db.get(matchedKey);
        byte existingFlag = existingRaw[0];
        byte newFlag = (byte) (existingFlag | (fromPrimary ? 0x01 : 0x02));

        if (newFlag != existingFlag) {
          existingRaw[0] = newFlag;
          db.put(matchedKey, existingRaw);
          Metrics.inc(METRIC_KEY, "provenance-updates");
        }

        byte[] updated = updateRepresentativeIfShorter(existingRaw, info);
        if (updated != existingRaw) {
          db.put(matchedKey, updated);
          Metrics.inc(METRIC_KEY, "shorter-uri-updates");
        }

        Metrics.inc(METRIC_KEY, "duplicates");
        return;
      }

      // 3. Store new unique content
      storeUnique(info, scopedDigestKey, collisionCount, fromPrimary);
      Metrics.inc(METRIC_KEY, "unique");

    } catch (Exception e) {
      log.error("Error processing: {}", e.getMessage(), e);
      Metrics.inc(METRIC_KEY, "errors");
    }
  }

  private void handleSortOnly(Object item) throws Exception {
    if (item instanceof RecordWarcUniversal rwu) {
      String warcType = rwu.warcType();
      if ("warcinfo".equalsIgnoreCase(warcType) || "metadata".equalsIgnoreCase(warcType)) {
        Metrics.inc(METRIC_KEY, "dropped-metadata");
        return;
      }
    }

    RecordInfo info = extractInfo(item);
    if (info == null || info.payload == null || info.payload.length == 0) {
      Metrics.inc(METRIC_KEY, "empty-dropped");
      return;
    }

    String dateBucket = bucketFromDate(info.date);
    String sourceName = bucketName(dateBucket);

    Map<String, String> headers = new java.util.LinkedHashMap<>(info.headers);
    headers.put("WARC-Target-URI", info.uri);
    headers.put("WARC-Date", info.date);
    headers.put("WARC-Type", "conversion");
    headers.put(HEADER_DIGEST, info.digest);
    headers.put("Content-Type", "text/plain; charset=utf-8");
    headers.put("X-Source-Warc", sourceName);
    if (info.crawlId != null && !info.crawlId.isBlank()) {
      headers.put("X-NAC-Crawl-ID", info.crawlId);
    }

    RecordWarcUniversal outputRec = new RecordWarcUniversal("conversion", headers, info.payload);
    byte[] warcBytes = WarcIO.toWarcBytes(outputRec);

    String sortUri = info.uri != null ? info.uri : "";
    // NUL keeps a URI prefix before its longer variants; sequence preserves duplicates.
    String key = "s|" + sourceName + "|" + info.digest + "|" + sortUri + '\0'
        + String.format("%020d", sortOnlySeq.getAndIncrement());
    db.put(key.getBytes(StandardCharsets.UTF_8), warcBytes);
    Metrics.inc(METRIC_KEY, "sort-only-stored");
  }

  private static String bucketFromDate(String date) {
    if (date == null || date.length() < 10) {
      return "unknown";
    }
    // YYYY-MM-DD...
    return date.substring(0, 4) + date.substring(5, 7) + date.substring(8, 10);
  }

  private static final class CrawlRange {
    final String startDay; // yyyy-MM-dd
    String endDay; // yyyy-MM-dd

    CrawlRange(String day) {
      this.startDay = day;
      this.endDay = day;
    }
  }

  private static final class EmissionCandidate {
    RecordWarcUniversal record;
    String crawlStartDay;
    String crawlStartYmd;
    String crawlFirstDay;
    String crawlLastDay;
    String firstSeen;
    String lastSeen;

    EmissionCandidate(RecordWarcUniversal record) {
      this.record = record;
    }
  }

  private void emitCrawlSeriesDeduplicatedRecords() throws Exception {
    java.util.Set<String> dayBuckets = new java.util.HashSet<>();

    try (org.rocksdb.RocksIterator it = db.newIterator()) {
      for (it.seekToFirst(); it.isValid(); it.next()) {
        String keyStr = new String(it.key(), StandardCharsets.UTF_8);
        if (!keyStr.contains(";")) {
          continue;
        }
        String day = extractDayFromDataKey(keyStr);
        if (day != null) {
          dayBuckets.add(day);
        }
      }
    }

    java.util.Map<String, CrawlRange> dayToRange = buildDayToCrawlRange(dayBuckets);
    java.util.Map<String, EmissionCandidate> deduped = new java.util.HashMap<>();

    try (org.rocksdb.RocksIterator it = db.newIterator()) {
      for (it.seekToFirst(); it.isValid(); it.next()) {
        String keyStr = new String(it.key(), StandardCharsets.UTF_8);
        if (!keyStr.contains(";")) {
          continue;
        }

        byte[] value = it.value();
        if (value == null || value.length < 2) {
          continue;
        }

        String day = extractDayFromDataKey(keyStr);
        if (day == null) {
          continue;
        }
        CrawlRange range = dayToRange.get(day);
        if (range == null) {
          continue;
        }

        byte[] warcRaw = java.util.Arrays.copyOfRange(value, 1, value.length);
        RecordWarcUniversal rec = RecordWarcUniversal.fromRaw(warcRaw);
        String digest = firstNonBlank(rec.headers().get(HEADER_DIGEST), rec.headers().get("warc-payload-digest"));
        String uri = firstNonBlank(rec.headers().get("WARC-Target-URI"), rec.headers().get("warc-target-uri"),
            rec.targetUri(), "");
        String recDate = firstNonBlank(rec.headers().get("WARC-Date"), rec.headers().get("warc-date"), day);

        String crawlStartYmd = range.startDay.replace("-", "");
        String dedupKey = buildCrawlDedupKey(crawlStartYmd, digest, uri);

        EmissionCandidate existing = deduped.get(dedupKey);
        if (existing == null) {
          EmissionCandidate candidate = new EmissionCandidate(rec);
          candidate.crawlStartDay = range.startDay;
          candidate.crawlStartYmd = crawlStartYmd;
          candidate.crawlFirstDay = range.startDay;
          candidate.crawlLastDay = range.endDay;
          candidate.firstSeen = recDate;
          candidate.lastSeen = recDate;
          deduped.put(dedupKey, candidate);
        } else {
          if ("global".equals(deduplicateScope)) {
            String existingUri = firstNonBlank(existing.record.headers().get("WARC-Target-URI"),
                existing.record.headers().get("warc-target-uri"), existing.record.targetUri());
            if (isBetterRepresentativeUri(uri, existingUri)) {
              existing.record = rec;
            }
          }
          if (existing.firstSeen.compareTo(recDate) > 0) {
            existing.firstSeen = recDate;
          }
          if (existing.lastSeen.compareTo(recDate) < 0) {
            existing.lastSeen = recDate;
          }
        }
      }
    }

    java.util.List<EmissionCandidate> sorted = new java.util.ArrayList<>(deduped.values());
    sorted.sort((a, b) -> {
      int cmpCrawl = a.crawlStartYmd.compareTo(b.crawlStartYmd);
      if (cmpCrawl != 0)
        return cmpCrawl;
      String da = firstNonBlank(a.record.headers().get(HEADER_DIGEST), a.record.headers().get("warc-payload-digest"),
          "");
      String db = firstNonBlank(b.record.headers().get(HEADER_DIGEST), b.record.headers().get("warc-payload-digest"),
          "");
      int cmpDigest = da.compareTo(db);
      if (cmpDigest != 0)
        return cmpDigest;
      String ua = firstNonBlank(a.record.headers().get("WARC-Target-URI"), a.record.headers().get("warc-target-uri"),
          a.record.targetUri(), "");
      String ub = firstNonBlank(b.record.headers().get("WARC-Target-URI"), b.record.headers().get("warc-target-uri"),
          b.record.targetUri(), "");
      return ua.compareTo(ub);
    });

    for (EmissionCandidate candidate : sorted) {
      RecordWarcUniversal rec = candidate.record;
      rec.headers().put("X-Source-Warc", bucketName(candidate.crawlStartYmd));
      rec.headers().put("X-NAC-Crawl-ID", candidate.crawlStartDay);
      rec.headers().put("x-nac-crawl-first-date", candidate.crawlFirstDay);
      rec.headers().put("x-nac-crawl-last-date", candidate.crawlLastDay);
      rec.headers().put("X-NAC-First-Seen", candidate.firstSeen);
      rec.headers().put("X-NAC-Last-Seen", candidate.lastSeen);
      rec.headers().put("nac-deduplicated", deduplicateScope);
      downstream.onNext(rec);
      Metrics.inc(METRIC_KEY, "emitted");
    }

    log.info("Emission complete. crawls={}, emitted={}", dayToRange.values().stream().map(r -> r.startDay).distinct().count(),
        sorted.size());
  }

  private java.util.Map<String, CrawlRange> buildDayToCrawlRange(java.util.Set<String> dayBuckets) {
    java.util.List<LocalDate> days = dayBuckets.stream()
        .map(d -> {
          try {
            return LocalDate.parse(d);
          } catch (Exception e) {
            return null;
          }
        })
        .filter(java.util.Objects::nonNull)
        .sorted()
        .toList();

    java.util.Map<String, CrawlRange> out = new java.util.HashMap<>();
    if (days.isEmpty()) {
      return out;
    }

    if (perDayBuckets) {
      for (LocalDate day : days) {
        String value = day.toString();
        out.put(value, new CrawlRange(value));
      }
      return out;
    }

    LocalDate start = days.get(0);
    LocalDate prev = start;
    for (int i = 1; i < days.size(); i++) {
      LocalDate cur = days.get(i);
      if (ChronoUnit.DAYS.between(prev, cur) > 1) {
        fillRange(out, start, prev);
        start = cur;
      }
      prev = cur;
    }
    fillRange(out, start, prev);
    return out;
  }

  private void fillRange(java.util.Map<String, CrawlRange> out, LocalDate start, LocalDate end) {
    CrawlRange range = new CrawlRange(start.toString());
    range.endDay = end.toString();
    LocalDate cur = start;
    while (!cur.isAfter(end)) {
      out.put(cur.toString(), range);
      cur = cur.plusDays(1);
    }
  }

  private String extractDayFromDataKey(String key) {
    int sep = key.indexOf('|');
    if (sep <= 0) {
      return null;
    }
    String day = key.substring(0, sep);
    return DATE_KEY_PATTERN.matcher(day).matches() ? day : null;
  }

  private String buildCrawlDedupKey(String crawlStartYmd, String digest, String uri) {
    String d = digest != null ? digest : "";
    String u = uri != null ? uri : "";
    if ("url".equals(deduplicateScope)) {
      return crawlStartYmd + "|" + d + "|" + u;
    }
    return crawlStartYmd + "|" + d;
  }

  private String bucketName(String ymd) {
    return perDayBuckets ? ymd : bucketPrefix + "-" + ymd;
  }

  private void emitSortOnlyRecords() {
    try {
      try (org.rocksdb.RocksIterator it = db.newIterator()) {
        for (it.seekToFirst(); it.isValid(); it.next()) {
          byte[] key = it.key();
          String keyStr = new String(key, StandardCharsets.UTF_8);
          if (!keyStr.startsWith("s|")) {
            continue;
          }

          byte[] value = it.value();
          if (value == null || value.length == 0) {
            continue;
          }
          RecordWarcUniversal rec = RecordWarcUniversal.fromRaw(value);
          downstream.onNext(rec);
          Metrics.inc(METRIC_KEY, "emitted");
        }
      }
    } catch (Exception e) {
      log.error("Error during sort-only emission: {}", e.getMessage(), e);
      Metrics.inc(METRIC_KEY, "emission-errors");
    } finally {
      closeDb();
      downstream.onComplete();
    }
  }

  @Override
  public void onError(Throwable throwable) {
    if (!completionStarted.compareAndSet(false, true)) {
      return;
    }
    try {
      downstream.onError(throwable);
    } finally {
      closeDb();
    }
  }

  private String primaryFilePattern;

  /**
   * Generate registry key based on deduplication scope.
   * - Merge mode: key always includes URL so URL-distinct captures remain
   * addressable in baseline output.
   * - Non-merge mode: global keeps digest-only key; URL mode uses digest+URL.
   */
  private String getRegistryKey(String digest, String uri) {
    if (doetMerge) {
      String safeUri = uri != null ? uri : "";
      return digest + "|" + safeUri;
    }
    if ("url".equals(deduplicateScope)) {
      return digest + "|" + uri;
    }
    return digest; // global mode (default)
  }

  /**
   * Process a RecordBatch atomically.
   * All records in the batch share the same digest.
   * This method determines provenance based on the presence of primary vs scan
   * records.
   *
   * For URL scope: processes each unique URL separately within the batch
   * For Global scope: processes the batch as a single unit
   */
  private void handleRecordBatch(RecordBatch batch) {
    String digest = batch.sharedDigest();
    int batchSize = batch.size();

    log.debug("Processing RecordBatch: digest={}, count={}, dateRange=[{}, {}]",
        digest.substring(0, Math.min(20, digest.length())),
        batchSize, batch.minDate(), batch.maxDate());

    // URL scope: process each URL separately within the batch
    if ("url".equals(deduplicateScope)) {
      handleRecordBatchPerUrl(batch, true);
      return;
    }

    // Global merge also preserves URL-distinct records (pywb-safe behavior).
    // Non-merge continues historical global behavior.
    if (doetMerge) {
      handleRecordBatchPerUrl(batch, false);
      return;
    }

    // Global scope (non-merge): process batch as a single unit
    handleRecordBatchGlobalScope(batch);
  }

  /**
   * Process RecordBatch per URL group.
   * Used by URL scope and merge/global mode to preserve URL-distinct captures.
   */
  private void handleRecordBatchPerUrl(RecordBatch batch, boolean urlScopeMode) {
    String digest = batch.sharedDigest();

    // Group records by URL
    java.util.Map<String, java.util.List<RecordWarcUniversal>> byUrl = new java.util.HashMap<>();
    for (RecordWarcUniversal record : batch.records()) {
      String uri = record.targetUri();
      if (uri == null) {
        uri = "";
      }
      byUrl.computeIfAbsent(uri, k -> new java.util.ArrayList<>()).add(record);
    }

    // Process each URL group separately
    for (java.util.Map.Entry<String, java.util.List<RecordWarcUniversal>> entry : byUrl.entrySet()) {
      String uri = entry.getKey();
      java.util.List<RecordWarcUniversal> urlRecords = entry.getValue();
      final String normalizedUri = uri != null ? uri : "";

      // Determine source composition in the current batch for this URL
      boolean hasPrimaryInBatch = false;
      boolean hasScanInBatch = false;
      RecordWarcUniversal primaryInBatch = null;
      RecordWarcUniversal anyInBatch = null;
      for (RecordWarcUniversal record : urlRecords) {
        if (anyInBatch == null) {
          anyInBatch = record;
        }
        if (isFromPrimary(record)) {
          hasPrimaryInBatch = true;
          if (primaryInBatch == null) {
            primaryInBatch = record;
          }
        } else {
          hasScanInBatch = true;
        }
      }

      // Create/update state for this digest+URL combination.
      // Keep source flags in state so provenance is stable even if producer splits
      // primary/secondary across multiple batches.
      String registryKey = getRegistryKey(digest, normalizedUri);
      DigestState state = digestRegistry.get(registryKey);
      if (state == null) {
        state = new DigestState();
        state.digest = digest;
        state.primaryUri = normalizedUri;
        state.currentUri = normalizedUri;
        state.chainLength = 0;
        state.status = "active";
        state.missingCount = 0; // Initialize temporal fields
        state.lastCrawlId = currentCrawlId;
        digestRegistry.put(registryKey, state);
      }

      state.seenPrimarySource = state.seenPrimarySource || hasPrimaryInBatch;
      state.seenSecondarySource = state.seenSecondarySource || hasScanInBatch;
      if (primaryInBatch != null && state.primaryRepresentative == null) {
        state.primaryRepresentative = primaryInBatch;
      }

      String provenance;
      if (state.seenPrimarySource && state.seenSecondarySource) {
        provenance = "merged";
      } else if (state.seenPrimarySource) {
        provenance = "base-only";
      } else {
        provenance = "new";
      }

      // Select representative for this URL with stable primary preference when merged
      RecordWarcUniversal representative;
      if ("merged".equals(provenance)) {
        representative = state.primaryRepresentative != null ? state.primaryRepresentative
            : (primaryInBatch != null ? primaryInBatch : anyInBatch);
      } else if ("base-only".equals(provenance)) {
        representative = primaryInBatch != null ? primaryInBatch
            : (state.primaryRepresentative != null ? state.primaryRepresentative : anyInBatch);
      } else {
        representative = anyInBatch;
      }
      if (representative != null) {
        state.contentLength = representative.rawBytes().length;
      }

      if (state.firstSeen == null || batch.minDate().isBefore(state.firstSeen)) {
        state.firstSeen = batch.minDate();
      }
      if (state.lastSeen == null || batch.maxDate().isAfter(state.lastSeen)) {
        state.lastSeen = batch.maxDate();
      }
      state.revisitCount += urlRecords.size();
      state.missingCount = 0; // Initialize for temporal headers
      state.lastCrawlId = currentCrawlId; // Track crawl ID
      state.provenance = provenance;
      state.recordToEmit = representative;
      state.seenInCurrentBatch = true;

      Metrics.inc(METRIC_KEY, provenance);
    }

    Metrics.inc(METRIC_KEY, urlScopeMode ? "batches-processed-url-scope" : "batches-processed-global-url-preserving");
    log.debug("Batch processed ({}): digest={}, URLs={}",
        urlScopeMode ? "URL scope" : "global url-preserving",
        digest.substring(0, Math.min(20, digest.length())), byUrl.size());
  }

  /**
   * Process RecordBatch for global scope - entire batch as one unit.
   */
  private void handleRecordBatchGlobalScope(RecordBatch batch) {
    for (RecordWarcUniversal record : batch.records()) {
      onNext(record);
    }
    Metrics.inc(METRIC_KEY, "batches-processed");
  }

  private boolean isFromPrimary(Object item) {
    if (primaryFilePattern == null || primaryFilePattern.isBlank())
      return false;
    String source = null;
    String sourceType = null;

    if (item instanceof RecordWarcUniversal rwu) {
      source = rwu.headers().get("X-Source-Warc");
      if (source == null)
        source = rwu.headers().get("x-source-warc");
      sourceType = rwu.headers().get("X-Source-Type");
      if (sourceType == null)
        sourceType = rwu.headers().get("x-source-type");
    } else if (item instanceof RecordWet rec) {
      source = rec.headers().get("X-Source-Warc");
      if (source == null)
        source = rec.headers().get("x-source-warc");
      sourceType = rec.headers().get("X-Source-Type");
      if (sourceType == null)
        sourceType = rec.headers().get("x-source-type");
    }

    // Robust provenance: explicit metadata takes precedence
    if (sourceType != null) {
      boolean isBaseline = "baseline".equalsIgnoreCase(sourceType);
      log.info("isFromPrimary (metadata): source={}, sourceType={}, isBaseline={}", source, sourceType, isBaseline);
      return isBaseline;
    }

    // Fallback: pattern matching for backward compatibility
    if (source == null)
      return false;

    boolean result = source.matches(primaryFilePattern) || source.contains(primaryFilePattern);
    // Note: This INFO logging is intentionally kept as it fixes a timing-dependent
    // bug (heisenbug)
    // The logging ensures proper evaluation of the pattern matching logic
    log.info("isFromPrimary (pattern): source={}, pattern={}, match={}", source, primaryFilePattern, result);
    return result;
  }

  private record RecordInfo(String digest, String uri, String date, String crawlId, byte[] payload,
      Map<String, String> headers) {
  }

  private RecordInfo extractInfo(Object item) throws java.io.IOException {
    String digest = null;
    String uri = null;
    String date = null;
    String crawlId;
    byte[] payload = null;
    Map<String, String> headers = new java.util.HashMap<>();

    log.debug("extractInfo: item type = {}", item.getClass().getSimpleName());

    if (item instanceof RecordWet rec) {
      digest = rec.digest();
      uri = rec.targetUri();
      date = rec.date();
      payload = rec.text() != null ? rec.text().getBytes(StandardCharsets.UTF_8) : rec.bodyBytes();
      headers.putAll(rec.headers());
      log.debug("  RecordWet: digest={}, uri={}", digest, uri);
    } else if (item instanceof RecordWarcUniversal rwu) {
      digest = rwu.headers().get(HEADER_DIGEST);
      if (digest == null)
        digest = rwu.headers().get("warc-payload-digest");
      if (digest == null)
        digest = rwu.headers().get("WARC-Block-Digest");
      if (digest == null)
        digest = rwu.headers().get("warc-block-digest");

      // Extract URI from headers with case-insensitive fallback
      uri = rwu.headers().get("WARC-Target-URI");
      if (uri == null)
        uri = rwu.headers().get("warc-target-uri");
      if (uri == null)
        uri = rwu.targetUri(); // Fallback to method

      // Extract date from headers with case-insensitive fallback
      date = rwu.headers().get("WARC-Date");
      if (date == null)
        date = rwu.headers().get("warc-date");
      if (date == null)
        date = rwu.warcDate(); // Fallback to method

      payload = WarcIO.getPayload(rwu.rawBytes());
      headers.putAll(rwu.headers());
      log.debug("  RecordWarcUniversal: digest={}, uri={}, payloadLen={}", digest, uri,
          (payload != null ? payload.length : 0));
    }

    if (payload == null || digest == null) {
      byte[] raw = (item instanceof RecordWarc) ? ((RecordWarc) item).rawBytes()
          : (item instanceof RecordWarcInFile rwf) ? rwf.rawBytes()
              : (item instanceof RecordWarcUniversal rwu) ? rwu.rawBytes() : null;

      if (raw != null) {
        try (WarcReader reader = new WarcReader(new java.io.ByteArrayInputStream(raw))) {
          WarcRecord record = reader.next().orElse(null);
          if (record != null) {
            digest = record.headers().first(HEADER_DIGEST)
                .or(() -> record.headers().first("WARC-Block-Digest"))
                .orElse(null);
            uri = record.headers().first("WARC-Target-URI").orElse(null);
            date = String.valueOf(record.date());
            payload = record.body().stream().readAllBytes();
            record.headers().map().forEach((k, v) -> {
              if (!v.isEmpty())
                headers.put(k, v.get(0));
            });
            log.debug("  jwarc extracted: digest={}, uri={}", digest, uri);
          }
        } catch (Exception e) {
          log.warn("  jwarc extraction failed: {}", e.getMessage(), e);
          return null;
        }
      }
    }

    if (digest == null && payload != null) {
      java.security.MessageDigest md = SHA256.get();
      md.reset();
      digest = "sha256:" + java.util.HexFormat.of().formatHex(md.digest(payload));
    }

    crawlId = extractCrawlId(headers, date);
    return (digest != null) ? new RecordInfo(digest, uri, date, crawlId, payload, headers) : null;
  }

  private byte[] findMatch(RecordInfo info, int collisionCount) throws org.rocksdb.RocksDBException {
    String scopedDigestKey = scopedDigestKey(info);
    for (int i = 0; i < collisionCount; i++) {
      byte[] dataKey = (scopedDigestKey + ";" + i).getBytes(StandardCharsets.UTF_8);
      byte[] existingRaw = db.get(dataKey);
      if (existingRaw != null && existingRaw.length > 1) {
        byte[] existingPayload = WarcIO.getPayload(java.util.Arrays.copyOfRange(existingRaw, 1, existingRaw.length));
        if (java.util.Arrays.equals(info.payload, existingPayload)) {
          return dataKey;
        }
      }
    }
    return null;
  }

  private void storeUnique(RecordInfo info, String scopedDigestKey, int collisionCount, boolean fromPrimary)
      throws org.rocksdb.RocksDBException, java.io.IOException {
    byte[] newDataKey = (scopedDigestKey + ";" + collisionCount).getBytes(StandardCharsets.UTF_8);

    Map<String, String> headers = new java.util.LinkedHashMap<>(info.headers);
    headers.put("WARC-Target-URI", info.uri);
    headers.put("WARC-Date", info.date);
    headers.put("X-NAC-Crawl-ID", info.crawlId);
    headers.put("X-Source-Warc", info.crawlId);
    headers.put("WARC-Type", "conversion");
    headers.put(HEADER_DIGEST, info.digest);
    headers.put("Content-Type", "text/plain; charset=utf-8");
    RecordWarcUniversal outputRec = new RecordWarcUniversal("conversion", headers, info.payload);

    byte[] warcBytes = WarcIO.toWarcBytes(outputRec);
    byte[] value = new byte[warcBytes.length + 1];
    value[0] = (byte) (fromPrimary ? 0x01 : 0x02);
    System.arraycopy(warcBytes, 0, value, 1, warcBytes.length);

    db.put(newDataKey, value);

    byte[] countKey = scopedDigestKey.getBytes(StandardCharsets.UTF_8);
    byte[] newCountVal = new byte[4];
    java.nio.ByteBuffer.wrap(newCountVal).putInt(collisionCount + 1);
    db.put(countKey, newCountVal);
  }

  private String scopedDigestKey(RecordInfo info) {
    String crawl = (info.crawlId == null || info.crawlId.isBlank()) ? "unknown" : info.crawlId;
    return crawl + "|" + info.digest;
  }

  private static String extractCrawlId(Map<String, String> headers, String date) {
    String crawlId = firstNonBlank(
        headers.get("X-NAC-Crawl-ID"),
        headers.get("x-nac-crawl-id"),
        headers.get("NAC-Crawl-ID"),
        headers.get("nac-crawl-id"));
    if (crawlId != null) {
      return crawlId.trim();
    }
    if (date != null && date.length() >= 10) {
      return date.substring(0, 10);
    }
    return "unknown";
  }

  private static String firstNonBlank(String... values) {
    for (String v : values) {
      if (v != null && !v.isBlank()) {
        return v;
      }
    }
    return null;
  }

  private byte[] updateRepresentativeIfShorter(byte[] existingRaw, RecordInfo incoming)
      throws java.io.IOException {
    if (incoming.uri == null || incoming.uri.isBlank()) {
      return existingRaw;
    }
    byte[] existingWarcRaw = java.util.Arrays.copyOfRange(existingRaw, 1, existingRaw.length);
    RecordWarcUniversal existing = RecordWarcUniversal.fromRaw(existingWarcRaw);
    String existingUri = existing.targetUri();
    if (existingUri == null || existingUri.isBlank()) {
      existingUri = existing.headers().get("WARC-Target-URI");
    }
    if (!isBetterRepresentativeUri(incoming.uri, existingUri)) {
      return existingRaw;
    }

    Map<String, String> headers = new java.util.LinkedHashMap<>(incoming.headers);
    headers.put("WARC-Target-URI", incoming.uri);
    headers.put("WARC-Date", incoming.date);
    headers.put("X-NAC-Crawl-ID", incoming.crawlId);
    headers.put("X-Source-Warc", incoming.crawlId);
    headers.put("WARC-Type", "conversion");
    headers.put(HEADER_DIGEST, incoming.digest);
    headers.put("Content-Type", "text/plain; charset=utf-8");

    RecordWarcUniversal replacement = new RecordWarcUniversal("conversion", headers, incoming.payload);
    byte[] replacementWarcRaw = WarcIO.toWarcBytes(replacement);
    byte[] updated = new byte[replacementWarcRaw.length + 1];
    updated[0] = existingRaw[0];
    System.arraycopy(replacementWarcRaw, 0, updated, 1, replacementWarcRaw.length);
    return updated;
  }

  private static boolean isBetterRepresentativeUri(String candidate, String existing) {
    if (candidate == null || candidate.isBlank()) {
      return false;
    }
    if (existing == null || existing.isBlank()) {
      return true;
    }
    if (candidate.length() != existing.length()) {
      return candidate.length() < existing.length();
    }
    return candidate.compareTo(existing) < 0;
  }

  // =========================================================================
  // Temporal Lifecycle Helper Methods
  // =========================================================================

  /**
   * Emit records from primary (old baseline) that never appeared in secondary
   * (new scan).
   * These are marked as "base-only" records.
   */
  /**
   * Emit all buffered merge records in sorted digest order (DOET format).
   * This ensures dual outputs (base + diff) maintain proper digest ordering.
   *
   * Records are buffered during onNext() and emitted here in sorted order to
   * fix DOET ordering violations (Task #45).
   */
  private void emitSortedMergeRecords() {
    log.info("Emitting {} buffered merge records in deterministic merge order", digestRegistry.size());

    // Collect all states with records to emit
    java.util.List<DigestState> statesToEmit = new java.util.ArrayList<>();
    for (DigestState state : digestRegistry.values()) {
      if (state.recordToEmit != null || state.deferredRecord != null) {
        statesToEmit.add(state);
      }
    }

    // Sort order:
    // - URL scope: url, digest, digest length, payload string
    // - Global scope: digest, digest length, payload string
    statesToEmit.sort((a, b) -> {
      if ("url".equals(deduplicateScope)) {
        String uriA = getSortUri(a);
        String uriB = getSortUri(b);
        int cmpUri = uriA.compareTo(uriB);
        if (cmpUri != 0) {
          return cmpUri;
        }
      }

      String digestA = a.digest != null ? a.digest : "";
      String digestB = b.digest != null ? b.digest : "";
      int cmpDigest = digestA.compareTo(digestB);
      if (cmpDigest != 0) {
        return cmpDigest;
      }

      int cmpDigestLen = Integer.compare(digestA.length(), digestB.length());
      if (cmpDigestLen != 0) {
        return cmpDigestLen;
      }

      String payloadA = getSortPayload(a);
      String payloadB = getSortPayload(b);
      int cmpPayload = payloadA.compareTo(payloadB);
      if (cmpPayload != 0) {
        return cmpPayload;
      }

      String uriA = getSortUri(a);
      String uriB = getSortUri(b);
      return uriA.compareTo(uriB);
    });

    // Emit in sorted order
    int emittedCount = 0;
    java.util.Map<String, Integer> provenanceCounts = new java.util.HashMap<>();

    for (DigestState state : statesToEmit) {
      Object recordToEmit = state.recordToEmit != null ? state.recordToEmit : state.deferredRecord;
      String provenance = state.provenance != null ? state.provenance : "base-only";

      if (recordToEmit != null) {
        // Ensure NAC-Merge-Result header is set before emission
        if (recordToEmit instanceof RecordWarcUniversal rwu) {
          rwu.headers().put("NAC-Merge-Result", provenance);
          rwu.headers().put("nac-deduplicated", deduplicateScope);

          // Add temporal lifecycle headers
          if (state.firstSeen != null) {
            rwu.headers().put("X-NAC-First-Seen", state.firstSeen.toString());
            rwu.headers().put("X-NAC-Last-Seen", state.lastSeen.toString());
            rwu.headers().put("X-NAC-Missing-Count", String.valueOf(state.missingCount));
            rwu.headers().put("X-NAC-Record-Revisit-Count", String.valueOf(state.revisitCount));
            rwu.headers().put("X-NAC-Status", state.status);
            rwu.headers().put("X-NAC-Crawl-ID", state.lastCrawlId);
          }
        }
        downstream.onNext(recordToEmit);
        Metrics.inc(METRIC_KEY, "emitted");
        provenanceCounts.merge(provenance, 1, Integer::sum);
        emittedCount++;
      }
    }

    log.info("Emitted {} records in deterministic merge order", emittedCount);
    log.info("Merge provenance breakdown:");
    provenanceCounts.entrySet().stream()
        .sorted(java.util.Map.Entry.comparingByKey())
        .forEach(e -> log.info("  {}: {} records", e.getKey(), e.getValue()));
  }

  private String getSortUri(DigestState state) {
    Object record = state.recordToEmit != null ? state.recordToEmit : state.deferredRecord;
    if (record instanceof RecordWarcUniversal rwu) {
      String uri = rwu.targetUri();
      if (uri != null) {
        return uri;
      }
    }
    if (state.currentUri != null) {
      return state.currentUri;
    }
    if (state.primaryUri != null) {
      return state.primaryUri;
    }
    return "";
  }

  private String getSortPayload(DigestState state) {
    Object record = state.recordToEmit != null ? state.recordToEmit : state.deferredRecord;
    if (record instanceof RecordWarcUniversal rwu && rwu.rawBytes() != null) {
      return new String(rwu.rawBytes(), StandardCharsets.UTF_8);
    }
    return "";
  }

  /**
   * Detect missing content and emit metadata-only records to baseline.
   * Called at the end of each batch processing.
   */
  private void detectMissingContent() {
    int missingDetected = 0;
    int missingThresholdReached = 0;

    for (DigestState state : digestRegistry.values()) {
      if (!state.seenInCurrentBatch && "active".equals(state.status)) {
        // Content not seen in current batch - increment missing counter
        state.missingCount++;
        state.lastCrawlId = currentCrawlId;
        missingDetected++;

        if (state.missingCount >= missingThreshold) {
          // Threshold reached - mark as missing
          state.status = "missing";
          missingThresholdReached++;
          log.warn("Content missing for {} crawls (threshold reached): digest={}, uri={}",
              state.missingCount, state.digest, state.primaryUri);

          // Emit metadata-only record with updated missing_count
          // This preserves history in baseline
          RecordWarcUniversal missingRecord = createMissingRecord(state);
          downstream.onNext(missingRecord);
          Metrics.inc(METRIC_KEY, "missing-detected");
        } else {
          // Not yet at threshold - just log and emit updated record
          log.info("Content missing (count {}/{}): digest={}, uri={}",
              state.missingCount, missingThreshold, state.digest, state.primaryUri);

          // Emit updated record with incremented missing count
          RecordWarcUniversal missingRecord = createMissingRecord(state);
          downstream.onNext(missingRecord);
          Metrics.inc(METRIC_KEY, "missing-incremented");
        }
      }

      // Reset flag for next batch
      state.seenInCurrentBatch = false;
    }

    if (missingDetected > 0) {
      log.info("Missing content summary: {} content items not seen in current batch ({} reached threshold)",
          missingDetected, missingThresholdReached);
    }
  }

  /**
   * Create metadata-only record for missing content.
   * Emitted to baseline when content is missing for N consecutive crawls.
   */
  private RecordWarcUniversal createMissingRecord(DigestState state) {
    Map<String, String> headers = new java.util.LinkedHashMap<>();
    headers.put("WARC-Type", "conversion");
    headers.put("WARC-Block-Digest", state.digest);
    headers.put("WARC-Target-URI", state.currentUri);
    headers.put("WARC-Date", state.lastSeenDate);
    headers.put("X-NAC-First-Seen", state.firstSeen.toString());
    headers.put("X-NAC-Last-Seen", state.lastSeen.toString());
    headers.put("X-NAC-Missing-Count", String.valueOf(state.missingCount));
    headers.put("X-NAC-Status", state.status);
    headers.put("X-NAC-Crawl-ID", state.lastCrawlId);
    headers.put("NAC-Merge-Result", "missing-content");

    // Add URI chain headers if present
    if (state.chainLength > 0) {
      headers.put("X-NAC-Primary-URI", state.primaryUri);
      headers.put("X-NAC-Chain-Length", String.valueOf(state.chainLength));
      if (state.previousUri != null) {
        headers.put("X-NAC-Previous-URI", state.previousUri);
      }
    }

    // Add baseline date if tracked
    if (state.baselineDate != null) {
      headers.put("X-NAC-Baseline-Date", state.baselineDate);
    }

    // Empty payload (metadata-only for missing)
    return new RecordWarcUniversal("conversion", headers, new byte[0]);
  }

  private void closeDb() {
    if (db != null) {
      db.close();
      db = null;
    }
    if (options != null) {
      options.close();
      options = null;
    }
    if (dbPathObj != null && java.nio.file.Files.exists(dbPathObj)) {
      try {
        deleteDirectory(dbPathObj.toFile());
        log.info("Deleted RocksDB temporary directory: {}", dbPathObj);
      } catch (java.io.IOException e) {
        log.warn("Failed to delete RocksDB directory {}: {}", dbPathObj, e.getMessage());
      }
      dbPathObj = null;
    }
  }
}
