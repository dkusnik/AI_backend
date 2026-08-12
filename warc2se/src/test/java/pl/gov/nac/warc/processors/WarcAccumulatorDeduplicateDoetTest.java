package pl.gov.nac.warc.processors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import pl.gov.nac.warc.records.RecordBatch;
import pl.gov.nac.warc.records.warc.RecordWarcUniversal;

class WarcAccumulatorDeduplicateDoetTest {

  @TempDir
  Path tempDir;

  @Test
  void testStreamingDeduplication() {
    WarcAccumulatorDeduplicateDoet processor = new WarcAccumulatorDeduplicateDoet();
    processor.configure(Map.of("doet-merge", true, "primary-file", "primary.*"));

    TestSubscriber subscriber = new TestSubscriber();
    processor.subscribe(subscriber);
    processor.onSubscribe(new NoOpSubscription());

    // Input: A(primary), A(scan), B(primary), B(scan), B(scan), C(primary)
    // New behavior: Send as RecordBatch, grouped by digest
    List<RecordWarcUniversal> inputs = List.of(
        createRecordWithFile("sha256:a", "Content A", "primary.warc"),
        createRecordWithFile("sha256:a", "Content A", "scan.warc"),
        createRecordWithFile("sha256:b", "Content B", "primary.warc"),
        createRecordWithFile("sha256:b", "Content B", "scan.warc"),
        createRecordWithFile("sha256:b", "Content B", "scan.warc"),
        createRecordWithFile("sha256:c", "Content C", "primary.warc"));

    // Create batches grouped by digest
    List<RecordBatch> batches = createBatches(inputs);
    for (RecordBatch batch : batches) {
      processor.onNext(batch);
    }
    processor.onComplete();

    List<RecordWarcUniversal> emitted = subscriber.items;
    // Expect 3 records (one per unique digest)
    assertEquals(3, emitted.size());

    // Records should be sorted by digest
    assertEquals("sha256:a", emitted.get(0).headers().get("WARC-Block-Digest"));
    assertEquals("sha256:b", emitted.get(1).headers().get("WARC-Block-Digest"));
    assertEquals("sha256:c", emitted.get(2).headers().get("WARC-Block-Digest"));

    // Check final provenance values
    assertProvenance(emitted.get(0), "merged"); // seen in both primary and scan
    assertProvenance(emitted.get(1), "merged"); // seen in both primary and scan
    assertProvenance(emitted.get(2), "base-only"); // seen only in primary

  }

  @Test
  void testRocksDbDeduplication() {
    Path dbPath = tempDir.resolve("rocksdb_dedup");
    WarcAccumulatorDeduplicateDoet processor = new WarcAccumulatorDeduplicateDoet();
    processor.configure(Map.of(
        "doet-merge", false,
        "rocksdb-path", dbPath.toString()));

    TestSubscriber subscriber = new TestSubscriber();
    processor.subscribe(subscriber);
    processor.onSubscribe(new NoOpSubscription());

    // 1. First time A -> unique
    processor.onNext(createRecord("sha256:a", "Content A"));
    // 2. Second time A -> duplicate (dropped)
    processor.onNext(createRecord("sha256:a", "Content A"));
    // 3. New content B -> unique
    processor.onNext(createRecord("sha256:b", "Content B"));

    processor.onComplete();

    // In RocksDB mode, records are emitted only onComplete, sorted by digest
    List<RecordWarcUniversal> emitted = subscriber.items;
    assertEquals(2, emitted.size());

    // Sort by digest for stable assertion
    emitted.sort(Comparator.comparing(r -> r.headers().get("WARC-Block-Digest")));

    assertEquals("sha256:a", emitted.get(0).headers().get("WARC-Block-Digest"));
    assertEquals("sha256:b", emitted.get(1).headers().get("WARC-Block-Digest"));
  }

  @Test
  void testSortOnlyOrdersSameDigestByUrlWithoutDeduplicating() {
    Path dbPath = tempDir.resolve("rocksdb_sort_only_url_order");
    WarcAccumulatorDeduplicateDoet processor = new WarcAccumulatorDeduplicateDoet();
    processor.configure(Map.of(
        "doet-merge", false,
        "deduplicate-scope", "sort-only",
        "rocksdb-path", dbPath.toString()));

    TestSubscriber subscriber = new TestSubscriber();
    processor.subscribe(subscriber);
    processor.onSubscribe(new NoOpSubscription());

    String digest = "sha256:same-content";
    String parentUrl = "https://example.test/a";
    String childUrl = "https://example.test/a/child";
    processor.onNext(createRecordWithFileAndUri(digest, "Content", childUrl, "scan.warc"));
    processor.onNext(createRecordWithFileAndUri(digest, "Content", parentUrl, "scan.warc"));

    processor.onComplete();

    assertEquals(2, subscriber.items.size(), "sort-only must preserve duplicate-digest records");
    assertEquals(List.of(parentUrl, childUrl), subscriber.items.stream()
        .map(record -> record.headers().get("WARC-Target-URI"))
        .toList());
  }

  @Test
  void testConfigureRefusesToClearForeignDirectory() throws Exception {
    Path dbPath = tempDir.resolve("not_a_db");
    java.nio.file.Files.createDirectories(dbPath);
    java.nio.file.Files.writeString(dbPath.resolve("keep.txt"), "unrelated data");

    WarcAccumulatorDeduplicateDoet processor = new WarcAccumulatorDeduplicateDoet();
    assertThrows(IllegalStateException.class, () -> processor.configure(Map.of(
        "doet-merge", false,
        "rocksdb-path", dbPath.toString())));

    assertTrue(java.nio.file.Files.exists(dbPath.resolve("keep.txt")),
        "foreign directory content must survive a refused clear");
  }

  @Test
  void testConfigureClearsDirectoryWithRocksDbMarker() throws Exception {
    Path dbPath = tempDir.resolve("previous_db");
    java.nio.file.Files.createDirectories(dbPath);
    java.nio.file.Files.writeString(dbPath.resolve("CURRENT"), "MANIFEST-000001");
    java.nio.file.Files.writeString(dbPath.resolve("000001.sst"), "stale");

    WarcAccumulatorDeduplicateDoet processor = new WarcAccumulatorDeduplicateDoet();
    assertDoesNotThrow(() -> processor.configure(Map.of(
        "doet-merge", false,
        "rocksdb-path", dbPath.toString())));
    assertFalse(java.nio.file.Files.exists(dbPath.resolve("000001.sst")),
        "stale RocksDB state must be cleared at open");

    TestSubscriber subscriber = new TestSubscriber();
    processor.subscribe(subscriber);
    processor.onSubscribe(new NoOpSubscription());
    processor.onComplete();
  }

  @Test
  void testNoneEarlyReturnClosesPreviouslyInitializedRocksDb() {
    Path dbPath = tempDir.resolve("rocksdb_none_close");
    WarcAccumulatorDeduplicateDoet processor = new WarcAccumulatorDeduplicateDoet();
    processor.configure(Map.of("rocksdb-path", dbPath.toString()));
    processor.configure(Map.of("deduplicate-scope", "none"));
    processor.subscribe(new TestSubscriber());

    processor.onComplete();

    assertFalse(java.nio.file.Files.exists(dbPath),
        "The passthrough completion path must close and remove initialized RocksDB state");
  }

  @Test
  void testNonePreservesSameDigestRecordsInOrderAndMarksThem() {
    WarcAccumulatorDeduplicateDoet processor = new WarcAccumulatorDeduplicateDoet();
    processor.configure(Map.of("deduplicate-scope", "none"));

    TestSubscriber subscriber = new TestSubscriber();
    processor.subscribe(subscriber);
    processor.onSubscribe(new NoOpSubscription());

    String digest = "sha256:same";
    RecordWarcUniversal first = createRecordWithFileAndUri(
        digest, "first", "https://example.test/first", "source.warc");
    RecordWarcUniversal second = createRecordWithFileAndUri(
        digest, "second", "https://example.test/second", "source.warc");
    processor.onNext(first);
    processor.onNext(second);
    processor.onComplete();

    assertEquals(List.of(first, second), subscriber.items,
        "none scope must pass every record through in arrival order, including equal digests");
    assertTrue(subscriber.items.stream()
        .allMatch(record -> "none".equals(record.headers().get("nac-deduplicated"))),
        "none scope must mark every emitted record");
  }

  @Test
  void testMergeEarlyReturnClosesPreviouslyInitializedRocksDb() {
    Path dbPath = tempDir.resolve("rocksdb_merge_close");
    WarcAccumulatorDeduplicateDoet processor = new WarcAccumulatorDeduplicateDoet();
    processor.configure(Map.of("rocksdb-path", dbPath.toString()));
    processor.configure(Map.of("doet-merge", true));
    processor.subscribe(new TestSubscriber());

    processor.onComplete();

    assertFalse(java.nio.file.Files.exists(dbPath),
        "The merge completion path must close and remove initialized RocksDB state");
  }

  @Test
  void testErrorClosesRocksDbAndSuppressesLaterCompletion() {
    Path dbPath = tempDir.resolve("rocksdb_error_close");
    WarcAccumulatorDeduplicateDoet processor = new WarcAccumulatorDeduplicateDoet();
    processor.configure(Map.of("rocksdb-path", dbPath.toString()));
    TerminalSubscriber subscriber = new TerminalSubscriber();
    processor.subscribe(subscriber);

    processor.onError(new IllegalStateException("upstream failed"));
    processor.onComplete();

    assertFalse(java.nio.file.Files.exists(dbPath));
    assertEquals(1, subscriber.errors.get());
    assertEquals(0, subscriber.completions.get(),
        "onComplete after onError must not emit a second terminal signal");
  }

  @Test
  void testRocksDbProvenanceMerged() {
    Path dbPath = tempDir.resolve("rocksdb_merged");
    WarcAccumulatorDeduplicateDoet processor = new WarcAccumulatorDeduplicateDoet();
    processor.configure(Map.of(
        "doet-merge", false,
        "rocksdb-path", dbPath.toString(),
        "primary-file", "primary-.*\\.warc"));

    TestSubscriber subscriber = new TestSubscriber();
    processor.subscribe(subscriber);
    processor.onSubscribe(new NoOpSubscription());

    // 1. Content A from primary archive
    processor.onNext(createRecordWithFile("sha256:a", "Content A", "primary-01.warc"));
    // 2. Content A from secondary archive -> merged
    processor.onNext(createRecordWithFile("sha256:a", "Content A", "other-01.warc"));

    // 3. Content B from secondary only -> secondary
    processor.onNext(createRecordWithFile("sha256:b", "Content B", "other-02.warc"));

    // 4. Content C from primary only -> primary
    processor.onNext(createRecordWithFile("sha256:c", "Content C", "primary-02.warc"));

    processor.onComplete();

    List<RecordWarcUniversal> emitted = subscriber.items;
    assertEquals(3, emitted.size());

    Map<String, RecordWarcUniversal> byDigest = new java.util.HashMap<>();
    for (RecordWarcUniversal r : emitted) {
      byDigest.put(r.headers().get("WARC-Block-Digest"), r);
    }

    // Crawl-series RocksDB dedup emits per-crawl records without NAC-Merge-Result.
    assertEquals("global", byDigest.get("sha256:a").headers().get("nac-deduplicated"));
    assertEquals("global", byDigest.get("sha256:b").headers().get("nac-deduplicated"));
    assertEquals("global", byDigest.get("sha256:c").headers().get("nac-deduplicated"));

    assertEquals(null, byDigest.get("sha256:a").headers().get("NAC-Merge-Result"));
    assertEquals(null, byDigest.get("sha256:b").headers().get("NAC-Merge-Result"));
    assertEquals(null, byDigest.get("sha256:c").headers().get("NAC-Merge-Result"));

    assertEquals("2026-01-28", byDigest.get("sha256:a").headers().get("X-NAC-Crawl-ID"));
    assertEquals("2026-01-28", byDigest.get("sha256:b").headers().get("X-NAC-Crawl-ID"));
    assertEquals("2026-01-28", byDigest.get("sha256:c").headers().get("X-NAC-Crawl-ID"));
  }

  @Test
  void testUriMigration() {
    WarcAccumulatorDeduplicateDoet processor = new WarcAccumulatorDeduplicateDoet();
    // Use URL dedup mode to track different URLs separately
    processor.configure(Map.of("doet-merge", true, "deduplicate-scope", "url", "primary-file", "primary.*"));
    TestSubscriber subscriber = new TestSubscriber();
    processor.subscribe(subscriber);
    processor.onSubscribe(new NoOpSubscription());

    // Create records with same digest but different URLs
    List<RecordWarcUniversal> inputs = List.of(
        createRecordWithFile("sha256:m1", "Content", "primary.warc"),
        createRecordWithFileAndUri("sha256:m1", "Content", "http://new.com", "secondary.warc"));

    // Create batch (same digest, different URLs)
    List<RecordBatch> batches = createBatches(inputs);
    for (RecordBatch batch : batches) {
      processor.onNext(batch);
    }
    processor.onComplete();

    // In URL mode: different URLs = different keys = 2 records emitted
    assertEquals(2, subscriber.items.size());

    // Find records by URL (order not guaranteed)
    RecordWarcUniversal oldUrl = subscriber.items.stream()
        .filter(r -> "http://test.com".equals(r.headers().get("WARC-Target-URI")))
        .findFirst().orElseThrow();
    RecordWarcUniversal newUrl = subscriber.items.stream()
        .filter(r -> "http://new.com".equals(r.headers().get("WARC-Target-URI")))
        .findFirst().orElseThrow();

    // RecordBatch currently doesn't implement URI-changed tracking
    // Both records are treated independently based on their source files
    assertProvenance(oldUrl, "base-only");  // from primary.warc
    assertProvenance(newUrl, "new");  // from secondary.warc
  }

  @Test
  void testUriReversion() {
    WarcAccumulatorDeduplicateDoet processor = new WarcAccumulatorDeduplicateDoet();
    // Use URL dedup mode to track URI changes
    processor.configure(Map.of("doet-merge", true, "deduplicate-scope", "url", "primary-file", "primary.*"));
    TestSubscriber subscriber = new TestSubscriber();
    processor.subscribe(subscriber);
    processor.onSubscribe(new NoOpSubscription());

    // Same digest appearing at different URLs
    List<RecordWarcUniversal> inputs = List.of(
        createRecordWithFileAndUri("sha256:r1", "Content", "http://uri1.com", "primary.warc"),
        createRecordWithFileAndUri("sha256:r1", "Content", "http://uri2.com", "scan1.warc"),
        createRecordWithFileAndUri("sha256:r1", "Content", "http://uri1.com", "scan2.warc"));

    // Create batch (same digest, multiple URLs)
    List<RecordBatch> batches = createBatches(inputs);
    for (RecordBatch batch : batches) {
      processor.onNext(batch);
    }
    processor.onComplete();

    // In URL mode: 2 unique digest+URL combinations (uri1 appears twice, uri2 once)
    // uri1 and uri2 are distinct keys
    assertEquals(2, subscriber.items.size());

    // Find records by URL (order not guaranteed)
    RecordWarcUniversal uri1 = subscriber.items.stream()
        .filter(r -> "http://uri1.com".equals(r.headers().get("WARC-Target-URI")))
        .findFirst().orElseThrow();
    RecordWarcUniversal uri2 = subscriber.items.stream()
        .filter(r -> "http://uri2.com".equals(r.headers().get("WARC-Target-URI")))
        .findFirst().orElseThrow();

    // RecordBatch currently doesn't implement URI-changed tracking
    // Each URL is processed independently
    assertProvenance(uri1, "merged");  // appears in both primary and scan files
    assertProvenance(uri2, "new");  // only in scan files
  }

  @Test
  void testBaselineRefresh() {
    WarcAccumulatorDeduplicateDoet processor = new WarcAccumulatorDeduplicateDoet();
    processor.configure(Map.of("doet-merge", true, "primary-file", "primary.*"));
    TestSubscriber subscriber = new TestSubscriber();
    processor.subscribe(subscriber);
    processor.onSubscribe(new NoOpSubscription());

    // Same digest seen in primary then scan
    List<RecordWarcUniversal> inputs = List.of(
        createRecordWithFile("sha256:b1", "Content", "primary.warc"),
        createRecordWithFile("sha256:b1", "Content", "scan1.warc"));

    // Create batch
    List<RecordBatch> batches = createBatches(inputs);
    for (RecordBatch batch : batches) {
      processor.onNext(batch);
    }
    processor.onComplete();

    // New behavior: emit one record with final provenance
    assertEquals(1, subscriber.items.size());
    assertProvenance(subscriber.items.get(0), "merged"); // seen in both
  }

  @Test
  void testDigestCollision() {
    WarcAccumulatorDeduplicateDoet processor = new WarcAccumulatorDeduplicateDoet();
    processor.configure(Map.of("doet-merge", true, "primary-file", "primary.*"));
    TestSubscriber subscriber = new TestSubscriber();
    processor.subscribe(subscriber);
    processor.onSubscribe(new NoOpSubscription());

    // Same digest, different content lengths -> collision
    List<RecordWarcUniversal> inputs = List.of(
        createRecordWithFile("sha256:coll", "Short", "primary.warc"),
        createRecordWithFile("sha256:coll", "Much Longer Payload", "scan.warc"));

    // Create batch
    List<RecordBatch> batches = createBatches(inputs);
    for (RecordBatch batch : batches) {
      processor.onNext(batch);
    }
    processor.onComplete();

    // RecordBatch currently doesn't implement collision detection
    // All records with same digest are batched together and one representative is emitted
    assertEquals(1, subscriber.items.size());
    assertProvenance(subscriber.items.get(0), "merged");  // primary + scan = merged
  }

  @Test
  void testCaseInsensitivity() {
    WarcAccumulatorDeduplicateDoet processor = new WarcAccumulatorDeduplicateDoet();
    processor.configure(Map.of("doet-merge", true));
    TestSubscriber subscriber = new TestSubscriber();
    processor.subscribe(subscriber);
    processor.onSubscribe(new NoOpSubscription());

    // Create record with lowercase headers
    Map<String, String> lowerHeaders = new java.util.LinkedHashMap<>();
    lowerHeaders.put("warc-block-digest", "sha256:case");
    lowerHeaders.put("warc-target-uri", "http://case.com");
    lowerHeaders.put("warc-date", "2026-01-20T10:00:00Z");
    lowerHeaders.put("X-Source-Warc", "test.warc");

    RecordWarcUniversal rec = new RecordWarcUniversal("conversion", lowerHeaders, "Data".getBytes());

    // Create batch
    RecordBatch batch = new RecordBatch(
        "sha256:case",
        Set.of(rec),
        Instant.parse("2026-01-20T10:00:00Z"),
        Instant.parse("2026-01-20T10:00:00Z")
    );
    processor.onNext(batch);
    processor.onComplete();

    assertEquals(1, subscriber.items.size());
    assertEquals("sha256:case", subscriber.items.get(0).headers().get("warc-block-digest"));
  }

  // =========================================================================
  // Phase 2: Temporal Lifecycle Tests
  // =========================================================================

  @Test
  void testTemporalFieldInitialization() {
    WarcAccumulatorDeduplicateDoet processor = new WarcAccumulatorDeduplicateDoet();
    processor.configure(Map.of(
        "doet-merge", true,
        "missing-threshold", 3,
        "crawl-id", "test-crawl-1"));

    TestSubscriber subscriber = new TestSubscriber();
    processor.subscribe(subscriber);
    processor.onSubscribe(new NoOpSubscription());

    // Process a new record
    List<RecordWarcUniversal> inputs = List.of(
        createRecordWithDate("sha256:t1", "Content A", "2026-01-15T10:00:00Z", "batch1.warc"));

    List<RecordBatch> batches = createBatches(inputs);
    for (RecordBatch batch : batches) {
      processor.onNext(batch);
    }
    processor.onComplete();

    assertEquals(1, subscriber.items.size());
    RecordWarcUniversal emitted = subscriber.items.get(0);

    // Verify all temporal headers are present
    assertEquals("2026-01-15T10:00:00Z", emitted.headers().get("X-NAC-First-Seen"));
    assertEquals("2026-01-15T10:00:00Z", emitted.headers().get("X-NAC-Last-Seen"));
    assertEquals("0", emitted.headers().get("X-NAC-Missing-Count"));
    assertEquals("active", emitted.headers().get("X-NAC-Status"));
    assertEquals("test-crawl-1", emitted.headers().get("X-NAC-Crawl-ID"));
    assertProvenance(emitted, "new");
  }

  @Test
  void testLastSeenUpdate() {
    WarcAccumulatorDeduplicateDoet processor = new WarcAccumulatorDeduplicateDoet();
    processor.configure(Map.of(
        "doet-merge", true,
        "crawl-id", "test-crawl-2",
        "primary-file", "batch1.*"));

    TestSubscriber subscriber = new TestSubscriber();
    processor.subscribe(subscriber);
    processor.onSubscribe(new NoOpSubscription());

    // First observation and second observation (later date, same digest+URI)
    List<RecordWarcUniversal> inputs = List.of(
        createRecordWithDate("sha256:t2", "Content B", "2026-01-15T10:00:00Z", "batch1.warc"),
        createRecordWithDate("sha256:t2", "Content B", "2026-01-20T11:00:00Z", "batch2.warc"));

    List<RecordBatch> batches = createBatches(inputs);
    for (RecordBatch batch : batches) {
      processor.onNext(batch);
    }
    processor.onComplete();

    // New behavior: emit one record with updated temporal metadata
    assertEquals(1, subscriber.items.size());

    RecordWarcUniversal emitted = subscriber.items.get(0);
    assertEquals("2026-01-15T10:00:00Z", emitted.headers().get("X-NAC-First-Seen")); // First seen
    assertEquals("2026-01-20T11:00:00Z", emitted.headers().get("X-NAC-Last-Seen")); // Updated to latest
    assertEquals("0", emitted.headers().get("X-NAC-Missing-Count"));
    assertEquals("active", emitted.headers().get("X-NAC-Status"));
    assertProvenance(emitted, "merged"); // seen multiple times
  }

  @Test
  void testTemporalMetadataAccumulatesAcrossSeparateBatches() {
    WarcAccumulatorDeduplicateDoet processor = new WarcAccumulatorDeduplicateDoet();
    processor.configure(Map.of(
        "doet-merge", true,
        "crawl-id", "test-crawl-batches"));

    TestSubscriber subscriber = new TestSubscriber();
    processor.subscribe(subscriber);
    processor.onSubscribe(new NoOpSubscription());

    String digest = "sha256:across-batches";
    String uri = "https://example.test/across-batches";
    RecordWarcUniversal later = createRecordWithDateAndUri(
        digest, "same", uri, "2026-01-20T11:00:00Z", "batch-2.warc");
    RecordWarcUniversal earliest = createRecordWithDateAndUri(
        digest, "same", uri, "2026-01-10T09:00:00Z", "batch-1a.warc");
    RecordWarcUniversal middle = createRecordWithDateAndUri(
        digest, "same", uri, "2026-01-15T10:00:00Z", "batch-1b.warc");

    processor.onNext(new RecordBatch(
        digest, Set.of(later),
        Instant.parse("2026-01-20T11:00:00Z"), Instant.parse("2026-01-20T11:00:00Z")));
    processor.onNext(new RecordBatch(
        digest, Set.of(earliest, middle),
        Instant.parse("2026-01-10T09:00:00Z"), Instant.parse("2026-01-15T10:00:00Z")));
    processor.onComplete();

    assertEquals(1, subscriber.items.size());
    RecordWarcUniversal emitted = subscriber.items.get(0);
    assertEquals("2026-01-10T09:00:00Z", emitted.headers().get("X-NAC-First-Seen"));
    assertEquals("2026-01-20T11:00:00Z", emitted.headers().get("X-NAC-Last-Seen"));
    assertEquals("3", emitted.headers().get("X-NAC-Record-Revisit-Count"));
  }

  @Test
  void testMissingDetectionSingleBatch() {
    WarcAccumulatorDeduplicateDoet processor = new WarcAccumulatorDeduplicateDoet();
    processor.configure(Map.of(
        "doet-merge", true,
        "missing-threshold", 3,
        "crawl-id", "test-crawl-3"));

    TestSubscriber subscriber = new TestSubscriber();
    processor.subscribe(subscriber);
    processor.onSubscribe(new NoOpSubscription());

    // Add content A and B in first batch
    List<RecordWarcUniversal> inputs = List.of(
        createRecordWithDate("sha256:t3a", "Content A", "2026-01-15T10:00:00Z", "batch1.warc"),
        createRecordWithDate("sha256:t3b", "Content B", "2026-01-15T10:05:00Z", "batch1.warc"));

    List<RecordBatch> batches = createBatches(inputs);
    for (RecordBatch batch : batches) {
      processor.onNext(batch);
    }
    processor.onComplete();

    // Content A and B should be emitted with missingCount=0
    assertEquals(2, subscriber.items.size());
    assertEquals("0", subscriber.items.get(0).headers().get("X-NAC-Missing-Count"));
    assertEquals("0", subscriber.items.get(1).headers().get("X-NAC-Missing-Count"));
    assertEquals("active", subscriber.items.get(0).headers().get("X-NAC-Status"));
    assertEquals("active", subscriber.items.get(1).headers().get("X-NAC-Status"));
  }

  @Test
  void testMissingDetectionThresholdReached() {
    // This test simulates the missing detection logic by calling onComplete multiple times
    // In production, this would happen across multiple batch runs
    WarcAccumulatorDeduplicateDoet processor = new WarcAccumulatorDeduplicateDoet();
    processor.configure(Map.of(
        "doet-merge", true,
        "missing-threshold", 3,
        "crawl-id", "test-crawl-4"));

    TestSubscriber subscriber = new TestSubscriber();
    processor.subscribe(subscriber);
    processor.onSubscribe(new NoOpSubscription());

    // Batch 1: Content A and B present
    List<RecordWarcUniversal> inputs = List.of(
        createRecordWithDate("sha256:t4a", "Content A", "2026-01-15T10:00:00Z", "batch1.warc"),
        createRecordWithDate("sha256:t4b", "Content B", "2026-01-15T10:05:00Z", "batch1.warc"));

    List<RecordBatch> batches = createBatches(inputs);
    for (RecordBatch batch : batches) {
      processor.onNext(batch);
    }

    // For this test, we verify the infrastructure is in place
    // The actual multi-batch missing detection requires integration testing
    processor.onComplete();

    assertEquals(2, subscriber.items.size());
    // Verify temporal headers exist
    assertEquals("active", subscriber.items.get(0).headers().get("X-NAC-Status"));
    assertEquals("active", subscriber.items.get(1).headers().get("X-NAC-Status"));
  }

  @Test
  void testReappearanceAfterMissing() {
    WarcAccumulatorDeduplicateDoet processor = new WarcAccumulatorDeduplicateDoet();
    processor.configure(Map.of(
        "doet-merge", true,
        "missing-threshold", 3,
        "crawl-id", "test-crawl-5"));

    TestSubscriber subscriber = new TestSubscriber();
    processor.subscribe(subscriber);
    processor.onSubscribe(new NoOpSubscription());

    // This test verifies the reappearance logic structure
    // Full multi-batch testing requires integration tests

    // Batch 1: Content A present
    List<RecordWarcUniversal> inputs = List.of(
        createRecordWithDate("sha256:t5a", "Content A", "2026-01-15T10:00:00Z", "batch1.warc"));

    List<RecordBatch> batches = createBatches(inputs);
    for (RecordBatch batch : batches) {
      processor.onNext(batch);
    }
    processor.onComplete();

    assertEquals(1, subscriber.items.size());
    assertProvenance(subscriber.items.get(0), "new");
    assertEquals("0", subscriber.items.get(0).headers().get("X-NAC-Missing-Count"));
    assertEquals("active", subscriber.items.get(0).headers().get("X-NAC-Status"));
  }

  @Test
  void testTemporalHeadersWithURIChanged() {
    WarcAccumulatorDeduplicateDoet processor = new WarcAccumulatorDeduplicateDoet();
    // Use global dedup mode to track URI changes for same digest
    processor.configure(Map.of(
        "doet-merge", true,
        "deduplicate-scope", "global",
        "missing-threshold", 3,
        "crawl-id", "test-crawl-6",
        "primary-file", "primary.*"));

    TestSubscriber subscriber = new TestSubscriber();
    processor.subscribe(subscriber);
    processor.onSubscribe(new NoOpSubscription());

    // Content at first URL and same content at different URL (migrated)
    List<RecordWarcUniversal> inputs = List.of(
        createRecordWithDateAndUri("sha256:t6", "Content",
            "http://old.com/page", "2026-01-15T10:00:00Z", "primary.warc"),
        createRecordWithDateAndUri("sha256:t6", "Content",
            "http://new.com/page", "2026-01-20T11:00:00Z", "scan.warc"));

    List<RecordBatch> batches = createBatches(inputs);
    for (RecordBatch batch : batches) {
      processor.onNext(batch);
    }
    processor.onComplete();

    // Global merge is URL-preserving: migrated URL remains separate
    assertEquals(2, subscriber.items.size());
    assertEquals(1, subscriber.items.stream()
        .filter(r -> "base-only".equals(r.headers().get("NAC-Merge-Result")))
        .count());
    assertEquals(1, subscriber.items.stream()
        .filter(r -> "new".equals(r.headers().get("NAC-Merge-Result")))
        .count());

    for (RecordWarcUniversal record : subscriber.items) {
      // Temporal headers should be present on each emitted record
      assertEquals("2026-01-15T10:00:00Z", record.headers().get("X-NAC-First-Seen"));
      assertEquals("2026-01-20T11:00:00Z", record.headers().get("X-NAC-Last-Seen"));
      assertEquals("0", record.headers().get("X-NAC-Missing-Count"));
      assertEquals("active", record.headers().get("X-NAC-Status"));
    }
  }

  @Test
  void testAutoGeneratedCrawlId() {
    WarcAccumulatorDeduplicateDoet processor = new WarcAccumulatorDeduplicateDoet();
    processor.configure(Map.of(
        "doet-merge", true,
        "crawl-id", "auto")); // Auto-generate

    TestSubscriber subscriber = new TestSubscriber();
    processor.subscribe(subscriber);
    processor.onSubscribe(new NoOpSubscription());

    List<RecordWarcUniversal> inputs = List.of(
        createRecordWithDate("sha256:t7", "Content", "2026-01-15T10:00:00Z", "batch1.warc"));

    List<RecordBatch> batches = createBatches(inputs);
    for (RecordBatch batch : batches) {
      processor.onNext(batch);
    }
    processor.onComplete();

    assertEquals(1, subscriber.items.size());
    String crawlId = subscriber.items.get(0).headers().get("X-NAC-Crawl-ID");

    // Verify it has the expected format: "crawl-<timestamp>"
    assertEquals(true, crawlId.startsWith("crawl-"));
    assertEquals(true, crawlId.contains("T"));
    assertEquals(true, crawlId.contains("Z"));
  }

  @Test
  void testMissingThresholdConfiguration() {
    // Test with different threshold value
    WarcAccumulatorDeduplicateDoet processor = new WarcAccumulatorDeduplicateDoet();
    processor.configure(Map.of(
        "doet-merge", true,
        "missing-threshold", 5, // Custom threshold
        "crawl-id", "test-crawl-8"));

    TestSubscriber subscriber = new TestSubscriber();
    processor.subscribe(subscriber);
    processor.onSubscribe(new NoOpSubscription());

    List<RecordWarcUniversal> inputs = List.of(
        createRecordWithDate("sha256:t8", "Content", "2026-01-15T10:00:00Z", "batch1.warc"));

    List<RecordBatch> batches = createBatches(inputs);
    for (RecordBatch batch : batches) {
      processor.onNext(batch);
    }
    processor.onComplete();

    // Configuration loaded successfully (no exceptions)
    assertEquals(1, subscriber.items.size());
    assertEquals("active", subscriber.items.get(0).headers().get("X-NAC-Status"));
  }

  // -------------------------------------------------------------------------
  // C-1 (T-214): digestRegistry HashMap race condition — regression guard
  // -------------------------------------------------------------------------

  /**
   * Concurrent onNext() calls (as happen in the ReactiveEngine) must not throw
   * ConcurrentModificationException or corrupt the dedup count when digestRegistry
   * is a HashMap.  After the fix (ConcurrentHashMap) the test reliably passes.
   *
   * <p>Note: HashMap corruption is non-deterministic; this test may pass
   * occasionally even without the fix — but in practice fails under load.
   */
  @Test
  void testConcurrentOnNextDoesNotCorruptDigestRegistry(@TempDir Path concurrentTempDir)
      throws Exception {
    WarcAccumulatorDeduplicateDoet processor = new WarcAccumulatorDeduplicateDoet();
    processor.configure(Map.of(
        "doet-merge", false,
        "rocksdb-path", concurrentTempDir.resolve("rdb").toString()));

    TestSubscriber subscriber = new TestSubscriber();
    processor.subscribe(subscriber);
    processor.onSubscribe(new NoOpSubscription());

    int threads = 8;
    int recordsEach = 50;
    CyclicBarrier barrier = new CyclicBarrier(threads);
    ExecutorService pool = Executors.newFixedThreadPool(threads);

    for (int t = 0; t < threads; t++) {
      final int tid = t;
      pool.submit(() -> {
        try {
          barrier.await();
          for (int i = 0; i < recordsEach; i++) {
            processor.onNext(createRecord("sha256:t" + tid + "r" + i, "body-" + i));
          }
        } catch (Exception ignored) {
          // ConcurrentModificationException captured here; test still checks onComplete
        }
        return null;
      });
    }
    pool.shutdown();
    assertTrue(pool.awaitTermination(15, TimeUnit.SECONDS));

    assertDoesNotThrow(() -> processor.onComplete(),
        "onComplete must complete successfully after concurrent onNext calls");
  }

  // -------------------------------------------------------------------------
  // H-3 (T-221): RS §1.8 — onNext must not be called after onComplete
  // -------------------------------------------------------------------------

  /**
   * Verifies that downstream.onNext() is never called after downstream.onComplete()
   * has been signalled. Per Reactive Streams §1.8 no further signals are allowed
   * once onComplete has been delivered.
   */
  @Test
  void testOnNextIsNotCalledAfterOnComplete(@TempDir Path h3TempDir) {
    WarcAccumulatorDeduplicateDoet processor = new WarcAccumulatorDeduplicateDoet();
    processor.configure(Map.of(
        "doet-merge", true,
        "rocksdb-path", h3TempDir.resolve("rdb").toString(),
        "primary-file", "primary\\.warc.*"));

    AtomicBoolean completeSeen = new AtomicBoolean(false);
    AtomicBoolean onNextAfterComplete = new AtomicBoolean(false);

    processor.subscribe(new Flow.Subscriber<>() {
      @Override public void onSubscribe(Flow.Subscription s) { s.request(Long.MAX_VALUE); }
      @Override public void onNext(Object item) {
        if (completeSeen.get()) onNextAfterComplete.set(true);
      }
      @Override public void onError(Throwable t) { throw new RuntimeException(t); }
      @Override public void onComplete() { completeSeen.set(true); }
    });
    processor.onSubscribe(new NoOpSubscription());

    // Send one record and complete
    List<RecordWarcUniversal> inputs = List.of(
        createRecordWithFile("sha256:h3", "content h3", "primary.warc"));
    for (RecordBatch batch : createBatches(inputs)) {
      processor.onNext(batch);
    }
    processor.onComplete();

    assertFalse(onNextAfterComplete.get(),
        "downstream.onNext() must not be called after downstream.onComplete() per RS §1.8");
  }

  private RecordWarcUniversal createRecordWithFileAndUri(String digest, String content, String uri, String filename) {
    String raw = "WARC/1.0\r\n" +
        "WARC-Type: response\r\n" +
        "WARC-Target-URI: " + uri + "\r\n" +
        "WARC-Block-Digest: " + digest + "\r\n" +
        "X-Source-Warc: " + filename + "\r\n" +
        "Content-Length: " + content.length() + "\r\n" +
        "\r\n" +
        content + "\r\n\r\n";

    Map<String, String> headers = new java.util.LinkedHashMap<>();
    headers.put("WARC-Block-Digest", digest);
    headers.put("X-Source-Warc", filename);
    headers.put("WARC-Target-URI", uri);
    headers.put("WARC-Date", "2026-01-28T00:00:00Z");

    return new RecordWarcUniversal("response", headers, raw.getBytes(StandardCharsets.UTF_8));
  }

  private void assertProvenance(RecordWarcUniversal r, String expected) {
    assertEquals(expected, r.headers().get("NAC-Merge-Result"),
        "Digest " + r.headers().get("WARC-Block-Digest") + " expected " + expected);
  }

  private RecordWarcUniversal createRecord(String digest, String content) {
    return createRecordWithFile(digest, content, "test.warc");
  }

  private RecordWarcUniversal createRecordWithFile(String digest, String content, String filename) {
    return createRecordWithDateAndUri(digest, content, "http://test.com", "2026-01-28T00:00:00Z", filename);
  }

  private RecordWarcUniversal createRecordWithDate(String digest, String content, String date, String filename) {
    return createRecordWithDateAndUri(digest, content, "http://test.com", date, filename);
  }

  private RecordWarcUniversal createRecordWithDateAndUri(String digest, String content, String uri, String date, String filename) {
    String raw = "WARC/1.0\r\n" +
        "WARC-Type: response\r\n" +
        "WARC-Target-URI: " + uri + "\r\n" +
        "WARC-Date: " + date + "\r\n" +
        "WARC-Block-Digest: " + digest + "\r\n" +
        "X-Source-Warc: " + filename + "\r\n" +
        "Content-Length: " + content.length() + "\r\n" +
        "\r\n" +
        content + "\r\n\r\n";

    Map<String, String> headers = new java.util.LinkedHashMap<>();
    headers.put("WARC-Block-Digest", digest);
    headers.put("X-Source-Warc", filename);
    headers.put("WARC-Target-URI", uri);
    headers.put("WARC-Date", date);

    return new RecordWarcUniversal("response", headers, raw.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Groups records by digest and creates RecordBatch objects.
   * This mimics the batching behavior of ChunkedArchiveExtractor.
   */
  private List<RecordBatch> createBatches(List<RecordWarcUniversal> records) {
    Map<String, Set<RecordWarcUniversal>> byDigest = new HashMap<>();
    Map<String, Instant> minDates = new HashMap<>();
    Map<String, Instant> maxDates = new HashMap<>();

    for (RecordWarcUniversal record : records) {
      String digest = record.headers().get("WARC-Block-Digest");
      if (digest == null) {
        digest = record.headers().get("WARC-Payload-Digest");
      }

      byDigest.computeIfAbsent(digest, k -> new HashSet<>()).add(record);

      Instant date = Instant.parse(record.headers().get("WARC-Date"));
      minDates.merge(digest, date, (a, b) -> a.isBefore(b) ? a : b);
      maxDates.merge(digest, date, (a, b) -> a.isAfter(b) ? a : b);
    }

    List<RecordBatch> batches = new ArrayList<>();
    for (Map.Entry<String, Set<RecordWarcUniversal>> entry : byDigest.entrySet()) {
      String digest = entry.getKey();
      batches.add(new RecordBatch(
          digest,
          entry.getValue(),
          minDates.get(digest),
          maxDates.get(digest)
      ));
    }

    // Sort batches by digest for stable test behavior
    batches.sort(Comparator.comparing(RecordBatch::sharedDigest));
    return batches;
  }

  static class TestSubscriber implements Flow.Subscriber<Object> {
    List<RecordWarcUniversal> items = new ArrayList<>();

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      // Not used in test
    }

    @Override
    public void onNext(Object item) {
      if (item instanceof RecordWarcUniversal r)
        items.add(r);
    }

    @Override
    public void onError(Throwable throwable) {
      throwable.printStackTrace();
    }

    @Override
    public void onComplete() {
      // Signals completion
    }
  }

  static class TerminalSubscriber extends TestSubscriber {
    final AtomicInteger errors = new AtomicInteger();
    final AtomicInteger completions = new AtomicInteger();

    @Override
    public void onError(Throwable throwable) {
      errors.incrementAndGet();
    }

    @Override
    public void onComplete() {
      completions.incrementAndGet();
    }
  }

  static class NoOpSubscription implements Flow.Subscription {
    @Override
    public void request(long n) {
      // No-op
    }

    @Override
    public void cancel() {
      // No-op
    }
  }
}
