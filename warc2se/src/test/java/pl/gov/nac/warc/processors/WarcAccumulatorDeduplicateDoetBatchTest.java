package pl.gov.nac.warc.processors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static pl.gov.nac.warc.testutil.ExpectedLogSilencer.runWithLoggerMuted;

import pl.gov.nac.warc.records.RecordBatch;
import pl.gov.nac.warc.records.warc.RecordWarcUniversal;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.Flow;

/**
 * Unit tests for RecordBatch processing in WarcAccumulatorDeduplicateDoet.
 *
 * Tests batch processing logic for merge mode including:
 * - Provenance determination (merged, base-only, new)
 * - Global vs URL scope handling
 * - Type acceptance and rejection
 */
class WarcAccumulatorDeduplicateDoetBatchTest {

    @TempDir
    Path tempDir;

    // Test 1: Accept RecordBatch in Merge Mode
    @Test
    @DisplayName("Should accept RecordBatch as input type in merge mode")
    void testAcceptRecordBatchInputType() {
        WarcAccumulatorDeduplicateDoet accumulator = createMergeAccumulator();

        List<Class<? extends pl.gov.nac.warc.records.Record>> acceptedTypes =
            accumulator.acceptedInputTypes();

        assertTrue(acceptedTypes.size() > 0);
        assertEquals(RecordBatch.class, acceptedTypes.get(0),
            "RecordBatch should be first accepted type in merge mode");
    }

    @Test
    @DisplayName("Should not negotiate RecordBatch input outside merge mode")
    void testRejectRecordBatchNegotiationOutsideMergeMode() {
        WarcAccumulatorDeduplicateDoet accumulator = createNonMergeAccumulator();

        assertFalse(accumulator.acceptedInputTypes().contains(RecordBatch.class),
            "Non-merge mode must negotiate individual records backed by RocksDB");
    }

    @Test
    @DisplayName("Should not swallow a defensively delivered batch outside merge mode")
    void testNonMergeBatchProducesOutput() {
        WarcAccumulatorDeduplicateDoet accumulator = createNonMergeAccumulator();
        TestSubscriber subscriber = new TestSubscriber();
        accumulator.subscribe(subscriber);
        accumulator.onSubscribe(new NoOpSubscription());

        String digest = "xxh128:nonmergebatch";
        RecordWarcUniversal record = createRecordWithSource(
            digest, "http://example.com/non-merge", "crawl1.wet.gz", "2026-01-01T10:00:00Z");
        accumulator.onNext(new RecordBatch(
            digest,
            Set.of(record),
            Instant.parse("2026-01-01T10:00:00Z"),
            Instant.parse("2026-01-01T10:00:00Z")));
        accumulator.onComplete();

        assertEquals(1, subscriber.items.size(),
            "A non-merge batch must reach the RocksDB-backed emission path");
    }

    // Test 2: Provenance - Merged (Primary + Scan)
    @Test
    @DisplayName("Should classify batch as 'merged' when both primary and scan present")
    void testProvenanceMerged() {
        WarcAccumulatorDeduplicateDoet accumulator = createMergeAccumulator();
        TestSubscriber subscriber = new TestSubscriber();
        accumulator.subscribe(subscriber);
        accumulator.onSubscribe(new NoOpSubscription());

        String digest = "xxh128:1234567890abcdef";
        RecordWarcUniversal primaryRecord = createRecordWithSource(
            digest, "http://example.com", "crawl1.wet.gz", "2026-01-01T10:00:00Z"
        );
        RecordWarcUniversal scanRecord = createRecordWithSource(
            digest, "http://example.com", "crawl2.wet.gz", "2026-02-01T10:00:00Z"
        );

        RecordBatch batch = new RecordBatch(
            digest,
            Set.of(primaryRecord, scanRecord),
            Instant.parse("2026-01-01T10:00:00Z"),
            Instant.parse("2026-02-01T10:00:00Z")
        );

        accumulator.onNext(batch);
        accumulator.onComplete();

        assertEquals(1, subscriber.items.size());
        assertProvenance(subscriber.items.get(0), "merged");
    }

    // Test 3: Provenance - Base-Only (Primary Only)
    @Test
    @DisplayName("Should classify per-URL records as 'base-only' when only primary present")
    void testProvenanceBaseOnly() {
        WarcAccumulatorDeduplicateDoet accumulator = createMergeAccumulator();
        TestSubscriber subscriber = new TestSubscriber();
        accumulator.subscribe(subscriber);
        accumulator.onSubscribe(new NoOpSubscription());

        String digest = "xxh128:1234567890abcdef";
        RecordWarcUniversal primaryRecord1 = createRecordWithSource(
            digest, "http://example.com/1", "crawl1.wet.gz", "2026-01-01T10:00:00Z"
        );
        RecordWarcUniversal primaryRecord2 = createRecordWithSource(
            digest, "http://example.com/2", "crawl1.wet.gz", "2026-01-01T11:00:00Z"
        );

        RecordBatch batch = new RecordBatch(
            digest,
            Set.of(primaryRecord1, primaryRecord2),
            Instant.parse("2026-01-01T10:00:00Z"),
            Instant.parse("2026-01-01T11:00:00Z")
        );

        accumulator.onNext(batch);
        accumulator.onComplete();

        assertEquals(2, subscriber.items.size());
        assertEquals(2, subscriber.items.stream()
            .filter(r -> "base-only".equals(r.headers().get("NAC-Merge-Result")))
            .count());
    }

    // Test 4: Provenance - New (Scan Only)
    @Test
    @DisplayName("Should classify per-URL records as 'new' when only scan present")
    void testProvenanceNew() {
        WarcAccumulatorDeduplicateDoet accumulator = createMergeAccumulator();
        TestSubscriber subscriber = new TestSubscriber();
        accumulator.subscribe(subscriber);
        accumulator.onSubscribe(new NoOpSubscription());

        String digest = "xxh128:1234567890abcdef";
        RecordWarcUniversal scanRecord1 = createRecordWithSource(
            digest, "http://example.com/1", "crawl2.wet.gz", "2026-02-01T10:00:00Z"
        );
        RecordWarcUniversal scanRecord2 = createRecordWithSource(
            digest, "http://example.com/2", "crawl2.wet.gz", "2026-02-01T11:00:00Z"
        );

        RecordBatch batch = new RecordBatch(
            digest,
            Set.of(scanRecord1, scanRecord2),
            Instant.parse("2026-02-01T10:00:00Z"),
            Instant.parse("2026-02-01T11:00:00Z")
        );

        accumulator.onNext(batch);
        accumulator.onComplete();

        assertEquals(2, subscriber.items.size());
        assertEquals(2, subscriber.items.stream()
            .filter(r -> "new".equals(r.headers().get("NAC-Merge-Result")))
            .count());
    }

    // Test 5: URL Scope - Same Digest, Different URLs
    @Test
    @DisplayName("Should handle URL scope correctly with same digest at different URLs")
    void testUrlScopeSameDigestDifferentUrls() {
        WarcAccumulatorDeduplicateDoet accumulator = createUrlScopeAccumulator();
        TestSubscriber subscriber = new TestSubscriber();
        accumulator.subscribe(subscriber);
        accumulator.onSubscribe(new NoOpSubscription());

        String digest = "xxh128:1234567890abcdef";
        RecordWarcUniversal record1 = createRecordWithSource(
            digest, "http://old.com/page", "crawl1.wet.gz", "2026-01-01T10:00:00Z"
        );
        RecordWarcUniversal record2 = createRecordWithSource(
            digest, "http://new.com/page", "crawl2.wet.gz", "2026-02-01T10:00:00Z"
        );

        RecordBatch batch = new RecordBatch(
            digest,
            Set.of(record1, record2),
            Instant.parse("2026-01-01T10:00:00Z"),
            Instant.parse("2026-02-01T10:00:00Z")
        );

        accumulator.onNext(batch);
        accumulator.onComplete();

        // Should emit 2 records: base-only for old.com, new for new.com
        assertEquals(2, subscriber.items.size());
    }

    // Test 6: URL Scope - Same Digest, Same URL
    @Test
    @DisplayName("Should handle URL scope correctly with same digest at same URL")
    void testUrlScopeSameDigestSameUrl() {
        WarcAccumulatorDeduplicateDoet accumulator = createUrlScopeAccumulator();
        TestSubscriber subscriber = new TestSubscriber();
        accumulator.subscribe(subscriber);
        accumulator.onSubscribe(new NoOpSubscription());

        String digest = "xxh128:1234567890abcdef";
        String uri = "http://example.com/page";
        RecordWarcUniversal primaryRecord = createRecordWithSource(
            digest, uri, "crawl1.wet.gz", "2026-01-01T10:00:00Z"
        );
        RecordWarcUniversal scanRecord = createRecordWithSource(
            digest, uri, "crawl2.wet.gz", "2026-02-01T10:00:00Z"
        );

        RecordBatch batch = new RecordBatch(
            digest,
            Set.of(primaryRecord, scanRecord),
            Instant.parse("2026-01-01T10:00:00Z"),
            Instant.parse("2026-02-01T10:00:00Z")
        );

        accumulator.onNext(batch);
        accumulator.onComplete();

        // Should emit 1 record: merged
        assertEquals(1, subscriber.items.size());
        assertProvenance(subscriber.items.get(0), "merged");
    }

    // Test 7: Global Scope - Multiple URLs Same Digest
    @Test
    @DisplayName("Should preserve URL-distinct records in global merge scope")
    void testGlobalScopeMultipleUrls() {
        WarcAccumulatorDeduplicateDoet accumulator = createMergeAccumulator();
        TestSubscriber subscriber = new TestSubscriber();
        accumulator.subscribe(subscriber);
        accumulator.onSubscribe(new NoOpSubscription());

        String digest = "xxh128:1234567890abcdef";
        RecordWarcUniversal r1 = createRecordWithSource(
            digest, "http://a.com", "crawl1.wet.gz", "2026-01-01T10:00:00Z"
        );
        RecordWarcUniversal r2 = createRecordWithSource(
            digest, "http://b.com", "crawl2.wet.gz", "2026-02-01T10:00:00Z"
        );

        RecordBatch batch = new RecordBatch(
            digest,
            Set.of(r1, r2),
            Instant.parse("2026-01-01T10:00:00Z"),
            Instant.parse("2026-02-01T10:00:00Z")
        );

        accumulator.onNext(batch);
        accumulator.onComplete();

        // Global merge is URL-preserving: different URL stays as separate docs
        assertEquals(2, subscriber.items.size());
        assertEquals(1, subscriber.items.stream()
            .filter(r -> "base-only".equals(r.headers().get("NAC-Merge-Result")))
            .count());
        assertEquals(1, subscriber.items.stream()
            .filter(r -> "new".equals(r.headers().get("NAC-Merge-Result")))
            .count());
    }

    // Test 8: Non-Batch Input Rejected in Merge Mode
    @Test
    @DisplayName("Should reject non-RecordBatch input in merge mode")
    void testNonBatchInputRejected() {
        WarcAccumulatorDeduplicateDoet accumulator = createMergeAccumulator();
        TestSubscriber subscriber = new TestSubscriber();
        accumulator.subscribe(subscriber);
        accumulator.onSubscribe(new NoOpSubscription());

        RecordWarcUniversal individualRecord = createRecordWithSource(
            "xxh128:1234", "http://example.com", "crawl1.wet.gz", "2026-01-01T10:00:00Z"
        );

        // In merge mode, individual records should be rejected/ignored
        runWithLoggerMuted(WarcAccumulatorDeduplicateDoet.class,
            () -> accumulator.onNext(individualRecord));
        accumulator.onComplete();

        // Should emit nothing (record rejected)
        assertEquals(0, subscriber.items.size());
    }

    // Test 9: Multiple Batches Sequential Processing
    @Test
    @DisplayName("Should process multiple batches sequentially")
    void testMultipleBatchesSequential() {
        WarcAccumulatorDeduplicateDoet accumulator = createMergeAccumulator();
        TestSubscriber subscriber = new TestSubscriber();
        accumulator.subscribe(subscriber);
        accumulator.onSubscribe(new NoOpSubscription());

        // Batch 1: digest A
        String digestA = "xxh128:aaaaaaaaaaaaaaaa";
        RecordBatch batch1 = new RecordBatch(
            digestA,
            Set.of(createRecordWithSource(digestA, "http://a.com", "crawl1.wet.gz", "2026-01-01T10:00:00Z")),
            Instant.parse("2026-01-01T10:00:00Z"),
            Instant.parse("2026-01-01T10:00:00Z")
        );

        // Batch 2: digest B
        String digestB = "xxh128:bbbbbbbbbbbbbbbb";
        RecordBatch batch2 = new RecordBatch(
            digestB,
            Set.of(createRecordWithSource(digestB, "http://b.com", "crawl2.wet.gz", "2026-02-01T10:00:00Z")),
            Instant.parse("2026-02-01T10:00:00Z"),
            Instant.parse("2026-02-01T10:00:00Z")
        );

        accumulator.onNext(batch1);
        accumulator.onNext(batch2);
        accumulator.onComplete();

        assertEquals(2, subscriber.items.size());
    }

    // Test 10: Batch Size Tracking
    @Test
    @DisplayName("Should preserve all URLs in batch processing")
    void testBatchSizeTracking() {
        WarcAccumulatorDeduplicateDoet accumulator = createMergeAccumulator();
        TestSubscriber subscriber = new TestSubscriber();
        accumulator.subscribe(subscriber);
        accumulator.onSubscribe(new NoOpSubscription());

        String digest = "xxh128:1234567890abcdef";
        RecordWarcUniversal r1 = createRecordWithSource(
            digest, "http://example.com/1", "crawl1.wet.gz", "2026-01-01T10:00:00Z"
        );
        RecordWarcUniversal r2 = createRecordWithSource(
            digest, "http://example.com/2", "crawl1.wet.gz", "2026-01-01T11:00:00Z"
        );
        RecordWarcUniversal r3 = createRecordWithSource(
            digest, "http://example.com/3", "crawl1.wet.gz", "2026-01-01T12:00:00Z"
        );

        RecordBatch batch = new RecordBatch(
            digest,
            Set.of(r1, r2, r3),
            Instant.parse("2026-01-01T10:00:00Z"),
            Instant.parse("2026-01-01T12:00:00Z")
        );

        accumulator.onNext(batch);
        accumulator.onComplete();

        assertEquals(3, subscriber.items.size());
        // Batch of 3 distinct URLs is preserved as 3 output records.
    }

    // Test 11: URL scope split batches
    @Test
    @DisplayName("Should merge URL-scope provenance when primary and scan arrive in separate batches")
    void testUrlScopeSplitBatchesMerged() {
        WarcAccumulatorDeduplicateDoet accumulator = createUrlScopeAccumulator();
        TestSubscriber subscriber = new TestSubscriber();
        accumulator.subscribe(subscriber);
        accumulator.onSubscribe(new NoOpSubscription());

        String digest = "xxh128:splitbatchdigest";
        String uri = "http://example.com/page";

        RecordWarcUniversal primaryRecord = createRecordWithSource(
            digest, uri, "crawl1.wet.gz", "2026-01-01T10:00:00Z"
        );
        RecordWarcUniversal scanRecord = createRecordWithSource(
            digest, uri, "crawl2.wet.gz", "2026-02-01T10:00:00Z"
        );

        RecordBatch primaryBatch = new RecordBatch(
            digest,
            Set.of(primaryRecord),
            Instant.parse("2026-01-01T10:00:00Z"),
            Instant.parse("2026-01-01T10:00:00Z")
        );
        RecordBatch scanBatch = new RecordBatch(
            digest,
            Set.of(scanRecord),
            Instant.parse("2026-02-01T10:00:00Z"),
            Instant.parse("2026-02-01T10:00:00Z")
        );

        accumulator.onNext(primaryBatch);
        accumulator.onNext(scanBatch);
        accumulator.onComplete();

        assertEquals(1, subscriber.items.size());
        assertProvenance(subscriber.items.get(0), "merged");
        assertEquals("url", subscriber.items.get(0).headers().get("nac-deduplicated"));
    }

    // Test 12: URL-scope ordering
    @Test
    @DisplayName("Should emit URL scope records ordered by target URI")
    void testUrlScopeOrderingByUri() {
        WarcAccumulatorDeduplicateDoet accumulator = createUrlScopeAccumulator();
        TestSubscriber subscriber = new TestSubscriber();
        accumulator.subscribe(subscriber);
        accumulator.onSubscribe(new NoOpSubscription());

        RecordWarcUniversal r1 = createRecordWithSource(
            "xxh128:aa", "http://z.example/page", "crawl1.wet.gz", "2026-01-01T10:00:00Z"
        );
        RecordWarcUniversal r2 = createRecordWithSource(
            "xxh128:bb", "http://a.example/page", "crawl1.wet.gz", "2026-01-01T10:00:00Z"
        );
        RecordWarcUniversal r3 = createRecordWithSource(
            "xxh128:cc", "http://m.example/page", "crawl1.wet.gz", "2026-01-01T10:00:00Z"
        );

        accumulator.onNext(new RecordBatch(
            "xxh128:aa", Set.of(r1),
            Instant.parse("2026-01-01T10:00:00Z"), Instant.parse("2026-01-01T10:00:00Z")));
        accumulator.onNext(new RecordBatch(
            "xxh128:bb", Set.of(r2),
            Instant.parse("2026-01-01T10:00:00Z"), Instant.parse("2026-01-01T10:00:00Z")));
        accumulator.onNext(new RecordBatch(
            "xxh128:cc", Set.of(r3),
            Instant.parse("2026-01-01T10:00:00Z"), Instant.parse("2026-01-01T10:00:00Z")));
        accumulator.onComplete();

        assertEquals(3, subscriber.items.size());
        assertEquals("http://a.example/page", subscriber.items.get(0).targetUri());
        assertEquals("http://m.example/page", subscriber.items.get(1).targetUri());
        assertEquals("http://z.example/page", subscriber.items.get(2).targetUri());
    }

    @Test
    @DisplayName("Should keep URL-scope deterministic ordering by URI then digest")
    void testUrlScopeOrderingUriThenDigest() {
        WarcAccumulatorDeduplicateDoet accumulator = createUrlScopeAccumulator();
        TestSubscriber subscriber = new TestSubscriber();
        accumulator.subscribe(subscriber);
        accumulator.onSubscribe(new NoOpSubscription());

        RecordWarcUniversal b = createRecordWithSource(
            "xxh128:b", "http://same.example/page", "crawl1.wet.gz", "2026-01-01T10:00:00Z");
        RecordWarcUniversal a = createRecordWithSource(
            "xxh128:a", "http://same.example/page", "crawl1.wet.gz", "2026-01-01T10:00:00Z");
        RecordWarcUniversal z = createRecordWithSource(
            "xxh128:z", "http://later.example/page", "crawl1.wet.gz", "2026-01-01T10:00:00Z");

        accumulator.onNext(new RecordBatch("xxh128:b", Set.of(b),
            Instant.parse("2026-01-01T10:00:00Z"), Instant.parse("2026-01-01T10:00:00Z")));
        accumulator.onNext(new RecordBatch("xxh128:a", Set.of(a),
            Instant.parse("2026-01-01T10:00:00Z"), Instant.parse("2026-01-01T10:00:00Z")));
        accumulator.onNext(new RecordBatch("xxh128:z", Set.of(z),
            Instant.parse("2026-01-01T10:00:00Z"), Instant.parse("2026-01-01T10:00:00Z")));
        accumulator.onComplete();

        assertEquals(3, subscriber.items.size());
        assertEquals("http://later.example/page", subscriber.items.get(0).targetUri());
        assertEquals("xxh128:z", subscriber.items.get(0).headers().get("WARC-Payload-Digest"));
        assertEquals("http://same.example/page", subscriber.items.get(1).targetUri());
        assertEquals("xxh128:a", subscriber.items.get(1).headers().get("WARC-Payload-Digest"));
        assertEquals("http://same.example/page", subscriber.items.get(2).targetUri());
        assertEquals("xxh128:b", subscriber.items.get(2).headers().get("WARC-Payload-Digest"));
    }

    @Test
    @DisplayName("Should keep migration counts stable in global and url scopes")
    void testMigrationCountsStableAcrossScopes() {
        List<RecordWarcUniversal> records = List.of(
            createRecordWithSource("xxh128:migrate", "http://old.example/doc", "crawl1.wet.gz", "2026-01-01T10:00:00Z"),
            createRecordWithSource("xxh128:migrate", "http://new.example/doc", "crawl2.wet.gz", "2026-02-01T10:00:00Z"));

        List<RecordWarcUniversal> globalOut = runWithScope("global", records);
        List<RecordWarcUniversal> urlOut = runWithScope("url", records);

        assertEquals(2, globalOut.size());
        assertEquals(2, urlOut.size());

        assertEquals(1, globalOut.stream().filter(r -> "base-only".equals(r.headers().get("NAC-Merge-Result"))).count());
        assertEquals(1, globalOut.stream().filter(r -> "new".equals(r.headers().get("NAC-Merge-Result"))).count());
        assertEquals(1, urlOut.stream().filter(r -> "base-only".equals(r.headers().get("NAC-Merge-Result"))).count());
        assertEquals(1, urlOut.stream().filter(r -> "new".equals(r.headers().get("NAC-Merge-Result"))).count());
    }

    @Test
    @DisplayName("Should always emit merge provenance and dedup scope headers")
    void testProvenanceHeadersAlwaysSet() {
        WarcAccumulatorDeduplicateDoet accumulator = createUrlScopeAccumulator();
        TestSubscriber subscriber = new TestSubscriber();
        accumulator.subscribe(subscriber);
        accumulator.onSubscribe(new NoOpSubscription());

        // merged
        accumulator.onNext(new RecordBatch(
            "xxh128:merged",
            Set.of(
                createRecordWithSource("xxh128:merged", "http://example.com/merged", "crawl1.wet.gz", "2026-01-01T10:00:00Z"),
                createRecordWithSource("xxh128:merged", "http://example.com/merged", "crawl2.wet.gz", "2026-02-01T10:00:00Z")),
            Instant.parse("2026-01-01T10:00:00Z"),
            Instant.parse("2026-02-01T10:00:00Z")));

        // base-only
        accumulator.onNext(new RecordBatch(
            "xxh128:base",
            Set.of(createRecordWithSource("xxh128:base", "http://example.com/base", "crawl1.wet.gz", "2026-01-01T10:00:00Z")),
            Instant.parse("2026-01-01T10:00:00Z"),
            Instant.parse("2026-01-01T10:00:00Z")));

        // new
        accumulator.onNext(new RecordBatch(
            "xxh128:new",
            Set.of(createRecordWithSource("xxh128:new", "http://example.com/new", "crawl2.wet.gz", "2026-02-01T10:00:00Z")),
            Instant.parse("2026-02-01T10:00:00Z"),
            Instant.parse("2026-02-01T10:00:00Z")));
        accumulator.onComplete();

        assertEquals(3, subscriber.items.size());
        for (RecordWarcUniversal r : subscriber.items) {
            assertNotNull(r.headers().get("NAC-Merge-Result"));
            assertNotNull(r.headers().get("nac-deduplicated"));
            assertEquals("url", r.headers().get("nac-deduplicated"));
        }
    }

    @Test
    @DisplayName("Should emit temporal headers with monotonic first/last seen")
    void testTemporalHeadersMonotonic() {
        WarcAccumulatorDeduplicateDoet accumulator = createUrlScopeAccumulator();
        TestSubscriber subscriber = new TestSubscriber();
        accumulator.subscribe(subscriber);
        accumulator.onSubscribe(new NoOpSubscription());

        accumulator.onNext(new RecordBatch(
            "xxh128:mono",
            Set.of(
                createRecordWithSource("xxh128:mono", "http://example.com/mono", "crawl1.wet.gz", "2026-01-01T10:00:00Z"),
                createRecordWithSource("xxh128:mono", "http://example.com/mono", "crawl2.wet.gz", "2026-02-01T10:00:00Z")),
            Instant.parse("2026-01-01T10:00:00Z"),
            Instant.parse("2026-02-01T10:00:00Z")));
        accumulator.onComplete();

        assertEquals(1, subscriber.items.size());
        RecordWarcUniversal out = subscriber.items.get(0);
        Instant firstSeen = Instant.parse(out.headers().get("X-NAC-First-Seen"));
        Instant lastSeen = Instant.parse(out.headers().get("X-NAC-Last-Seen"));
        assertTrue(!firstSeen.isAfter(lastSeen), "first_seen must be <= last_seen");
    }

    @Test
    @DisplayName("Should produce deterministic merge output across repeated runs")
    void testDeterministicOutputAcrossRepeatedRuns() {
        List<RecordWarcUniversal> records = List.of(
            createRecordWithSource("xxh128:r2", "http://z.example", "crawl1.wet.gz", "2026-01-01T10:00:00Z"),
            createRecordWithSource("xxh128:r1", "http://a.example", "crawl2.wet.gz", "2026-01-01T10:00:00Z"),
            createRecordWithSource("xxh128:r1", "http://a.example", "crawl1.wet.gz", "2026-01-01T09:00:00Z"));

        List<String> run1 = materializeForComparison(runWithScope("url", records));
        List<String> run2 = materializeForComparison(runWithScope("url", records));

        assertEquals(run1, run2);
    }

    private List<RecordWarcUniversal> runWithScope(String scope, List<RecordWarcUniversal> records) {
        Path dbPath = tempDir.resolve("rocksdb-" + scope + "-" + System.nanoTime());
        WarcAccumulatorDeduplicateDoet accumulator = new WarcAccumulatorDeduplicateDoet();
        Map<String, Object> config = new HashMap<>();
        config.put("doet-merge", true);
        config.put("deduplicate-scope", scope);
        config.put("rocksdb-path", dbPath.toString());
        config.put("primary-file", "crawl1.*");
        accumulator.configure(config);

        TestSubscriber subscriber = new TestSubscriber();
        accumulator.subscribe(subscriber);
        accumulator.onSubscribe(new NoOpSubscription());

        Map<String, List<RecordWarcUniversal>> byDigest = new HashMap<>();
        for (RecordWarcUniversal record : records) {
            byDigest.computeIfAbsent(record.headers().get("WARC-Payload-Digest"), k -> new ArrayList<>()).add(record);
        }

        List<String> digests = new ArrayList<>(byDigest.keySet());
        digests.sort(Comparator.naturalOrder());
        for (String digest : digests) {
            List<RecordWarcUniversal> group = byDigest.get(digest);
            Instant min = group.stream().map(r -> Instant.parse(r.headers().get("WARC-Date"))).min(Comparator.naturalOrder()).orElseThrow();
            Instant max = group.stream().map(r -> Instant.parse(r.headers().get("WARC-Date"))).max(Comparator.naturalOrder()).orElseThrow();
            accumulator.onNext(new RecordBatch(digest, Set.copyOf(group), min, max));
        }
        accumulator.onComplete();
        return subscriber.items;
    }

    private List<String> materializeForComparison(List<RecordWarcUniversal> outputs) {
        List<String> lines = new ArrayList<>();
        for (RecordWarcUniversal r : outputs) {
            lines.add(
                r.headers().get("WARC-Target-URI") + "|" +
                r.headers().get("WARC-Payload-Digest") + "|" +
                r.headers().get("NAC-Merge-Result") + "|" +
                r.headers().get("X-NAC-First-Seen") + "|" +
                r.headers().get("X-NAC-Last-Seen"));
        }
        return lines;
    }

    // Helper Methods

    private WarcAccumulatorDeduplicateDoet createMergeAccumulator() {
        Path dbPath = tempDir.resolve("rocksdb-" + System.nanoTime());
        WarcAccumulatorDeduplicateDoet accumulator = new WarcAccumulatorDeduplicateDoet();
        Map<String, Object> config = new HashMap<>();
        config.put("doet-merge", true);
        config.put("deduplicate-scope", "global");
        config.put("rocksdb-path", dbPath.toString());
        config.put("primary-file", "crawl1.*");  // Fixed: was "primary-file-pattern"
        accumulator.configure(config);
        return accumulator;
    }

    private WarcAccumulatorDeduplicateDoet createNonMergeAccumulator() {
        Path dbPath = tempDir.resolve("rocksdb-non-merge-" + System.nanoTime());
        WarcAccumulatorDeduplicateDoet accumulator = new WarcAccumulatorDeduplicateDoet();
        Map<String, Object> config = new HashMap<>();
        config.put("doet-merge", false);
        config.put("deduplicate-scope", "global");
        config.put("rocksdb-path", dbPath.toString());
        accumulator.configure(config);
        return accumulator;
    }

    private WarcAccumulatorDeduplicateDoet createUrlScopeAccumulator() {
        Path dbPath = tempDir.resolve("rocksdb-" + System.nanoTime());
        WarcAccumulatorDeduplicateDoet accumulator = new WarcAccumulatorDeduplicateDoet();
        Map<String, Object> config = new HashMap<>();
        config.put("doet-merge", true);
        config.put("deduplicate-scope", "url");
        config.put("rocksdb-path", dbPath.toString());
        config.put("primary-file", "crawl1.*");  // Fixed: was "primary-file-pattern"
        accumulator.configure(config);
        return accumulator;
    }

    private RecordWarcUniversal createRecordWithSource(
            String digest, String uri, String sourceFile, String date) {
        String raw = "WARC/1.0\r\n" +
            "WARC-Type: conversion\r\n" +
            "WARC-Target-URI: " + uri + "\r\n" +
            "WARC-Date: " + date + "\r\n" +
            "WARC-Payload-Digest: " + digest + "\r\n" +
            "X-Source-Warc: " + sourceFile + "\r\n" +
            "Content-Type: text/plain\r\n" +
            "Content-Length: 12\r\n" +
            "\r\n" +
            "Test content\r\n\r\n";

        Map<String, String> headers = new HashMap<>();
        headers.put("WARC-Type", "conversion");
        headers.put("WARC-Target-URI", uri);
        headers.put("WARC-Date", date);
        headers.put("WARC-Payload-Digest", digest);
        headers.put("X-Source-Warc", sourceFile);
        headers.put("Content-Type", "text/plain");

        return new RecordWarcUniversal("conversion", headers, raw.getBytes(StandardCharsets.UTF_8));
    }

    private void assertProvenance(RecordWarcUniversal r, String expected) {
        assertEquals(expected, r.headers().get("NAC-Merge-Result"),
            "Digest " + r.headers().get("WARC-Payload-Digest") + " expected " + expected);
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
