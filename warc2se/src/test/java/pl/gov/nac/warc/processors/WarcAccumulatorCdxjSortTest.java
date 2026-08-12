package pl.gov.nac.warc.processors;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import pl.gov.nac.warc.records.cdx.RecordCdxStructured;

class WarcAccumulatorCdxjSortTest {

    @TempDir
    java.nio.file.Path tempDir;

    @Test
    void testInMemorySorting() throws Exception {
        WarcAccumulatorCdxjSort sorter = new WarcAccumulatorCdxjSort();

        // Configure with small threshold to stay in-memory
        sorter.configure(Map.of(
                "memory-threshold-mb", 100,
                "max-records-in-memory", 1000,
                "rocksdb-path", tempDir.toString() + "/cdxj-sort"));

        // Collector for output records
        List<RecordCdxStructured> collected = new ArrayList<>();
        CountDownLatch completeLatch = new CountDownLatch(1);

        sorter.subscribe(new Flow.Subscriber<>() {
            private Flow.Subscription sub;

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                this.sub = subscription;
                subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(Object item) {
                if (item instanceof RecordCdxStructured cdx) {
                    collected.add(cdx);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                completeLatch.countDown();
            }

            @Override
            public void onComplete() {
                completeLatch.countDown();
            }
        });

        // Create unsorted records
        RecordCdxStructured rec1 = createRecord("org,example)/page1", "20240201000000");
        RecordCdxStructured rec2 = createRecord("com,example)/page2", "20240101000000");
        RecordCdxStructured rec3 = createRecord("com,example)/page1", "20240301000000");
        RecordCdxStructured rec4 = createRecord("com,example)/page1", "20240101000000");

        // Feed records in random order
        sorter.onSubscribe(new Flow.Subscription() {
            @Override
            public void request(long n) {
            }

            @Override
            public void cancel() {
            }
        });

        sorter.onNext(rec1);
        sorter.onNext(rec2);
        sorter.onNext(rec3);
        sorter.onNext(rec4);
        sorter.onComplete();

        // Wait for completion
        assertTrue(completeLatch.await(5, TimeUnit.SECONDS), "Sorting should complete");

        // Verify sorting
        assertEquals(4, collected.size(), "All records should be emitted");

        // Expected order:
        // 1. com,example)/page1 20240101000000
        // 2. com,example)/page1 20240301000000
        // 3. com,example)/page2 20240101000000
        // 4. org,example)/page1 20240201000000

        assertEquals("com,example)/page1", collected.get(0).surtKey());
        assertEquals("20240101000000", collected.get(0).timestamp());

        assertEquals("com,example)/page1", collected.get(1).surtKey());
        assertEquals("20240301000000", collected.get(1).timestamp());

        assertEquals("com,example)/page2", collected.get(2).surtKey());
        assertEquals("20240101000000", collected.get(2).timestamp());

        assertEquals("org,example)/page1", collected.get(3).surtKey());
        assertEquals("20240201000000", collected.get(3).timestamp());
    }

    @Test
    void testRocksDBSpill() throws Exception {
        WarcAccumulatorCdxjSort sorter = new WarcAccumulatorCdxjSort();

        // Configure with tiny threshold to force RocksDB
        sorter.configure(Map.of(
                "memory-threshold-mb", 1,
                "max-records-in-memory", 2, // Force spill after 2 records
                "rocksdb-path", tempDir.toString() + "/cdxj-sort"));

        List<RecordCdxStructured> collected = new ArrayList<>();
        CountDownLatch completeLatch = new CountDownLatch(1);

        sorter.subscribe(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(Object item) {
                if (item instanceof RecordCdxStructured cdx) {
                    collected.add(cdx);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                fail("Should not error: " + throwable.getMessage());
            }

            @Override
            public void onComplete() {
                completeLatch.countDown();
            }
        });

        // Create records
        RecordCdxStructured rec1 = createRecord("zzz,last)/page", "20240101000000");
        RecordCdxStructured rec2 = createRecord("aaa,first)/page", "20240101000000");
        RecordCdxStructured rec3 = createRecord("mmm,middle)/page", "20240101000000");

        sorter.onSubscribe(new Flow.Subscription() {
            @Override
            public void request(long n) {
            }

            @Override
            public void cancel() {
            }
        });

        sorter.onNext(rec1);
        sorter.onNext(rec2);
        sorter.onNext(rec3); // This should trigger RocksDB spill
        sorter.onComplete();

        assertTrue(completeLatch.await(10, TimeUnit.SECONDS), "Sorting should complete");

        // Verify sorting with RocksDB
        assertEquals(3, collected.size(), "All records should be emitted");
        assertEquals("aaa,first)/page", collected.get(0).surtKey());
        assertEquals("mmm,middle)/page", collected.get(1).surtKey());
        assertEquals("zzz,last)/page", collected.get(2).surtKey());
    }

    private RecordCdxStructured createRecord(String surt, String timestamp) {
        return new RecordCdxStructured(
                surt,
                timestamp,
                "http://example.com",
                "text/html",
                200,
                "sha256:abc123",
                0L,
                1000L,
                "test.warc.gz",
                Map.of(),
                true);
    }
}
