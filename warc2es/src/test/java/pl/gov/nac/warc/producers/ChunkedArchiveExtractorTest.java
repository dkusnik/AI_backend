package pl.gov.nac.warc.producers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static pl.gov.nac.warc.testutil.ExpectedLogSilencer.runWithLoggerMuted;

import java.util.concurrent.atomic.AtomicInteger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import pl.gov.nac.warc.records.RecordBatch;
import pl.gov.nac.warc.records.warc.RecordWarcUniversal;

public class ChunkedArchiveExtractorTest {

  @TempDir
  Path tempDir;

  @Test
  public void testKWayMerge() throws Exception {
    // Setup 3 sorted files
    // Digests: A, C, E, G, I
    Path f1 = createWarcFile("f1.warc",
        pair("sha256:a", "A"),
        pair("sha256:c", "C"),
        pair("sha256:i", "I"));

    // Digests: B, D
    Path f2 = createWarcFile("f2.warc",
        pair("sha256:b", "B"),
        pair("sha256:d", "D"));

    // Digests: E, F, H
    Path f3 = createWarcFile("f3.warc",
        pair("sha256:e", "E"),
        pair("sha256:f", "F"),
        pair("sha256:h", "H"));

    ChunkedArchiveExtractor producer = new ChunkedArchiveExtractor();
    producer.configure(Map.of(
        "inputFiles", List.of(f1.toString(), f2.toString(), f3.toString()),
        "doet-merge", true));

    TestSubscriber subscriber = new TestSubscriber();
    producer.subscribe(subscriber);
    producer.startProducing();

    subscriber.awaitComplete();

    // Expected Order: a, b, c, d, e, f, h, i
    List<String> expectedDigests = List.of(
        "sha256:a", "sha256:b", "sha256:c", "sha256:d",
        "sha256:e", "sha256:f", "sha256:h", "sha256:i");

    // In merge mode, ChunkedArchiveExtractor emits RecordBatch
    List<String> actualDigests = subscriber.batches.stream()
        .map(batch -> batch.sharedDigest())
        .toList();

    assertEquals(expectedDigests, actualDigests, "Merge order incorrect");
  }

  @Test
  public void testPanicOnUnsortedInput() throws Exception {
    // Unsorted: B comes before A
    Path f1 = createWarcFile("unsorted.warc",
        pair("sha256:b", "B"),
        pair("sha256:a", "A"));

    ChunkedArchiveExtractor producer = new ChunkedArchiveExtractor();
    producer.configure(Map.of(
        "inputFiles", List.of(f1.toString()),
        "doet-merge", true));

    TestSubscriber subscriber = new TestSubscriber();
    producer.subscribe(subscriber);
    runWithLoggerMuted(ChunkedArchiveExtractor.class, producer::startProducing);

    // We assume startProducing blocks/finishes for the test logic or subscriber
    // tracks error
    // The implementation calls onError

    assertTrue(subscriber.error.get() != null, "Should have errored");
    assertTrue(subscriber.error.get().getMessage().contains("PANIC"), "Error should be a PANIC");
  }

  // -------------------------------------------------------------------------
  // H-4 (T-222): request(n) must be honoured without dropping records
  // -------------------------------------------------------------------------

  /**
   * A temporary lack of demand must pause production, not discard the records that
   * arrive before the subscriber requests the rest.
   */
  @Test
  void testFiniteDemandPausesWithoutDroppingRecords() throws Exception {
    Path warc = createWarcFile("demand.warc",
        pair("sha256:d1", "Record1"),
        pair("sha256:d2", "Record2"),
        pair("sha256:d3", "Record3"));

    ChunkedArchiveExtractor producer = new ChunkedArchiveExtractor();
    producer.configure(Map.of("inputFiles", List.of(warc.toString()), "doet-merge", false));

    AtomicInteger received = new AtomicInteger();
    AtomicReference<Flow.Subscription> subscription = new AtomicReference<>();
    AtomicReference<Throwable> error = new AtomicReference<>();
    CountDownLatch firstRecord = new CountDownLatch(1);
    CountDownLatch terminal = new CountDownLatch(1);
    producer.subscribe(new Flow.Subscriber<>() {
      @Override
      public void onSubscribe(Flow.Subscription s) {
        subscription.set(s);
        s.request(1);
      }

      @Override
      public void onNext(Object item) {
        received.incrementAndGet();
        firstRecord.countDown();
      }

      @Override
      public void onError(Throwable t) {
        error.set(t);
        terminal.countDown();
      }

      @Override
      public void onComplete() {
        terminal.countDown();
      }
    });

    Thread production = Thread.ofVirtual().start(producer::startProducing);
    assertTrue(firstRecord.await(2, TimeUnit.SECONDS), "First requested record was not emitted");
    assertEquals(1, received.get(), "Producer emitted beyond the initial request(1)");

    subscription.get().request(2);
    assertTrue(terminal.await(2, TimeUnit.SECONDS), "Producer did not finish after demand resumed");
    production.join(2_000);

    assertFalse(production.isAlive(), "Producer remained blocked after sufficient demand");
    assertNull(error.get(), "Finite demand must not fail production");
    assertEquals(3, received.get(), "Records were discarded while downstream demand was zero");
  }

  @Test
  void testSequentialBypassFailureSignalsOnError() {
    Path missing = tempDir.resolve("missing.warc.gz");

    ChunkedArchiveExtractor producer = new ChunkedArchiveExtractor();
    producer.configure(Map.of(
        "inputFiles", List.of(missing.toString()),
        "globalConcurrencyCap", 1,
        "doet-merge", false));

    TestSubscriber subscriber = new TestSubscriber();
    producer.subscribe(subscriber);
    runWithLoggerMuted(ChunkedArchiveExtractor.class, producer::startProducing);

    assertTrue(subscriber.error.get() != null, "Sequential bypass failure must signal onError");
    assertFalse(subscriber.complete, "Producer must not signal onComplete after onError");
  }

  // --- Helpers ---

  private Path createWarcFile(String name, String[]... records) throws IOException {
    Path p = tempDir.resolve(name);
    StringBuilder sb = new StringBuilder();
    for (String[] pair : records) {
      String digest = pair[0];
      String content = pair[1];
      sb.append("WARC/1.0\r\n");
      sb.append("WARC-Type: response\r\n");
      sb.append("WARC-Target-URI: http://e.com\r\n");
      sb.append("WARC-Block-Digest: ").append(digest).append("\r\n");
      sb.append("Content-Length: ").append(content.length()).append("\r\n");
      sb.append("\r\n");
      sb.append(content);
      sb.append("\r\n\r\n");
    }
    Files.writeString(p, sb.toString());
    return p;
  }

  private String[] pair(String k, String v) {
    return new String[] { k, v };
  }

  private String getDigest(RecordWarcUniversal r) {
    return r.headers().get("warc-block-digest");
  }

  static class TestSubscriber implements Flow.Subscriber<Object> {
    List<RecordWarcUniversal> items = new ArrayList<>();
    List<RecordBatch> batches = new ArrayList<>();
    AtomicReference<Throwable> error = new AtomicReference<>();
    boolean complete = false;

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      subscription.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(Object item) {
      if (item instanceof RecordWarcUniversal r) {
        items.add(r);
      } else if (item instanceof RecordBatch batch) {
        batches.add(batch);
      }
    }

    @Override
    public void onError(Throwable throwable) {
      error.set(throwable);
    }

    @Override
    public void onComplete() {
      complete = true;
    }

    void awaitComplete() {
      // In synchronous mode (which startProducingMerged is), this is already done.
      // But if async, we'd wait.
    }
  }
}
