package pl.gov.nac.warc.processors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static pl.gov.nac.warc.testutil.ExpectedLogSilencer.runWithLoggerMuted;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import pl.gov.nac.warc.records.warc.RecordWarcUniversal;

class WarcDecoratorLanguageDetectTest {

  @Test
  void testPolishDetection() throws InterruptedException {
    WarcDecoratorLanguageDetect processor = new WarcDecoratorLanguageDetect();
    processor.configure(Map.of("min-text-length", 5)); // Lower limit for test

    TestSubscriber subscriber = new TestSubscriber();
    processor.subscribe(subscriber);
    processor.onSubscribe(new TestSubscription());

    // Polish text
    String polishText = "To jest przykładowy tekst w języku polskim. Powinien zostać wykryty jako polski.";
    processor.onNext(createRecord("pl1", polishText));

    // Check results
    assertEquals(1, subscriber.items.size());
    RecordWarcUniversal result = subscriber.items.get(0);

    assertEquals("pl", result.headers().get("WARC-Identified-Content-Language"));
    // Confidence should be high
    float confidence = Float.parseFloat(result.headers().get("WARC-Language-Confidence"));
    assertTrue(confidence > 0.8, "Confidence should be > 0.8 for clear Polish text");
  }

  @Test
  void testEnglishDetection() {
    WarcDecoratorLanguageDetect processor = new WarcDecoratorLanguageDetect();
    processor.configure(Map.of("min-text-length", 5));

    TestSubscriber subscriber = new TestSubscriber();
    processor.subscribe(subscriber);
    processor.onSubscribe(new TestSubscription());

    // English text
    String englishText = "This is a sample text in English language. It should be detected as English.";
    processor.onNext(createRecord("en1", englishText));

    assertEquals(1, subscriber.items.size());
    RecordWarcUniversal result = subscriber.items.get(0);

    assertEquals("en", result.headers().get("WARC-Identified-Content-Language"));
  }

  @Test
  void testShortText() {
    WarcDecoratorLanguageDetect processor = new WarcDecoratorLanguageDetect();
    processor.configure(Map.of("min-text-length", 50)); // Default high limit

    TestSubscriber subscriber = new TestSubscriber();
    processor.subscribe(subscriber);
    processor.onSubscribe(new TestSubscription());

    // Short text
    String shortText = "Too short.";
    processor.onNext(createRecord("short1", shortText));

    assertEquals(1, subscriber.items.size());
    RecordWarcUniversal result = subscriber.items.get(0);

    assertEquals("und/short", result.headers().get("WARC-Identified-Content-Language"));
    assertEquals("0.00", result.headers().get("WARC-Language-Confidence"));
  }

  @Test
  void testMinTextLengthBoundaryShortVsLong() {
    WarcDecoratorLanguageDetect processor = new WarcDecoratorLanguageDetect();
    processor.configure(Map.of("min-text-length", 20));

    TestSubscriber subscriber = new TestSubscriber();
    processor.subscribe(subscriber);
    processor.onSubscribe(new TestSubscription());

    processor.onNext(createRecord("short-boundary", "too short"));
    processor.onNext(createRecord("long-boundary",
        "This sentence is long enough for deterministic language detection in English."));

    assertEquals(2, subscriber.items.size());
    assertEquals("und/short", subscriber.items.get(0).headers().get("WARC-Identified-Content-Language"));
    assertEquals("en", subscriber.items.get(1).headers().get("WARC-Identified-Content-Language"));
  }

  @Test
  void testPassthrough() {
    WarcDecoratorLanguageDetect processor = new WarcDecoratorLanguageDetect();
    processor.configure(Map.of());

    TestSubscriber subscriber = new TestSubscriber();
    processor.subscribe(subscriber);
    processor.onSubscribe(new TestSubscription());

    // Warcinfo record
    RecordWarcUniversal warcinfo = new RecordWarcUniversal("warcinfo", Map.of(), "info".getBytes());
    processor.onNext(warcinfo);

    assertEquals(1, subscriber.items.size());
    RecordWarcUniversal result = subscriber.items.get(0);

    // Should NOT have language headers
    assertFalse(result.headers().containsKey("WARC-Identified-Content-Language"));
  }

  // -------------------------------------------------------------------------
  // H-1 (T-219): onError() must shut down worker pool — RED TEST
  // -------------------------------------------------------------------------

  /**
   * When onError() is called while fastText workers are running, all worker
   * subprocesses must be destroyed. Previously onError() forwarded the error
   * downstream but never called shutdownWorkerPool(), leaking subprocesses.
   */
  @Test
  void testOnErrorShutsDownWorkerPool(@TempDir Path tempDir) throws Exception {
    // Long-running script: responds to every line with a label
    Path script = tempDir.resolve("ft_loop.sh");
    Files.writeString(script,
        "#!/bin/sh\nwhile true; do read line; echo '__label__pl 0.99'; done\n");
    assertTrue(script.toFile().setExecutable(true));

    WarcDecoratorLanguageDetect processor = new WarcDecoratorLanguageDetect();
    processor.configure(Map.of(
        "use-fasttext", true,
        "fasttext-path", script.toString(),
        "fasttext-model-path", "/dev/null",
        "fasttext-process-count", 2));

    AtomicReference<Throwable> capturedError = new AtomicReference<>();
    TestSubscriber sub = new TestSubscriber() {
      @Override
      public void onError(Throwable t) { capturedError.set(t); }
    };
    processor.subscribe(sub);
    processor.onSubscribe(new TestSubscription());

    processor.onError(new RuntimeException("upstream failure"));
    Thread.sleep(300);

    assertNotNull(capturedError.get(), "Error must be forwarded downstream");

    // Verify no ft_loop.sh processes remain alive.
    // Check process arguments (not command, which is just "/bin/sh").
    boolean anyAlive = ProcessHandle.allProcesses()
        .anyMatch(p -> p.info().arguments()
            .map(args -> java.util.Arrays.stream(args)
                .anyMatch(a -> a.contains("ft_loop")))
            .orElse(false));
    assertFalse(anyAlive,
        "All fastText worker processes must be destroyed after onError()");
  }

  // -------------------------------------------------------------------------
  // C-3 (T-216): Dead worker must NOT be returned to pool after failed replace
  // -------------------------------------------------------------------------

  /**
   * When a fastText worker dies and the replacement process also exits quickly
   * (simulating a failed replacement), subsequent detection calls must not
   * block or corrupt the pool. The fix uses a boolean flag in the finally block
   * so the dead worker is never returned.
   *
   * <p>This test verifies the non-blocking property: with 1 worker and a
   * die-after-one-call script, the second detection call must complete quickly
   * (no hang on workerPool.take()) regardless of whether replacement succeeds.
   */
  @Test
  void testDeadWorkerDoesNotHangPool(@TempDir Path tempDir) throws Exception {
    // Script that reads one line then exits
    Path script = tempDir.resolve("fake_ft.sh");
    Files.writeString(script, "#!/bin/sh\nread line\nexit 1\n");
    assertTrue(script.toFile().setExecutable(true));

    WarcDecoratorLanguageDetect processor = new WarcDecoratorLanguageDetect();
    processor.configure(Map.of(
        "use-fasttext", true,
        "fasttext-path", script.toString(),
        "fasttext-model-path", "/dev/null",
        "fasttext-process-count", 1));

    TestSubscriber sub = new TestSubscriber();
    processor.subscribe(sub);
    processor.onSubscribe(new TestSubscription());

    String text = "x".repeat(200);
    // Both calls must complete within 4s — no hang on workerPool.take()
    CompletableFuture<Void> first = CompletableFuture.runAsync(
        () -> runWithLoggerMuted(WarcDecoratorLanguageDetect.class,
            () -> processor.onNext(createRecord("http://a.com", text))));
    first.get(2, TimeUnit.SECONDS);

    CompletableFuture<Void> second = CompletableFuture.runAsync(
        () -> runWithLoggerMuted(WarcDecoratorLanguageDetect.class,
            () -> processor.onNext(createRecord("http://b.com", text))));
    second.get(2, TimeUnit.SECONDS);

    assertEquals(2, sub.items.size(),
        "Both records must be forwarded downstream even when fastText fails");
  }

  // -------------------------------------------------------------------------
  // H-2 (T-220): Concurrent Tika detection must not corrupt results
  // -------------------------------------------------------------------------

  /**
   * Multiple virtual threads calling onNext() concurrently must each get a
   * correct language result. If LanguageDetector is shared without synchronisation
   * its internal mutable state could cause wrong results.
   */
  @Test
  void testConcurrentTikaDetectionDoesNotCorruptResults() throws InterruptedException {
    WarcDecoratorLanguageDetect processor = new WarcDecoratorLanguageDetect();
    processor.configure(Map.of("min-text-length", 10));

    CopyOnWriteArrayList<String> results = new CopyOnWriteArrayList<>();
    processor.subscribe(new TestSubscriber() {
      @Override
      public void onNext(RecordWarcUniversal item) {
        results.add(item.headers().get("WARC-Identified-Content-Language"));
      }
    });
    processor.onSubscribe(new TestSubscription());

    String polishText = "Ministerstwo Spraw Zagranicznych Rzeczypospolitej Polskiej należy do instytucji państwowych.";
    int count = 50;
    CountDownLatch latch = new CountDownLatch(count);
    try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
      for (int i = 0; i < count; i++) {
        final int idx = i;
        pool.submit(() -> {
          processor.onNext(createRecord("http://test.com/" + idx, polishText));
          latch.countDown();
        });
      }
      assertTrue(latch.await(10, TimeUnit.SECONDS),
          "All concurrent detections must complete within 10s");
    }

    assertEquals(count, results.size(), "All records must be forwarded");
    long wrong = results.stream()
        .filter(lang -> lang != null && !lang.startsWith("pl") && !lang.startsWith("und"))
        .count();
    assertEquals(0, wrong,
        "No record should get a wrong non-Polish language result; got: " + results);
  }

  // Helper Methods

  private RecordWarcUniversal createRecord(String id, String text) {
    // Determine content type and raw bytes.
    // For "conversion" records, the raw bytes are the text itself.
    Map<String, String> headers = new LinkedHashMap<>();
    headers.put("WARC-Type", "conversion");
    headers.put("WARC-Record-ID", "<urn:uuid:" + id + ">");
    headers.put("Content-Type", "text/plain");

    return new RecordWarcUniversal("conversion", headers, text.getBytes(StandardCharsets.UTF_8));
  }

  static class TestSubscriber implements Flow.Subscriber<RecordWarcUniversal> {
    final List<RecordWarcUniversal> items = new ArrayList<>();

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      subscription.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(RecordWarcUniversal item) {
      items.add(item);
    }

    @Override
    public void onError(Throwable throwable) {
      throw new RuntimeException(throwable);
    }

    @Override
    public void onComplete() {
    }
  }

  static class TestSubscription implements Flow.Subscription {
    @Override
    public void request(long n) {
    }

    @Override
    public void cancel() {
    }
  }
}
