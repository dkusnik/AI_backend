package pl.gov.nac.warc.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Flow;
import java.util.zip.GZIPInputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import pl.gov.nac.warc.consumers.ConsumerCodec;
import pl.gov.nac.warc.processors.WarcAccumulatorDeduplicateDoet;
import pl.gov.nac.warc.records.RecordBatch;
import pl.gov.nac.warc.records.warc.RecordWarcUniversal;
import pl.gov.nac.warc.reactive.Metrics;

class DoetMergeIntegrationTest {

  @TempDir
  Path tempDir;

  @Test
  void testFullMergeFlow() throws IOException {
    Metrics.reset();
    Path baseOut = tempDir.resolve("new-baseline.wet.gz");
    Path diffOut = tempDir.resolve("diff.wet.gz");

    // 1. Initialize Consumer
    ConsumerCodec consumer = new ConsumerCodec();
    consumer.configure(Map.of(
        "file", baseOut.toString(),
        "diff-output", diffOut.toString(),
        "split-provenance", true));
    consumer.beforeCheck(Map.of());

    // 2. Initialize Processor (Accumulator)
    WarcAccumulatorDeduplicateDoet processor = new WarcAccumulatorDeduplicateDoet();
    processor.configure(Map.of(
        "doet-merge", true,
        "primary-file", "baseline\\.warc"));

    // 3. Chain them: Consumer (Subscriber) subscribes to Processor (Publisher)
    processor.subscribe(consumer);

    // In reactive streams, the subscriber must request items via the subscription.
    // ConsumerWarcBase.onSubscribe calls request(Long.MAX_VALUE).
    // But we need to call startConsuming() to initialize the output streams.
    consumer.onSubscribe(new NoOpSubscription());
    consumer.startConsuming();

    // 4. Feed records into the processor (acting as if from a producer)
    // Merge mode now requires RecordBatch input

    // Scenario A: Baseline Primary (from baseline.warc)
    // Scenario B: Baseline Refresh (same URI, same digest, from scan.warc)
    // Both share digest sha256:1, so they go in same batch
    List<RecordWarcUniversal> batch1Records = List.of(
        createRecord("sha256:1", "Content 1", "baseline.warc", "http://a.com/1"),
        createRecord("sha256:1", "Content 1", "scan.warc", "http://a.com/1"));

    // Scenario C: New Content (new digest)
    // Should go to BOTH baseline and diff
    List<RecordWarcUniversal> batch2Records = List.of(
        createRecord("sha256:2", "Content 2", "scan.warc", "http://a.com/2"));

    // Create batches and send to processor
    List<RecordBatch> batches = createBatches(batch1Records);
    batches.addAll(createBatches(batch2Records));

    for (RecordBatch batch : batches) {
      processor.onNext(batch);
    }

    processor.onComplete();
    assertEquals(0, consumer.publishOutputs());

    // 5. Verify Outputs exist and are non-empty
    assertTrue(Files.exists(baseOut), "Baseline output should exist");
    assertTrue(Files.size(baseOut) > 0, "Baseline output should not be empty");
    assertTrue(Files.exists(diffOut), "Diff output should exist");
    assertTrue(Files.size(diffOut) > 0, "Diff output should not be empty");

    String baseContent = readGzip(baseOut);
    String diffContent = readGzip(diffOut);

    // Baseline should have entries for all URIs
    assertContains(baseContent, "http://a.com/1");
    assertContains(baseContent, "http://a.com/2");

    // Check Diff URIs
    // Content 1 at original URI (Scenario B) - merged provenance
    // Content 2 (Scenario C) should be in diff - new provenance

    assertContainsBinary(diffContent, "http://a.com/2"); // New content

    // Note: URI-changed detection is not yet implemented in RecordBatch handlers
    // See Task #66 for collision detection and advanced provenance features
  }

  private RecordWarcUniversal createRecord(String digest, String content, String filename, String uri) {
    Map<String, String> headers = new java.util.LinkedHashMap<>();
    headers.put("WARC-Block-Digest", digest);
    headers.put("X-Source-Warc", filename);
    headers.put("WARC-Target-URI", uri);
    headers.put("WARC-Date", "2026-01-27T10:00:00Z");
    headers.put("WARC-Type", "conversion");
    headers.put("Content-Length", String.valueOf(content.length()));

    String raw = String.format("WARC/1.0\r\n" +
        "WARC-Type: conversion\r\n" +
        "WARC-Target-URI: %s\r\n" +
        "WARC-Date: 2026-01-27T10:00:00Z\r\n" +
        "WARC-Block-Digest: %s\r\n" +
        "X-Source-Warc: %s\r\n" +
        "Content-Length: %d\r\n" +
        "\r\n" +
        "%s\r\n\r\n", uri, digest, filename, content.length(), content);

    return new RecordWarcUniversal("conversion", headers, raw.getBytes(StandardCharsets.UTF_8));
  }

  private String readGzip(Path p) throws IOException {
    try (GZIPInputStream is = new GZIPInputStream(Files.newInputStream(p))) {
      return new String(is.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  private void assertContains(String content, String s) {
    assertTrue(content.contains(s), "Content should contain: " + s);
  }

  private void assertContainsBinary(String content, String s) {
    assertTrue(content.contains(s), "Content should contain string: " + s);
  }

  private void assertContainsHeader(String content, String header, String value) {
    String search = header + ": " + value;
    assertTrue(content.contains(search), "Content should contain header: " + search);
  }

  /**
   * Groups records by digest and creates RecordBatch objects.
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

  static class NoOpSubscription implements Flow.Subscription {
    @Override
    public void request(long n) {
    }

    @Override
    public void cancel() {
    }
  }
}
