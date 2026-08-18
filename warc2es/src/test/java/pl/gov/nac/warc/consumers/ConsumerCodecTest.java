package pl.gov.nac.warc.consumers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Flow;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import pl.gov.nac.warc.records.warc.RecordWarcUniversal;
import pl.gov.nac.warc.reactive.Metrics;

// Task-ID: T-201
class ConsumerCodecTest {

  @TempDir
  Path tempDir;

  @BeforeEach
  void resetMetrics() {
    Metrics.reset();
  }

  @Test
  void testSplitProvenance() throws IOException {
    Path out = tempDir.resolve("out.warc.gz");

    ConsumerCodec consumer = new ConsumerCodec();
    consumer.configure(Map.of(
        "file", out.toString(),
        "diff-output", tempDir.resolve("out-merged.warc.gz").toString(), // NEW
        "split-provenance", true));
    consumer.beforeCheck(Map.of());

    TestSubscription sub = new TestSubscription();
    consumer.onSubscribe(sub);
    consumer.startConsuming();

    // 1. Base-only Record (primary file only)
    consumer.onNext(createRecord("p1", "base-only"));

    // 2. Merged Record (both primary and scan)
    consumer.onNext(createRecord("m1", "merged"));

    // 3. New Record (scan file only)
    consumer.onNext(createRecord("s1", "new"));

    consumer.onComplete();
    assertEquals(0, consumer.publishOutputs());

    // Check Main Output (Primary)
    assertTrue(Files.exists(out), "Main output should exist");
    long mainSize = Files.size(out);
    assertTrue(mainSize > 0, "Main output should not be empty");

    // Check Split Output (Merged)
    Path splitOut = tempDir.resolve("out-merged.warc.gz");
    assertTrue(Files.exists(splitOut), "Split output should exist");
    long splitSize = Files.size(splitOut);
    assertTrue(splitSize > 0, "Split output should not be empty");

    // Verify content routing:
    // - "base-only" → main only
    // - "merged" → BOTH main and split
    // - "new" → BOTH main and split

    String mainContent = readFile(out);

    assertTrue(mainContent.contains("WARC-Record-ID: <urn:uuid:p1>"), "Main should contain p1 (base-only)");
    assertTrue(mainContent.contains("WARC-Record-ID: <urn:uuid:m1>"), "Main should contain m1 (merged goes to BOTH)");
    assertTrue(mainContent.contains("WARC-Record-ID: <urn:uuid:s1>"), "Main should contain s1 (new goes to BOTH)");

    String splitContent = readFile(splitOut);

    assertTrue(!splitContent.contains("WARC-Record-ID: <urn:uuid:p1>"), "Split output should NOT contain p1 (base-only)");
    assertTrue(splitContent.contains("WARC-Record-ID: <urn:uuid:m1>"), "Split should contain m1 (merged)");
    assertTrue(splitContent.contains("WARC-Record-ID: <urn:uuid:s1>"), "Split should contain s1 (new)");
  }

  @Test
  void testRecordOrderWrittenToWarcinfoWhenConfigured() throws IOException {
    Path out = tempDir.resolve("ordered.doet.gz");
    ConsumerCodec consumer = new ConsumerCodec();
    consumer.configure(Map.of(
        "file", out.toString(),
        "format", "doet",
        "record-order", "surt-ascending"));
    consumer.beforeCheck(Map.of());
    consumer.onSubscribe(new TestSubscription());
    consumer.startConsuming();
    consumer.onNext(createRecord("order-configured", "new"));
    consumer.onComplete();
    assertEquals(0, consumer.publishOutputs());

    String content = readFile(out);
    assertTrue(content.contains("WARC-Type: warcinfo"), "warcinfo should be emitted");
    assertTrue(content.contains("NAC-record-order: surt-ascending"),
        "record-order should be propagated to warcinfo");
  }

  @Test
  void testRecordOrderNotWrittenWhenNotConfigured() throws IOException {
    Path out = tempDir.resolve("unordered.doet.gz");
    ConsumerCodec consumer = new ConsumerCodec();
    consumer.configure(Map.of(
        "file", out.toString(),
        "format", "doet"));
    consumer.beforeCheck(Map.of());
    consumer.onSubscribe(new TestSubscription());
    consumer.startConsuming();
    consumer.onNext(createRecord("order-default", "new"));
    consumer.onComplete();
    assertEquals(0, consumer.publishOutputs());

    String content = readFile(out);
    assertTrue(content.contains("WARC-Type: warcinfo"), "warcinfo should be emitted");
    assertTrue(!content.contains("NAC-record-order:"),
        "record-order should be omitted when config is absent");
  }

  @Test
  void testProvenancePairWrittenToGeneratedWarcinfo() throws IOException {
    Path out = tempDir.resolve("provenance.wet.gz");
    ConsumerCodec consumer = new ConsumerCodec();
    consumer.configure(Map.of("file", out.toString(), "format", "wet"));
    consumer.beforeCheck(Map.of());
    consumer.onSubscribe(new TestSubscription());
    consumer.startConsuming();
    RecordWarcUniversal record = createRecord("provenance", "new");
    record.headers().put("X-NAC-URL-ID", "site-1");
    record.headers().put("X-NAC-Crawl-ID", "crawl-2");
    consumer.onNext(record);
    consumer.onComplete();
    assertEquals(0, consumer.publishOutputs());

    String content = readFile(out);
    assertTrue(content.contains("X-NAC-URL-ID: site-1"));
    assertTrue(content.contains("X-NAC-Crawl-ID: crawl-2"));
  }

  @Test
  void publicationReportContainsAggregateAndPerArtifactWetStats() throws Exception {
    Path outputDir = tempDir.resolve("per-day");
    Path report = tempDir.resolve("publication.json");
    ConsumerCodec consumer = new ConsumerCodec();
    consumer.configure(Map.of(
        "file", outputDir.toString(),
        "output-format", "multi-warc",
        "output-name-template", "{source}.wet.gz",
        "compression", "none",
        "cdx-sidecar", false,
        "check-order", "off",
        "publication-report", report.toString()));
    assertTrue(consumer.beforeCheck(Map.of()));
    consumer.onSubscribe(new TestSubscription());
    consumer.startConsuming();

    consumer.onNext(createStatsRecord(
        "20260102", "second", "2026-01-02T03:04:05Z",
        "Text/HTML; charset=UTF-8", "PL"));
    consumer.onNext(createStatsRecord(
        "20260101", "first-day", "2026-01-01T01:02:03Z",
        "application/pdf", null));
    consumer.onComplete();
    assertEquals(0, consumer.publishOutputs());

    JsonNode stats = new ObjectMapper().readTree(report.toFile()).get("output_stats");
    assertEquals(2, stats.get("count").asLong(), "the generated WARCINFO records must not be counted");
    assertEquals("second".length() + "first-day".length(), stats.get("content_bytes").asLong());
    assertEquals(1, stats.get("mime_types").get("text/html").asLong());
    assertEquals(1, stats.get("mime_types").get("application/pdf").asLong());
    assertEquals(1, stats.get("languages").get("pl").asLong());
    assertEquals(1, stats.get("missing_language").asLong());
    assertEquals(0, stats.get("missing_mimetype").asLong());
    assertEquals("2026-01-01T01:02:03Z", stats.get("date_min").asText());
    assertEquals("2026-01-02T03:04:05Z", stats.get("date_max").asText());

    JsonNode artifacts = stats.get("artifacts");
    assertEquals(2, artifacts.size());
    assertEquals(outputDir.resolve("20260101.wet.gz").toString(), artifacts.get(0).get("path").asText());
    assertEquals(1, artifacts.get(0).get("count").asLong());
    assertEquals("first-day".length(), artifacts.get(0).get("content_bytes").asLong());
    assertEquals(1, artifacts.get(0).get("mime_types").get("application/pdf").asLong());
    assertEquals(1, artifacts.get(0).get("missing_language").asLong());
    assertEquals(outputDir.resolve("20260102.wet.gz").toString(), artifacts.get(1).get("path").asText());
    assertEquals(1, artifacts.get(1).get("languages").get("pl").asLong());
  }

  @Test
  void testSplitProvenanceRoutesByMergeResult() throws IOException {
    Path out = tempDir.resolve("routing-main.warc.gz");
    Path diff = tempDir.resolve("routing-diff.warc.gz");

    ConsumerCodec consumer = new ConsumerCodec();
    consumer.configure(Map.of(
        "file", out.toString(),
        "diff-output", diff.toString(),
        "split-provenance", true));
    consumer.beforeCheck(Map.of());
    consumer.onSubscribe(new TestSubscription());
    consumer.startConsuming();

    consumer.onNext(createRecord("base", "base-only"));
    consumer.onNext(createRecord("merged", "merged"));
    consumer.onNext(createRecord("new", "new"));
    consumer.onNext(createRecord("moved", "uri-changed"));
    consumer.onNext(createRecord("reverted", "uri-reverted"));
    consumer.onComplete();
    assertEquals(0, consumer.publishOutputs());

    String mainContent = readFile(out);
    String diffContent = readFile(diff);

    assertTrue(mainContent.contains("WARC-Record-ID: <urn:uuid:base>"));
    assertTrue(mainContent.contains("WARC-Record-ID: <urn:uuid:merged>"));
    assertTrue(mainContent.contains("WARC-Record-ID: <urn:uuid:new>"));
    assertTrue(mainContent.contains("WARC-Record-ID: <urn:uuid:moved>"));
    assertTrue(mainContent.contains("WARC-Record-ID: <urn:uuid:reverted>"));

    assertTrue(!diffContent.contains("WARC-Record-ID: <urn:uuid:base>"),
        "base-only should not go to diff output");
    assertTrue(diffContent.contains("WARC-Record-ID: <urn:uuid:merged>"),
        "merged should go to diff output");
    assertTrue(diffContent.contains("WARC-Record-ID: <urn:uuid:new>"),
        "new should go to diff output");
    assertTrue(diffContent.contains("WARC-Record-ID: <urn:uuid:moved>"),
        "uri-changed should go to diff output");
    assertTrue(diffContent.contains("WARC-Record-ID: <urn:uuid:reverted>"),
        "uri-reverted should go to diff output");
  }

  @Test
  void testCdxSidecarCreatedWhenEnabled() throws IOException {
    Path out = tempDir.resolve("with-cdx.warc.gz");
    Path cdx = tempDir.resolve("with-cdx.cdxj");

    ConsumerCodec consumer = new ConsumerCodec();
    consumer.configure(Map.of(
        "file", out.toString(),
        "format", "warc",
        "cdx-sidecar", true));
    consumer.beforeCheck(Map.of());
    consumer.onSubscribe(new TestSubscription());
    consumer.startConsuming();
    consumer.onNext(createRecord("cdx-enabled", "new"));
    consumer.onComplete();
    assertEquals(0, consumer.publishOutputs());

    assertTrue(Files.exists(out), "main output should exist");
    assertTrue(Files.exists(cdx), "cdx sidecar should be created when enabled");
    assertTrue(Files.size(cdx) > 0, "cdx sidecar should not be empty");
  }

  @Test
  void testCdxSidecarSuppressedWhenDisabled() throws IOException {
    Path out = tempDir.resolve("without-cdx.warc.gz");
    Path cdx = tempDir.resolve("without-cdx.cdxj");

    ConsumerCodec consumer = new ConsumerCodec();
    consumer.configure(Map.of(
        "file", out.toString(),
        "format", "warc",
        "cdx-sidecar", false));
    consumer.beforeCheck(Map.of());
    consumer.onSubscribe(new TestSubscription());
    consumer.startConsuming();
    consumer.onNext(createRecord("cdx-disabled", "new"));
    consumer.onComplete();
    assertEquals(0, consumer.publishOutputs());

    assertTrue(Files.exists(out), "main output should exist");
    assertTrue(!Files.exists(cdx), "cdx sidecar should not be created when disabled");
  }

  @Test
  void testMultiWarcAbsoluteSourceStaysInsideOutputDirectory() throws IOException {
    Path inputDir = tempDir.resolve("absolute-input");
    Path outputDir = tempDir.resolve("absolute-output");
    Files.createDirectories(inputDir);

    Path absoluteSource = inputDir.resolve("source.warc.gz").toAbsolutePath();
    ConsumerCodec consumer = configuredMultiWarcConsumer(outputDir);
    consumer.onNext(createRecord("absolute-source", "new", absoluteSource.toString()));
    consumer.onComplete();
    assertEquals(0, consumer.publishOutputs());

    assertTrue(Files.exists(outputDir.resolve("source.doet.gz")),
        "An absolute source token must be reduced to a safe name beneath the output directory");
    assertTrue(!Files.exists(inputDir.resolve("source.doet.gz")),
        "An absolute source token must not escape to the input directory");
  }

  @Test
  void testMultiWarcParentSourceStaysInsideOutputDirectory() throws IOException {
    Path outputDir = tempDir.resolve("parent-output");
    ConsumerCodec consumer = configuredMultiWarcConsumer(outputDir);
    consumer.onNext(createRecord("parent-source", "new", "../escaped.warc.gz"));
    consumer.onComplete();
    assertEquals(0, consumer.publishOutputs());

    assertTrue(Files.exists(outputDir.resolve("escaped.doet.gz")),
        "A parent-relative source token must be reduced to a safe name beneath the output directory");
    assertTrue(!Files.exists(tempDir.resolve("escaped.doet.gz")),
        "A parent-relative source token must not escape the output directory");
  }

  @Test
  void testMultiWarcEscapingTemplateIsRejected() {
    Path outputDir = tempDir.resolve("template-output");
    ConsumerCodec consumer = configuredMultiWarcConsumer(outputDir, "../{source}.doet.gz");

    assertThrows(IllegalStateException.class,
        () -> consumer.onNext(createRecord("template-source", "new", "source.warc.gz")));
    assertTrue(!Files.exists(tempDir.resolve("source.doet.gz")),
        "An escaping output template must not create a file outside the output directory");
  }

  private ConsumerCodec configuredMultiWarcConsumer(Path outputDir) {
    return configuredMultiWarcConsumer(outputDir, "{source}.doet.gz");
  }

  private ConsumerCodec configuredMultiWarcConsumer(Path outputDir, String outputNameTemplate) {
    ConsumerCodec consumer = new ConsumerCodec();
    consumer.configure(Map.of(
        "file", outputDir.toString(),
        "output-format", "multi-warc",
        "output-name-template", outputNameTemplate));
    consumer.beforeCheck(Map.of());
    consumer.onSubscribe(new TestSubscription());
    consumer.startConsuming();
    return consumer;
  }

  private String readFile(Path p) throws IOException {
    // Decompress GZIP for checking
    try (java.io.InputStream is = new java.util.zip.GZIPInputStream(Files.newInputStream(p))) {
      return new String(is.readAllBytes());
    }
  }

  private RecordWarcUniversal createRecord(String id, String provenance) {
    return createRecord(id, provenance, null);
  }

  private RecordWarcUniversal createRecord(String id, String provenance, String source) {
    String payload = "data-" + id;
    String raw = "WARC/1.0\r\n" +
        "WARC-Type: response\r\n" +
        "WARC-Record-ID: <urn:uuid:" + id + ">\r\n" +
        "NAC-Merge-Result: " + provenance + "\r\n" +
        "Content-Length: " + payload.length() + "\r\n" +
        "\r\n" +
        payload + "\r\n\r\n";

    Map<String, String> headers = new java.util.LinkedHashMap<>();
    headers.put("WARC-Type", "response");
    headers.put("WARC-Record-ID", "<urn:uuid:" + id + ">");
    headers.put("NAC-Merge-Result", provenance);
    if (source != null) {
      headers.put("X-Source-Warc", source);
    }

    // ConsumerCodec writes from raw bytes or converts from universal.
    // It uses WarcIO.toWarcBytes(rec) if universal.
    return new RecordWarcUniversal("response", headers, raw.getBytes());
  }

  private RecordWarcUniversal createStatsRecord(
      String source, String payload, String date, String mimetype, String language) {
    byte[] body = payload.getBytes(StandardCharsets.UTF_8);
    Map<String, String> headers = new LinkedHashMap<>();
    headers.put("WARC-Type", "conversion");
    headers.put("WARC-Target-URI", "https://example.test/" + source);
    headers.put("WARC-Date", date);
    headers.put("X-Source-Warc", source);
    headers.put("Content-Type", "text/plain; charset=utf-8");
    headers.put("Content-Length", String.valueOf(body.length));
    headers.put("WARC-Identified-Content-Type", mimetype);
    if (language != null) {
      headers.put("WARC-Identified-Content-Language", language);
    }
    return new RecordWarcUniversal("conversion", headers, body);
  }

  static class TestSubscription implements Flow.Subscription {
    @Override
    public void request(long n) {
      // no-op for test
    }

    @Override
    public void cancel() {
      // no-op for test
    }
  }
}
