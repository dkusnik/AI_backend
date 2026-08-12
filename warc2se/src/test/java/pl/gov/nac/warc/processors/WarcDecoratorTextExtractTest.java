package pl.gov.nac.warc.processors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static pl.gov.nac.warc.testutil.ExpectedLogSilencer.runWithLoggerMuted;

import java.nio.charset.StandardCharsets;
import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.apache.pdfbox.pdmodel.font.Standard14Fonts;

import pl.gov.nac.warc.records.warc.RecordWarcUniversal;
import pl.gov.nac.warc.utils.ReadabilityNative;

class WarcDecoratorTextExtractTest {

  @TempDir
  Path tempDir;

  @Test
  void testHtmlExtractionBaselineGolden() {
    WarcDecoratorTextExtract decorator = new WarcDecoratorTextExtract();
    decorator.configure(Map.of(
        "use-native-readability", false,
        "extract-title", true));

    TestSubscriber subscriber = new TestSubscriber();
    decorator.subscribe(subscriber);
    decorator.onSubscribe(new NoOpSubscription());

    String expected = "This baseline sample verifies deterministic plain text extraction output from HTML conversion records.";
    String html = "<html><head><title>Golden HTML Sample</title></head><body>" +
        "<main><p>" + expected + "</p></main></body></html>";

    decorator.onNext(createHtmlRecord(html, "http://test.com/golden"));

    assertEquals(1, subscriber.items.size());
    RecordWarcUniversal decorated = subscriber.items.get(0);
    assertEquals("conversion", decorated.warcType());
    assertEquals("Golden HTML Sample", decorated.headers().get("WARC-Extracted-Title"));
    assertEquals(expected, new String(decorated.rawBytes(), StandardCharsets.UTF_8));
  }

  @Test
  void testJsoupFallbackDoesNotDuplicateNestedScreenReaderText() {
    WarcDecoratorTextExtract decorator = new WarcDecoratorTextExtract();
    decorator.configure(Map.of(
        "use-native-readability", false,
        "extract-title", false));

    TestSubscriber subscriber = new TestSubscriber();
    decorator.subscribe(subscriber);
    decorator.onSubscribe(new NoOpSubscription());

    String hidden = "Accessibility-only lead";
    String html = "<html><body><main><span class=\"screen-reader-text\">" + hidden + "</span>" +
        "<p>This short content keeps the record on the Jsoup fallback path.</p></main></body></html>";
    decorator.onNext(createHtmlRecord(html, "http://test.com/nested-screen-reader"));

    assertEquals(1, subscriber.items.size());
    String text = new String(subscriber.items.get(0).rawBytes(), StandardCharsets.UTF_8);
    assertEquals(1, occurrences(text, hidden));
  }

  @Test
  void testJsoupFallbackPrependsAbsentScreenReaderTextOnce() {
    WarcDecoratorTextExtract decorator = new WarcDecoratorTextExtract();
    decorator.configure(Map.of(
        "use-native-readability", false,
        "extract-title", false));

    TestSubscriber subscriber = new TestSubscriber();
    decorator.subscribe(subscriber);
    decorator.onSubscribe(new NoOpSubscription());

    String hidden = "Accessibility lead outside content";
    String html = "<html><body><span class=\"screen-reader-text\">" + hidden + "</span>" +
        "<main><p>This main content is deliberately long enough to win the first semantic tier.</p></main>" +
        "</body></html>";
    decorator.onNext(createHtmlRecord(html, "http://test.com/absent-screen-reader"));

    assertEquals(1, subscriber.items.size());
    String text = new String(subscriber.items.get(0).rawBytes(), StandardCharsets.UTF_8);
    assertEquals(1, occurrences(text, hidden));
    assertTrue(text.startsWith(hidden));
  }

  @Test
  void testJsoupFallbackNestedSemanticContentIsEmittedOnce() {
    WarcDecoratorTextExtract decorator = new WarcDecoratorTextExtract();
    decorator.configure(Map.of(
        "use-native-readability", false,
        "extract-title", false));

    TestSubscriber subscriber = new TestSubscriber();
    decorator.subscribe(subscriber);
    decorator.onSubscribe(new NoOpSubscription());

    String unique = "Nested semantic content must be emitted exactly once by the fallback extractor.";
    String html = "<html><body><main><article><p>" + unique + "</p></article></main></body></html>";
    decorator.onNext(createHtmlRecord(html, "http://test.com/nested-semantic-content"));

    assertEquals(1, subscriber.items.size());
    String text = new String(subscriber.items.get(0).rawBytes(), StandardCharsets.UTF_8);
    assertEquals(1, occurrences(text, unique));
  }

  @Test
  void testPdfExtractionBaselineGolden() {
    WarcDecoratorTextExtract decorator = new WarcDecoratorTextExtract();
    decorator.configure(Map.of(
        "use-native-readability", false,
        "drop-on-failure", true));

    TestSubscriber subscriber = new TestSubscriber();
    decorator.subscribe(subscriber);
    decorator.onSubscribe(new NoOpSubscription());

    String expected = "Golden PDF baseline text content.";
    RecordWarcUniversal pseudoPdf = createHttpResponseRecord(
        "application/pdf",
        createPdfPayload(expected),
        "http://test.com/golden.pdf");

    decorator.onNext(pseudoPdf);

    assertEquals(1, subscriber.items.size());
    RecordWarcUniversal out = subscriber.items.get(0);
    assertEquals("conversion", out.warcType());
    String body = new String(out.rawBytes(), StandardCharsets.UTF_8);
    assertTrue(body.contains(expected));
  }

  @Test
  void testPdftotextAvailabilityRequiresSuccessfulExit() throws Exception {
    Path marker = Files.createTempFile(tempDir, "pdftotext-invoked-", ".marker");
    Files.delete(marker);
    Path stub = createExecutableStub("""
        #!/bin/bash
        if [[ "${1:-}" == "-v" ]]; then
          exit 7
        fi
        : > %s
        exit 0
        """.formatted(shellQuote(marker)));

    WarcDecoratorTextExtract decorator = new WarcDecoratorTextExtract();
    runWithLoggerMuted(WarcDecoratorTextExtract.class, () -> decorator.configure(Map.of(
        "use-native-readability", false,
        "use-pdftotext", true,
        "pdftotext-path", stub.toString(),
        "pdftotext-timeout-seconds", 1,
        "drop-on-failure", false)));

    TestSubscriber subscriber = new TestSubscriber();
    decorator.subscribe(subscriber);
    decorator.onSubscribe(new NoOpSubscription());
    decorator.onNext(createHttpResponseRecord(
        "application/pdf", createPdfPayload("Availability status"), "http://test.com/status.pdf"));

    assertFalse(Files.exists(marker),
        "A non-zero pdftotext -v status must disable the external extractor");
  }

  @Test
  void testPdftotextAvailabilityProbeTimesOutAndDestroysChild() throws Exception {
    Path pidFile = Files.createTempFile(tempDir, "pdftotext-probe-", ".pid");
    Files.delete(pidFile);
    Path stub = createExecutableStub("""
        #!/bin/bash
        printf '%%s\n' "$$" > %s
        exec sleep 4
        """.formatted(shellQuote(pidFile)));

    WarcDecoratorTextExtract decorator = new WarcDecoratorTextExtract();
    long started = System.nanoTime();
    runWithLoggerMuted(WarcDecoratorTextExtract.class, () -> decorator.configure(Map.of(
        "use-native-readability", false,
        "use-pdftotext", true,
        "pdftotext-path", stub.toString(),
        "pdftotext-timeout-seconds", 1)));
    long elapsedMillis = (System.nanoTime() - started) / 1_000_000;

    assertTrue(elapsedMillis < 3_500,
        "A hanging availability probe exceeded its configured timeout: " + elapsedMillis + "ms");
    assertProcessStopped(pidFile);
  }

  @Test
  void testPdftotextExtractionTimesOutAndDestroysChild() throws Exception {
    Path pidFile = Files.createTempFile(tempDir, "pdftotext-extract-", ".pid");
    Files.delete(pidFile);
    Path stub = createExecutableStub("""
        #!/bin/bash
        if [[ "${1:-}" == "-v" ]]; then
          exit 0
        fi
        printf '%%s\n' "$$" > %s
        exec sleep 4
        """.formatted(shellQuote(pidFile)));

    WarcDecoratorTextExtract decorator = new WarcDecoratorTextExtract();
    runWithLoggerMuted(WarcDecoratorTextExtract.class, () -> decorator.configure(Map.of(
        "use-native-readability", false,
        "use-pdftotext", true,
        "pdftotext-path", stub.toString(),
        "pdftotext-timeout-seconds", 1,
        "drop-on-failure", false)));
    TestSubscriber subscriber = new TestSubscriber();
    decorator.subscribe(subscriber);
    decorator.onSubscribe(new NoOpSubscription());

    long started = System.nanoTime();
    runWithLoggerMuted(WarcDecoratorTextExtract.class,
        () -> decorator.onNext(createHttpResponseRecord(
            "application/pdf", createPdfPayload("Timeout fallback"), "http://test.com/timeout.pdf")));
    long elapsedMillis = (System.nanoTime() - started) / 1_000_000;

    assertTrue(elapsedMillis < 3_500,
        "A hanging extraction exceeded its configured timeout: " + elapsedMillis + "ms");
    assertEquals(1, subscriber.items.size(), "Timed-out pdftotext must fall back to Tika");
    assertProcessStopped(pidFile);
  }

  @Test
  void testNativeReadabilityFallbackEquivalenceBoundsNoCrash() {
    String html = "<html><head><title>Native Toggle</title></head><body>" +
        "<main><p>This paragraph is long enough for extraction and fallback comparison.</p>" +
        "<p>Another sentence keeps the sample deterministic for output shape checks.</p></main>" +
        "</body></html>";

    RecordWarcUniversal record = createHtmlRecord(html, "http://test.com/native-fallback");

    WarcDecoratorTextExtract nativeEnabled = new WarcDecoratorTextExtract();
    nativeEnabled.configure(Map.of(
        "use-native-readability", true,
        "drop-on-failure", false));
    TestSubscriber enabledSub = new TestSubscriber();
    nativeEnabled.subscribe(enabledSub);
    nativeEnabled.onSubscribe(new NoOpSubscription());
    nativeEnabled.onNext(record);

    WarcDecoratorTextExtract nativeDisabled = new WarcDecoratorTextExtract();
    nativeDisabled.configure(Map.of(
        "use-native-readability", false,
        "drop-on-failure", false));
    TestSubscriber disabledSub = new TestSubscriber();
    nativeDisabled.subscribe(disabledSub);
    nativeDisabled.onSubscribe(new NoOpSubscription());
    nativeDisabled.onNext(record);

    assertEquals(1, enabledSub.items.size());
    assertEquals(1, disabledSub.items.size());

    String enabledBody = new String(enabledSub.items.get(0).rawBytes(), StandardCharsets.UTF_8);
    String disabledBody = new String(disabledSub.items.get(0).rawBytes(), StandardCharsets.UTF_8);
    assertFalse(enabledBody.isBlank());
    assertFalse(disabledBody.isBlank());
    assertTrue(enabledBody.contains("fallback comparison"));
    assertTrue(disabledBody.contains("fallback comparison"));

    double ratio = (double) enabledBody.length() / (double) disabledBody.length();
    assertTrue(ratio > 0.4 && ratio < 2.5,
        "Native/non-native output lengths diverged too much: ratio=" + ratio);
  }

  @Test
  void testNativeExtractionHonorsHeadingAndLinkFlagsIndependently() {
    assumeTrue(ReadabilityNative.isAvailable());

    String heading = "Unique Native Heading";
    String link = "Unique Native Link";
    String html = "<html><body><article><h2>" + heading + "</h2>" +
        "<p>This article has enough meaningful text for the native readability path. " +
        "It deliberately contains several sentences so the selected primary content is stable.</p>" +
        "<a href=\"https://example.test/destination\">" + link + "</a>" +
        "<p>Another paragraph makes the extraction result deterministic across both paths.</p>" +
        "</article></body></html>";

    String neither = extractNativeHtml(html, false, false);
    String headingsOnly = extractNativeHtml(html, true, false);
    String linksOnly = extractNativeHtml(html, false, true);
    String both = extractNativeHtml(html, true, true);

    int baseHeadingCount = occurrences(neither, heading);
    int baseLinkCount = occurrences(neither, link);
    assertEquals(baseHeadingCount + 1, occurrences(headingsOnly, heading));
    assertEquals(baseLinkCount, occurrences(headingsOnly, link));
    assertEquals(baseHeadingCount, occurrences(linksOnly, heading));
    assertEquals(baseLinkCount + 1, occurrences(linksOnly, link));
    assertEquals(baseHeadingCount + 1, occurrences(both, heading));
    assertEquals(baseLinkCount + 1, occurrences(both, link));
  }

  @Test
  void testHtmlExtraction() {
    WarcDecoratorTextExtract decorator = new WarcDecoratorTextExtract();
    decorator.configure(Map.of("extract-title", true));

    TestSubscriber subscriber = new TestSubscriber();
    decorator.subscribe(subscriber);
    decorator.onSubscribe(new NoOpSubscription());

    // More substantial HTML to satisfy Readability4J
    String htmlBody = "<h1>The Grand Title</h1>" +
        "<p>This is a significant paragraph of text that should definitely be considered " +
        "as the main content of the article by any readability algorithm. It has enough " +
        "information and length to be recognized as the primary text payload of the page.</p>" +
        "<p>Another paragraph to ensure we have enough block-level elements for extraction " +
        "purposes. This helps in demonstrating the robust nature of the extraction logic.</p>";

    String html = "<html><head><title>My Article Title</title></head><body>" +
        "<header><nav>Link 1</nav></header>" +
        "<main>" + htmlBody + "</main>" +
        "<footer>Footer info</footer>" +
        "</body></html>";

    RecordWarcUniversal record = createHtmlRecord(html, "http://test.com/article");
    decorator.onNext(record);

    assertEquals(1, subscriber.items.size());
    RecordWarcUniversal decorated = subscriber.items.get(0);

    assertEquals("conversion", decorated.warcType());
    // Native readability (Rust JNI) is used when the .so is available (default on dev machine).
    // It prefers the article <h1> over <head><title> and may exclude the h1 from body text.
    // OR-assertions below cover both native and Java R4J fallback paths.
    String extractedTitle = decorated.headers().get("WARC-Extracted-Title");
    assertTrue(extractedTitle != null && !extractedTitle.isBlank(),
        "Expected a non-blank extracted title");
    assertTrue(extractedTitle.equals("My Article Title") || extractedTitle.equals("The Grand Title"),
        "Unexpected title: " + extractedTitle);

    String body = new String(decorated.rawBytes(), StandardCharsets.UTF_8);
    // Native readability may move h1 to title rather than body text
    assertTrue(body.contains("The Grand Title") || extractedTitle.contains("The Grand Title"),
        "The Grand Title should appear in body or title");
    assertTrue(body.contains("significant paragraph"));
    // Boilerplate should be removed
    assertTrue(!body.contains("Link 1"));
    assertTrue(!body.contains("Footer info"));
  }

  @Test
  void testNormalization() {
    WarcDecoratorTextExtract decorator = new WarcDecoratorTextExtract();
    decorator.configure(Map.of("extract-normalize", true));

    TestSubscriber subscriber = new TestSubscriber();
    decorator.subscribe(subscriber);
    decorator.onSubscribe(new NoOpSubscription());

    // Text with excessive whitespace and non-normalized characters
    // NFKC normalization: e + ́ (combining acute) -> é (precomposed)
    String rawText = "  Excessive    whitespace  and  normalized \u0065\u0301  ";
    RecordWarcUniversal record = createTextRecord(rawText);
    decorator.onNext(record);

    RecordWarcUniversal decorated = subscriber.items.get(0);
    String body = new String(decorated.rawBytes(), StandardCharsets.UTF_8);

    assertEquals("Excessive whitespace and normalized \u00E9", body);
  }

  @Test
  void testMinTextLength() {
    WarcDecoratorTextExtract decorator = new WarcDecoratorTextExtract();
    decorator.configure(Map.of(
        "extract-min-text-length", 50,
        "drop-on-failure", true));

    TestSubscriber subscriber = new TestSubscriber();
    decorator.subscribe(subscriber);
    decorator.onSubscribe(new NoOpSubscription());

    // Short text -> SHOULD DROP
    decorator.onNext(createTextRecord("Too short."));
    // Long text -> SHOULD KEEP
    decorator.onNext(
        createTextRecord(
            "This is a sufficiently long text that exceeds the fifty characters minimum requirement. It is even longer now."));

    assertEquals(1, subscriber.items.size());
    assertTrue(new String(subscriber.items.get(0).rawBytes()).startsWith("This is a"));
  }

  @Test
  void testMixedMimeOnlyAllowedOutputRecords() {
    WarcDecoratorTextExtract decorator = new WarcDecoratorTextExtract();
    decorator.configure(Map.of(
        "drop-on-failure", true,
        "use-native-readability", false));

    TestSubscriber subscriber = new TestSubscriber();
    decorator.subscribe(subscriber);
    decorator.onSubscribe(new NoOpSubscription());

    decorator.onNext(createHtmlRecord(
        "<html><body><main><p>HTML path should be kept with extracted text.</p></main></body></html>",
        "http://test.com/html"));
    decorator.onNext(createHttpResponseRecord(
        "application/json",
        "{\"a\":1}".getBytes(StandardCharsets.UTF_8),
        "http://test.com/json"));
    decorator.onNext(createTextRecord("Plain text should also be kept."));
    runWithLoggerMuted(WarcDecoratorTextExtract.class,
        () -> decorator.onNext(createHttpResponseRecord(
            "application/pdf",
            new byte[] { 0x25, 0x50, 0x44, 0x46, 0x2D }, // "%PDF-" header only (invalid)
            "http://test.com/pdf-invalid")));

    assertEquals(2, subscriber.items.size());
    assertEquals("conversion", subscriber.items.get(0).warcType());
    assertEquals("conversion", subscriber.items.get(1).warcType());
    assertTrue(new String(subscriber.items.get(0).rawBytes(), StandardCharsets.UTF_8).contains("HTML path"));
    assertTrue(new String(subscriber.items.get(1).rawBytes(), StandardCharsets.UTF_8).contains("Plain text"));
  }

  @Test
  void testMinTextLengthBoundaryValues() {
    WarcDecoratorTextExtract decorator = new WarcDecoratorTextExtract();
    decorator.configure(Map.of(
        "extract-min-text-length", 10,
        "drop-on-failure", true));

    TestSubscriber subscriber = new TestSubscriber();
    decorator.subscribe(subscriber);
    decorator.onSubscribe(new NoOpSubscription());

    decorator.onNext(createTextRecord("123456789")); // N-1
    decorator.onNext(createTextRecord("1234567890")); // N
    decorator.onNext(createTextRecord("12345678901")); // N+1

    assertEquals(2, subscriber.items.size());
    assertEquals("1234567890", new String(subscriber.items.get(0).rawBytes(), StandardCharsets.UTF_8));
    assertEquals("12345678901", new String(subscriber.items.get(1).rawBytes(), StandardCharsets.UTF_8));
  }

  @Test
  void testHybridFallbackEnabledVsDisabled() {
    StringBuilder linkBlock = new StringBuilder();
    for (int i = 0; i < 500; i++) {
      linkBlock.append("<a href='/m").append(i).append("'>menu-item-").append(i).append("</a>");
    }
    String html = "<html><head><title>Hybrid Fallback</title></head><body>" +
        "<nav>" + linkBlock + "</nav>" +
        "<article><p>core article text.</p></article></body></html>";

    WarcDecoratorTextExtract enabled = new WarcDecoratorTextExtract();
    enabled.configure(Map.of(
        "extract-hybrid-fallback", true,
        "extract-hybrid-threshold", 10000,
        "use-native-readability", false));

    TestSubscriber enabledSub = new TestSubscriber();
    enabled.subscribe(enabledSub);
    enabled.onSubscribe(new NoOpSubscription());
    enabled.onNext(createHtmlRecord(html, "http://test.com/hybrid-enabled"));

    WarcDecoratorTextExtract disabled = new WarcDecoratorTextExtract();
    disabled.configure(Map.of(
        "extract-hybrid-fallback", false,
        "extract-hybrid-threshold", 10000,
        "use-native-readability", false));

    TestSubscriber disabledSub = new TestSubscriber();
    disabled.subscribe(disabledSub);
    disabled.onSubscribe(new NoOpSubscription());
    disabled.onNext(createHtmlRecord(html, "http://test.com/hybrid-disabled"));

    assertEquals(1, enabledSub.items.size());
    assertEquals(1, disabledSub.items.size());

    String enabledBody = new String(enabledSub.items.get(0).rawBytes(), StandardCharsets.UTF_8);
    String disabledBody = new String(disabledSub.items.get(0).rawBytes(), StandardCharsets.UTF_8);

    assertTrue(enabledBody.contains("core article text"));
    assertTrue(disabledBody.contains("core article text"));
    assertTrue(enabledBody.length() >= disabledBody.length());
    assertFalse(enabledBody.isBlank());
    assertFalse(disabledBody.isBlank());
  }

  @Test
  void testTitleExtractionPresenceAndAbsence() {
    String html = "<html><head><title>Toggle Title</title></head><body><main><p>Main body text for title toggle test.</p></main></body></html>";

    WarcDecoratorTextExtract withTitle = new WarcDecoratorTextExtract();
    withTitle.configure(Map.of("extract-title", true, "use-native-readability", false));
    TestSubscriber withTitleSub = new TestSubscriber();
    withTitle.subscribe(withTitleSub);
    withTitle.onSubscribe(new NoOpSubscription());
    withTitle.onNext(createHtmlRecord(html, "http://test.com/title-on"));

    WarcDecoratorTextExtract withoutTitle = new WarcDecoratorTextExtract();
    withoutTitle.configure(Map.of("extract-title", false, "use-native-readability", false));
    TestSubscriber withoutTitleSub = new TestSubscriber();
    withoutTitle.subscribe(withoutTitleSub);
    withoutTitle.onSubscribe(new NoOpSubscription());
    withoutTitle.onNext(createHtmlRecord(html, "http://test.com/title-off"));

    assertEquals("Toggle Title", withTitleSub.items.get(0).headers().get("WARC-Extracted-Title"));
    assertFalse(withoutTitleSub.items.get(0).headers().containsKey("WARC-Extracted-Title"));
  }

  @Test
  void testMalformedInputDropPolicy() {
    RecordWarcUniversal invalidPdf = createHttpResponseRecord(
        "application/pdf",
        new byte[] { 0x25, 0x50, 0x44, 0x46, 0x2D, 0x00, 0x00, 0x00 },
        "http://test.com/bad-pdf");

    WarcDecoratorTextExtract dropEnabled = new WarcDecoratorTextExtract();
    dropEnabled.configure(Map.of("drop-on-failure", true));
    TestSubscriber dropEnabledSub = new TestSubscriber();
    dropEnabled.subscribe(dropEnabledSub);
    dropEnabled.onSubscribe(new NoOpSubscription());
    runWithLoggerMuted(WarcDecoratorTextExtract.class,
        () -> dropEnabled.onNext(invalidPdf));

    WarcDecoratorTextExtract dropDisabled = new WarcDecoratorTextExtract();
    dropDisabled.configure(Map.of("drop-on-failure", false));
    TestSubscriber dropDisabledSub = new TestSubscriber();
    dropDisabled.subscribe(dropDisabledSub);
    dropDisabled.onSubscribe(new NoOpSubscription());
    runWithLoggerMuted(WarcDecoratorTextExtract.class,
        () -> dropDisabled.onNext(invalidPdf));

    assertEquals(0, dropEnabledSub.items.size());
    assertEquals(1, dropDisabledSub.items.size());
    assertEquals("response", dropDisabledSub.items.get(0).warcType());
  }

  @Test
  void testExtractOutputPreservesRequiredNacHeaders() {
    WarcDecoratorTextExtract decorator = new WarcDecoratorTextExtract();
    decorator.configure(Map.of("use-native-readability", false));

    TestSubscriber subscriber = new TestSubscriber();
    decorator.subscribe(subscriber);
    decorator.onSubscribe(new NoOpSubscription());

    Map<String, String> extraHeaders = Map.of(
        "NAC-Website-ID", "site-123",
        "NAC-Crawl-ID", "crawl-456",
        "X-NAC-First-Seen", "2026-01-01T00:00:00Z");

    String html = "<html><head><title>NAC Header Test</title></head><body><main><p>This body should be extracted and headers preserved.</p></main></body></html>";
    decorator.onNext(createHtmlRecord(html, "http://test.com/nac", extraHeaders));

    assertEquals(1, subscriber.items.size());
    RecordWarcUniversal out = subscriber.items.get(0);
    assertEquals("conversion", out.warcType());
    assertEquals("site-123", out.headers().get("NAC-Website-ID"));
    assertEquals("crawl-456", out.headers().get("NAC-Crawl-ID"));
    assertEquals("2026-01-01T00:00:00Z", out.headers().get("X-NAC-First-Seen"));
    assertEquals("text/plain; charset=utf-8", out.headers().get("Content-Type"));
  }

  @Test
  void testHttpContentTypeParsing() {
    WarcDecoratorTextExtract decorator = new WarcDecoratorTextExtract();
    decorator.configure(Map.of());

    TestSubscriber subscriber = new TestSubscriber();
    decorator.subscribe(subscriber);
    decorator.onSubscribe(new NoOpSubscription());

    // Record with application/http; msgtype=response, but internal is text/html
    String payload = "HTTP/1.1 200 OK\r\n" +
        "Content-Type: text/html; charset=utf-8\r\n" +
        "\r\n" +
        "<html><body><h1>Hello World</h1><p>Main content of the page.</p></body></html>";

    Map<String, String> headers = new java.util.LinkedHashMap<>();
    headers.put("WARC-Type", "response");
    headers.put("WARC-Target-URI", "http://test.com");
    headers.put("Content-Type", "application/http; msgtype=response");

    String warcFull = "WARC/1.0\r\n" +
        "WARC-Type: response\r\n" +
        "Content-Type: application/http; msgtype=response\r\n" +
        "\r\n" +
        payload + "\r\n\r\n";

    RecordWarcUniversal record = new RecordWarcUniversal("response", headers,
        warcFull.getBytes(StandardCharsets.UTF_8));

    decorator.onNext(record);

    assertEquals(1, subscriber.items.size());
    RecordWarcUniversal decorated = subscriber.items.get(0);
    assertEquals("text/html", decorated.headers().get("WARC-Identified-Content-Type"));
    // Native readability may move h1 to title; check paragraph content is present
    String bodyText = new String(decorated.rawBytes());
    assertTrue(bodyText.contains("Hello World") || bodyText.contains("Main content"),
        "Expected extracted body to contain page text, got: " + bodyText);
  }

  // --- Helpers ---

  private String extractNativeHtml(String html, boolean preserveHeadings, boolean preserveLinks) {
    WarcDecoratorTextExtract decorator = new WarcDecoratorTextExtract();
    decorator.configure(Map.of(
        "use-native-readability", true,
        "extract-preserve-headings", preserveHeadings,
        "extract-preserve-links", preserveLinks,
        "extract-title", false));
    TestSubscriber subscriber = new TestSubscriber();
    decorator.subscribe(subscriber);
    decorator.onSubscribe(new NoOpSubscription());
    decorator.onNext(createHtmlRecord(html, "https://example.test/flags"));
    assertEquals(1, subscriber.items.size());
    return new String(subscriber.items.get(0).rawBytes(), StandardCharsets.UTF_8);
  }

  private RecordWarcUniversal createHtmlRecord(String html, String url) {
    return createHtmlRecord(html, url, Map.of());
  }

  private RecordWarcUniversal createHtmlRecord(String html, String url, Map<String, String> extraHeaders) {
    Map<String, String> headers = new java.util.LinkedHashMap<>();
    headers.put("WARC-Type", "response");
    headers.put("WARC-Target-URI", url);
    headers.put("Content-Type", "application/http; msgtype=response");
    headers.putAll(extraHeaders);

    String http = "HTTP/1.1 200 OK\r\n" +
        "Content-Type: text/html; charset=utf-8\r\n" +
        "\r\n" +
        html;

    String raw = "WARC/1.0\r\n" +
        "WARC-Type: response\r\n" +
        "WARC-Target-URI: " + url + "\r\n" +
        "Content-Type: application/http; msgtype=response\r\n" +
        "Content-Length: " + http.getBytes(StandardCharsets.UTF_8).length + "\r\n" +
        "\r\n" +
        http + "\r\n\r\n";
    return new RecordWarcUniversal("response", headers, raw.getBytes(StandardCharsets.UTF_8));
  }

  private RecordWarcUniversal createHttpResponseRecord(String payloadContentType, byte[] payload, String url) {
    Map<String, String> headers = new LinkedHashMap<>();
    headers.put("WARC-Type", "response");
    headers.put("WARC-Target-URI", url);
    headers.put("Content-Type", "application/http; msgtype=response");

    byte[] httpHeader = ("HTTP/1.1 200 OK\r\n" +
        "Content-Type: " + payloadContentType + "\r\n" +
        "\r\n").getBytes(StandardCharsets.ISO_8859_1);
    byte[] httpPayload = new byte[httpHeader.length + payload.length];
    System.arraycopy(httpHeader, 0, httpPayload, 0, httpHeader.length);
    System.arraycopy(payload, 0, httpPayload, httpHeader.length, payload.length);

    byte[] warcHeader = ("WARC/1.0\r\n" +
        "WARC-Type: response\r\n" +
        "WARC-Target-URI: " + url + "\r\n" +
        "Content-Type: application/http; msgtype=response\r\n" +
        "Content-Length: " + httpPayload.length + "\r\n" +
        "\r\n").getBytes(StandardCharsets.ISO_8859_1);

    byte[] trailer = "\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1);
    byte[] raw = new byte[warcHeader.length + httpPayload.length + trailer.length];
    System.arraycopy(warcHeader, 0, raw, 0, warcHeader.length);
    System.arraycopy(httpPayload, 0, raw, warcHeader.length, httpPayload.length);
    System.arraycopy(trailer, 0, raw, warcHeader.length + httpPayload.length, trailer.length);

    return new RecordWarcUniversal("response", headers, raw);
  }

  private RecordWarcUniversal createTextRecord(String text) {
    Map<String, String> headers = new java.util.LinkedHashMap<>();
    headers.put("WARC-Type", "conversion");
    headers.put("Content-Type", "text/plain");

    String raw = "WARC/1.0\r\n" +
        "WARC-Type: conversion\r\n" +
        "Content-Type: text/plain\r\n" +
        "\r\n" +
        text + "\r\n\r\n";
    return new RecordWarcUniversal("conversion", headers, raw.getBytes(StandardCharsets.UTF_8));
  }

  private static int occurrences(String text, String needle) {
    return text.split(java.util.regex.Pattern.quote(needle), -1).length - 1;
  }

  static class TestSubscriber implements Flow.Subscriber<RecordWarcUniversal> {
    List<RecordWarcUniversal> items = new ArrayList<>();

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
    }

    @Override
    public void onNext(RecordWarcUniversal item) {
      items.add(item);
    }

    @Override
    public void onError(Throwable throwable) {
    }

    @Override
    public void onComplete() {
    }
  }

  static class NoOpSubscription implements Flow.Subscription {
    @Override
    public void request(long n) {
    }

    @Override
    public void cancel() {
    }
  }

  private byte[] createPdfPayload(String text) {
    try (PDDocument document = new PDDocument();
        ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      PDPage page = new PDPage();
      document.addPage(page);
      try (PDPageContentStream stream = new PDPageContentStream(document, page)) {
        stream.beginText();
        stream.setFont(new PDType1Font(Standard14Fonts.FontName.HELVETICA), 12);
        stream.newLineAtOffset(50, 700);
        stream.showText(text);
        stream.endText();
      }
      document.save(out);
      return out.toByteArray();
    } catch (Exception e) {
      throw new IllegalStateException("Failed to create PDF payload fixture", e);
    }
  }

  private Path createExecutableStub(String source) throws Exception {
    Path stub = Files.createTempFile(tempDir, "pdftotext-stub-", ".sh");
    Files.writeString(stub, source);
    assertTrue(stub.toFile().setExecutable(true), "Failed to make pdftotext stub executable");
    return stub;
  }

  private static String shellQuote(Path path) {
    return "'" + path.toString().replace("'", "'\"'\"'") + "'";
  }

  private static void assertProcessStopped(Path pidFile) throws Exception {
    assertTrue(Files.exists(pidFile), "Sleeping pdftotext stub did not record its PID");
    long pid = Long.parseLong(Files.readString(pidFile).trim());
    assertFalse(ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false),
        "pdftotext child remained alive after timeout");
  }
}
