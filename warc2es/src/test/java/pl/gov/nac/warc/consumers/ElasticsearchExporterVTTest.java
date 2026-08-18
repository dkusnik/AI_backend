package pl.gov.nac.warc.consumers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static pl.gov.nac.warc.testutil.ExpectedLogSilencer.runWithLoggerMuted;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.Test;

import pl.gov.nac.warc.reactive.Metrics;
import pl.gov.nac.warc.records.warc.RecordWarcInFile;
import pl.gov.nac.warc.records.warc.RecordWarcUniversal;
import pl.gov.nac.warc.utils.ElasticsearchHttpClient.Document;

public class ElasticsearchExporterVTTest {

  @Test
  void testConvertToDocumentMapping() {
    ElasticsearchExporterVT consumer = new ElasticsearchExporterVT();
    // Initialize with minimal config to avoid NPE in Log/ingestTimestamp if
    // accessed,
    // but convertToDocument doesn't use instance state except ingestTimestamp which
    // is string.
    consumer.configure(Map.of(
        "es-url", "http://localhost:9200",
        "index", "test",
        "url-id", "website123",
        "crawl-id", "crawl2026"));

    String raw = "WARC/1.0\r\n" +
        "WARC-Type: response\r\n" +
        "WARC-Target-URI: http://example.com\r\n" +
        "WARC-Date: 2026-01-01T12:00:00Z\r\n" +
        "WARC-Record-ID: <urn:uuid:123>\r\n" +
        "WARC-Block-Digest: sha256:digestABC\r\n" +
        "X-NAC-URL-ID: website123\r\n" +
        "X-NAC-Crawl-ID: crawl2026\r\n" +
        "NAC-First-Encountered: 2025-12-31T23:59:59Z\r\n" +
        "Content-Type: text/plain\r\n" +
        "Content-Length: 5\r\n" +
        "\r\n" +
        "Hello\r\n\r\n";

    Map<String, String> headers = new HashMap<>();
    headers.put("WARC-Type", "response");
    headers.put("WARC-Target-URI", "http://example.com");
    headers.put("WARC-Date", "2026-01-01T12:00:00Z");
    headers.put("WARC-Record-ID", "<urn:uuid:123>");
    headers.put("WARC-Block-Digest", "sha256:digestABC");
    headers.put("X-NAC-URL-ID", "website123");
    headers.put("X-NAC-Crawl-ID", "crawl2026");
    headers.put("NAC-First-Encountered", "2025-12-31T23:59:59Z");

    RecordWarcUniversal record = new RecordWarcUniversal("response", headers, raw.getBytes(StandardCharsets.UTF_8));

    Document doc = consumer.convertToDocument(record);

    assertNotNull(doc);
    Map<String, Object> source = doc.source();

    // Check core fields
    assertEquals("http://example.com", source.get("warc-uri"));
    assertEquals("http://example.com\u001E2026-01-01T12:00:00Z", doc.id());
    assertEquals("sha256:digestABC", source.get("warc-digest"));
    assertEquals("Hello", source.get("content"));

    assertEquals("website123", source.get("nac-url-id"));
    assertEquals("crawl2026", source.get("nac-crawl-id"));
    // Legacy NAC-First-Encountered is no longer used as a fallback.
    assertEquals("2026-01-01T12:00:00Z", source.get("nac-first-seen"));
    assertEquals("2026-01-01T12:00:00Z", source.get("nac-last-seen"));
    assertNotNull(source.get("@timestamp"), "@timestamp should be present for data streams");

    assertFalse(source.containsKey("warc-type"));
    assertFalse(source.containsKey("wet-digest"));
    assertFalse(source.containsKey("ingest-date"));
    assertFalse(source.containsKey("first-encountered"));
    assertFalse(source.containsKey("last-encountered"));
    assertFalse(source.containsKey("website-id"));
    assertFalse(source.containsKey("url-id"));
    assertFalse(source.containsKey("crawl-id"));
  }

  // =========================================================================
  // Helper Methods
  // =========================================================================

  private RecordWarcUniversal createWarcRecord(String uri, String date, String digest, String content,
      Map<String, String> additionalHeaders) {
    String raw = "WARC/1.0\r\n" +
        "WARC-Type: conversion\r\n" +
        "WARC-Target-URI: " + uri + "\r\n" +
        "WARC-Date: " + date + "\r\n" +
        "WARC-Record-ID: <urn:uuid:" + java.util.UUID.randomUUID() + ">\r\n" +
        "WARC-Block-Digest: " + digest + "\r\n";

    // Add additional headers to raw
    for (Map.Entry<String, String> entry : additionalHeaders.entrySet()) {
      raw += entry.getKey() + ": " + entry.getValue() + "\r\n";
    }

    raw += "Content-Type: text/plain\r\n" +
        "Content-Length: " + content.length() + "\r\n" +
        "\r\n" +
        content + "\r\n\r\n";

    Map<String, String> headers = new HashMap<>();
    headers.put("WARC-Type", "conversion");
    headers.put("WARC-Target-URI", uri);
    headers.put("WARC-Date", date);
    headers.put("WARC-Block-Digest", digest);
    headers.putAll(additionalHeaders);

    return new RecordWarcUniversal("conversion", headers, raw.getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void testExpectedProvenanceMustMatchRecord() {
    ElasticsearchExporterVT consumer = new ElasticsearchExporterVT();
    consumer.configure(Map.of(
        "es-url", "http://localhost:9200",
        "index", "test",
        "url-id", "url-from-record",
        "crawl-id", "123"));

    Map<String, String> headers = Map.of(
        "X-NAC-URL-ID", "url-from-record",
        "X-NAC-Crawl-ID", "crawl-from-record",
        "X-NAC-First-Seen", "2026-01-15T10:30:00Z");

    RecordWarcUniversal record = createWarcRecord(
        "http://test.gov.pl/article",
        "2026-01-20T10:00:00Z",
        "sha256:crawl-override",
        "Test content",
        headers);

    IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
        () -> consumer.convertToDocument(record));
    assertTrue(error.getMessage().contains("provenance mismatch"));
    assertTrue(error.getMessage().contains("integrity"));
  }

  @Test
  void testExpectedProvenanceRequiresEmbeddedHeaders() {
    ElasticsearchExporterVT consumer = new ElasticsearchExporterVT();
    consumer.configure(Map.of(
        "es-url", "http://localhost:9200",
        "index", "test",
        "url-id", "expected-url",
        "crawl-id", "expected-crawl"));

    RecordWarcUniversal record = createWarcRecord(
        "http://test.gov.pl/article",
        "2026-01-20T10:00:00Z",
        "sha256:missing-provenance",
        "Test content",
        Map.of());

    assertThrows(IllegalArgumentException.class, () -> consumer.convertToDocument(record));
  }

  @Test
  void testMatchingExpectedProvenanceIsIndexedFromTheRecord() {
    ElasticsearchExporterVT consumer = new ElasticsearchExporterVT();
    consumer.configure(Map.of(
        "es-url", "http://localhost:9200",
        "index", "test",
        "url-id", "expected-url",
        "crawl-id", "expected-crawl"));

    Map<String, String> headers = Map.of(
        "X-NAC-URL-ID", "expected-url",
        "X-NAC-Crawl-ID", "expected-crawl",
        "X-NAC-First-Seen", "2026-01-15T10:30:00Z");

    RecordWarcUniversal record = createWarcRecord(
        "http://test.gov.pl/article",
        "2026-01-20T10:00:00Z",
        "sha256:matching-provenance",
        "Test content",
        headers);

    Document doc = consumer.convertToDocument(record);
    assertNotNull(doc);
    assertEquals("expected-url", doc.source().get("nac-url-id"));
    assertEquals("expected-crawl", doc.source().get("nac-crawl-id"));
  }

  @Test
  void testWarcinfoProvenanceIsValidatedButNotIndexed() {
    ElasticsearchExporterVT consumer = new ElasticsearchExporterVT();
    consumer.configure(Map.of(
        "es-url", "http://localhost:9200",
        "index", "test",
        "url-id", "expected-url",
        "crawl-id", "expected-crawl"));

    RecordWarcUniversal matching = new RecordWarcUniversal(
        "warcinfo",
        Map.of("Content-Type", "application/warc-fields"),
        new byte[0]).bodyBytes((
            "X-NAC-URL-ID: expected-url\r\n" +
            "X-NAC-Crawl-ID: expected-crawl\r\n").getBytes(StandardCharsets.UTF_8));

    assertNull(consumer.convertToDocument(matching));

    RecordWarcUniversal mismatched = new RecordWarcUniversal(
        "warcinfo",
        Map.of("Content-Type", "application/warc-fields"),
        new byte[0]).bodyBytes((
            "X-NAC-URL-ID: other-url\r\n" +
            "X-NAC-Crawl-ID: expected-crawl\r\n").getBytes(StandardCharsets.UTF_8));

    IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
        () -> consumer.convertToDocument(mismatched));
    assertTrue(error.getMessage().contains("provenance mismatch"));
  }

  // =========================================================================
  // Phase 2: Temporal Lifecycle Tests
  // =========================================================================

  @Test
  void testCompositeDocumentId_UriAndFirstSeen() {
    ElasticsearchExporterVT consumer = new ElasticsearchExporterVT();
    consumer.configure(Map.of("es-url", "http://localhost:9200", "index", "test"));

    Map<String, String> temporalHeaders = Map.of(
        "X-NAC-First-Seen", "2026-01-15T10:30:00Z",
        "X-NAC-Last-Seen", "2026-01-20T10:00:00Z",
        "X-NAC-Missing-Count", "0",
        "X-NAC-Status", "active");

    RecordWarcUniversal record = createWarcRecord(
        "http://test.gov.pl/article",
        "2026-01-20T10:00:00Z",
        "sha256:abc123",
        "Test content",
        temporalHeaders);

    Document doc = consumer.convertToDocument(record);

    assertNotNull(doc);
    // Verify composite document ID format: uri|first_seen
    assertEquals("http://test.gov.pl/article\u001E2026-01-15T10:30:00Z", doc.id());
  }

  @Test
  void testCompositeDocumentId_FallsBackToRecordDate() {
    ElasticsearchExporterVT consumer = new ElasticsearchExporterVT();
    consumer.configure(Map.of("es-url", "http://localhost:9200", "index", "test"));

    // No X-NAC-First-Seen header
    RecordWarcUniversal record = createWarcRecord(
        "http://test.gov.pl/article",
        "2026-01-20T10:00:00Z",
        "sha256:abc123",
        "Test content",
        Map.of());

    Document doc = consumer.convertToDocument(record);

    assertNotNull(doc);
    // Should fallback to uri|warc-date when X-NAC-First-Seen is absent
    assertEquals("http://test.gov.pl/article\u001E2026-01-20T10:00:00Z", doc.id());
  }

  @Test
  void testCompositeDocumentId_DoesNotUseInvocationStartDate() {
    ElasticsearchExporterVT consumer = new ElasticsearchExporterVT();
    consumer.configure(Map.of(
        "es-url", "http://localhost:9200",
        "index", "test",
        "start-date", "1999-01-01"));
    RecordWarcUniversal record = createWarcRecord(
        "http://test.gov.pl/start-date",
        "2026-01-20T10:00:00Z",
        "sha256:start-date",
        "Test content",
        Map.of());

    Document doc = consumer.convertToDocument(record);

    assertEquals("http://test.gov.pl/start-date\u001E2026-01-20T10:00:00Z", doc.id());
  }

  @Test
  void testCompositeDocumentId_UsesDigestOnlyWhenUriIsAbsent() {
    ElasticsearchExporterVT consumer = new ElasticsearchExporterVT();
    consumer.configure(Map.of("es-url", "http://localhost:9200", "index", "test"));
    String raw = "WARC/1.0\r\n"
        + "WARC-Type: conversion\r\n"
        + "WARC-Date: 2026-01-20T10:00:00Z\r\n"
        + "WARC-Block-Digest: sha256:digest-only\r\n"
        + "Content-Length: 7\r\n\r\ncontent\r\n\r\n";
    RecordWarcUniversal record = RecordWarcUniversal.fromRaw(raw.getBytes(StandardCharsets.UTF_8));

    Document doc = consumer.convertToDocument(record);

    assertNotNull(doc);
    assertEquals("digest-only", doc.id());
  }

  @Test
  void testDuplicateIdentityIsIdempotentOnlyForIdenticalDocument() {
    ElasticsearchExporterVT consumer = new ElasticsearchExporterVT();
    consumer.configure(Map.of("es-url", "http://localhost:9200", "index", "test"));
    RecordWarcUniversal first = createWarcRecord(
        "http://test.gov.pl/duplicate", "2026-01-20T10:00:00Z", "sha256:same", "same", Map.of());
    RecordWarcUniversal identical = createWarcRecord(
        "http://test.gov.pl/duplicate", "2026-01-20T10:00:00Z", "sha256:same", "same", Map.of());
    RecordWarcUniversal conflicting = createWarcRecord(
        "http://test.gov.pl/duplicate", "2026-01-20T10:00:00Z", "sha256:different", "different", Map.of());

    assertNotNull(consumer.convertToDocument(first));
    assertEquals(null, consumer.convertToDocument(identical));
    IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
        () -> consumer.convertToDocument(conflicting));
    assertTrue(error.getMessage().contains("integrity error"));
  }

  @Test
  void testTemporalFieldExtraction() {
    ElasticsearchExporterVT consumer = new ElasticsearchExporterVT();
    consumer.configure(Map.of("es-url", "http://localhost:9200", "index", "test"));

    Map<String, String> temporalHeaders = Map.of(
        "X-NAC-First-Seen", "2026-01-15T10:30:00Z",
        "X-NAC-Last-Seen", "2026-01-20T10:00:00Z",
        "X-NAC-Missing-Count", "2",
        "X-NAC-Status", "active",
        "X-NAC-Crawl-ID", "crawl-2026-01-20");

    RecordWarcUniversal record = createWarcRecord(
        "http://test.gov.pl/article",
        "2026-01-20T10:00:00Z",
        "sha256:xyz789",
        "Test content for temporal tracking",
        temporalHeaders);

    Document doc = consumer.convertToDocument(record);

    assertNotNull(doc);
    Map<String, Object> source = doc.source();

    // Verify all temporal fields are extracted
    assertEquals("2026-01-15T10:30:00Z", source.get("nac-first-seen"));
    assertEquals("2026-01-20T10:00:00Z", source.get("nac-last-seen"));
    assertEquals(2, source.get("nac-missing-count"));
    assertEquals("active", source.get("nac-status"));
  }

  @Test
  void testTemporalFieldExtraction_CaseInsensitive() {
    ElasticsearchExporterVT consumer = new ElasticsearchExporterVT();
    consumer.configure(Map.of("es-url", "http://localhost:9200", "index", "test"));

    // Lowercase headers (should still work)
    Map<String, String> temporalHeaders = Map.of(
        "x-nac-first-seen", "2026-01-15T10:30:00Z",
        "x-nac-last-seen", "2026-01-20T10:00:00Z",
        "x-nac-missing-count", "1",
        "x-nac-status", "missing");

    RecordWarcUniversal record = createWarcRecord(
        "http://test.gov.pl/article",
        "2026-01-20T10:00:00Z",
        "sha256:def456",
        "Test content with lowercase headers",
        temporalHeaders);

    Document doc = consumer.convertToDocument(record);

    assertNotNull(doc);
    Map<String, Object> source = doc.source();

    // Verify case-insensitive extraction works
    assertEquals("2026-01-15T10:30:00Z", source.get("nac-first-seen"));
    assertEquals("2026-01-20T10:00:00Z", source.get("nac-last-seen"));
    assertEquals(1, source.get("nac-missing-count"));
    assertEquals("missing", source.get("nac-status"));
  }

  @Test
  void testTemporalFieldExtraction_MissingStatus() {
    ElasticsearchExporterVT consumer = new ElasticsearchExporterVT();
    consumer.configure(Map.of("es-url", "http://localhost:9200", "index", "test"));

    Map<String, String> temporalHeaders = Map.of(
        "X-NAC-First-Seen", "2026-01-10T08:00:00Z",
        "X-NAC-Last-Seen", "2026-01-15T09:00:00Z",
        "X-NAC-Missing-Count", "3",
        "X-NAC-Status", "missing");

    RecordWarcUniversal record = createWarcRecord(
        "http://test.gov.pl/article",
        "2026-01-25T12:00:00Z",
        "sha256:ghi789",
        "", // Empty content for missing record
        temporalHeaders);

    Document doc = consumer.convertToDocument(record);

    // Empty content should result in null document (filtered out)
    assertEquals(null, doc);
  }

  @Test
  void testTemporalFieldExtraction_PartialHeaders() {
    ElasticsearchExporterVT consumer = new ElasticsearchExporterVT();
    consumer.configure(Map.of("es-url", "http://localhost:9200", "index", "test"));

    // Only first-seen present
    Map<String, String> temporalHeaders = Map.of(
        "X-NAC-First-Seen", "2026-01-15T10:30:00Z");

    RecordWarcUniversal record = createWarcRecord(
        "http://test.gov.pl/article",
        "2026-01-20T10:00:00Z",
        "sha256:partial123",
        "Test partial temporal headers",
        temporalHeaders);

    Document doc = consumer.convertToDocument(record);

    assertNotNull(doc);
    Map<String, Object> source = doc.source();

    // Only present headers should be in document
    assertEquals("2026-01-15T10:30:00Z", source.get("nac-first-seen"));
    assertEquals("2026-01-20T10:00:00Z", source.get("nac-last-seen"));
    assertEquals(null, source.get("nac-missing-count")); // Not present
    assertEquals(null, source.get("nac-status")); // Not present
  }

  @Test
  void testCompositeDocumentId_WithURIChanged() {
    ElasticsearchExporterVT consumer = new ElasticsearchExporterVT();
    consumer.configure(Map.of("es-url", "http://localhost:9200", "index", "test"));

    Map<String, String> temporalHeaders = new HashMap<>();
    temporalHeaders.put("X-NAC-First-Seen", "2026-01-10T09:00:00Z"); // From original URI
    temporalHeaders.put("X-NAC-Last-Seen", "2026-01-25T14:00:00Z");
    temporalHeaders.put("X-NAC-Primary-URI", "http://old-site.gov.pl/article");
    temporalHeaders.put("X-NAC-Chain-Length", "1");
    temporalHeaders.put("NAC-Provenance", "uri-changed");

    RecordWarcUniversal record = createWarcRecord(
        "http://new-site.gov.pl/moved-article",
        "2026-01-25T14:00:00Z",
        "sha256:same-content",
        "Content that was migrated to new URI",
        temporalHeaders);

    Document doc = consumer.convertToDocument(record);

    assertNotNull(doc);
    // Document ID should use CURRENT URI (not primary) + first_seen
    // This creates separate document for content at new location
    assertEquals("http://new-site.gov.pl/moved-article\u001E2026-01-10T09:00:00Z", doc.id());

    Map<String, Object> source = doc.source();
    assertEquals("http://new-site.gov.pl/moved-article", source.get("warc-uri"));
    assertEquals("2026-01-10T09:00:00Z", source.get("nac-first-seen"));
  }

  @Test
  void testTemporalFieldExtraction_Reappeared() {
    ElasticsearchExporterVT consumer = new ElasticsearchExporterVT();
    consumer.configure(Map.of("es-url", "http://localhost:9200", "index", "test"));

    Map<String, String> temporalHeaders = Map.of(
        "X-NAC-First-Seen", "2026-01-10T08:00:00Z",
        "X-NAC-Last-Seen", "2026-02-01T10:00:00Z",
        "X-NAC-Missing-Count", "0", // Reset to 0 after reappearance
        "X-NAC-Status", "active", // Changed back to active
        "NAC-Provenance", "reappeared");

    RecordWarcUniversal record = createWarcRecord(
        "http://test.gov.pl/article",
        "2026-02-01T10:00:00Z",
        "sha256:reappeared",
        "Content that reappeared after being missing",
        temporalHeaders);

    Document doc = consumer.convertToDocument(record);

    assertNotNull(doc);
    Map<String, Object> source = doc.source();

    assertEquals("2026-01-10T08:00:00Z", source.get("nac-first-seen"));
    assertEquals("2026-02-01T10:00:00Z", source.get("nac-last-seen"));
    assertEquals(0, source.get("nac-missing-count")); // Reset after reappearance
    assertEquals("active", source.get("nac-status"));
  }

  // =========================================================================
  // Phase 3: Merge Provenance Tests (Task #50)
  // =========================================================================

  @Test
  void testMergeProvenance_BaseOnly() {
    ElasticsearchExporterVT consumer = new ElasticsearchExporterVT();
    consumer.configure(Map.of("es-url", "http://localhost:9200", "index", "test"));

    Map<String, String> mergeHeaders = Map.of(
        "NAC-Merge-Result", "base-only",
        "NAC-Deduplicated", "global",
        "X-NAC-First-Seen", "2026-01-01T10:00:00Z",
        "X-NAC-Last-Seen", "2026-01-01T10:00:00Z",
        "X-NAC-Record-Revisit-Count", "1");

    RecordWarcUniversal record = createWarcRecord(
        "http://test.gov.pl/old-article",
        "2026-01-01T10:00:00Z",
        "sha256:old-content",
        "Content from baseline not in new scan",
        mergeHeaders);

    Document doc = consumer.convertToDocument(record);

    assertNotNull(doc);
    Map<String, Object> source = doc.source();

    // Verify merge provenance fields
    assertEquals("base-only", source.get("nac-merge-result"));
    assertEquals("base-only", source.get("merge-provenance")); // Alias
    assertEquals("global", source.get("nac-deduplicated"));
    assertEquals(1, source.get("nac-revisit-count"));
  }

  @Test
  void testMergeProvenance_Merged() {
    ElasticsearchExporterVT consumer = new ElasticsearchExporterVT();
    consumer.configure(Map.of("es-url", "http://localhost:9200", "index", "test"));

    Map<String, String> mergeHeaders = Map.of(
        "NAC-Merge-Result", "merged",
        "NAC-Deduplicated", "global",
        "X-NAC-First-Seen", "2026-01-01T10:00:00Z",
        "X-NAC-Last-Seen", "2026-02-01T10:00:00Z",
        "X-NAC-Record-Revisit-Count", "2",
        "X-NAC-Status", "active");

    RecordWarcUniversal record = createWarcRecord(
        "http://test.gov.pl/stable-article",
        "2026-02-01T10:00:00Z",
        "sha256:stable-content",
        "Content unchanged between crawls",
        mergeHeaders);

    Document doc = consumer.convertToDocument(record);

    assertNotNull(doc);
    Map<String, Object> source = doc.source();

    assertEquals("merged", source.get("nac-merge-result"));
    assertEquals("merged", source.get("merge-provenance"));
    assertEquals("global", source.get("nac-deduplicated"));
    assertEquals(2, source.get("nac-revisit-count"));
    assertEquals("active", source.get("nac-status"));
  }

  @Test
  void testMergeProvenance_New() {
    ElasticsearchExporterVT consumer = new ElasticsearchExporterVT();
    consumer.configure(Map.of("es-url", "http://localhost:9200", "index", "test"));

    Map<String, String> mergeHeaders = Map.of(
        "NAC-Merge-Result", "new",
        "NAC-Deduplicated", "url",
        "X-NAC-First-Seen", "2026-02-01T10:00:00Z",
        "X-NAC-Last-Seen", "2026-02-01T10:00:00Z",
        "X-NAC-Record-Revisit-Count", "1",
        "X-NAC-Status", "active");

    RecordWarcUniversal record = createWarcRecord(
        "http://test.gov.pl/new-article",
        "2026-02-01T10:00:00Z",
        "sha256:new-content",
        "Newly discovered content",
        mergeHeaders);

    Document doc = consumer.convertToDocument(record);

    assertNotNull(doc);
    Map<String, Object> source = doc.source();

    assertEquals("new", source.get("nac-merge-result"));
    assertEquals("new", source.get("merge-provenance"));
    assertEquals("url", source.get("nac-deduplicated")); // URL-scoped dedup
    assertEquals(1, source.get("nac-revisit-count"));
  }

  @Test
  void testMergeProvenance_UriChanged() {
    ElasticsearchExporterVT consumer = new ElasticsearchExporterVT();
    consumer.configure(Map.of("es-url", "http://localhost:9200", "index", "test"));

    Map<String, String> mergeHeaders = Map.of(
        "NAC-Merge-Result", "uri-changed",
        "NAC-Deduplicated", "global",
        "X-NAC-First-Seen", "2026-01-01T10:00:00Z",
        "X-NAC-Last-Seen", "2026-02-01T10:00:00Z",
        "X-NAC-Primary-URI", "http://test.gov.pl/old-location",
        "X-NAC-Previous-URI", "http://test.gov.pl/old-location",
        "X-NAC-Chain-Length", "1",
        "X-NAC-Record-Revisit-Count", "2");

    RecordWarcUniversal record = createWarcRecord(
        "http://test.gov.pl/new-location",
        "2026-02-01T10:00:00Z",
        "sha256:relocated-content",
        "Content that moved to new URL",
        mergeHeaders);

    Document doc = consumer.convertToDocument(record);

    assertNotNull(doc);
    Map<String, Object> source = doc.source();

    // Verify merge result
    assertEquals("uri-changed", source.get("nac-merge-result"));
    assertEquals("uri-changed", source.get("merge-provenance"));
    assertEquals("global", source.get("nac-deduplicated"));
    assertEquals(2, source.get("nac-revisit-count"));

    // Verify URI chain tracking
    assertEquals("http://test.gov.pl/old-location", source.get("nac-primary-uri"));
    assertEquals("http://test.gov.pl/old-location", source.get("nac-previous-uri"));
    assertEquals(1, source.get("nac-chain-length"));
  }

  @Test
  void testMergeProvenance_MultipleUriChanges() {
    ElasticsearchExporterVT consumer = new ElasticsearchExporterVT();
    consumer.configure(Map.of("es-url", "http://localhost:9200", "index", "test"));

    Map<String, String> mergeHeaders = Map.of(
        "NAC-Merge-Result", "uri-changed",
        "NAC-Deduplicated", "global",
        "X-NAC-First-Seen", "2026-01-01T10:00:00Z",
        "X-NAC-Last-Seen", "2026-03-01T10:00:00Z",
        "X-NAC-Primary-URI", "http://test.gov.pl/original",
        "X-NAC-Previous-URI", "http://test.gov.pl/second-location",
        "X-NAC-Chain-Length", "2",
        "X-NAC-Record-Revisit-Count", "3");

    RecordWarcUniversal record = createWarcRecord(
        "http://test.gov.pl/third-location",
        "2026-03-01T10:00:00Z",
        "sha256:migrated-content",
        "Content that migrated through multiple URLs",
        mergeHeaders);

    Document doc = consumer.convertToDocument(record);

    assertNotNull(doc);
    Map<String, Object> source = doc.source();

    assertEquals("uri-changed", source.get("nac-merge-result"));
    assertEquals("http://test.gov.pl/original", source.get("nac-primary-uri"));
    assertEquals("http://test.gov.pl/second-location", source.get("nac-previous-uri"));
    assertEquals(2, source.get("nac-chain-length"));
    assertEquals(3, source.get("nac-revisit-count"));
  }

  @Test
  void testMergeProvenance_PartialFields() {
    ElasticsearchExporterVT consumer = new ElasticsearchExporterVT();
    consumer.configure(Map.of("es-url", "http://localhost:9200", "index", "test"));

    // Only merge-result, no other merge fields
    Map<String, String> mergeHeaders = Map.of(
        "NAC-Merge-Result", "new");

    RecordWarcUniversal record = createWarcRecord(
        "http://test.gov.pl/minimal",
        "2026-02-01T10:00:00Z",
        "sha256:minimal-merge",
        "Content with minimal merge metadata",
        mergeHeaders);

    Document doc = consumer.convertToDocument(record);

    assertNotNull(doc);
    Map<String, Object> source = doc.source();

    // Only present fields should be indexed
    assertEquals("new", source.get("nac-merge-result"));
    assertEquals("new", source.get("merge-provenance"));
    assertEquals(null, source.get("nac-deduplicated")); // Not present
    assertEquals(null, source.get("nac-revisit-count")); // Not present
    assertEquals(null, source.get("nac-primary-uri")); // Not present
  }

  /**
   * H-5 (T-223): RecordWarcInFile must not appear in acceptedInputTypes()
   * because extractMetadata() returns null for it, silently dropping the record.
   * The fix removes RecordWarcInFile from the accepted types list so the pipeline
   * negotiator rejects misconfigured pipelines at startup with a clear error.
   */
  @Test
  void testRecordWarcInFileIsNotInAcceptedInputTypes() {
    ElasticsearchExporterVT consumer = new ElasticsearchExporterVT();
    consumer.configure(Map.of("es-url", "http://localhost:9200", "index", "test"));

    assertFalse(consumer.acceptedInputTypes().contains(RecordWarcInFile.class),
        "RecordWarcInFile must not be listed in acceptedInputTypes() because "
            + "extractMetadata() returns null for it, causing silent record drops. "
            + "Remove it so the type negotiator catches misconfigured pipelines at startup.");
  }

  @Test
  void testAfterCheckFailsOnBatchFailure() {
    Metrics.reset();
    try {
      Metrics.inc("es-exporter-vt", "batch-failures");

      ElasticsearchExporterVT consumer = new ElasticsearchExporterVT();
      runWithLoggerMuted(ElasticsearchExporterVT.class,
          () -> assertEquals(1, consumer.afterCheck(Map.of())));
    } finally {
      Metrics.reset();
    }
  }

  @Test
  void testAfterCheckFailsWhenIndexedCountIsShort() {
    Metrics.reset();
    try {
      Metrics.add("es-exporter-vt", "recordsIn", 2);
      Metrics.add("es-exporter-vt", "indexed", 1);

      ElasticsearchExporterVT consumer = new ElasticsearchExporterVT();
      runWithLoggerMuted(ElasticsearchExporterVT.class,
          () -> assertEquals(1, consumer.afterCheck(Map.of())));
    } finally {
      Metrics.reset();
    }
  }

  @Test
  void testInterruptedPermitWaitPreservesBatch() throws Exception {
    Metrics.reset();
    ElasticsearchExporterVT consumer = new ElasticsearchExporterVT();
    Field batchField = ElasticsearchExporterVT.class.getDeclaredField("batch");
    batchField.setAccessible(true);
    @SuppressWarnings("unchecked")
    List<Document> batch = (List<Document>) batchField.get(consumer);
    batch.add(Document.of("doc", Map.of("content", "test")));

    Field inFlightField = ElasticsearchExporterVT.class.getDeclaredField("inFlight");
    inFlightField.setAccessible(true);
    inFlightField.set(consumer, new Semaphore(0));

    Method flushBatch = ElasticsearchExporterVT.class.getDeclaredMethod("flushBatch");
    flushBatch.setAccessible(true);
    Thread.currentThread().interrupt();
    try {
      runWithLoggerMuted(ElasticsearchExporterVT.class, () -> {
        try {
          flushBatch.invoke(consumer);
        } catch (ReflectiveOperationException e) {
          throw new IllegalStateException(e);
        }
      });
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
      Metrics.reset();
    }

    assertEquals(1, batch.size(), "An interrupted permit wait must not discard the pending batch");
  }

  @Test
  void testSuccessfulInlineRecoveryIsNotCountedAsSubmitFailure() throws Exception {
    Metrics.reset();
    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext("/_bulk", exchange -> {
      try {
        byte[] body = "{\"errors\":false,\"items\":[{\"index\":{\"_id\":\"doc\",\"status\":200}}]}"
            .getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, body.length);
        exchange.getResponseBody().write(body);
      } finally {
        exchange.close();
      }
    });
    server.start();

    ElasticsearchExporterVT consumer = new ElasticsearchExporterVT();
    try {
      consumer.configure(Map.of(
          "es-url", "http://localhost:" + server.getAddress().getPort(),
          "index", "test",
          "batch-size", 1));

      Field executorField = ElasticsearchExporterVT.class.getDeclaredField("vtExecutor");
      executorField.setAccessible(true);
      ((ExecutorService) executorField.get(consumer)).shutdown();

      RecordWarcUniversal record = createWarcRecord(
          "http://example.com/inline",
          "2026-01-20T10:00:00Z",
          "sha256:inline",
          "Inline recovery",
          Map.of());
      runWithLoggerMuted(ElasticsearchExporterVT.class, () -> consumer.onNext(record));

      assertEquals(0, consumer.afterCheck(Map.of()),
          "A successfully recovered submission must not force a failed exit");
    } finally {
      consumer.onComplete();
      server.stop(0);
      Metrics.reset();
    }
  }

  @Test
  void testTerminalFlushDoesNotHoldBatchMonitorWhileWaitingForPermit() throws Exception {
    ElasticsearchExporterVT consumer = new ElasticsearchExporterVT();
    Field batchField = ElasticsearchExporterVT.class.getDeclaredField("batch");
    batchField.setAccessible(true);
    @SuppressWarnings("unchecked")
    List<Document> batch = (List<Document>) batchField.get(consumer);
    batch.add(Document.of("doc", Map.of("content", "test")));

    Field inFlightField = ElasticsearchExporterVT.class.getDeclaredField("inFlight");
    inFlightField.setAccessible(true);
    inFlightField.set(consumer, new Semaphore(0));

    Thread terminal = Thread.ofVirtual().start(consumer::onComplete);
    Thread probe = null;
    try {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
      while (terminal.getState() != Thread.State.WAITING && System.nanoTime() < deadline) {
        Thread.onSpinWait();
      }
      assertEquals(Thread.State.WAITING, terminal.getState());

      CountDownLatch monitorAcquired = new CountDownLatch(1);
      probe = Thread.ofVirtual().start(() -> {
        synchronized (batch) {
          monitorAcquired.countDown();
        }
      });
      assertTrue(monitorAcquired.await(1, TimeUnit.SECONDS),
          "Backpressure must not make the batch monitor unavailable");
    } finally {
      terminal.interrupt();
      terminal.join(TimeUnit.SECONDS.toMillis(1));
      if (probe != null) {
        probe.join(TimeUnit.SECONDS.toMillis(1));
      }
    }
  }
}
