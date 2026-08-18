package pl.gov.nac.warc.utils;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

import pl.gov.nac.warc.utils.ElasticsearchHttpClient.Document;

/**
 * Tests for {@link ElasticsearchHttpClient} NDJSON serialization correctness.
 *
 * <p>C-2 (T-215): {@code escapeJson()} only escaped {@code \} and {@code "}.
 * Control characters ({@code \n}, {@code \r}, {@code \t}, {@code \u0000}–
 * {@code \u001F}) in document IDs or index names produce invalid NDJSON —
 * Elasticsearch rejects the entire bulk batch silently.
 */
class ElasticsearchHttpClientTest {

  /**
   * Spin up a minimal HTTP server, send a bulk request with a doc-ID that
   * contains newline + CR + tab + a raw control char, and assert the action
   * line in the captured request body contains no raw newline / CR.
   */
  @Test
  void testEscapeJsonHandlesControlCharacters() throws Exception {
    AtomicReference<String> capturedBody = new AtomicReference<>();

    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext("/_bulk", exchange -> {
      try {
        byte[] body = exchange.getRequestBody().readAllBytes();
        capturedBody.set(new String(body, UTF_8));
        String resp = "{\"errors\":false,\"items\":[{\"index\":{\"_id\":\"x\",\"status\":200}}]}";
        exchange.sendResponseHeaders(200, resp.getBytes(UTF_8).length);
        exchange.getResponseBody().write(resp.getBytes(UTF_8));
      } finally {
        exchange.close();
      }
    });
    server.start();
    int port = server.getAddress().getPort();

    // Doc ID containing every problematic character class
    String badId = "url\nwith\rnewlines\tand\u0001control";

    try (var client = new ElasticsearchHttpClient("http://localhost:" + port, 0,
        java.time.Duration.ofMillis(100), 1.0)) {
      client.bulk("test-index", List.of(Document.of(badId, Map.of("content", "test"))));
    } finally {
      server.stop(0);
    }

    String body = capturedBody.get();
    // The action line is the first line of NDJSON — it must not contain raw CR/LF
    String actionLine = body.split("\n")[0];

    assertFalse(actionLine.contains("\n"),
        "Raw newline in action line breaks NDJSON framing");
    assertFalse(actionLine.contains("\r"),
        "Raw CR in action line breaks NDJSON framing");
    assertTrue(actionLine.contains("\\n") || !badId.contains("\n"),
        "Newline must be escaped as \\n in action line");
    assertTrue(actionLine.contains("\\r") || !badId.contains("\r"),
        "CR must be escaped as \\r in action line");
    assertTrue(actionLine.contains("\\t") || !badId.contains("\t"),
        "Tab must be escaped as \\t in action line");
  }

  @Test
  void testEscapeJsonHandlesNullBytes() throws Exception {
    AtomicReference<String> capturedBody = new AtomicReference<>();

    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext("/_bulk", exchange -> {
      try {
        capturedBody.set(new String(exchange.getRequestBody().readAllBytes(), UTF_8));
        String resp = "{\"errors\":false,\"items\":[{\"index\":{\"_id\":\"x\",\"status\":200}}]}";
        exchange.sendResponseHeaders(200, resp.getBytes(UTF_8).length);
        exchange.getResponseBody().write(resp.getBytes(UTF_8));
      } finally {
        exchange.close();
      }
    });
    server.start();
    int port = server.getAddress().getPort();

    String idWithNull = "url-with-\u0000-null";

    try (var client = new ElasticsearchHttpClient("http://localhost:" + port, 0,
        java.time.Duration.ofMillis(100), 1.0)) {
      client.bulk("idx", List.of(Document.of(idWithNull, Map.of("x", "y"))));
    } finally {
      server.stop(0);
    }

    String actionLine = capturedBody.get().split("\n")[0];
    assertFalse(actionLine.contains("\u0000"),
        "NUL byte must be escaped — raw NUL in JSON is invalid");
    assertTrue(actionLine.contains("\\u0000"),
        "NUL must appear as \\u0000 in action line");
  }

  @Test
  void testDataStreamCreateConflictIsReportedAsBulkError() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext("/_bulk", exchange -> {
      try {
        String resp = "{\"errors\":true,\"items\":[{\"create\":{\"status\":409,\"error\":{\"type\":\"version_conflict_engine_exception\",\"reason\":\"document already exists\"}}}]}";
        exchange.sendResponseHeaders(200, resp.getBytes(UTF_8).length);
        exchange.getResponseBody().write(resp.getBytes(UTF_8));
      } finally {
        exchange.close();
      }
    });
    server.start();

    try (var client = new ElasticsearchHttpClient("http://localhost:" + server.getAddress().getPort(), 0,
        java.time.Duration.ofMillis(100), 1.0)) {
      var result = client.bulk("test-stream", List.of(Document.of("known", Map.of("content", "test"))), true);
      assertEquals(0, result.indexed());
      assertEquals(1, result.errors());
      assertTrue(result.hasErrors());
      assertEquals(List.of("document already exists"), result.errorMessages());
    } finally {
      server.stop(0);
    }
  }

  @Test
  void testTooManyRequestsIsRetriedInsteadOfParsedAsSuccess() throws Exception {
    assertStatusIsRetried(429);
  }

  @Test
  void testServiceUnavailableIsRetried() throws Exception {
    assertStatusIsRetried(503);
  }

  @Test
  void testNonRetryableHttpFailureIsNotParsedAsSuccess() throws Exception {
    AtomicInteger requests = new AtomicInteger();
    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext("/_bulk", exchange -> {
      try {
        requests.incrementAndGet();
        byte[] body = "{\"error\":{\"type\":\"mapper_parsing_exception\"},\"status\":400}".getBytes(UTF_8);
        exchange.sendResponseHeaders(400, body.length);
        exchange.getResponseBody().write(body);
      } finally {
        exchange.close();
      }
    });
    server.start();

    try (var client = new ElasticsearchHttpClient(
        "http://localhost:" + server.getAddress().getPort(), 2,
        java.time.Duration.ZERO, 1.0)) {
      var result = client.bulk("test-index", List.of(Document.of("doc", Map.of("content", "test"))));
      assertEquals(1, requests.get(), "A non-retryable 400 response must not be retried");
      assertEquals(0, result.indexed(), "A non-2xx response must never be parsed as bulk success");
      assertTrue(result.hasErrors());
    } finally {
      server.stop(0);
    }
  }

  private void assertStatusIsRetried(int retryableStatus) throws Exception {
    AtomicInteger requests = new AtomicInteger();
    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext("/_bulk", exchange -> {
      try {
        int request = requests.incrementAndGet();
        String response;
        int status;
        if (request == 1) {
          status = retryableStatus;
          response = "{\"error\":{\"type\":\"es_rejected_execution_exception\"},\"status\":"
              + retryableStatus + "}";
        } else {
          status = 200;
          response = "{\"errors\":false,\"items\":[{\"index\":{\"_id\":\"doc\",\"status\":200}}]}";
        }
        byte[] body = response.getBytes(UTF_8);
        exchange.sendResponseHeaders(status, body.length);
        exchange.getResponseBody().write(body);
      } finally {
        exchange.close();
      }
    });
    server.start();

    try (var client = new ElasticsearchHttpClient(
        "http://localhost:" + server.getAddress().getPort(), 1,
        java.time.Duration.ZERO, 1.0)) {
      var result = client.bulk("test-index", List.of(Document.of("doc", Map.of("content", "test"))));
      assertEquals(2, requests.get(), "HTTP " + retryableStatus + " must be retried");
      assertEquals(1, result.indexed());
      assertEquals(0, result.errors());
    } finally {
      server.stop(0);
    }
  }
}
