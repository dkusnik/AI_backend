package pl.gov.nac.warc.reactive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

class ProcessingResultTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @BeforeEach
  void resetMetrics() {
    Metrics.reset();
  }

  @Test
  void startupFailureHasNullEngineMetricsAndSanitizedError() throws Exception {
    ProcessingResult result = ProcessingResult.create(
        12, false, false, false, 7,
        ProcessingResult.CONFIGURATION_ERROR,
        "failed http://alice:secret@example.test password=hunter2\n at stack.Frame");

    JsonNode json = mapper.readTree(result.toJson());
    assertEquals("warc2es.processing/v1", json.get("schema").asText());
    assertEquals("error", json.get("status").asText());
    assertEquals(12, json.get("exit_code").asInt());
    assertTrue(json.get("records_in").isNull());
    assertTrue(json.get("metrics").isNull());
    assertEquals(1, json.get("errors").asInt());
    assertFalse(json.get("error").get("message").asText().contains("secret"));
    assertFalse(json.get("error").get("message").asText().contains("hunter2"));
    assertFalse(json.get("error").get("message").asText().contains("\n"));
    assertFalse(json.get("error").get("message").asText().contains("stack.Frame"));
  }

  @Test
  void dryRunReportsZeroCountsAndAvailableMetrics() throws Exception {
    Metrics.inc("Example-Module", "someCounter");

    ProcessingResult result = ProcessingResult.create(0, true, true, true, 3, null, null);
    JsonNode json = mapper.readTree(result.toJson());

    assertEquals("dry_run", json.get("status").asText());
    assertEquals(0, json.get("records_in").asLong());
    assertEquals(0, json.get("records_out").asLong());
    assertEquals(0, json.get("records_skipped").asLong());
    assertEquals(0, json.get("records_indexed").asLong());
    assertEquals(1,
        json.get("metrics").get("counters").get("example_module").get("some_counter").asLong());
    assertNull(result.error());
  }

  @Test
  void completedRunUsesEngineAndElasticsearchCounters() throws Exception {
    Metrics.set("engine", "recordsIn", 9);
    Metrics.set("engine", "recordsOut", 7);
    Metrics.set("es-exporter-vt", "indexed", 6);

    ProcessingResult result = ProcessingResult.create(0, false, true, true, 11, null, null);
    JsonNode json = mapper.readTree(result.toJson());

    assertEquals("ok", json.get("status").asText());
    assertEquals(9, json.get("records_in").asLong());
    assertEquals(7, json.get("records_out").asLong());
    assertEquals(2, json.get("records_skipped").asLong());
    assertEquals(6, json.get("records_indexed").asLong());
    assertEquals("warc2es.metrics/v1", json.get("metrics").get("schema").asText());
  }

  @Test
  void writeToEmitsOneCompactObjectAndOneNewline() throws Exception {
    ProcessingResult result = ProcessingResult.create(0, false, false, false, 1, null, null);
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();

    assertTrue(result.writeTo(new PrintStream(bytes, true, StandardCharsets.UTF_8)));

    String output = bytes.toString(StandardCharsets.UTF_8);
    assertEquals(1, output.lines().count());
    assertTrue(output.endsWith(System.lineSeparator()));
    assertFalse(output.substring(0, output.length() - System.lineSeparator().length()).contains("\n"));
    mapper.readTree(output);
  }
}
