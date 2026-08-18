package pl.gov.nac.warc.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import pl.gov.nac.warc.config.ConfigResolver.ProcessorEntry;

class ConfigResolverTest {

  @Test
  void returnsSimplePipelineWithoutCopyingIt() {
    PipelineDef simple = pipeline("simple", "reader", List.of("decorate"), "writer");

    PipelineDef resolved = ConfigResolver.resolvePipeline(
        "simple", Map.of("simple", simple), new ModuleRegistry());

    assertSame(simple, resolved);
  }

  @Test
  void flattensMultiStagePipelineInDeclaredOrder() {
    PipelineDef first = pipeline("first", "reader", List.of("first-proc"), null);
    PipelineDef middle = pipeline("middle", null, List.of("middle-proc"), null);
    PipelineDef last = pipeline("last", null, List.of("last-proc"), "writer");
    PipelineDef combined = new PipelineDef(
        "combined", "combined description", null, List.of(), null,
        List.of("first"), List.of("middle"), List.of("last"),
        Map.of("producer.reader", Map.of("mode", "native")), List.of());
    Map<String, PipelineDef> pipelines = new LinkedHashMap<>();
    pipelines.put("combined", combined);
    pipelines.put("first", first);
    pipelines.put("middle", middle);
    pipelines.put("last", last);

    PipelineDef resolved = ConfigResolver.resolvePipeline("combined", pipelines, new ModuleRegistry());

    assertEquals("reader", resolved.producer);
    assertEquals(List.of("first-proc", "middle-proc", "last-proc"), resolved.processorsRaw);
    assertEquals("writer", resolved.consumer);
    assertEquals(combined.overrides, resolved.overrides);
    assertEquals(List.of(), resolved.beforeStages);
    assertEquals(List.of(), resolved.mainStages);
    assertEquals(List.of(), resolved.afterStages);
  }

  @Test
  void synthesizesUnknownEndpointStagesFromModuleOverrides() {
    ModuleRegistry modules = new ModuleRegistry();
    modules.producers.put("reader", module("reader"));
    modules.consumers.put("writer", module("writer"));
    PipelineDef combined = new PipelineDef(
        "combined", "", null, List.of(), null,
        List.of("virtual-input"), List.of(), List.of("virtual-output"),
        Map.of(
            "virtual-input", Map.of("module", "reader"),
            "virtual-output", Map.of("module", "writer")),
        List.of());

    PipelineDef resolved = ConfigResolver.resolvePipeline(
        "combined", Map.of("combined", combined), modules);

    assertEquals("reader", resolved.producer);
    assertEquals("writer", resolved.consumer);
  }

  @Test
  void rejectsUnknownPipelinesAndEmptyMultiStageDefinitions() {
    assertThrows(IllegalArgumentException.class,
        () -> ConfigResolver.resolvePipeline("missing", Map.of(), new ModuleRegistry()));

    PipelineDef empty = new PipelineDef(
        "empty", "", null, List.of(), null,
        List.of(), List.of(), List.of(), Map.of(), List.of());
    assertThrows(IllegalArgumentException.class,
        () -> ConfigResolver.resolvePipeline("empty", Map.of("empty", empty), new ModuleRegistry()));
  }

  @Test
  void flattensAndAppliesInlineProcessorConfiguration() {
    PipelineDef def = pipeline("simple", "reader", List.of(
        "plain",
        Map.of("configured", Map.of("config", Map.of("enabled", true, "limit", 4)))),
        "writer");

    List<ProcessorEntry> entries = ConfigResolver.flattenProcessors(def);
    List<Map<String, Object>> configs = new ArrayList<>();
    configs.add(new LinkedHashMap<>(Map.of("existing", "kept")));
    configs.add(new LinkedHashMap<>(Map.of("limit", 1)));
    ConfigResolver.applyInlineOverrides(entries, configs);

    assertEquals("plain", entries.get(0).name());
    assertEquals(Map.of(), entries.get(0).inlineConfig());
    assertEquals("configured", entries.get(1).name());
    assertEquals(Map.of("enabled", true, "limit", 4), entries.get(1).inlineConfig());
    assertEquals(Map.of("existing", "kept"), configs.get(0));
    assertEquals(Map.of("enabled", true, "limit", 4), configs.get(1));
  }

  private static PipelineDef pipeline(String name, String producer, List<Object> processors, String consumer) {
    return new PipelineDef(
        name, "", producer, processors, consumer,
        List.of(), List.of(), List.of(), Map.of(), List.of());
  }

  private static ModuleDef module(String name) {
    return new ModuleDef(name, Object.class.getName(), null, Map.of(), List.of());
  }
}
