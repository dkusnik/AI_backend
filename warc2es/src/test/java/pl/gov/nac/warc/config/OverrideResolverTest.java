package pl.gov.nac.warc.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class OverrideResolverTest {

  @AfterEach
  void clearCliOverrides() {
    OverrideResolver.CliOverrides.load(new String[0]);
  }

  @Test
  void resolvesStageThenStageModuleThenModulePrecedence() {
    Map<String, Object> overrides = new LinkedHashMap<>();
    overrides.put("producer", Map.of(
        "timeout", 1,
        "nested", new LinkedHashMap<>(Map.of("stage", true))));
    overrides.put("producer.reader", Map.of(
        "timeout", 2,
        "nested", new LinkedHashMap<>(Map.of("stageModule", true))));
    overrides.put("reader", Map.of(
        "timeout", 3,
        "nested", new LinkedHashMap<>(Map.of("module", true))));

    Map<String, Object> result = OverrideResolver.resolveForModule("producer", "reader", overrides);

    assertEquals(3, result.get("timeout"));
    assertEquals(Map.of("stage", true, "stageModule", true, "module", true), result.get("nested"));
  }

  @Test
  void expandsFlatDottedKeysButNormalizesOnlyTopLevelLogicalScalars() {
    Map<String, Object> overrides = new LinkedHashMap<>();
    overrides.put("producer.reader.engine.mode", "/tmp/config/parallel");
    overrides.put("producer.reader.output", "C:\\config\\bytes\\");

    Map<String, Object> result = OverrideResolver.resolveForModule("producer", "reader", overrides);

    assertEquals(Map.of("mode", "/tmp/config/parallel"), result.get("engine"));
    assertEquals("bytes", result.get("output"));
  }

  @Test
  void appliesExactWildcardAndModuleOnlyCliForms() {
    OverrideResolver.CliOverrides.load(new String[] {
        "--producer.reader.limit=10",
        "--producer.*.enabled=false",
        "--reader.names=one,two"
    });
    Map<String, Object> target = new LinkedHashMap<>();

    OverrideResolver.applyCliOverrides("producer", "reader", target);

    assertEquals(10, target.get("limit"));
    assertEquals(false, target.get("enabled"));
    assertEquals(List.of("one", "two"), target.get("names"));
  }

  @Test
  void deepMergeRecursesAndReplacesScalarValues() {
    Map<String, Object> target = new LinkedHashMap<>();
    target.put("nested", new LinkedHashMap<>(Map.of("keep", 1, "replace", 2)));
    target.put("scalar", "before");

    OverrideResolver.deepMerge(target, Map.of(
        "nested", Map.of("replace", 3, "add", 4),
        "scalar", "after"));

    assertEquals(Map.of("keep", 1, "replace", 3, "add", 4), target.get("nested"));
    assertEquals("after", target.get("scalar"));
  }
}
