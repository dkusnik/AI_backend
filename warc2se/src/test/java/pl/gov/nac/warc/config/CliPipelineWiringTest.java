package pl.gov.nac.warc.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

class CliPipelineWiringTest {

  @Test
  void testWarcCliCommandsMapToExpectedPipelineIds() throws Exception {
    String warcCli = readResourceText("main/dist/bin/warc-cli");
    String esCli = readResourceText("main/dist/bin/es-cli");

    assertFunctionRunsPipeline(warcCli, "cmd_extract", "warc2wet");
    assertFunctionRunsPipeline(warcCli, "cmd_dedupe", "dedupe");
    assertFunctionRunsPipeline(warcCli, "cmd_grep", "warc-grep");
    assertFunctionRunsPipeline(warcCli, "cmd_convert", "warc2warc");
    assertFunctionRunsPipeline(warcCli, "cmd_merge", "doet-merge");
    assertFunctionRunsPipeline(warcCli, "cmd_info", "warc-info");
    assertFunctionRunsPipeline(warcCli, "cmd_validate", "warc-validate");
    assertFunctionRunsPipeline(warcCli, "cmd_regen_cdxj", "warc2warc");
    assertFunctionRunsPipeline(warcCli, "cmd_regen_digests", "warc2warc");
    assertFunctionRunsPipeline(warcCli, "cmd_regen_zip", "warc2warc");

    assertTrue(esCli.contains("run_pipeline es-loader"),
        "es-cli load command should dispatch to es-loader pipeline");
  }

  @Test
  void testActivePipelinesReferenceOnlyConfiguredModules() throws Exception {
    Map<String, Object> root = loadMainConfig();
    Map<String, Object> pipelines = asMap(root.get("pipelines"));
    Map<String, Object> modules = asMap(root.get("modules"));

    Set<String> producerIds = asMap(modules.get("producers")).keySet();
    Set<String> processorIds = asMap(modules.get("processors")).keySet();
    Set<String> consumerIds = asMap(modules.get("consumers")).keySet();
    Set<String> checkerIds = asMap(modules.get("checkers")).keySet();

    Set<String> active = Set.of(
        "warc2wet", "warc-grep", "warc2warc", "dedupe",
        "doet-merge", "warc-validate", "warc-info", "es-loader");

    List<String> missing = new ArrayList<>();
    for (String pipelineId : active) {
      Map<String, Object> pipeline = asMap(pipelines.get(pipelineId));
      Set<String> refs = referencedModules(pipeline, producerIds, processorIds, consumerIds, checkerIds);
      for (String ref : refs) {
        if (!producerIds.contains(ref) && !processorIds.contains(ref)
            && !consumerIds.contains(ref) && !checkerIds.contains(ref)) {
          missing.add(pipelineId + ":" + ref);
        }
      }
    }

    assertTrue(missing.isEmpty(), "Active pipelines reference unknown modules: " + missing);
  }

  @Test
  void testNoOrphanModuleReferencesInActiveCliPipelines() throws Exception {
    Map<String, Object> root = loadMainConfig();
    Map<String, Object> pipelines = asMap(root.get("pipelines"));
    Map<String, Object> modules = asMap(root.get("modules"));

    Set<String> producerIds = asMap(modules.get("producers")).keySet();
    Set<String> processorIds = asMap(modules.get("processors")).keySet();
    Set<String> consumerIds = asMap(modules.get("consumers")).keySet();
    Set<String> checkerIds = asMap(modules.get("checkers")).keySet();

    Set<String> active = Set.of(
        "warc2wet", "warc-grep", "warc2warc", "dedupe",
        "doet-merge", "warc-validate", "warc-info", "es-loader");

    Set<String> allReferences = new LinkedHashSet<>();
    for (String pipelineId : active) {
      Map<String, Object> pipeline = asMap(pipelines.get(pipelineId));
      allReferences.addAll(referencedModules(pipeline, producerIds, processorIds, consumerIds, checkerIds));
    }

    assertFalse(allReferences.isEmpty(), "Expected active pipelines to reference at least one module");
    for (String ref : allReferences) {
      assertTrue(
          producerIds.contains(ref) || processorIds.contains(ref)
              || consumerIds.contains(ref) || checkerIds.contains(ref),
          "Referenced module is not registered: " + ref);
    }
  }

  @Test
  void testValidateUsesArchiveJwarcAndOthersUseArchiveChunked() throws Exception {
    Map<String, Object> root = loadMainConfig();
    Map<String, Object> pipelines = asMap(root.get("pipelines"));

    Map<String, Object> validate = asMap(pipelines.get("warc-validate"));
    assertEquals("archive-jwarc", validate.get("producer"),
        "warc-validate must use archive-jwarc producer");

    for (String p : List.of("warc-grep", "warc2warc", "dedupe", "es-loader")) {
      Map<String, Object> def = asMap(pipelines.get(p));
      assertEquals("archive-chunked", def.get("producer"),
          p + " must use archive-chunked producer");
    }

    Map<String, Object> warc2wet = asMap(pipelines.get("warc2wet"));
    Map<String, Object> overrides = asMap(warc2wet.get("overrides"));
    Map<String, Object> input = asMap(overrides.get("input"));
    assertEquals("archive-chunked", input.get("module"),
        "warc2wet input stage must resolve to archive-chunked");

    Map<String, Object> doesMerge = asMap(pipelines.get("doet-merge"));
    List<Object> chain = asList(doesMerge.get("chain"));
    assertTrue(chain.contains("archive-chunked"),
        "doet-merge chain should include archive-chunked");
  }

  private Set<String> referencedModules(
      Map<String, Object> pipeline,
      Set<String> producerIds,
      Set<String> processorIds,
      Set<String> consumerIds,
      Set<String> checkerIds) {
    Set<String> refs = new LinkedHashSet<>();

    addIfString(refs, pipeline.get("producer"));
    addIfString(refs, pipeline.get("consumer"));
    addStringList(refs, pipeline.get("processors"));
    addStringList(refs, pipeline.get("before"));
    addStringList(refs, pipeline.get("after"));

    Map<String, Object> overrides = asMap(pipeline.get("overrides"));
    for (Map.Entry<String, Object> e : overrides.entrySet()) {
      Map<String, Object> stage = asMap(e.getValue());
      Object mod = stage.get("module");
      if (mod instanceof String s && !s.isBlank()) {
        refs.add(s);
      }
    }

    List<Object> chain = asList(pipeline.get("chain"));
    for (Object tokenObj : chain) {
      if (!(tokenObj instanceof String token) || token.isBlank()) {
        continue;
      }
      if (producerIds.contains(token) || processorIds.contains(token)
          || consumerIds.contains(token) || checkerIds.contains(token)) {
        refs.add(token);
      }
    }

    return refs;
  }

  private void assertFunctionRunsPipeline(String script, String functionName, String pipelineName) {
    String quotedFunction = Pattern.quote(functionName + "()");
    String quotedPipeline = Pattern.quote("run_pipeline " + pipelineName);
    Pattern p = Pattern.compile(quotedFunction + "[\\s\\S]*?" + quotedPipeline);
    assertTrue(p.matcher(script).find(), functionName + " should invoke " + pipelineName);
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> asMap(Object o) {
    return o instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of();
  }

  @SuppressWarnings("unchecked")
  private List<Object> asList(Object o) {
    return o instanceof List<?> l ? (List<Object>) l : List.of();
  }

  private void addStringList(Set<String> refs, Object o) {
    for (Object v : asList(o)) {
      addIfString(refs, v);
    }
  }

  private void addIfString(Set<String> refs, Object o) {
    if (o instanceof String s && !s.isBlank()) {
      refs.add(s);
    }
  }

  private Map<String, Object> loadMainConfig() throws Exception {
    try (InputStream is = getClass().getClassLoader().getResourceAsStream("config.yaml")) {
      assertNotNull(is, "config.yaml must be on classpath");
      Object loaded = new Yaml().load(is);
      return asMap(loaded);
    }
  }

  private String readResourceText(String pathInSrcMain) throws Exception {
    Path p = Path.of("src", pathInSrcMain);
    assertTrue(Files.exists(p), "File not found: " + p);
    return Files.readString(p, StandardCharsets.UTF_8);
  }
}
