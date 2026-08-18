package pl.gov.nac.warc.config;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class ConfigResolver {

  private static final Logger log = LogManager.getLogger(ConfigResolver.class);

  private ConfigResolver() {
  }

  public static PipelineDef resolvePipeline(String name, Map<String, PipelineDef> pipelines, ModuleRegistry modules) {
    PipelineDef def = pipelines.get(name);
    if (def == null) {
      throw new IllegalArgumentException("Unknown pipeline: " + name);
    }

    // Simple pipeline -> return as-is
    if (def.producer != null || !def.processorsRaw.isEmpty() || def.consumer != null) {
      return def;
    }

    // Multi-stage pipeline -> flatten stages

    List<String> allStages = new ArrayList<>();
    allStages.addAll(def.beforeStages);
    allStages.addAll(def.mainStages);
    allStages.addAll(def.afterStages);

    if (allStages.isEmpty()) {
      throw new IllegalArgumentException("Multi-stage pipeline '" + name + "' has no stages");
    }

    // Step 1: Pre-resolve virtual stages (stages defined only in overrides)
    Map<String, PipelineDef> allPipelines = new LinkedHashMap<>(pipelines);

    for (int idx = 0; idx < allStages.size(); idx++) {
      String stageName = allStages.get(idx);
      if (allPipelines.containsKey(stageName)) {
        continue;
      }

      // Check if defined in overrides as a module
      if (def.overrides != null && def.overrides.containsKey(stageName)) {
        Object override = def.overrides.get(stageName);
        if (override instanceof Map) {
          Map<?, ?> map = (Map<?, ?>) override;
          Object moduleName = map.get("module");

              if (moduleName instanceof String) {
                log.debug("Found override for {}, module={}", stageName, moduleName);
                String mod = (String) moduleName;
                boolean firstStage = idx == 0;
                boolean lastStage = idx == allStages.size() - 1;

                String prod = null;
                String cons = null;
                List<Object> procs = new ArrayList<>();

                if (firstStage) {
                  if (modules.getProducer(mod) == null) {
                    throw new IllegalArgumentException(
                        "Stage '" + stageName + "' is first and must reference a producer module, got: " + mod);
                  }
                  prod = mod;
                } else if (lastStage) {
                  if (modules.getConsumer(mod) == null) {
                    throw new IllegalArgumentException(
                        "Stage '" + stageName + "' is last and must reference a consumer module, got: " + mod);
                  }
                  cons = mod;
                } else {
                  if (modules.getProcessor(mod) == null) {
                    throw new IllegalArgumentException(
                        "Stage '" + stageName + "' is intermediate and must reference a processor module, got: " + mod);
                  }
                  procs.add(mod);
                }

                PipelineDef synthetic = new PipelineDef(
                    stageName,
                    "Synthetic stage from overrides",
                    prod,
                    procs,
                    cons,
                    List.of(), List.of(), List.of(), Map.of(), List.of());
                allPipelines.put(stageName, synthetic);
                continue;
              }
            }
          }

      throw new IllegalArgumentException("Unknown pipeline stage: " + stageName +
          " (and not defined as module in overrides)");
    }

    // First stage defines producer
    PipelineDef first = allPipelines.get(allStages.get(0));
    if (first == null || first.producer == null) {
      throw new IllegalArgumentException("First stage '" + allStages.get(0) + "' has no producer");
    }

    // Last stage defines consumer
    PipelineDef last = allPipelines.get(allStages.get(allStages.size() - 1));
    if (last == null || last.consumer == null) {
      throw new IllegalArgumentException(
          "Last stage '" + (last != null ? last.name : "null") + "' has no consumer");
    }

    // Flatten processors (everything in between, plus potentially parts of
    // first/last if consistent?)
    // Existing logic just takes ALL processors from ALL stages.
    List<Object> processors = new ArrayList<>();
    for (String s : allStages) {
      PipelineDef stage = allPipelines.get(s);
      if (stage == null) {
        throw new IllegalArgumentException("Unknown stage pipeline: " + s);
      }
      processors.addAll(stage.processorsRaw);
    }

    // For multi-stage top-level pipeline, we keep its overrides
    return new PipelineDef(
        name,
        def.description,
        first.producer,
        processors,
        last.consumer,
        List.of(), // before stages not used after flattening
        List.of(), // main stages not used
        List.of(), // after stages not used
        def.overrides,
        def.argsRaw);
  }

  public static List<ProcessorEntry> flattenProcessors(PipelineDef def) {
    List<ProcessorEntry> out = new ArrayList<>();

    for (Object raw : def.processorsRaw) {
      if (raw instanceof String name) {
        out.add(new ProcessorEntry(name, Map.of()));
      } else if (raw instanceof Map<?, ?> map) {
        Map.Entry<?, ?> entry = map.entrySet().iterator().next();
        String name = entry.getKey().toString();
        Map<String, Object> inline = new LinkedHashMap<>();
        if (entry.getValue() instanceof Map<?, ?> inner) {
          Object cfg = inner.get("config");
          if (cfg instanceof Map<?, ?> cfgMap) {
            for (Map.Entry<?, ?> cfgEntry : cfgMap.entrySet()) {
              inline.put(cfgEntry.getKey().toString(), cfgEntry.getValue());
            }
          }
        }
        out.add(new ProcessorEntry(name, inline));
      } else {
        throw new IllegalArgumentException("Invalid processor entry: " + raw);
      }
    }

    return out;
  }

  public static record ProcessorEntry(String name, Map<String, Object> inlineConfig) {
  }

  public static void applyInlineOverrides(
      List<ProcessorEntry> entries,
      List<Map<String, Object>> configs) {
    for (int i = 0; i < entries.size(); i++) {
      Map<String, Object> inline = entries.get(i).inlineConfig;
      if (!inline.isEmpty()) {
        log.info("Applying Tier 2 (Inline) override for {}", entries.get(i).name);
        OverrideResolver.deepMerge(configs.get(i), inline);
      }
    }
  }

  public static void applyPipelineOverrides(PipelineDef def,
      ModuleRegistry modules,
      Map<String, Object> producerConfig,
      List<String> processorNames,
      List<Map<String, Object>> processorConfigs,
      Map<String, Object> consumerConfig,
      List<String> beforeNames,
      List<Map<String, Object>> beforeConfigs,
      List<String> afterNames,
      List<Map<String, Object>> afterConfigs) {

    Map<String, Object> pipelineOverrides = def.overrides;
    if (pipelineOverrides == null || pipelineOverrides.isEmpty()) {
      return;
    }

    log.info("Pipeline '{}' overrides detected: {}", def.name, pipelineOverrides.keySet());

    // Producer
    if (def.producer != null) {
      Map<String, Object> prodOverride = OverrideResolver.resolveForModule("producer", def.producer,
          pipelineOverrides);

      if (!prodOverride.isEmpty()) {
        log.info("Merging producer overrides into {}: {}", def.producer, prodOverride);
        OverrideResolver.deepMerge(producerConfig, prodOverride);
      }
    }

    // Processors
    for (int i = 0; i < processorNames.size(); i++) {
      String procName = processorNames.get(i);
      Map<String, Object> procCfg = processorConfigs.get(i);

      Map<String, Object> procOverride = OverrideResolver.resolveForModule("processor", procName,
          pipelineOverrides);

      if (!procOverride.isEmpty()) {
        log.info("Merging processor overrides into {}", procName);
        OverrideResolver.deepMerge(procCfg, procOverride);
      }
    }

    // Nested overrides: when a processor is enabled and has 'overrides' map, merge
    // those into other processors
    // This allows extract.overrides.grep to enable and configure grep when extract
    // is enabled
    for (int i = 0; i < processorNames.size(); i++) {
      Map<String, Object> procCfg = processorConfigs.get(i);
      Object enabledVal = procCfg.get("enabled");
      boolean isEnabled = enabledVal instanceof Boolean b ? b : "true".equalsIgnoreCase(String.valueOf(enabledVal));

      if (isEnabled && procCfg.containsKey("overrides")) {
        @SuppressWarnings("unchecked")
        Map<String, Object> nestedOverrides = (Map<String, Object>) procCfg.get("overrides");
        if (nestedOverrides != null) {
          for (Map.Entry<String, Object> entry : nestedOverrides.entrySet()) {
            String targetName = entry.getKey();
            @SuppressWarnings("unchecked")
            Map<String, Object> targetOverride = (Map<String, Object>) entry.getValue();

            // Find the target processor and merge the nested override
            for (int j = 0; j < processorNames.size(); j++) {
              if (processorNames.get(j).equals(targetName)) {
                log.info("Applying nested override from {} to {}", processorNames.get(i), targetName);
                OverrideResolver.deepMerge(processorConfigs.get(j), targetOverride);
              }
            }
          }
        }
      }
    }

    // Consumer
    if (def.consumer != null) {
      Map<String, Object> consOverride = OverrideResolver.resolveForModule("consumer", def.consumer,
          pipelineOverrides);

      if (!consOverride.isEmpty()) {
        log.info("Merging consumer overrides into {}", def.consumer);
        OverrideResolver.deepMerge(consumerConfig, consOverride);
      }
    }

    // Before checkers - try both "before.name" and "checker.name" prefixes
    for (int i = 0; i < beforeNames.size(); i++) {
      String chkName = beforeNames.get(i);
      Map<String, Object> chkCfg = beforeConfigs.get(i);

      // Try stage-specific prefix first, then generic checker prefix
      Map<String, Object> chkOverride = OverrideResolver.resolveForModule("before", chkName, pipelineOverrides);
      if (chkOverride.isEmpty()) {
        chkOverride = OverrideResolver.resolveForModule("checker", chkName, pipelineOverrides);
      }

      if (!chkOverride.isEmpty()) {
        log.info("Merging before-checker overrides into {}", chkName);
        OverrideResolver.deepMerge(chkCfg, chkOverride);
      }
    }

    // After checkers - try both "after.name" and "checker.name" prefixes
    for (int i = 0; i < afterNames.size(); i++) {
      String chkName = afterNames.get(i);
      Map<String, Object> chkCfg = afterConfigs.get(i);

      // Try stage-specific prefix first, then generic checker prefix
      Map<String, Object> chkOverride = OverrideResolver.resolveForModule("after", chkName, pipelineOverrides);
      if (chkOverride.isEmpty()) {
        chkOverride = OverrideResolver.resolveForModule("checker", chkName, pipelineOverrides);
      }

      if (!chkOverride.isEmpty()) {
        log.info("Merging after-checker overrides into {}", chkName);
        OverrideResolver.deepMerge(chkCfg, chkOverride);
      }
    }
  }

  /**
   * Apply nested overrides from enabled processors that have 'overrides' map.
   * This is called AFTER CLI overrides so that
   * --processor.extract-text.enabled=true
   * can trigger nested overrides like extract.overrides.grep.
   */
  public static void applyNestedOverrides(List<String> processorNames, List<Map<String, Object>> processorConfigs) {
    for (int i = 0; i < processorNames.size(); i++) {
      Map<String, Object> procCfg = processorConfigs.get(i);
      Object enabledVal = procCfg.get("enabled");
      boolean isEnabled = enabledVal instanceof Boolean b ? b : "true".equalsIgnoreCase(String.valueOf(enabledVal));

      if (isEnabled && procCfg.containsKey("overrides")) {
        @SuppressWarnings("unchecked")
        Map<String, Object> nestedOverrides = (Map<String, Object>) procCfg.get("overrides");
        if (nestedOverrides != null) {
          for (Map.Entry<String, Object> entry : nestedOverrides.entrySet()) {
            String targetName = entry.getKey();
            @SuppressWarnings("unchecked")
            Map<String, Object> targetOverride = (Map<String, Object>) entry.getValue();

            // Find the target processor and merge the nested override
            for (int j = 0; j < processorNames.size(); j++) {
              if (processorNames.get(j).equals(targetName)) {
                log.info("Applying nested override from {} to {}", processorNames.get(i), targetName);
                OverrideResolver.deepMerge(processorConfigs.get(j), targetOverride);
              }
            }
          }
        }
      }
    }
  }
}
