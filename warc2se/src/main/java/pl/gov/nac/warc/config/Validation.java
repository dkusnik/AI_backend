package pl.gov.nac.warc.config;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class Validation {

    private Validation() {
    }

    public static void validateBaseStructure(Map<String, Object> global,
            Map<String, PipelineDef> pipelines,
            ModuleRegistry modules) {

        Objects.requireNonNull(global, "global section cannot be null");
        Objects.requireNonNull(pipelines, "pipelines section cannot be null");
        Objects.requireNonNull(modules, "modules registry cannot be null");

        validateGlobal(global);
        validateModuleRegistry(modules);
        validatePipelinesExist(pipelines);
        validatePipelineShapes(pipelines);
    }

    public static void validateEffectivePipeline(PipelineDef def,
            ModuleRegistry modules) {

        if (def.producer == null) {
            throw new IllegalArgumentException("Effective pipeline has no producer");
        }
        if (def.consumer == null) {
            throw new IllegalArgumentException("Effective pipeline has no consumer");
        }

        if (modules.getProducer(def.producer) == null) {
            throw new IllegalArgumentException("Unknown producer module: " + def.producer);
        }
        if (modules.getConsumer(def.consumer) == null) {
            throw new IllegalArgumentException("Unknown consumer module: " + def.consumer);
        }

        for (Object raw : def.processorsRaw) {
            String name;
            if (raw instanceof String) {
                name = (String) raw;
            } else if (raw instanceof Map) {
                name = ((Map<?, ?>) raw).keySet().iterator().next().toString();
            } else {
                throw new IllegalArgumentException("Invalid processor entry in effective pipeline: " + raw);
            }

            if (modules.getProcessor(name) == null) {
                throw new IllegalArgumentException("Unknown processor module: " + name);
            }
        }
    }

    public static void validateResolvedPipeline(PipelineDef def,
            List<ConfigResolver.ProcessorEntry> resolvedStages,
            ModuleRegistry modules) {
        validateEffectivePipeline(def, modules);

        Objects.requireNonNull(resolvedStages, "resolvedStages cannot be null");

        for (ConfigResolver.ProcessorEntry entry : resolvedStages) {
            if (entry == null || entry.name() == null || entry.name().isBlank()) {
                throw new IllegalArgumentException("Resolved processor stage has empty name");
            }
            if (modules.getProcessor(entry.name()) == null) {
                throw new IllegalArgumentException("Unknown resolved processor module: " + entry.name());
            }
        }

        for (String checker : def.beforeStages) {
            if (modules.getChecker(checker) == null) {
                throw new IllegalArgumentException("Unknown before-checker module: " + checker);
            }
        }
        for (String checker : def.afterStages) {
            if (modules.getChecker(checker) == null) {
                throw new IllegalArgumentException("Unknown after-checker module: " + checker);
            }
        }
    }

    private static void validateGlobal(Map<String, Object> global) {
        Object cap = global.getOrDefault("globalConcurrencyCap", 0);
        Object timeout = global.getOrDefault("shutdownTimeoutSeconds", 60);

        try {
            Integer.parseInt(cap.toString());
            Integer.parseInt(timeout.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("global numeric fields must be integers");
        }
    }

    private static void validateModuleRegistry(ModuleRegistry modules) {
        modules.producers.values().forEach(m -> {
            if (m.className == null || m.className.isBlank()) {
                throw new IllegalArgumentException("Producer module '" + m.name + "' has no className");
            }
        });
        modules.processors.values().forEach(m -> {
            if (m.className == null || m.className.isBlank()) {
                throw new IllegalArgumentException("Processor module '" + m.name + "' has no className");
            }
        });
        modules.consumers.values().forEach(m -> {
            if (m.className == null || m.className.isBlank()) {
                throw new IllegalArgumentException("Consumer module '" + m.name + "' has no className");
            }
        });
        modules.checkers.values().forEach(m -> {
            if (m.className == null || m.className.isBlank()) {
                throw new IllegalArgumentException("Checker module '" + m.name + "' has no className");
            }
        });
    }

    private static void validatePipelinesExist(Map<String, PipelineDef> pipelines) {
        if (pipelines.isEmpty()) {
            throw new IllegalArgumentException("No pipelines defined");
        }
    }

    /**
     * SIMPLE:
     * - producer / processors / consumer
     * - may also have before / after (used as checker lists)
     *
     * MULTI-STAGE:
     * - stages (+ optional before/after as stage names)
     * - must not have producer / processors / consumer
     */
    private static void validatePipelineShapes(Map<String, PipelineDef> pipelines) {

        for (PipelineDef def : pipelines.values()) {

            boolean hasSimple = def.producer != null ||
                    (def.processorsRaw != null && !def.processorsRaw.isEmpty()) ||
                    def.consumer != null;

            boolean hasStages = def.mainStages != null && !def.mainStages.isEmpty();

            if (hasSimple && hasStages) {
                throw new IllegalArgumentException(
                        "Pipeline '" + def.name + "' mixes simple and multi-stage fields");
            }

            if (!hasSimple && !hasStages) {
                throw new IllegalArgumentException(
                        "Pipeline '" + def.name + "' has neither simple fields nor stages");
            }
        }
    }
}
