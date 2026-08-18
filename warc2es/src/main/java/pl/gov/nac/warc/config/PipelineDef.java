package pl.gov.nac.warc.config;

import java.util.List;
import java.util.Map;

public final class PipelineDef {
  public final String name;
  public final String description;

  // Simple pipeline
  public String producer;
  public final List<Object> processorsRaw;
  public String consumer;

  // Multi-stage pipeline (stage names)
  public final List<String> beforeStages; // for multi-stage: stage names; for simple: checker names
  public final List<String> mainStages; // stages
  public final List<String> afterStages; // for multi-stage: stage names; for simple: checker names

  // Pipeline-level overrides (hierarchical, not dotted keys)
  public final Map<String, Object> overrides;

  // Pipeline-level arg definitions for short flags and positional args
  public final List<Object> argsRaw;

  public PipelineDef(String name,
      String description,
      String producer,
      List<Object> processorsRaw,
      String consumer,
      List<String> beforeStages,
      List<String> mainStages,
      List<String> afterStages,
      Map<String, Object> overrides,
      List<Object> argsRaw) {

    this.name = name;
    this.description = description;

    this.producer = producer;
    this.processorsRaw = processorsRaw == null ? List.of() : processorsRaw;
    this.consumer = consumer;

    this.beforeStages = beforeStages == null ? List.of() : beforeStages;
    this.mainStages = mainStages == null ? List.of() : mainStages;
    this.afterStages = afterStages == null ? List.of() : afterStages;

    this.overrides = overrides == null ? Map.of() : overrides;
    this.argsRaw = argsRaw == null ? List.of() : argsRaw;
  }
}
