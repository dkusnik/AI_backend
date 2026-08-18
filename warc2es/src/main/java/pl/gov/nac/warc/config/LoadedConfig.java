package pl.gov.nac.warc.config;

import java.util.List;
import java.util.Map;

import pl.gov.nac.warc.reactive.ReactiveInterfaces;

public final class LoadedConfig {

  public final Map<String, Object> rawYaml;

  public final PipelineDef pipelineDef;

  public final ReactiveInterfaces.ReactiveProducer<?> producer;
  public final List<ReactiveInterfaces.ReactiveProcessor<?, ?>> processors;
  public final ReactiveInterfaces.ReactiveConsumer<?> consumer;

  // Effective configs (module defaults + pipeline overrides)
  public final Map<String, Object> producerConfig;
  public final List<Map<String, Object>> processorConfigs;
  public final Map<String, Object> consumerConfig;

  public final List<String> resolvedStages;

  public final ModuleRegistry modules;

  public final String engineType; // "virtual" or "reactive"
  public final String resultFormat; // "human" or "json"
  public final int globalConcurrencyCap;
  public final int recordSizeThresholdMB; // Records >= this use count-based throttling
  public final int maxRecords; // Queue capacity floor and parallel-GZIP worker cap
  public final int shutdownTimeoutSeconds;
  public final boolean parallelGzip;
  public final int parallelGzipLevel;
  public final boolean isalEnabled;

  // Pipeline-level checkers
  public final List<ReactiveInterfaces.ReactiveModule> beforeCheckers;
  public final List<Map<String, Object>> beforeCheckerConfigs;
  public final List<ReactiveInterfaces.ReactiveModule> afterCheckers;
  public final List<Map<String, Object>> afterCheckerConfigs;
  public final boolean isVerbose;
  public final boolean isDryRun;
  public final boolean isBenchmark;

  /** Silent mode - suppresses all non-essential output */
  public final boolean isSilent;

  /** Progress output mode */
  public final ProgressMode progressMode;

  /** Final report output mode */
  public final FinalReportMode finalReportMode;

  /** If true, write final report to stderr instead of stdout */
  public final boolean finalReportToStderr;

  /** If true, suppress all log output */
  public final boolean logCliNone;

  public LoadedConfig(Map<String, Object> rawYaml,
      PipelineDef pipelineDef,
      ReactiveInterfaces.ReactiveProducer<?> producer,
      List<ReactiveInterfaces.ReactiveProcessor<?, ?>> processors,
      ReactiveInterfaces.ReactiveConsumer<?> consumer,
      Map<String, Object> producerConfig,
      List<Map<String, Object>> processorConfigs,
      Map<String, Object> consumerConfig,
      List<String> resolvedStages,
      ModuleRegistry modules,
      String engineType,
      String resultFormat,
      int globalConcurrencyCap,
      int recordSizeThresholdMB,
      int maxRecords,
      int shutdownTimeoutSeconds,
      List<ReactiveInterfaces.ReactiveModule> beforeCheckers,
      List<Map<String, Object>> beforeCheckerConfigs,
      List<ReactiveInterfaces.ReactiveModule> afterCheckers,
      List<Map<String, Object>> afterCheckerConfigs,
      boolean isVerbose,
      boolean isDryRun,
      boolean isBenchmark,
      boolean isSilent,
      ProgressMode progressMode,
      FinalReportMode finalReportMode,
      boolean finalReportToStderr,
      boolean logCliNone,
      boolean parallelGzip,
      int parallelGzipLevel,
      boolean isalEnabled) {

    this.rawYaml = rawYaml;
    this.pipelineDef = pipelineDef;
    this.isVerbose = isVerbose;
    this.isDryRun = isDryRun;
    this.isBenchmark = isBenchmark;
    this.isSilent = isSilent;
    this.progressMode = progressMode;
    this.finalReportMode = finalReportMode;
    this.finalReportToStderr = finalReportToStderr;
    this.logCliNone = logCliNone;

    this.producer = producer;
    this.processors = processors;
    this.consumer = consumer;

    this.producerConfig = producerConfig;
    this.processorConfigs = processorConfigs;
    this.consumerConfig = consumerConfig;

    this.resolvedStages = resolvedStages;
    this.modules = modules;

    this.engineType = engineType;
    this.resultFormat = resultFormat;
    this.globalConcurrencyCap = globalConcurrencyCap;
    this.recordSizeThresholdMB = recordSizeThresholdMB;
    this.maxRecords = maxRecords;
    this.shutdownTimeoutSeconds = shutdownTimeoutSeconds;
    this.parallelGzip = parallelGzip;
    this.parallelGzipLevel = parallelGzipLevel;
    this.isalEnabled = isalEnabled;

    this.beforeCheckers = beforeCheckers;
    this.beforeCheckerConfigs = beforeCheckerConfigs;
    this.afterCheckers = afterCheckers;
    this.afterCheckerConfigs = afterCheckerConfigs;
  }

  public String engineType() {
    return engineType;
  }

  public boolean isJsonResult() {
    return "json".equals(resultFormat);
  }

  public int concurrency() {
    return globalConcurrencyCap;
  }

  public PipelineDef pipelineDef() {
    return pipelineDef;
  }

  public Map<String, Object> resolvedConfig() {
    return rawYaml;
  }
}
