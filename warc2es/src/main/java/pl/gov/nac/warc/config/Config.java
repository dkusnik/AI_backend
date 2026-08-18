package pl.gov.nac.warc.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.Yaml;

import pl.gov.nac.warc.reactive.ReactiveInterfaces;
import pl.gov.nac.warc.reactive.VerbosityManager;
import pl.gov.nac.warc.reactive.VerbosityManager.VerbosityMode;

/**
 * Central configuration loader for the reactive pipeline system.
 */
public final class Config {

  private static final Logger log = LogManager.getLogger(Config.class);

  private Config() {
  }

  public static final class CliSignalException extends Exception {
    private final int exitCode;
    private final String output;

    public CliSignalException(int exitCode, String output) {
      super(output);
      this.exitCode = exitCode;
      this.output = output;
    }

    public int exitCode() {
      return exitCode;
    }

    public String output() {
      return output;
    }
  }

  private record EffectiveConfigs(
      Map<String, Object> producerConfig,
      List<String> processorNames,
      List<Map<String, Object>> processorConfigs,
      Map<String, Object> consumerConfig,
      List<Map<String, Object>> beforeCheckerCfgs,
      List<Map<String, Object>> afterCheckerCfgs) {
  }

  // =====================================================================
  // MAIN LOAD METHOD
  // =====================================================================

  @SuppressWarnings("unchecked")
  public static LoadedConfig load(String[] args) throws Exception {
    return load(args, null);
  }

  @SuppressWarnings("unchecked")
  public static LoadedConfig load(String[] args, Path explicitConfigPath) throws Exception {

    // Apply verbosity as early as possible to silence warnings from
    // OverrideResolver
    VerbosityMode earlyMode = extractVerbosityMode(args);
    VerbosityManager.apply(earlyMode);

    OverrideResolver.CliOverrides.load(args);

    // ------------------------------------------------------------
    // HANDLE GLOBAL FLAGS (--help, --version) WITHOUT PIPELINE
    // ------------------------------------------------------------
    boolean hasPipeline = args.length > 0 && !args[0].startsWith("-");
    String pipelineName = hasPipeline ? args[0] : null;

    // ------------------------------------------------------------
    // LOAD YAML (filesystem OR classpath)
    // ------------------------------------------------------------
    YamlLoadResult yamlResult = loadYamlMap(explicitConfigPath);
    Map<String, Object> rawYaml = yamlResult.yaml();

    // ------------------------------------------------------------
    // EXTRACT SECTIONS
    // ------------------------------------------------------------
    Map<String, Object> global = (Map<String, Object>) rawYaml.get("global");
    Map<String, Object> pipelinesRaw = (Map<String, Object>) rawYaml.get("pipelines");
    Map<String, Object> modulesRaw = (Map<String, Object>) rawYaml.get("modules"); // Legacy
    Map<String, Object> componentsRaw = (Map<String, Object>) rawYaml.get("components"); // Phase 3

    if (global == null || pipelinesRaw == null || (modulesRaw == null && componentsRaw == null)) {
      throw new IllegalStateException("Config missing required sections (global, pipelines, and modules/components)");
    }

    // ------------------------------------------------------------
    // BUILD MODULE REGISTRY
    // ------------------------------------------------------------
    ModuleRegistry modules = buildModuleRegistry(modulesRaw, componentsRaw);

    // ------------------------------------------------------------
    // BUILD PIPELINE DEFINITIONS
    // ------------------------------------------------------------
    Map<String, PipelineDef> pipelines = buildPipelineDefs(pipelinesRaw);

    // ------------------------------------------------------------
    // VALIDATE BASE STRUCTURE
    // ------------------------------------------------------------
    Validation.validateBaseStructure(global, pipelines, modules);

    // ------------------------------------------------------------
    // RESOLVE PIPELINE (if specified)
    // ------------------------------------------------------------
    PipelineDef def = null;
    if (pipelineName != null) {
      def = ConfigResolver.resolvePipeline(pipelineName, pipelines, modules);
    } else {
      // Minimal dummy def to allow parsing global args
      def = new PipelineDef("none", "No pipeline", "", List.of(), "", List.of(), List.of(), List.of(), Map.of(),
          List.of());
    }

    // Apply CLI overrides for producer/consumer selection
    Object cliProducerObj = OverrideResolver.CliOverrides.get().get("producer");
    if (cliProducerObj != null) {
      String cliProducer = cliProducerObj.toString();
      log.info("Overriding producer: {} -> {}", def.producer, cliProducer);
      def.producer = cliProducer;
    }
    Object cliConsumerObj = OverrideResolver.CliOverrides.get().get("consumer");
    if (cliConsumerObj != null) {
      String cliConsumer = cliConsumerObj.toString();
      log.info("Overriding consumer: {} -> {}", def.consumer, cliConsumer);
      def.consumer = cliConsumer;
    }

    // ------------------------------------------------------------
    // FLATTEN PROCESSORS (Moved up for module scanning)
    // ------------------------------------------------------------
    List<ConfigResolver.ProcessorEntry> resolvedStages = ConfigResolver.flattenProcessors(def);

    // ------------------------------------------------------------
    // COLLECT & PARSE ARGS
    // Global + Pipeline + Active Module Args
    // ------------------------------------------------------------
    List<ArgParser.ArgDef> allArgDefs = collectArgumentDefinitions(global, def, resolvedStages, modules);

    // Parse
    String[] argsForParsing = hasPipeline ? java.util.Arrays.copyOfRange(args, 1, args.length) : args;
    Map<String, Object> parsedArgs = ArgParser.parse(argsForParsing, allArgDefs);

    // Help check
    // Help check
    if (Boolean.TRUE.equals(parsedArgs.get("global.help"))) {
      List<ArgParser.ArgDef> globals = allArgDefs.stream()
          .filter(a -> a.targetPath().startsWith("global.")).collect(java.util.stream.Collectors.toList());
      List<ArgParser.ArgDef> others = allArgDefs.stream()
          .filter(a -> !a.targetPath().startsWith("global.")).collect(java.util.stream.Collectors.toList());
      throw new CliSignalException(0, ArgParser.generateHelp(pipelineName, globals, others));
    }

    // Version check
    if (Boolean.TRUE.equals(parsedArgs.get("global.version"))) {
      throw new CliSignalException(0, BuildInfo.version());
    }

    if (pipelineName == null) {
      throw new IllegalArgumentException("Missing pipeline name (unless --help or --version is used)");
    }

    applyParsedArgsToOverrides(parsedArgs, def);

    String resultFormat = Objects.toString(
        parsedArgs.getOrDefault("global.resultFormat", global.get("resultFormat")), "human")
        .trim().toLowerCase(Locale.ROOT);
    if (!"human".equals(resultFormat) && !"json".equals(resultFormat)) {
      throw new IllegalArgumentException(
          "Invalid --result-format: " + resultFormat + " (expected human or json)");
    }

    // ------------------------------------------------------------
    // VALIDATE EFFECTIVE PIPELINE
    // ------------------------------------------------------------
    Validation.validateResolvedPipeline(def, resolvedStages, modules);

    // ------------------------------------------------------------
    // RESOLVE PATHS IN MODULE CONFIGS
    // (applied to registry configs; overrides are merged later)
    // ------------------------------------------------------------
    resolveModulePaths(modules);

    // ------------------------------------------------------------
    // BUILD EFFECTIVE CONFIGS (module defaults + pipeline overrides)
    // ------------------------------------------------------------
    EffectiveConfigs effective = buildEffectiveConfigs(def, resolvedStages, modules, global);
    Map<String, Object> producerConfig = effective.producerConfig();
    List<String> processorNames = effective.processorNames();
    List<Map<String, Object>> processorConfigs = effective.processorConfigs();
    Map<String, Object> consumerConfig = effective.consumerConfig();
    List<Map<String, Object>> beforeCheckerCfgs = effective.beforeCheckerCfgs();
    List<Map<String, Object>> afterCheckerCfgs = effective.afterCheckerCfgs();

    // ------------------------------------------------------------
    // APPLY PIPELINE-LEVEL OVERRIDES (hierarchical, no dotted keys)
    // ------------------------------------------------------------
    ConfigResolver.applyPipelineOverrides(
        def,
        modules,
        producerConfig,
        processorNames,
        processorConfigs,
        consumerConfig,
        def.beforeStages,
        beforeCheckerCfgs,
        def.afterStages,
        afterCheckerCfgs);

    // --- Tier 2: Inline/Element Overrides (merged after Tier 3) ---
    ConfigResolver.applyInlineOverrides(resolvedStages, processorConfigs);

    // --- Tier 1: Command Line Overrides (Highest Precedence) ---
    OverrideResolver.applyCliOverrides("producer", def.producer, producerConfig);
    // Also apply "input.*" as alias for producer (for flattened namespace)
    OverrideResolver.applyCliOverrides("input", null, producerConfig);
    for (int i = 0; i < processorNames.size(); i++) {
      OverrideResolver.applyCliOverrides("processor", processorNames.get(i), processorConfigs.get(i));
    }
    OverrideResolver.applyCliOverrides("consumer", def.consumer, consumerConfig);
    // Also apply "output.*" as alias for consumer (for flattened namespace)
    OverrideResolver.applyCliOverrides("output", null, consumerConfig);

    // FIX: Apply CLI overrides to checkers as well
    for (int i = 0; i < def.beforeStages.size(); i++) {
      String checkerName = def.beforeStages.get(i);
      // Allow overriding via --before.name or --checker.name
      OverrideResolver.applyCliOverrides("before", checkerName, beforeCheckerCfgs.get(i));
      OverrideResolver.applyCliOverrides("checker", checkerName, beforeCheckerCfgs.get(i));
    }
    for (int i = 0; i < def.afterStages.size(); i++) {
      String checkerName = def.afterStages.get(i);
      OverrideResolver.applyCliOverrides("after", checkerName, afterCheckerCfgs.get(i));
      OverrideResolver.applyCliOverrides("checker", checkerName, afterCheckerCfgs.get(i));
    }

    // --- Nested Overrides (after CLI overrides) ---
    // When a processor is enabled (via YAML or CLI) and has nested 'overrides' map,
    // merge those into other processors. This allows extract.overrides.grep to
    // enable and configure grep when extract is enabled via CLI.
    ConfigResolver.applyNestedOverrides(processorNames, processorConfigs);

    // ------------------------------------------------------------
    // DRY RUN & VERBOSE
    // ------------------------------------------------------------
    Object dryRunVal = global.get("dryRun");
    Object cliDryRunObj = OverrideResolver.CliOverrides.get().get("global.dryRun");
    String cliDryRun = cliDryRunObj != null ? cliDryRunObj.toString() : null;
    boolean isDryRun = (Boolean.TRUE.equals(dryRunVal) || "true".equalsIgnoreCase(String.valueOf(dryRunVal)))
        || (cliDryRun != null && "true".equalsIgnoreCase(cliDryRun));

    Object verboseVal = global.get("verbose");
    Object cliVerboseObj = OverrideResolver.CliOverrides.get().get("global.verbose");
    String cliVerbose = cliVerboseObj != null ? cliVerboseObj.toString() : null;
    boolean isVerbose = (Boolean.TRUE.equals(verboseVal) || "true".equalsIgnoreCase(String.valueOf(verboseVal)))
        || (cliVerbose != null && "true".equalsIgnoreCase(cliVerbose));

    Object benchVal = global.get("benchmark");
    Object cliBenchObj = OverrideResolver.CliOverrides.get().get("global.benchmark");
    String cliBench = cliBenchObj != null ? cliBenchObj.toString() : null;
    boolean isBenchmark = (Boolean.TRUE.equals(benchVal) || "true".equalsIgnoreCase(String.valueOf(benchVal)))
        || (cliBench != null && "true".equalsIgnoreCase(cliBench));

    // ------------------------------------------------------------
    // SILENT MODE & PROGRESS/REPORT PRESETS
    // ------------------------------------------------------------
    Object silentVal = global.get("silent");
    Object cliSilentObj = OverrideResolver.CliOverrides.get().get("global.silent");
    String cliSilent = cliSilentObj != null ? cliSilentObj.toString() : null;
    boolean isSilent = (Boolean.TRUE.equals(silentVal) || "true".equalsIgnoreCase(String.valueOf(silentVal)))
        || (cliSilent != null && "true".equalsIgnoreCase(cliSilent));

    // Progress Mode
    ProgressMode progressMode = ProgressMode.DEFAULT;
    Object cliProgressNoneObj = OverrideResolver.CliOverrides.get().get("global.progressNone");
    String cliProgressNone = cliProgressNoneObj != null ? cliProgressNoneObj.toString() : null;
    Object cliProgressVerboseObj = OverrideResolver.CliOverrides.get().get("global.verbose");
    String cliProgressVerbose = cliProgressVerboseObj != null ? cliProgressVerboseObj.toString() : null;

    if (isSilent || "true".equalsIgnoreCase(cliProgressNone)) {
      progressMode = ProgressMode.NONE;
    } else if (isVerbose || "true".equalsIgnoreCase(cliProgressVerbose)) {
      progressMode = ProgressMode.VERBOSE;
    }

    // Final Report Mode
    FinalReportMode finalReportMode = FinalReportMode.FULL;
    Object cliFinalReportNoneObj = OverrideResolver.CliOverrides.get().get("global.finalReportNone");
    String cliFinalReportNone = cliFinalReportNoneObj != null ? cliFinalReportNoneObj.toString() : null;
    Object cliFinalReportSummaryObj = OverrideResolver.CliOverrides.get().get("global.finalReportSummary");
    String cliFinalReportSummary = cliFinalReportSummaryObj != null ? cliFinalReportSummaryObj.toString() : null;
    Object cliFinalReportFullObj = OverrideResolver.CliOverrides.get().get("global.finalReportFull");
    String cliFinalReportFull = cliFinalReportFullObj != null ? cliFinalReportFullObj.toString() : null;

    if (isSilent || "true".equalsIgnoreCase(cliFinalReportNone)) {
      finalReportMode = FinalReportMode.NONE;
    } else if ("true".equalsIgnoreCase(cliFinalReportSummary)) {
      finalReportMode = FinalReportMode.SUMMARY;
    } else if (isVerbose || "true".equalsIgnoreCase(cliFinalReportFull)) {
      finalReportMode = FinalReportMode.FULL;
    }

    // Report Destination
    Object cliFinalReportToObj = OverrideResolver.CliOverrides.get().get("global.finalReportTo");
    String cliFinalReportTo = cliFinalReportToObj != null ? cliFinalReportToObj.toString() : null;
    boolean finalReportToStderr = "stderr".equalsIgnoreCase(cliFinalReportTo);

    // Log CLI Control
    Object cliLogCliNoneObj = OverrideResolver.CliOverrides.get().get("global.logCliNone");
    String cliLogCliNone = cliLogCliNoneObj != null ? cliLogCliNoneObj.toString() : null;
    boolean logCliNone = isSilent || "true".equalsIgnoreCase(cliLogCliNone);

    // Verbosity Mode Determination
    VerbosityMode verbosity = VerbosityMode.BRIEF;
    Object cliVerbosityObj = OverrideResolver.CliOverrides.get().get("global.verbosity");
    String cliVerbosity = cliVerbosityObj != null ? cliVerbosityObj.toString() : null;
    Object yamlVerbosity = global.get("verbosity");

    if (isSilent) {
      verbosity = VerbosityMode.SILENT;
    } else if (isBenchmark) {
      verbosity = VerbosityMode.BENCHMARK;
    } else if (isDryRun || isVerbose) {
      verbosity = VerbosityMode.VERBOSE;
    } else if (cliVerbosity != null) {
      verbosity = VerbosityMode.valueOf(cliVerbosity.toUpperCase());
    } else if (yamlVerbosity != null) {
      verbosity = VerbosityMode.valueOf(yamlVerbosity.toString().toUpperCase());
    }

    // Special CLI flags that override preset
    if (OverrideResolver.CliOverrides.get().containsKey("global.brief")) {
      verbosity = VerbosityMode.BRIEF;
    } else if (OverrideResolver.CliOverrides.get().containsKey("global.development")) {
      verbosity = VerbosityMode.DEVELOPMENT;
    } else if (OverrideResolver.CliOverrides.get().containsKey("global.debug")) {
      verbosity = VerbosityMode.DEBUG;
    }

    // Apply again after YAML+CLI resolution so profile/global verbosity takes effect.
    // Early apply still happens before parsing to reduce noisy startup logs.
    VerbosityManager.apply(verbosity);
    log.info("Config: {}", yamlResult.source());

    // ------------------------------------------------------------
    // CONFIG DUMP (verbose OR dry-run)
    // ------------------------------------------------------------
    if (isVerbose || isDryRun) {
      String format = Objects.toString(parsedArgs.get("global.format"), "DOT").toUpperCase();
      if ("JSON".equals(format)) {
        dumpConfigJson(def, producerConfig, processorNames, processorConfigs, consumerConfig, beforeCheckerCfgs,
            afterCheckerCfgs);
      } else {
        log.info("\n========== EFFECTIVE CONFIGULATION (DOT FORMAT) ==========");
        dumpConfig("producer." + def.producer, producerConfig);
        for (int i = 0; i < processorNames.size(); i++) {
          dumpConfig("processor." + processorNames.get(i), processorConfigs.get(i));
        }
        dumpConfig("consumer." + def.consumer, consumerConfig);
        for (int i = 0; i < beforeCheckerCfgs.size(); i++) {
          dumpConfig("checker." + def.beforeStages.get(i), beforeCheckerCfgs.get(i));
        }
        for (int i = 0; i < afterCheckerCfgs.size(); i++) {
          dumpConfig("checker." + def.afterStages.get(i), afterCheckerCfgs.get(i));
        }
        log.info("========================================================\n");
      }
    }

    // ------------------------------------------------------------
    // INSTANTIATE MODULES WITH EFFECTIVE CONFIGS
    // ------------------------------------------------------------
    ReactiveInterfaces.ReactiveProducer<?> producer = ConfigInstantiator.instantiateProducer(def.producer, modules,
        producerConfig);

    List<ReactiveInterfaces.ReactiveProcessor<?, ?>> processors = ConfigInstantiator.instantiateProcessors(
        processorNames,
        processorConfigs, modules);

    ReactiveInterfaces.ReactiveConsumer<?> consumer = ConfigInstantiator.instantiateConsumer(def.consumer, modules,
        consumerConfig);

    // ------------------------------------------------------------
    // PIPELINE-LEVEL CHECKERS (simple pipelines)
    // ------------------------------------------------------------
    List<ReactiveInterfaces.ReactiveModule> beforeCheckers = ConfigInstantiator.instantiateCheckerList(
        def.beforeStages,
        beforeCheckerCfgs, modules);

    List<ReactiveInterfaces.ReactiveModule> afterCheckers = ConfigInstantiator.instantiateCheckerList(
        def.afterStages,
        afterCheckerCfgs, modules);

    // ------------------------------------------------------------
    // GLOBAL SETTINGS
    // ------------------------------------------------------------
    // ------------------------------------------------------------
    // GLOBAL SETTINGS (Triple Namespace: global.engine.*)
    // ------------------------------------------------------------
    Map<String, Object> cli = OverrideResolver.CliOverrides.get();
    Map<String, Object> engine = (Map<String, Object>) global.getOrDefault("engine", Map.of());

    // Helper to resolve: CLI -> engine.key -> global.key -> default
    java.util.function.Function<String, String> resolve = (key) -> {
      // 1a. CLI (engine.key - new canonical path)
      if (cli.containsKey("engine." + key))
        return cli.get("engine." + key).toString();
      // 1b. CLI (global.engine.key - full path)
      if (cli.containsKey("global.engine." + key))
        return cli.get("global.engine." + key).toString();
      if (cli.containsKey("global.global" + Character.toUpperCase(key.charAt(0)) + key.substring(1)))
        return cli.get("global.global" + Character.toUpperCase(key.charAt(0)) + key.substring(1)).toString(); // mapping
                                                                                                              // legacy
                                                                                                              // CLI

      // 2. YAML engine.*
      if (engine.containsKey(key))
        return engine.get(key).toString();

      // 3. YAML global.* (Legacy)
      // Map new key to legacy key: concurrency -> globalConcurrencyCap
      String legacyKey = key;
      if (key.equals("concurrency"))
        legacyKey = "globalConcurrencyCap";
      else if (key.equals("shutdownTimeout"))
        legacyKey = "shutdownTimeoutSeconds";

      if (global.containsKey(legacyKey))
        return global.get(legacyKey).toString();

      return null; // Use hard default
    };

    int globalCap = Integer.parseInt(Objects.requireNonNullElse(resolve.apply("concurrency"), "0"));
    int recordSizeThresholdMB = Integer
        .parseInt(Objects.requireNonNullElse(resolve.apply("recordSizeThresholdMB"), "10"));
    int maxRecords = Integer.parseInt(Objects.requireNonNullElse(resolve.apply("maxRecords"), "5"));
    int shutdownTimeout = Integer.parseInt(Objects.requireNonNullElse(resolve.apply("shutdownTimeout"), "60"));
    String engineType = Objects.requireNonNullElse(resolve.apply("type"), "virtual");
    boolean parallelGzip = Boolean.parseBoolean(Objects.requireNonNullElse(resolve.apply("parallelGzip"), "false"));
    int parallelGzipLevel = Integer.parseInt(Objects.requireNonNullElse(resolve.apply("compressionLevel"), "6"));
    boolean isalEnabled = Boolean.parseBoolean(Objects.requireNonNullElse(resolve.apply("isalEnabled"), "true"));

    // ------------------------------------------------------------
    // RETURN LOADED CONFIG
    // ------------------------------------------------------------
    return new LoadedConfig(
        rawYaml,
        def,
        producer,
        processors,
        consumer,
        producerConfig,
        processorConfigs,
        consumerConfig,
        processorNames,
        modules,
        engineType,
        resultFormat,
        globalCap,
        recordSizeThresholdMB,
        maxRecords,
        shutdownTimeout,
        beforeCheckers,
        beforeCheckerCfgs,
        afterCheckers,
        afterCheckerCfgs,
        isVerbose,
        isDryRun,
        isBenchmark,
        isSilent,
        progressMode,
        finalReportMode,
        finalReportToStderr,
        logCliNone,
        parallelGzip,
        parallelGzipLevel,
        isalEnabled);
  }

  private static List<ArgParser.ArgDef> collectArgumentDefinitions(
      Map<String, Object> global,
      PipelineDef def,
      List<ConfigResolver.ProcessorEntry> resolvedStages,
      ModuleRegistry modules) {
    List<ArgParser.ArgDef> allArgDefs = new ArrayList<>();
    allArgDefs.add(ArgParser.resultFormatDefinition());
    allArgDefs.addAll(ArgParser.buildArgDefs(global.get("args")));
    allArgDefs.addAll(ArgParser.buildArgDefs(def.argsRaw));

    java.util.function.BiConsumer<ModuleDef, String> injectArgs = (m, prefix) -> {
      if (m != null && !m.args.isEmpty()) {
        for (ArgParser.ArgDef d : ArgParser.buildArgDefs(m.args)) {
          allArgDefs.add(new ArgParser.ArgDef(
              d.shortOpt(), d.longOpt(), d.index(), d.isRemainder(), d.type(),
              prefix + d.targetPath()));
        }
      }
    };

    if (def.producer != null && !def.producer.isEmpty()) {
      String prodPrefix = def.producer.startsWith("producer.") ? def.producer + "." : "producer." + def.producer + ".";
      injectArgs.accept(modules.getProducer(def.producer), prodPrefix);
    }
    if (def.consumer != null && !def.consumer.isEmpty()) {
      String consPrefix = def.consumer.startsWith("consumer.") ? def.consumer + "." : "consumer." + def.consumer + ".";
      injectArgs.accept(modules.getConsumer(def.consumer), consPrefix);
    }
    for (ConfigResolver.ProcessorEntry entry : resolvedStages) {
      String procPrefix = entry.name().startsWith("processor.") ? entry.name() + "."
          : "processor." + entry.name() + ".";
      injectArgs.accept(modules.getProcessor(entry.name()), procPrefix);
    }
    for (String name : def.beforeStages) {
      injectArgs.accept(modules.getChecker(name), "checker." + name + ".");
    }
    for (String name : def.afterStages) {
      injectArgs.accept(modules.getChecker(name), "checker." + name + ".");
    }
    return allArgDefs;
  }

  private static void applyParsedArgsToOverrides(Map<String, Object> parsedArgs, PipelineDef def) {
    for (var entry : parsedArgs.entrySet()) {
      String path = entry.getKey();
      Object value = entry.getValue();

      if (path.contains(".*")) {
        if (path.startsWith("producer.*.") && def.producer != null) {
          String replacement = def.producer.startsWith("producer.") ? def.producer + "."
              : "producer." + def.producer + ".";
          path = path.replace("producer.*.", replacement);
        } else if (path.startsWith("consumer.*.") && def.consumer != null) {
          String replacement = def.consumer.startsWith("consumer.") ? def.consumer + "."
              : "consumer." + def.consumer + ".";
          path = path.replace("consumer.*.", replacement);
        }
      } else if (path.equals("producer.files") && def.producer != null) {
        String prefix = def.producer.startsWith("producer.") ? def.producer : "producer." + def.producer;
        path = prefix + ".files";
      }

      if (!OverrideResolver.CliOverrides.get().containsKey(path)) {
        if (value instanceof List<?> list) {
          String strValue = list.stream()
              .map(Object::toString)
              .collect(java.util.stream.Collectors.joining(","));
          log.info("Applying parsed arg: {} = {} (List with {} items)", path, strValue, list.size());
          OverrideResolver.CliOverrides.get().put(path, value);
        } else {
          String strValue = value.toString();
          log.info("Applying parsed arg: {} = {}", path, strValue);
          OverrideResolver.CliOverrides.get().put(path, strValue);
        }
      }
    }
  }

  private static EffectiveConfigs buildEffectiveConfigs(
      PipelineDef def,
      List<ConfigResolver.ProcessorEntry> resolvedStages,
      ModuleRegistry modules,
      Map<String, Object> global) {
    Map<String, Object> producerConfig = copyConfig(modules.getProducer(def.producer).config);
    producerConfig.put("prescanCounts", global.getOrDefault("prescanCounts", "fast"));

    List<Map<String, Object>> processorConfigs = new ArrayList<>();
    List<String> processorNames = new ArrayList<>();
    for (ConfigResolver.ProcessorEntry entry : resolvedStages) {
      processorNames.add(entry.name());
      processorConfigs.add(copyConfig(modules.getProcessor(entry.name()).config));
    }

    Map<String, Object> consumerConfig = copyConfig(modules.getConsumer(def.consumer).config);

    List<Map<String, Object>> beforeCheckerCfgs = new ArrayList<>();
    for (String name : def.beforeStages) {
      ModuleDef m = modules.getChecker(name);
      if (m == null) {
        throw new IllegalArgumentException("Unknown checker: " + name);
      }
      beforeCheckerCfgs.add(copyConfig(m.config));
    }

    List<Map<String, Object>> afterCheckerCfgs = new ArrayList<>();
    for (String name : def.afterStages) {
      ModuleDef m = modules.getChecker(name);
      if (m == null) {
        throw new IllegalArgumentException("Unknown checker: " + name);
      }
      afterCheckerCfgs.add(copyConfig(m.config));
    }

    return new EffectiveConfigs(
        producerConfig,
        processorNames,
        processorConfigs,
        consumerConfig,
        beforeCheckerCfgs,
        afterCheckerCfgs);
  }

  // =====================================================================
  // MODULE REGISTRY BUILDER
  // =====================================================================

  @SuppressWarnings("unchecked")
  private static ModuleRegistry buildModuleRegistry(Map<String, Object> modulesRaw, Map<String, Object> componentsRaw) {
    ModuleRegistry reg = new ModuleRegistry();

    // PHASE 3: Components (Flat)
    if (componentsRaw != null) {
      for (var e : componentsRaw.entrySet()) {
        String key = e.getKey(); // e.g., "producer.archive-chunked"
        Map<String, Object> val = (Map<String, Object>) e.getValue();

        // Extract meta-fields
        String className = Objects.toString(val.get("className"));
        String logLevel = Objects.toString(val.getOrDefault("logLevel", "INFO"));
        List<Object> args = (List<Object>) val.getOrDefault("args", List.of());

        // Remaining fields are config
        Map<String, Object> config = new LinkedHashMap<>(val);
        config.remove("className");
        config.remove("logLevel");
        config.remove("args");

        ModuleDef def = new ModuleDef(key, className, logLevel, config, args);

        if (key.startsWith("producer.")) {
          reg.producers.put(key, def);
        } else if (key.startsWith("processor.")) {
          reg.processors.put(key, def);
        } else if (key.startsWith("consumer.")) {
          reg.consumers.put(key, def);
        } else if (key.startsWith("checker.")) {
          reg.checkers.put(key, def);
        }
      }
      return reg;
    }

    // PHASE 2: Legacy Modules (Nested)
    if (modulesRaw != null) {
      Map<String, Object> prod = (Map<String, Object>) modulesRaw.get("producers");
      Map<String, Object> proc = (Map<String, Object>) modulesRaw.get("processors");
      Map<String, Object> cons = (Map<String, Object>) modulesRaw.get("consumers");
      Map<String, Object> chk = (Map<String, Object>) modulesRaw.get("checkers");

      buildModuleGroup(prod, reg.producers);
      buildModuleGroup(proc, reg.processors);
      buildModuleGroup(cons, reg.consumers);
      buildModuleGroup(chk, reg.checkers);
    }

    return reg;
  }

  // Legacy helper
  @SuppressWarnings("unchecked")
  private static void buildModuleGroup(Map<String, Object> raw, Map<String, ModuleDef> out) {
    if (raw == null)
      return;

    for (var e : raw.entrySet()) {
      String name = e.getKey();
      Map<String, Object> m = (Map<String, Object>) e.getValue();

      String className = Objects.toString(m.get("className"));
      String logLevel = Objects.toString(m.getOrDefault("logLevel", "INFO"));
      Map<String, Object> cfg = (Map<String, Object>) m.getOrDefault("config", Map.of());
      List<Object> args = (List<Object>) m.getOrDefault("args", List.of());

      out.put(name, new ModuleDef(name, className, logLevel, cfg, args));
    }
  }

  // =====================================================================
  // PIPELINE DEF BUILDER
  // =====================================================================

  @SuppressWarnings("unchecked")
  private static Map<String, PipelineDef> buildPipelineDefs(Map<String, Object> raw) {
    Map<String, PipelineDef> out = new LinkedHashMap<>();

    for (var e : raw.entrySet()) {
      String name = e.getKey();
      Map<String, Object> p = (Map<String, Object>) e.getValue();

      List<String> chain = (List<String>) p.get("chain");
      String producer;
      List<Object> processors;
      String consumer;

      // Helper to resolve alias from overrides (e.g., "input" -> "archive-jwarc")
      Map<String, Object> overrides = (Map<String, Object>) p.getOrDefault("overrides", Map.of());
      java.util.function.Function<String, String> resolveAlias = alias -> {
        if (overrides.containsKey(alias)) {
          Object override = overrides.get(alias);
          if (override instanceof Map) {
            Object module = ((Map<?, ?>) override).get("module");
            if (module instanceof String) {
              return (String) module;
            }
          }
        }
        return alias; // Return as-is if no override found
      };

      if (chain != null && !chain.isEmpty()) {
        if (chain.size() < 2) {
          throw new IllegalArgumentException("Pipeline '" + name + "' chain must have at least producer and consumer");
        }
        // Resolve aliases for producer and consumer
        producer = resolveAlias.apply(chain.get(0));
        consumer = resolveAlias.apply(chain.get(chain.size() - 1));
        // Resolve aliases for processors too
        List<String> rawProcessors = chain.subList(1, chain.size() - 1);
        processors = new ArrayList<>();
        for (String proc : rawProcessors) {
          processors.add(resolveAlias.apply(proc));
        }
      } else {
        producer = (String) p.get("producer");
        processors = (List<Object>) p.getOrDefault("processors", List.of());
        consumer = (String) p.get("consumer");
      }

      out.put(name, new PipelineDef(
          name,
          Objects.toString(p.get("description"), ""),
          producer,
          processors,
          consumer,
          (List<String>) p.getOrDefault("before", List.of()),
          (List<String>) p.getOrDefault("stages", List.of()),
          (List<String>) p.getOrDefault("after", List.of()),
          (Map<String, Object>) p.getOrDefault("overrides", Map.of()),
          (List<Object>) p.getOrDefault("args", List.of())));
    }

    return out;
  }

  // =====================================================================
  // PATH RESOLUTION
  // =====================================================================

  private static void resolveModulePaths(ModuleRegistry modules) {
    for (ModuleDef m : modules.producers.values()) {
      PathResolver.resolvePaths(m.config);
    }
    for (ModuleDef m : modules.processors.values()) {
      PathResolver.resolvePaths(m.config);
    }
    for (ModuleDef m : modules.consumers.values()) {
      PathResolver.resolvePaths(m.config);
    }
    for (ModuleDef m : modules.checkers.values()) {
      PathResolver.resolvePaths(m.config);
    }
  }

  // =====================================================================
  // EFFECTIVE CONFIG HELPERS
  // =====================================================================

  private static Map<String, Object> copyConfig(Map<String, Object> cfg) {
    if (cfg == null || cfg.isEmpty()) {
      return new LinkedHashMap<>();
    }
    return new LinkedHashMap<>(cfg);
  }

  private static void dumpConfig(String prefix, Map<String, Object> config) {
    if (config == null || config.isEmpty()) {
      return;
    }
    // Sort keys for consistent output
    new TreeMap<>(config).forEach((k, v) -> log.info("{}.{} = {}", prefix, k, v));
  }

  private static void dumpConfigJson(PipelineDef def, Map<String, Object> producer, List<String> procNames,
      List<Map<String, Object>> procs, Map<String, Object> consumer, List<Map<String, Object>> before,
      List<Map<String, Object>> after) {
    Map<String, Object> root = new LinkedHashMap<>();
    root.put("pipeline", def.name);
    root.put("producer", Map.of("name", def.producer, "config", producer));
    List<Map<String, Object>> procList = new ArrayList<>();
    for (int i = 0; i < procNames.size(); i++) {
      procList.add(Map.of("name", procNames.get(i), "config", procs.get(i)));
    }
    root.put("processors", procList);
    root.put("consumer", Map.of("name", def.consumer, "config", consumer));
    log.info("\n{}", new Yaml().dumpAsMap(root));
  }

  @SuppressWarnings("unchecked")
  private record YamlLoadResult(Map<String, Object> yaml, String source) {}

  private static YamlLoadResult loadYamlMap(Path explicitPath) throws IOException {
    Map<String, Object> raw = null;
    String source = null;

    List<Path> searchPaths = List.of(
        Path.of("config-phase3.yaml"),
        Path.of("config.yaml"),
        Path.of("conf/config.yaml"),
        Path.of("../conf/config.yaml"));

    Path found = explicitPath;
    if (found == null) {
      String envConfig = System.getenv("WARC_CONFIG_FILE");
      if (envConfig != null && !envConfig.isBlank()) {
        found = Path.of(envConfig);
      }
    }
    if (found == null) {
      for (Path p : searchPaths) {
        if (Files.exists(p.toAbsolutePath())) {
          found = p.toAbsolutePath();
          break;
        }
      }
    }

    if (found != null) {
      source = found.toString();
      try (InputStream in = Files.newInputStream(found)) {
        raw = new Yaml().load(in);
      } catch (Exception e) {
        log.error("Error loading config: {}", e.getMessage());
      }
    } else {
      try (InputStream in = Config.class.getResourceAsStream("/config.yaml")) {
        if (in != null) {
          source = "classpath:/config.yaml";
          raw = new Yaml().load(in);
        }
      }
    }

    if (raw == null) {
      throw new IllegalStateException("Config file not found on filesystem or classpath");
    }
    return new YamlLoadResult(raw, source);
  }

  private static VerbosityMode extractVerbosityMode(String[] args) {
    for (String arg : args) {
      if ("--silent".equals(arg) || "-s".equals(arg))
        return VerbosityMode.SILENT;
      if ("--brief".equals(arg))
        return VerbosityMode.BRIEF;
      if ("--benchmark".equals(arg))
        return VerbosityMode.BENCHMARK;
      if ("--verbose".equals(arg) || "-v".equals(arg))
        return VerbosityMode.VERBOSE;
      if ("--development".equals(arg))
        return VerbosityMode.DEVELOPMENT;
      if ("--debug".equals(arg))
        return VerbosityMode.DEBUG;
    }
    return VerbosityMode.BRIEF;
  }
}
