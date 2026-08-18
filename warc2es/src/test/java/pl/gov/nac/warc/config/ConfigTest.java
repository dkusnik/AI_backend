package pl.gov.nac.warc.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import pl.gov.nac.warc.reactive.VerbosityManager;
import pl.gov.nac.warc.reactive.VerbosityManager.VerbosityMode;

class ConfigTest {

  @TempDir
  Path tempDir;

  private Path configPath;

  @BeforeEach
  void setUp() throws Exception {
    configPath = tempDir.resolve("config.yaml");
    String yaml = """
        global:
          engine:
            concurrency: 2
          args:
            - [null, "--force", "boolean", "output.force"]

        pipelines:
          test-pipeline:
            producer: noop-prod
            processors: [noop-proc]
            consumer: noop-cons
            chain: [noop-prod, noop-proc, noop-cons]
            overrides:
              producer.noop-prod:
                key: val-default

        modules:
          producers:
            noop-prod:
              className: pl.gov.nac.warc.producers.NoOpExtractor
              config:
                key: prod-default
                other: other-default
              args:
                - [null, "--prod-key", "string", "key"]

          processors:
            noop-proc:
              className: pl.gov.nac.warc.processors.ProcessorNoOp
              config:
                key: proc-default
              args:
                - [null, "--proc-key", "string", "key"]

          consumers:
            noop-cons:
              className: pl.gov.nac.warc.consumers.NoOpExporter
              config:
                key: cons-default
              args:
                - [null, "--cons-key", "string", "key"]
        """;
    Files.writeString(configPath, yaml);
  }

  @Test
  void testDefaults() throws Exception {
    LoadedConfig config = Config.load(new String[] { "test-pipeline" }, configPath);

    assertEquals(2, config.globalConcurrencyCap);
    assertEquals("human", config.resultFormat);

    Map<String, Object> prodConfig = config.producerConfig;
    assertEquals("val-default", prodConfig.get("key"), "Pipeline override should take precedence over module default");
    assertEquals("other-default", prodConfig.get("other"));
  }

  @Test
  void testNoOverrideVerbosityDefaultsToBrief() throws Exception {
    String previousVerbosity = System.getProperty("warc.verbosity");
    try {
      Config.load(new String[] { "test-pipeline" }, configPath);
      assertEquals("brief", System.getProperty("warc.verbosity"));
    } finally {
      if (previousVerbosity == null) {
        System.clearProperty("warc.verbosity");
      } else {
        System.setProperty("warc.verbosity", previousVerbosity);
      }
    }
  }

  @Test
  void testJsonResultFormatIsBuiltInAndImmutable() throws Exception {
    LoadedConfig config = Config.load(
        new String[] { "test-pipeline", "--result-format=json" }, configPath);

    assertEquals("json", config.resultFormat);
    assertTrue(config.isJsonResult());
  }

  @Test
  void testForceTargetsTheActiveOutputConsumer() throws Exception {
    LoadedConfig config = Config.load(
        new String[] { "test-pipeline", "--force" }, configPath);

    assertEquals(true, config.consumerConfig.get("force"));
  }

  @Test
  void testInvalidResultFormatFailsFast() {
    assertThrows(IllegalArgumentException.class, () -> Config.load(
        new String[] { "test-pipeline", "--result-format=xml" }, configPath));
  }

  @Test
  void testCliOverrides() throws Exception {
    // Test overriding producer config via CLI
    String[] args = {
        "test-pipeline",
        "--prod-key=cli-value",
        "--engine.concurrency=8"
    };

    LoadedConfig config = Config.load(args, configPath);

    assertEquals(8, config.globalConcurrencyCap);

    Map<String, Object> prodConfig = config.producerConfig;
    assertEquals("cli-value", prodConfig.get("key"),
        "CLI override should take precedence over pipeline and module defaults");
  }

  @Test
  void testExplicitCliOverrides() throws Exception {
    // Test explicit key override
    String[] args = {
        "test-pipeline",
        "--producer.noop-prod.other=cli-other"
    };

    LoadedConfig config = Config.load(args, configPath);

    Map<String, Object> prodConfig = config.producerConfig;
    assertEquals("cli-other", prodConfig.get("other"));
  }

  @Test
  void testInvalidYaml() throws Exception {
    Path invalidPath = tempDir.resolve("invalid.yaml");
    Files.writeString(invalidPath, ":: invalid yaml ::");

    try {
      assertThrows(IllegalStateException.class,
          () -> Config.load(new String[] { "test-pipeline", "--silent" }, invalidPath));
    } finally {
      VerbosityManager.apply(VerbosityMode.BRIEF);
    }
  }

  @Test
  void testMissingPipeline() throws Exception {
    assertThrows(RuntimeException.class, () -> {
      Config.load(new String[] { "non-existent-pipeline" }, configPath);
    });
  }

  @Test
  void testUnknownOptionFailsFast() {
    assertThrows(IllegalArgumentException.class, () -> {
      Config.load(new String[] { "test-pipeline", "--unknown-flag=1" }, configPath);
    });
  }

  @Test
  void testUnknownOverrideNamespaceFailsFast() {
    assertThrows(IllegalArgumentException.class, () -> {
      Config.load(new String[] { "test-pipeline", "--typo.namespace.value=1" }, configPath);
    });
  }

  @Test
  void testMissingOptionValueFailsFast() {
    assertThrows(IllegalArgumentException.class, () -> {
      Config.load(new String[] { "test-pipeline", "--prod-key" }, configPath);
    });
  }

  @Test
  void testCanonicalGlobalEngineOverridesApplied() throws Exception {
    LoadedConfig config = Config.load(new String[] {
        "test-pipeline",
        "--engine.concurrency=8",
        "--engine.type=reactive"
    }, configPath);

    assertEquals(8, config.globalConcurrencyCap);
    assertEquals("reactive", config.engineType);
  }

  @Test
  void testConsumerWildcardOverrideTargetsActiveConsumerOnly() throws Exception {
    LoadedConfig config = Config.load(new String[] {
        "test-pipeline",
        "--consumer.*.key=wild-value"
    }, configPath);

    assertEquals("wild-value", config.consumerConfig.get("key"));
    // Non-active modules in registry should remain unchanged.
    assertEquals("cons-default", config.modules.getConsumer("noop-cons").config.get("key"));
  }

  @Test
  void testOverridePrecedencePipelineThenCliDeterministic() throws Exception {
    LoadedConfig config = Config.load(new String[] {
        "test-pipeline",
        "--producer.noop-prod.key=cli-value"
    }, configPath);

    // Module default: prod-default
    // Pipeline override: val-default
    // CLI override should win.
    assertEquals("cli-value", config.producerConfig.get("key"));
  }

  @Test
  void testPathResolutionRelativeAbsoluteAndEnvExpanded() throws Exception {
    Path absFile = tempDir.resolve("absolute-file.txt");
    Files.writeString(absFile, "x");

    Path cfgWithPaths = tempDir.resolve("config-paths.yaml");
    String yaml = """
        global:
          engine:
            concurrency: 1
          args: []

        pipelines:
          test-pipeline:
            producer: noop-prod
            processors: []
            consumer: noop-cons

        modules:
          producers:
            noop-prod:
              className: pl.gov.nac.warc.producers.NoOpExtractor
              config:
                file: "relative-input.warc.gz"
                path: "${UNSET_PATH_FOR_TEST:-relative-default.cdxj}"
                files:
                  - "rel-a.warc.gz"
                  - "%s"

          consumers:
            noop-cons:
              className: pl.gov.nac.warc.consumers.NoOpExporter
              config: {}
        """.formatted(absFile.toString().replace("\\", "\\\\"));
    Files.writeString(cfgWithPaths, yaml);

    LoadedConfig config = Config.load(new String[] { "test-pipeline" }, cfgWithPaths);

    assertTrue(Path.of(config.producerConfig.get("file").toString()).isAbsolute());
    assertTrue(Path.of(config.producerConfig.get("path").toString()).isAbsolute());
    @SuppressWarnings("unchecked")
    var files = (java.util.List<Object>) config.producerConfig.get("files");
    assertEquals(2, files.size());
    assertTrue(Path.of(files.get(0).toString()).isAbsolute());
    assertEquals(absFile.toString(), files.get(1).toString());
  }
}
