package pl.gov.nac.warc.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

// Task-ID: T-200
class PathResolverTest {

  @Test
  void resolvesEnvAndDefaultExpressions() {
    Map<String, Object> cfg = new HashMap<>();
    cfg.put("file", "${HOME}/warc-output.warc.gz");
    cfg.put("outputFile", "${PATH_RESOLVER_NOT_SET:-/tmp/path-resolver-default.warc.gz}");

    PathResolver.resolvePaths(cfg);

    String fromHome = (String) cfg.get("file");
    String fromDefault = (String) cfg.get("outputFile");

    assertTrue(fromHome.startsWith(System.getenv("HOME") + "/"));
    assertEquals("/tmp/path-resolver-default.warc.gz", fromDefault);
  }

  @Test
  void resolvesHomeAndRelativePaths() {
    Map<String, Object> cfg = new HashMap<>();
    cfg.put("path", "~/warc-work");
    cfg.put("file", "src/test/resources/sample.warc.gz");

    PathResolver.resolvePaths(cfg);

    String homeExpanded = (String) cfg.get("path");
    String relativeResolved = (String) cfg.get("file");

    assertEquals(System.getProperty("user.home") + "/warc-work", homeExpanded);
    assertEquals(Path.of("src/test/resources/sample.warc.gz").toAbsolutePath().normalize().toString(), relativeResolved);
  }

  @Test
  void throwsOnMissingEnvWithoutDefault() {
    Map<String, Object> cfg = new HashMap<>();
    cfg.put("file", "${PATH_RESOLVER_MISSING_VAR_TEST}/a.warc.gz");

    assertThrows(IllegalArgumentException.class, () -> PathResolver.resolvePaths(cfg));
  }
}
