package pl.gov.nac.warc.performance;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Compliance tests are single-run checks and must stay separate from
 * multi-run benchmark statistics.
 */
class PerformanceComplianceTest {

  @Test
  void testComplianceScriptsExistAsDedicatedArtifacts() {
    assertTrue(Files.exists(Path.of("src/main/dist/testing/scripts/bench/tc-perf-002-extract-mime-branches.sh")));
    assertTrue(Files.exists(Path.of("src/main/dist/testing/scripts/bench/tc-perf-003-jfr-wrapper.sh")));
    assertTrue(Files.exists(Path.of("src/main/dist/testing/scripts/bench/tc-perf-004-compliance-b1-throughput.sh")));
    assertTrue(Files.exists(Path.of("src/main/dist/testing/scripts/bench/tc-perf-005-compliance-b2-throughput.sh")));
    assertTrue(Files.exists(Path.of("src/main/dist/testing/scripts/bench/tc-perf-006-compliance-memory.sh")));
    assertTrue(Files.exists(Path.of("src/main/dist/testing/scripts/bench/tc-perf-007-compliance-output.sh")));
    assertTrue(Files.exists(Path.of("src/main/dist/testing/scripts/bench/tc-perf-008-compliance-protocol-separation.sh")));
  }

  @Test
  void testComplianceScriptsAreSingleRunNotStatistical() throws Exception {
    List<Path> scripts = List.of(
        Path.of("src/main/dist/testing/scripts/bench/tc-perf-002-extract-mime-branches.sh"),
        Path.of("src/main/dist/testing/scripts/bench/tc-perf-003-jfr-wrapper.sh"),
        Path.of("src/main/dist/testing/scripts/bench/tc-perf-004-compliance-b1-throughput.sh"),
        Path.of("src/main/dist/testing/scripts/bench/tc-perf-005-compliance-b2-throughput.sh"),
        Path.of("src/main/dist/testing/scripts/bench/tc-perf-006-compliance-memory.sh"),
        Path.of("src/main/dist/testing/scripts/bench/tc-perf-007-compliance-output.sh"));

    for (Path p : scripts) {
      String body = Files.readString(p, StandardCharsets.UTF_8);
      assertFalse(body.contains("# @runs:"), p + " should not declare multi-run benchmark loops");
      assertFalse(body.contains("for run in"), p + " should not iterate statistical runs");
    }
  }

  @Test
  void testComplianceAndBenchmarkProtocolAreSeparated() throws Exception {
    String compliance = Files.readString(
        Path.of("src/main/dist/testing/scripts/bench/tc-perf-008-compliance-protocol-separation.sh"),
        StandardCharsets.UTF_8);
    String benchmark = Files.readString(
        Path.of("src/main/dist/testing/scripts/integration/bench-optimization.sh"),
        StandardCharsets.UTF_8);

    assertTrue(compliance.contains("compliance and benchmark protocol separated"));
    assertTrue(benchmark.contains("7 runs per scenario"),
        "Benchmark protocol script should remain multi-run and separate from compliance");
  }
}
