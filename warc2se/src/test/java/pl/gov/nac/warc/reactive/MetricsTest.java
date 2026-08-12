package pl.gov.nac.warc.reactive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class MetricsTest {

  @AfterEach
  void cleanUp() {
    Metrics.reset();
  }

  @Test
  void benchmarkModeRetainsExitCodeCounters() {
    Metrics.benchmarkMode = true;

    Metrics.inc("consumer", "errors");
    Metrics.add("producer", "failed-inputs", 2);

    assertEquals(1, Metrics.get("consumer", "errors"),
        "Benchmark mode must not convert a failed consumer into exit zero");
    assertEquals(2, Metrics.get("producer", "failed-inputs"),
        "Benchmark mode must retain failure counts used by afterCheck");
  }

  @Test
  void resetClearsBenchmarkMode() {
    Metrics.benchmarkMode = true;

    Metrics.reset();

    assertFalse(Metrics.benchmarkMode,
        "A benchmark run must not poison later pipelines in the same JVM");
  }
}
