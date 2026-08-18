package pl.gov.nac.warc.checkers;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.gov.nac.warc.reactive.Metrics;
import pl.gov.nac.warc.reactive.ReactiveInterfaces;

public final class CheckerNoOp implements ReactiveInterfaces.ReactiveModule {

  private static final Logger log = LogManager.getLogger(CheckerNoOp.class);
  private static final String METRIC_KEY = "checker";

  @Override
  public void configure(Map<String, Object> cfg) {
    Metrics.setModuleHeader(METRIC_KEY, "No-op checker");
  }

  @Override
  public boolean beforeCheck(Map<String, Object> cfg) {
    log.info("beforeCheck");
    Metrics.inc(METRIC_KEY, "beforeCalls");
    return true;
  }

  @Override
  public int afterCheck(Map<String, Object> cfg) {
    log.info("afterCheck");
    Metrics.inc(METRIC_KEY, "afterCalls");
    return 0;
  }
}
