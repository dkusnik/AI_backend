package pl.gov.nac.warc.consumers;

import java.util.Map;
import java.util.concurrent.Flow;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.gov.nac.warc.reactive.Metrics;
import pl.gov.nac.warc.reactive.ReactiveInterfaces;

/**
 * No-op consumer that discards all records.
 * Accepts all Record types (uses interface default).
 */
public final class NoOpExporter implements ReactiveInterfaces.ReactiveConsumer<Object> {

  private static final Logger log = LogManager.getLogger(NoOpExporter.class);
  private static final String METRIC_KEY = "consumer";

  @Override
  public void configure(Map<String, Object> cfg) {
    Metrics.setModuleHeader(METRIC_KEY, "No-op consumer");
  }

  @Override
  public boolean beforeCheck(Map<String, Object> cfg) {
    log.info("beforeCheck");
    return true;
  }

  @Override
  public int afterCheck(Map<String, Object> cfg) {
    log.info("afterCheck");
    return 0;
  }

  @Override
  public void onSubscribe(Flow.Subscription subscription) {
    log.info("onSubscribe");
    subscription.request(Long.MAX_VALUE);
  }

  @Override
  public void onNext(Object item) {
    log.debug("onNext: {}", item);
    Metrics.inc(METRIC_KEY, "recordsInRaw");
  }

  @Override
  public void onError(Throwable throwable) {
    log.error("onError: {}", throwable.getMessage(), throwable);
  }

  @Override
  public void onComplete() {
    log.info("onComplete");
  }
}
