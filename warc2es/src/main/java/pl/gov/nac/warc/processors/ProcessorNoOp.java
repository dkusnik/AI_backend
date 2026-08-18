package pl.gov.nac.warc.processors;

import java.util.Map;
import java.util.concurrent.Flow;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.gov.nac.warc.reactive.Metrics;
import pl.gov.nac.warc.reactive.ReactiveInterfaces;

/**
 * No-op processor that passes all records through unchanged.
 * Accepts and emits all Record types (uses interface defaults).
 */
public final class ProcessorNoOp implements ReactiveInterfaces.ReactiveProcessor<Object, Object> {

  private static final Logger log = LogManager.getLogger(ProcessorNoOp.class);
  private static final String METRIC_KEY = "noop";

  private Flow.Subscriber<? super Object> downstream;

  @Override
  public void configure(Map<String, Object> cfg) {
    Metrics.setModuleHeader(METRIC_KEY, "No-Op Processor");
    log.info("Configured");
  }

  @Override
  public boolean beforeCheck(Map<String, Object> cfg) {
    log.debug("beforeCheck");
    return true;
  }

  @Override
  public int afterCheck(Map<String, Object> cfg) {
    log.debug("afterCheck");
    return 0;
  }

  @Override
  public void subscribe(Flow.Subscriber<? super Object> subscriber) {
    this.downstream = subscriber;
    log.debug("subscribe");
  }

  @Override
  public void onSubscribe(Flow.Subscription subscription) {
    log.debug("onSubscribe");
    downstream.onSubscribe(subscription);
  }

  @Override
  public void onNext(Object item) {
    log.debug("onNext: {}", () -> item); // Using lambda for potentially expensive item.toString()
    Metrics.inc(METRIC_KEY, "recordsInRaw");
    Metrics.inc(METRIC_KEY, "recordsOutRaw");
    Metrics.inc(METRIC_KEY, "passed"); // Added from instruction
    downstream.onNext(item);
  }

  @Override
  public void onError(Throwable throwable) {
    log.error("onError: {}", throwable.getMessage(), throwable);
    downstream.onError(throwable);
  }

  @Override
  public void onComplete() {
    log.debug("onComplete");
    downstream.onComplete();
  }
}
