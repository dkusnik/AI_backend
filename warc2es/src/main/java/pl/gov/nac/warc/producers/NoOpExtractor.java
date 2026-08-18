package pl.gov.nac.warc.producers;

import java.util.Map;
import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.gov.nac.warc.reactive.Metrics;
import pl.gov.nac.warc.reactive.ReactiveInterfaces;

public final class NoOpExtractor extends SubmissionPublisher<Object>
    implements ReactiveInterfaces.ReactiveProducer<Object> {

  private static final Logger log = LogManager.getLogger(NoOpExtractor.class);
  private static final String METRIC_KEY = "producer";

  @Override
  public void configure(Map<String, Object> cfg) {
    Metrics.setModuleHeader(METRIC_KEY, "No-op producer");
  }

  // Uses interface defaults: empty sets for accepts/emits (legacy compatibility)

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
  public void subscribe(Flow.Subscriber<? super Object> subscriber) {
    log.info("subscribe");
    super.subscribe(subscriber);
  }

  public void startProducing() {
    log.info("Producing empty record");
    Metrics.inc(METRIC_KEY, "recordsOutRaw");
    submit(new byte[] { 1, 2, 3 });
    close();
  }
}
