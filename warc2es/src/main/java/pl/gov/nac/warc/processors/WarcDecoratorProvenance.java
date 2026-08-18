package pl.gov.nac.warc.processors;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;
import java.util.regex.Pattern;

import pl.gov.nac.warc.reactive.Metrics;
import pl.gov.nac.warc.reactive.ReactiveInterfaces;
import pl.gov.nac.warc.records.Record;
import pl.gov.nac.warc.records.warc.RecordWarcUniversal;

/** Stamps the operator-supplied provenance pair onto derived WARC records. */
public final class WarcDecoratorProvenance
    implements ReactiveInterfaces.ReactiveProcessor<RecordWarcUniversal, RecordWarcUniversal> {

  static final String URL_ID_HEADER = "X-NAC-URL-ID";
  static final String CRAWL_ID_HEADER = "X-NAC-Crawl-ID";
  private static final String METRIC_KEY = "provenance-decorator";
  private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z0-9._-]{1,128}");

  private Flow.Subscriber<? super RecordWarcUniversal> downstream;
  private String urlId;
  private String crawlId;
  private boolean enabled;

  @Override
  public boolean isEnabled(Map<String, Object> cfg) {
    return hasValue(cfg.get("url-id")) || hasValue(cfg.get("crawl-id"));
  }

  @Override
  public List<Class<? extends Record>> acceptedInputTypes() {
    return List.of(RecordWarcUniversal.class);
  }

  @Override
  public List<Class<? extends Record>> emittedOutputTypes() {
    return List.of(RecordWarcUniversal.class);
  }

  @Override
  public void configure(Map<String, Object> cfg) {
    Metrics.setModuleHeader(METRIC_KEY, "Provenance Decorator");
    boolean hasUrlId = hasValue(cfg.get("url-id"));
    boolean hasCrawlId = hasValue(cfg.get("crawl-id"));
    if (!hasUrlId && !hasCrawlId) {
      enabled = false;
      return;
    }
    urlId = requireIdentifier(cfg, "url-id");
    crawlId = requireIdentifier(cfg, "crawl-id");
    enabled = true;
  }

  private static boolean hasValue(Object value) {
    return value != null && !value.toString().isBlank();
  }

  private static String requireIdentifier(Map<String, Object> cfg, String key) {
    Object raw = cfg.get(key);
    String value = raw == null ? "" : raw.toString();
    if (!IDENTIFIER.matcher(value).matches()) {
      throw new IllegalArgumentException(key + " must match [A-Za-z0-9._-]{1,128}");
    }
    return value;
  }

  @Override
  public void subscribe(Flow.Subscriber<? super RecordWarcUniversal> subscriber) {
    downstream = subscriber;
  }

  @Override
  public void onSubscribe(Flow.Subscription subscription) {
    downstream.onSubscribe(subscription);
  }

  @Override
  public void onNext(RecordWarcUniversal item) {
    if (!enabled) {
      downstream.onNext(item);
      return;
    }
    Map<String, String> headers = new LinkedHashMap<>(item.headers());
    headers.put(URL_ID_HEADER, urlId);
    headers.put(CRAWL_ID_HEADER, crawlId);
    RecordWarcUniversal stamped = new RecordWarcUniversal(item.warcType(), headers, item.rawBytes());
    Metrics.inc(METRIC_KEY, "stamped");
    downstream.onNext(stamped);
  }

  @Override
  public void onError(Throwable throwable) {
    downstream.onError(throwable);
  }

  @Override
  public void onComplete() {
    downstream.onComplete();
  }
}
