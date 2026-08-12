package pl.gov.nac.warc.processors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;

import org.junit.jupiter.api.Test;

import pl.gov.nac.warc.records.warc.RecordWarcUniversal;

class WarcDecoratorProvenanceTest {

  @Test
  void stampsCanonicalPairOnPayloadAndWarcinfoRecords() {
    WarcDecoratorProvenance processor = new WarcDecoratorProvenance();
    processor.configure(Map.of("url-id", "site-1", "crawl-id", "crawl_2"));
    CollectingSubscriber downstream = new CollectingSubscriber();
    processor.subscribe(downstream);
    processor.onSubscribe(new NoopSubscription());

    processor.onNext(record("conversion"));
    processor.onNext(record("warcinfo"));

    assertEquals(2, downstream.records.size());
    for (RecordWarcUniversal record : downstream.records) {
      assertEquals("site-1", record.headers().get("X-NAC-URL-ID"));
      assertEquals("crawl_2", record.headers().get("X-NAC-Crawl-ID"));
    }
  }

  @Test
  void replacesPreexistingProvenanceWithTheInvocationPair() {
    WarcDecoratorProvenance processor = new WarcDecoratorProvenance();
    processor.configure(Map.of("url-id", "expected-url", "crawl-id", "expected-crawl"));
    CollectingSubscriber downstream = new CollectingSubscriber();
    processor.subscribe(downstream);
    processor.onSubscribe(new NoopSubscription());
    RecordWarcUniversal record = record("conversion");
    record.headers().put("X-NAC-URL-ID", "old-url");
    record.headers().put("X-NAC-Crawl-ID", "old-crawl");

    processor.onNext(record);

    assertEquals("expected-url", downstream.records.getFirst().headers().get("X-NAC-URL-ID"));
    assertEquals("expected-crawl", downstream.records.getFirst().headers().get("X-NAC-Crawl-ID"));
  }

  @Test
  void requiresBothIdentifiersWithTheFrozenGrammar() {
    WarcDecoratorProvenance processor = new WarcDecoratorProvenance();
    assertThrows(IllegalArgumentException.class, () -> processor.configure(Map.of("url-id", "site")));
    assertThrows(IllegalArgumentException.class,
        () -> processor.configure(Map.of("url-id", "site/escape", "crawl-id", "crawl")));
    assertThrows(IllegalArgumentException.class,
        () -> processor.configure(Map.of("url-id", "site", "crawl-id", "x".repeat(129))));
  }

  @Test
  void emptyConfigurationIsAPassThroughForTheRawPipeline() {
    WarcDecoratorProvenance processor = new WarcDecoratorProvenance();
    processor.configure(Map.of("url-id", "", "crawl-id", ""));
    CollectingSubscriber downstream = new CollectingSubscriber();
    processor.subscribe(downstream);
    processor.onSubscribe(new NoopSubscription());
    RecordWarcUniversal input = record("conversion");

    processor.onNext(input);

    assertEquals(input, downstream.records.getFirst());
  }

  private static RecordWarcUniversal record(String type) {
    return new RecordWarcUniversal(type, Map.of("WARC-Type", type), "body".getBytes(StandardCharsets.UTF_8));
  }

  private static final class NoopSubscription implements Flow.Subscription {
    @Override public void request(long n) { }
    @Override public void cancel() { }
  }

  private static final class CollectingSubscriber implements Flow.Subscriber<RecordWarcUniversal> {
    private final List<RecordWarcUniversal> records = new ArrayList<>();
    @Override public void onSubscribe(Flow.Subscription subscription) { }
    @Override public void onNext(RecordWarcUniversal item) { records.add(item); }
    @Override public void onError(Throwable throwable) { throw new AssertionError(throwable); }
    @Override public void onComplete() { }
  }
}
