package pl.gov.nac.warc.reactive;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;

import org.junit.jupiter.api.Test;

import pl.gov.nac.warc.config.FinalReportMode;
import pl.gov.nac.warc.config.LoadedConfig;
import pl.gov.nac.warc.config.ModuleRegistry;
import pl.gov.nac.warc.config.PipelineDef;
import pl.gov.nac.warc.config.ProgressMode;

class VirtualThreadEnginePublicationTest {

  @Test
  void publishesOnlyAfterConsumerAndLaterChecksPass() {
    List<String> events = new ArrayList<>();
    RecordingConsumer consumer = new RecordingConsumer(events, 0);
    ReactiveInterfaces.ReactiveModule laterChecker = checker(events, 0);

    int exit = new VirtualThreadEngine(config(new CompletingProducer(), consumer, laterChecker)).run();

    assertEquals(0, exit);
    assertEquals(List.of("consumer-after", "later-after", "publish"), events);
  }

  @Test
  void laterCheckFailureDiscardsInsteadOfPublishing() {
    List<String> events = new ArrayList<>();
    RecordingConsumer consumer = new RecordingConsumer(events, 0);
    ReactiveInterfaces.ReactiveModule laterChecker = checker(events, 1);

    int exit = new VirtualThreadEngine(config(new CompletingProducer(), consumer, laterChecker)).run();

    assertEquals(1, exit);
    assertEquals(List.of("consumer-after", "later-after", "discard"), events);
  }

  @Test
  void processingFailureDiscardsInsteadOfPublishing() {
    List<String> events = new ArrayList<>();
    RecordingConsumer consumer = new RecordingConsumer(events, 0);

    int exit = new VirtualThreadEngine(config(new FailingProducer(), consumer, checker(events, 0))).run();

    assertEquals(1, exit);
    assertEquals(List.of("consumer-after", "later-after", "discard"), events);
  }

  @Test
  void publicationFailureContributesToEngineExit() {
    List<String> events = new ArrayList<>();
    RecordingConsumer consumer = new RecordingConsumer(events, 3);

    int exit = new VirtualThreadEngine(config(new CompletingProducer(), consumer, checker(events, 0))).run();

    assertEquals(3, exit);
    assertEquals(List.of("consumer-after", "later-after", "publish"), events);
  }

  private static ReactiveInterfaces.ReactiveModule checker(List<String> events, int result) {
    return new ReactiveInterfaces.ReactiveModule() {
      @Override
      public void configure(Map<String, Object> cfg) {
      }

      @Override
      public int afterCheck(Map<String, Object> cfg) {
        events.add("later-after");
        return result;
      }
    };
  }

  private static LoadedConfig config(
      ReactiveInterfaces.ReactiveProducer<Object> producer,
      RecordingConsumer consumer,
      ReactiveInterfaces.ReactiveModule laterChecker) {
    PipelineDef pipeline = new PipelineDef(
        "publication-test", "publication test", "producer", List.of(), "consumer",
        List.of(), List.of(), List.of(), Map.of(), List.of());
    return new LoadedConfig(
        Map.of(), pipeline, producer, List.of(), consumer,
        Map.of(), List.of(), Map.of(), List.of(), new ModuleRegistry(),
        "virtual", "human", 1, 1, 1, 5,
        List.of(), List.of(), List.of(laterChecker), List.of(Map.of()),
        false, false, false, true, ProgressMode.NONE, FinalReportMode.NONE,
        false, true, false, 6, false);
  }

  private static class CompletingProducer implements ReactiveInterfaces.ReactiveProducer<Object> {
    private Flow.Subscriber<? super Object> subscriber;

    @Override
    public void configure(Map<String, Object> cfg) {
    }

    @Override
    public void subscribe(Flow.Subscriber<? super Object> subscriber) {
      this.subscriber = subscriber;
      subscriber.onSubscribe(new Flow.Subscription() {
        @Override public void request(long n) {
        }
        @Override public void cancel() {
        }
      });
    }

    @Override
    public void startProducing() {
      subscriber.onComplete();
    }
  }

  private static final class FailingProducer extends CompletingProducer {
    @Override
    public void startProducing() {
      throw new IllegalStateException("injected producer failure");
    }
  }

  private static final class RecordingConsumer implements ReactiveInterfaces.ReactiveConsumer<Object> {
    private final List<String> events;
    private final int publicationResult;

    private RecordingConsumer(List<String> events, int publicationResult) {
      this.events = events;
      this.publicationResult = publicationResult;
    }

    @Override
    public void configure(Map<String, Object> cfg) {
    }

    @Override
    public int afterCheck(Map<String, Object> cfg) {
      events.add("consumer-after");
      return 0;
    }

    @Override
    public int publishOutputs() {
      events.add("publish");
      return publicationResult;
    }

    @Override
    public void discardOutputs() {
      events.add("discard");
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      subscription.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(Object item) {
    }

    @Override
    public void onError(Throwable throwable) {
    }

    @Override
    public void onComplete() {
    }
  }
}
