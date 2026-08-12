package pl.gov.nac.warc.reactive;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;

import org.junit.jupiter.api.Test;

import pl.gov.nac.warc.records.Record;
import pl.gov.nac.warc.records.warc.RecordWarcUniversal;
import pl.gov.nac.warc.records.warc.RecordWet;

/**
 * H-6 (T-224): VirtualThreadEngine.negotiatePipeline() must reject incompatible
 * adjacent module types at startup rather than silently passing Record.class.
 */
class VirtualThreadEngineNegotiateTest {

  // ---- minimal stubs ----

  static ReactiveInterfaces.ReactiveProducer<?> producerEmitting(
      Class<? extends Record> type) {
    return new ReactiveInterfaces.ReactiveProducer<Object>() {
      @Override public void configure(Map<String, Object> cfg) {}
      @Override public void startProducing() {}
      @Override public void subscribe(Flow.Subscriber<? super Object> s) {}
      @Override public List<Class<? extends Record>> emittedOutputTypes() {
        return List.of(type);
      }
    };
  }

  @SuppressWarnings("rawtypes")
  static ReactiveInterfaces.ReactiveProcessor<?, ?> processorAccepting(
      Class<? extends Record> type) {
    return new ReactiveInterfaces.ReactiveProcessor<Object, Object>() {
      @Override public void configure(Map<String, Object> cfg) {}
      @Override public void subscribe(Flow.Subscriber<? super Object> s) {}
      @Override public void onSubscribe(Flow.Subscription s) {}
      @Override public void onNext(Object item) {}
      @Override public void onError(Throwable t) {}
      @Override public void onComplete() {}
      @Override public List<Class<? extends Record>> acceptedInputTypes() {
        return List.of(type);
      }
    };
  }

  @SuppressWarnings("rawtypes")
  static ReactiveInterfaces.ReactiveConsumer<?> consumerAccepting(
      Class<? extends Record> type) {
    return new ReactiveInterfaces.ReactiveConsumer<Object>() {
      @Override public void configure(Map<String, Object> cfg) {}
      @Override public void onSubscribe(Flow.Subscription s) {}
      @Override public void onNext(Object item) {}
      @Override public void onError(Throwable t) {}
      @Override public void onComplete() {}
      @Override public void startConsuming() {}
      @Override public List<Class<? extends Record>> acceptedInputTypes() {
        return List.of(type);
      }
    };
  }

  // ---- tests ----

  /**
   * Producer emits RecordWarcUniversal; processor only accepts RecordWet.
   * Before fix: negotiatePipeline() silently passed Record.class → ClassCastException later.
   * After fix: IllegalStateException at negotiation time.
   */
  @Test
  void testNegotiateDetectsTypeMismatch() {
    var producer   = producerEmitting(RecordWarcUniversal.class);
    var processor  = processorAccepting(RecordWet.class);          // incompatible
    var consumer   = consumerAccepting(RecordWet.class);

    assertThrows(IllegalStateException.class,
        () -> VirtualThreadEngine.negotiateChain(producer, List.of(processor), consumer),
        "Incompatible adjacent types must be rejected at negotiation, not at runtime");
  }

  /**
   * Regression: compatible chain must not throw.
   * Producer → RecordWarcUniversal → Processor accepts RecordWarcUniversal → Consumer.
   */
  @Test
  void testNegotiateAcceptsCompatibleChain() {
    var producer   = producerEmitting(RecordWarcUniversal.class);
    var processor  = processorAccepting(RecordWarcUniversal.class);
    var consumer   = consumerAccepting(RecordWarcUniversal.class);

    assertDoesNotThrow(
        () -> VirtualThreadEngine.negotiateChain(producer, List.of(processor), consumer),
        "Compatible chain must not throw");
  }

  /**
   * Regression: empty processor list is a valid chain.
   */
  @Test
  void testNegotiateAcceptsEmptyProcessorList() {
    var producer = producerEmitting(RecordWarcUniversal.class);
    var consumer = consumerAccepting(RecordWarcUniversal.class);

    assertDoesNotThrow(
        () -> VirtualThreadEngine.negotiateChain(producer, List.of(), consumer),
        "Producer → Consumer direct chain must not throw");
  }
}
