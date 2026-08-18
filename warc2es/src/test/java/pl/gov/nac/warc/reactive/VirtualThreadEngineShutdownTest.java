package pl.gov.nac.warc.reactive;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

class VirtualThreadEngineShutdownTest {

  @Test
  void testSigIntShutdownStopsDispatcherWorkers() throws Exception {
    assertSignalStopsDispatcher();
  }

  @Test
  void testSigTermShutdownStopsDispatcherWorkers() throws Exception {
    assertSignalStopsDispatcher();
  }

  private void assertSignalStopsDispatcher() throws Exception {
    AtomicBoolean shutdownRequested = new AtomicBoolean(false);
    CountDownLatch completed = new CountDownLatch(1);
    Flow.Subscriber<Object> downstream = new Flow.Subscriber<>() {
      @Override public void onSubscribe(Flow.Subscription subscription) {}
      @Override public void onNext(Object item) {}
      @Override public void onError(Throwable throwable) {
        completed.countDown();
      }
      @Override public void onComplete() {
        completed.countDown();
      }
    };

    Class<?> dispatcherType = Arrays.stream(VirtualThreadEngine.class.getDeclaredClasses())
        .filter(type -> type.getSimpleName().equals("ParallelDispatcher"))
        .findFirst()
        .orElseThrow();
    Constructor<?> constructor = dispatcherType.getDeclaredConstructors()[0];
    constructor.setAccessible(true);
    Object shutdownArgument = constructor.getParameterTypes()[3] == boolean.class
        ? false
        : shutdownRequested;
    Object dispatcher = constructor.newInstance(downstream, 1, 2, shutdownArgument, null);
    Method start = dispatcherType.getDeclaredMethod("start");
    start.setAccessible(true);
    start.invoke(dispatcher);

    try {
      shutdownRequested.set(true);
      assertTrue(completed.await(1, TimeUnit.SECONDS),
          "A shutdown request must be observed by already-running dispatcher workers");
    } finally {
      Field shutdownField = dispatcherType.getDeclaredField("shutdown");
      shutdownField.setAccessible(true);
      Object current = shutdownField.get(dispatcher);
      if (current instanceof AtomicBoolean shutdown) {
        shutdown.set(true);
      } else {
        shutdownField.setBoolean(dispatcher, true);
      }
      completed.await(1, TimeUnit.SECONDS);
    }
  }
}
