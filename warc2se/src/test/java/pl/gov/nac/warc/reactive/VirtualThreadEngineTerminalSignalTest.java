package pl.gov.nac.warc.reactive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

class VirtualThreadEngineTerminalSignalTest {

  @Test
  void testManyWorkersSendExactlyOneCompletion() throws Exception {
    int workerCount = 2;
    AtomicInteger completions = new AtomicInteger();
    AtomicInteger errors = new AtomicInteger();
    Flow.Subscriber<Object> downstream = terminalCountingSubscriber(completions, errors);
    Object dispatcher = newDispatcher(downstream, workerCount, new AtomicBoolean(true));
    Field workersDoneField = dispatcher.getClass().getDeclaredField("workersDoneLatch");
    workersDoneField.setAccessible(true);
    workersDoneField.set(dispatcher, new CoordinatedCountDownLatch(workerCount));
    Method workerLoop = dispatcher.getClass().getDeclaredMethod("workerLoop", int.class);
    workerLoop.setAccessible(true);

    CountDownLatch ready = new CountDownLatch(workerCount);
    CountDownLatch start = new CountDownLatch(1);
    Thread[] workers = new Thread[workerCount];
    for (int i = 0; i < workerCount; i++) {
      int workerId = i;
      workers[i] = Thread.ofVirtual().start(() -> {
        ready.countDown();
        try {
          start.await();
          workerLoop.invoke(dispatcher, workerId);
        } catch (ReflectiveOperationException e) {
          throw new IllegalStateException(e);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
    }

    assertTrue(ready.await(1, TimeUnit.SECONDS));
    start.countDown();
    for (Thread worker : workers) {
      worker.join(TimeUnit.SECONDS.toMillis(1));
      assertFalse(worker.isAlive());
    }

    assertEquals(1, completions.get(), "Worker completion must be signalled exactly once");
    assertEquals(0, errors.get());
  }

  @Test
  void testErrorIsSerializedAfterActiveOnNext() throws Exception {
    AtomicBoolean inOnNext = new AtomicBoolean();
    AtomicBoolean concurrentError = new AtomicBoolean();
    AtomicInteger completions = new AtomicInteger();
    AtomicInteger errors = new AtomicInteger();
    CountDownLatch onNextEntered = new CountDownLatch(1);
    CountDownLatch releaseOnNext = new CountDownLatch(1);
    CountDownLatch errorDelivered = new CountDownLatch(1);

    Flow.Subscriber<Object> downstream = new Flow.Subscriber<>() {
      @Override public void onSubscribe(Flow.Subscription subscription) {}
      @Override public void onNext(Object item) {
        inOnNext.set(true);
        onNextEntered.countDown();
        try {
          releaseOnNext.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          inOnNext.set(false);
        }
      }
      @Override public void onError(Throwable throwable) {
        concurrentError.set(inOnNext.get());
        errors.incrementAndGet();
        errorDelivered.countDown();
      }
      @Override public void onComplete() {
        completions.incrementAndGet();
      }
    };

    Object dispatcher = newDispatcher(downstream, 1, new AtomicBoolean(false));
    Method start = dispatcher.getClass().getDeclaredMethod("start");
    start.setAccessible(true);
    start.invoke(dispatcher);

    @SuppressWarnings("unchecked")
    Flow.Subscriber<Object> input = (Flow.Subscriber<Object>) dispatcher;
    input.onNext("record");
    assertTrue(onNextEntered.await(30, TimeUnit.SECONDS),
        "worker did not enter downstream onNext");

    Thread errorThread = Thread.ofVirtual().start(
        () -> input.onError(new IllegalStateException("producer failed")));
    try {
      assertFalse(errorDelivered.await(200, TimeUnit.MILLISECONDS),
          "onError must wait until an active onNext call has returned");
    } finally {
      releaseOnNext.countDown();
    }

    errorThread.join(TimeUnit.SECONDS.toMillis(1));
    assertTrue(errorDelivered.await(1, TimeUnit.SECONDS));
    assertFalse(concurrentError.get());
    assertEquals(1, errors.get());
    assertEquals(0, completions.get(), "An error terminal must suppress completion");
  }

  private Object newDispatcher(
      Flow.Subscriber<Object> downstream,
      int workerCount,
      AtomicBoolean shutdown) throws Exception {
    Class<?> dispatcherType = Arrays.stream(VirtualThreadEngine.class.getDeclaredClasses())
        .filter(type -> type.getSimpleName().equals("ParallelDispatcher"))
        .findFirst()
        .orElseThrow();
    Constructor<?> constructor = dispatcherType.getDeclaredConstructors()[0];
    constructor.setAccessible(true);
    return constructor.newInstance(downstream, workerCount, 128, shutdown, null);
  }

  private Flow.Subscriber<Object> terminalCountingSubscriber(
      AtomicInteger completions,
      AtomicInteger errors) {
    return new Flow.Subscriber<>() {
      @Override public void onSubscribe(Flow.Subscription subscription) {}
      @Override public void onNext(Object item) {}
      @Override public void onError(Throwable throwable) {
        errors.incrementAndGet();
      }
      @Override public void onComplete() {
        completions.incrementAndGet();
      }
    };
  }

  private static final class CoordinatedCountDownLatch extends CountDownLatch {
    private final CountDownLatch allCountedDown;

    private CoordinatedCountDownLatch(int count) {
      super(count);
      allCountedDown = new CountDownLatch(count);
    }

    @Override
    public void countDown() {
      super.countDown();
      allCountedDown.countDown();
      try {
        allCountedDown.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
