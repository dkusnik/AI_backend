package pl.gov.nac.warc.reactive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

class SerializedSubscriberTest {

  @Test
  void testBoundedMicroBatchHandoffNoLoss() throws Exception {
    List<Integer> received = new CopyOnWriteArrayList<>();
    AtomicBoolean completed = new AtomicBoolean(false);

    Flow.Subscriber<Integer> downstream = new Flow.Subscriber<>() {
      @Override
      public void onSubscribe(Flow.Subscription subscription) {
      }

      @Override
      public void onNext(Integer item) {
        received.add(item);
      }

      @Override
      public void onError(Throwable throwable) {
      }

      @Override
      public void onComplete() {
        completed.set(true);
      }
    };

    SerializedSubscriber<Integer> serialized = new SerializedSubscriber<>(downstream, 4);
    serialized.onSubscribe(new NoOpSubscription());

    Thread producer = new Thread(() -> {
      for (int start = 0; start < 50; start += 5) {
        List<Integer> batch = new ArrayList<>(5);
        for (int i = 0; i < 5; i++) {
          batch.add(start + i);
        }
        serialized.onNext(batch);
      }
      serialized.onComplete();
    });

    producer.start();
    producer.join(3000);

    assertTrue(!producer.isAlive(), "Producer thread blocked unexpectedly");
    assertTrue(completed.get(), "Downstream completion not signaled");
    assertEquals(50, received.size(), "Lost records during micro-batch handoff");
    for (int i = 0; i < 50; i++) {
      assertEquals(i, received.get(i));
    }
  }

  private static final class NoOpSubscription implements Flow.Subscription {
    @Override
    public void request(long n) {
    }

    @Override
    public void cancel() {
    }
  }
}
