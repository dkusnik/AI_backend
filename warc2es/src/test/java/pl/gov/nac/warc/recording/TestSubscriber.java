package pl.gov.nac.warc.recording;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;

public class TestSubscriber<T> implements Flow.Subscriber<T> {
  private final List<T> items = new ArrayList<>();
  private final CountDownLatch latch = new CountDownLatch(1);
  private Throwable error;
  private Flow.Subscription subscription;

  @Override
  public void onSubscribe(Flow.Subscription subscription) {
    this.subscription = subscription;
    subscription.request(Long.MAX_VALUE);
  }

  @Override
  public void onNext(T item) {
    items.add(item);
  }

  @Override
  public void onError(Throwable throwable) {
    this.error = throwable;
    latch.countDown();
  }

  @Override
  public void onComplete() {
    latch.countDown();
  }

  public void awaitCompletion() throws InterruptedException {
    if (!latch.await(5, TimeUnit.SECONDS)) {
      throw new RuntimeException("Timeout waiting for completion");
    }
    if (error != null) {
      throw new RuntimeException(error);
    }
  }

  public List<T> getItems() {
    return items;
  }
}
