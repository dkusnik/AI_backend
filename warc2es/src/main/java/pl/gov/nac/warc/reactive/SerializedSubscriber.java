package pl.gov.nac.warc.reactive;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Subscriber implementation that serializes concurrent calls to onNext
 * into a sequential stream for the downstream subscriber, complying with
 * Reactive Streams specs.
 *
 * Uses a drain loop pattern.
 *
 * @param <T> The type of element signaled.
 */
public final class SerializedSubscriber<T> implements Flow.Subscriber<T> {

  private final Flow.Subscriber<? super T> actual;
  private final java.util.concurrent.BlockingQueue<T> queue;
  private final AtomicInteger wip = new AtomicInteger();
  private volatile boolean done;
  // Must be volatile: onError() writes on one thread; drain() reads on whichever
  // thread wins the wip CAS. Without volatile the JMM provides no happens-before guarantee
  // and the drain loop may observe null, emitting onComplete() instead of onError().
  private volatile Throwable error;

  public SerializedSubscriber(Flow.Subscriber<? super T> actual) {
    this(actual, 16); // Default capacity
  }

  public SerializedSubscriber(Flow.Subscriber<? super T> actual, int capacity) {
    this.actual = actual;
    this.queue = new java.util.concurrent.LinkedBlockingQueue<>(capacity);
  }

  @Override
  public void onSubscribe(Flow.Subscription subscription) {
    actual.onSubscribe(subscription);
  }

  @Override
  public void onNext(T item) {
    if (done) {
      return;
    }
    if (item == null) {
      onError(new NullPointerException("onNext called with null"));
      return;
    }
    try {
      queue.put(item);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      onError(e);
      return;
    }
    drain();
  }

  public void onNext(java.util.Collection<? extends T> items) {
    if (done || items == null || items.isEmpty()) {
      return;
    }
    for (T item : items) {
      if (item == null) {
        onError(new NullPointerException("onNext called with null element in collection"));
        return;
      }
      while (!done) {
        if (queue.offer(item)) {
          break;
        }
        // Queue is full: drain pending elements before enqueueing more.
        drain();
      }
      if (done) {
        return;
      }
    }
    drain();
  }

  @Override
  public void onError(Throwable throwable) {
    if (done) {
      return;
    }
    error = throwable;
    done = true;
    drain();
  }

  @Override
  public void onComplete() {
    if (done) {
      return;
    }
    done = true;
    drain();
  }

  private void drain() {
    if (wip.getAndIncrement() != 0) {
      return;
    }

    int missed = 1;
    final Flow.Subscriber<? super T> a = actual;
    final java.util.concurrent.BlockingQueue<T> q = queue;

    for (;;) {
      for (;;) {
        boolean d = done;
        T item = q.poll();
        boolean empty = (item == null);

        if (d && empty) {
          if (error != null) {
            a.onError(error);
          } else {
            a.onComplete();
          }
          return;
        }

        if (empty) {
          break;
        }

        try {
          a.onNext(item);
        } catch (Throwable t) {
          error = t;
          done = true;
          a.onError(t);
          return;
        }
      }

      missed = wip.addAndGet(-missed);
      if (missed == 0) {
        break;
      }
    }
  }
}
