package pl.gov.nac.warc.reactive;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Unified subscriber wrapper with composable behaviors.
 * Composes completion, memory-tracking, and error-propagation behavior.
 *
 * Usage:
 *
 * <pre>
 * CompositeSubscriber.wrap(consumer)
 *     .withLatch(latch)
 *     .withMemoryTracking()
 *     .build();
 * </pre>
 */
public final class CompositeSubscriber<T> implements Flow.Subscriber<T> {
  private static final Logger log = LogManager.getLogger(CompositeSubscriber.class);

  private final Flow.Subscriber<T> delegate;

  // Optional: Latch countdown (CompletionSubscriber behavior)
  private final CountDownLatch latch;

  // Optional: Memory tracking
  private final boolean trackMemory;

  // Optional: Error flag set on onError (for exit-code propagation)
  private final AtomicBoolean errorFlag;

  private CompositeSubscriber(Builder<T> builder) {
    this.delegate = builder.delegate;
    this.latch = builder.latch;
    this.trackMemory = builder.trackMemory;
    this.errorFlag = builder.errorFlag;
  }

  public static <T> Builder<T> wrap(Flow.Subscriber<T> delegate) {
    return new Builder<>(delegate);
  }

  @Override
  public void onSubscribe(Flow.Subscription subscription) {
    delegate.onSubscribe(subscription);
  }

  @Override
  public void onNext(T item) {
    if (trackMemory) {
      Metrics.updatePeakMemory();
    }
    delegate.onNext(item);
  }

  @Override
  public void onError(Throwable throwable) {
    if (errorFlag != null) {
      errorFlag.set(true);
    }
    try {
      delegate.onError(throwable);
    } finally {
      if (latch != null) {
        latch.countDown();
      }
    }
  }

  @Override
  public void onComplete() {
    log.info("onComplete received, forwarding to delegate");
    try {
      delegate.onComplete();
    } finally {
      if (latch != null) {
        log.info("Countdown latch");
        latch.countDown();
      }
    }
  }

  // =========================================================================
  // Builder
  // =========================================================================

  public static final class Builder<T> {
    private final Flow.Subscriber<T> delegate;
    private CountDownLatch latch;
    private boolean trackMemory;
    private AtomicBoolean errorFlag;

    private Builder(Flow.Subscriber<T> delegate) {
      this.delegate = delegate;
    }

    /**
     * Countdown latch on complete/error (CompletionSubscriber behavior).
     */
    public Builder<T> withLatch(CountDownLatch l) {
      this.latch = l;
      return this;
    }

    /**
     * Track peak memory on each onNext.
     */
    public Builder<T> withMemoryTracking() {
      this.trackMemory = true;
      return this;
    }

    /**
     * Set this flag to {@code true} when onError is called, so callers can
     * propagate a non-zero exit code after the pipeline completes.
     */
    public Builder<T> withErrorFlag(AtomicBoolean flag) {
      this.errorFlag = flag;
      return this;
    }

    public CompositeSubscriber<T> build() {
      return new CompositeSubscriber<>(this);
    }
  }
}
