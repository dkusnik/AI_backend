package pl.gov.nac.warc.utils;

import pl.gov.nac.warc.reactive.Metrics;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * A thread-safe pool of fixed-size byte arrays.
 */
public final class BufferPool {
    private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024; // 1MB
    public static final BufferPool INSTANCE = new BufferPool();

    private static final int MAX_POOLED_BUFFERS = 64;
    private final ConcurrentLinkedDeque<byte[]> pool = new ConcurrentLinkedDeque<>();
    private final java.util.concurrent.atomic.AtomicInteger pooledCount = new java.util.concurrent.atomic.AtomicInteger(
            0);
    private final int bufferSize;

    public BufferPool() {
        this(DEFAULT_BUFFER_SIZE);
    }

    public BufferPool(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    /**
     * Borrows a buffer from the pool or allocates a new one if pool is empty.
     */
    public PooledBuffer borrow() {
        Metrics.inc("pool", "borrow");
        byte[] arr = pool.pollFirst();
        if (arr == null) {
            Metrics.inc("pool", "allocate");
            arr = new byte[bufferSize];
        } else {
            Metrics.inc("pool", "hit");
            pooledCount.decrementAndGet();
        }
        return new PooledBuffer(arr, this);
    }

    /**
     * Returns a buffer to the pool.
     */
    public void release(PooledBuffer buffer) {
        // Only return buffers of the standard size to the pool
        if (buffer.array.length == bufferSize) {
            if (pooledCount.get() >= MAX_POOLED_BUFFERS) {
                Metrics.inc("pool", "discard_full");
                return;
            }
            Metrics.inc("pool", "release");
            pool.offerFirst(buffer.array);
            pooledCount.incrementAndGet();
        } else {
            Metrics.inc("pool", "discard_large");
        }
    }

    /**
     * Handles cases where we need a buffer larger than the pool's standard size.
     */
    public PooledBuffer ensureCapacity(int requiredSize) {
        if (requiredSize <= bufferSize) {
            return borrow();
        }
        // One-off allocation for large records
        return new PooledBuffer(new byte[requiredSize], null);
    }
}
