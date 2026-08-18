package pl.gov.nac.warc.utils;

/**
 * A wrapper for a pooled byte array.
 * Carries the actual length of data stored in the (potentially larger) array.
 */
public final class PooledBuffer {
    public byte[] array;
    public int length;
    private BufferPool pool;

    public PooledBuffer(byte[] array, BufferPool pool) {
        this.array = array;
        this.pool = pool;
    }

    /**
     * Swaps the internal array for a larger one.
     * If we relocated, we should probably stop being poolable if the new size is
     * non-standard.
     */
    public void relocate(byte[] newArray, boolean stillPoolable) {
        this.array = newArray;
        if (!stillPoolable) {
            this.pool = null;
        }
    }

    /**
     * Returns this buffer to the pool if one was assigned.
     */
    public void release() {
        if (pool != null) {
            pool.release(this);
        }
    }
}
