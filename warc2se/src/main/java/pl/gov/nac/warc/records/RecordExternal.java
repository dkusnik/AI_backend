package pl.gov.nac.warc.records;

import java.util.Optional;

/**
 * Record with data stored externally (file, database, network).
 * Memory footprint is minimal - only metadata is in heap.
 * 
 * <p>
 * Features:
 * </p>
 * <ul>
 * <li>Not cached by default - data loaded on demand</li>
 * <li>Memory footprint only while actively passed</li>
 * <li>Optional cache field (gated by in-flight limit)</li>
 * <li>May know actualDataSize for budgeting</li>
 * </ul>
 * 
 * @see RecordFile for file references
 */
public non-sealed interface RecordExternal extends Record {

    /**
     * If true, receiver becomes sole owner and should clean up after processing.
     * Used for temporary files that should be deleted after consumption.
     * 
     * @return true if ownership is transferred to receiver
     */
    default boolean handoverOwnership() {
        return false;
    }

    /**
     * Optional cached data loaded on demand.
     * Cache is typically SoftReference-backed and may be evicted under memory
     * pressure.
     * 
     * @return cached data bytes if available
     */
    default Optional<byte[]> cachedData() {
        return Optional.empty();
    }

    /**
     * Load data into cache. Called when in-flight limit allows.
     * Implementations should use SoftReference to allow GC under pressure.
     * 
     * @return the loaded data
     * @throws java.io.IOException if data cannot be loaded
     */
    default byte[] ensureCached() throws java.io.IOException {
        return cachedData()
                .orElseThrow(() -> new UnsupportedOperationException("Caching not supported for " + typeName()));
    }

    /**
     * Evict cached data to free memory.
     */
    default void evictCache() {
        // Default: no-op if no caching
    }
}
