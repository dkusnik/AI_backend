package pl.gov.nac.warc.records;

import pl.gov.nac.warc.records.warc.RecordWarcUniversal;

import java.time.Instant;
import java.util.Collections;
import java.util.Set;
import java.util.HashSet;

/**
 * A batch of WARC records sharing the same content digest.
 * Used for atomic processing of related records in merge operations.
 *
 * <p>Guarantees:
 * <ul>
 *   <li>All records in the batch have identical digest (content hash)</li>
 *   <li>Records are provided as an unmodifiable set (order doesn't matter)</li>
 *   <li>Aggregate metadata (min/max dates) pre-computed by producer</li>
 * </ul>
 *
 * <p>This record type enables processors to handle groups of related records
 * atomically without implementing digest-change detection logic.
 *
 * <p>Used in merge pipelines where the producer (ChunkedArchiveExtractor) emits
 * batches based on type negotiation with downstream processors that require
 * atomic batch processing (WarcAccumulatorDeduplicateDoet in merge mode).
 */
public record RecordBatch(
    String sharedDigest,
    Set<RecordWarcUniversal> records,
    Instant minDate,
    Instant maxDate
) implements RecordInMemory {

    /**
     * Compact constructor with validation.
     */
    public RecordBatch {
        if (sharedDigest == null || sharedDigest.isBlank()) {
            throw new IllegalArgumentException("sharedDigest cannot be null or blank");
        }
        if (records == null || records.isEmpty()) {
            throw new IllegalArgumentException("records cannot be null or empty");
        }

        // Defensive copy - ensure immutability
        records = Collections.unmodifiableSet(new HashSet<>(records));

        // Validate all records have matching digest
        for (RecordWarcUniversal record : records) {
            String recordDigest = extractDigest(record);
            if (recordDigest == null) {
                throw new IllegalArgumentException(
                    "Record missing digest: " + record.targetUri()
                );
            }
            if (!sharedDigest.equals(recordDigest)) {
                throw new IllegalArgumentException(
                    "Record digest mismatch: expected " + sharedDigest + ", got " + recordDigest
                );
            }
        }

        // Validate date range
        if (minDate == null || maxDate == null) {
            throw new IllegalArgumentException("minDate and maxDate cannot be null");
        }
        if (minDate.isAfter(maxDate)) {
            throw new IllegalArgumentException(
                "minDate (" + minDate + ") cannot be after maxDate (" + maxDate + ")"
            );
        }
    }

    /**
     * Get the number of records in this batch.
     */
    public int size() {
        return records.size();
    }

    /**
     * Extract digest from WARC record headers.
     * Checks both WARC-Payload-Digest and warc-payload-digest (case-insensitive).
     */
    private static String extractDigest(RecordWarcUniversal record) {
        String digest = record.headers().get("WARC-Payload-Digest");
        if (digest == null) {
            digest = record.headers().get("warc-payload-digest");
        }
        if (digest == null) {
            digest = record.headers().get("WARC-Block-Digest");
        }
        if (digest == null) {
            digest = record.headers().get("warc-block-digest");
        }
        // Match MergeCursor fallback: records with no digest (e.g. warcinfo) get
        // the zero hash so they form their own batch and don't break validation.
        if (digest == null) {
            digest = "xxh128:00000000000000000000000000000000";
        }
        return digest;
    }

    @Override
    public String typeName() {
        return "RecordBatch";
    }

    @Override
    public long actualDataSize() {
        // Sum of all record sizes in the batch
        return records.stream()
            .mapToLong(RecordWarcUniversal::actualDataSize)
            .sum();
    }

    @Override
    public String toString() {
        String digestPrefix = sharedDigest.length() > 20
            ? sharedDigest.substring(0, 20) + "..."
            : sharedDigest;
        return "RecordBatch[digest=" + digestPrefix +
               ", count=" + records.size() +
               ", dateRange=[" + minDate + " to " + maxDate + "]]";
    }
}
