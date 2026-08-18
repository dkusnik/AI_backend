package pl.gov.nac.warc.records;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import pl.gov.nac.warc.records.warc.RecordWarcUniversal;

import java.time.Instant;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;

/**
 * Unit tests for RecordBatch record type.
 */
class RecordBatchTest {

    // Test 1: Valid RecordBatch Creation
    @Test
    @DisplayName("Should create valid RecordBatch with proper fields")
    void testValidRecordBatchCreation() {
        // Arrange
        String digest = "xxh128:1234567890abcdef";
        RecordWarcUniversal record1 = createRecordWithDigest(digest, "http://example.com/1");
        RecordWarcUniversal record2 = createRecordWithDigest(digest, "http://example.com/2");
        Set<RecordWarcUniversal> records = Set.of(record1, record2);
        Instant minDate = Instant.parse("2026-01-01T10:00:00Z");
        Instant maxDate = Instant.parse("2026-01-02T10:00:00Z");

        // Act
        RecordBatch batch = new RecordBatch(digest, records, minDate, maxDate);

        // Assert
        assertEquals(digest, batch.sharedDigest());
        assertEquals(2, batch.size());
        assertEquals(minDate, batch.minDate());
        assertEquals(maxDate, batch.maxDate());
        assertTrue(batch.records().contains(record1));
        assertTrue(batch.records().contains(record2));
    }

    // Test 2: Null Digest Validation
    @Test
    @DisplayName("Should throw exception for null digest")
    void testNullDigestThrowsException() {
        // Arrange
        RecordWarcUniversal record = createRecordWithDigest("xxh128:1234", "http://example.com");
        Set<RecordWarcUniversal> records = Set.of(record);
        Instant now = Instant.now();

        // Act & Assert
        assertThrows(IllegalArgumentException.class, () -> {
            new RecordBatch(null, records, now, now);
        });
    }

    // Test 3: Blank Digest Validation
    @Test
    @DisplayName("Should throw exception for blank digest")
    void testBlankDigestThrowsException() {
        RecordWarcUniversal record = createRecordWithDigest("xxh128:1234", "http://example.com");
        Set<RecordWarcUniversal> records = Set.of(record);
        Instant now = Instant.now();

        assertThrows(IllegalArgumentException.class, () -> {
            new RecordBatch("", records, now, now);
        });

        assertThrows(IllegalArgumentException.class, () -> {
            new RecordBatch("   ", records, now, now);
        });
    }

    // Test 4: Null Records Validation
    @Test
    @DisplayName("Should throw exception for null records")
    void testNullRecordsThrowsException() {
        String digest = "xxh128:1234567890abcdef";
        Instant now = Instant.now();

        assertThrows(IllegalArgumentException.class, () -> {
            new RecordBatch(digest, null, now, now);
        });
    }

    // Test 5: Empty Records Validation
    @Test
    @DisplayName("Should throw exception for empty records set")
    void testEmptyRecordsThrowsException() {
        String digest = "xxh128:1234567890abcdef";
        Set<RecordWarcUniversal> emptySet = Set.of();
        Instant now = Instant.now();

        assertThrows(IllegalArgumentException.class, () -> {
            new RecordBatch(digest, emptySet, now, now);
        });
    }

    // Test 6: Digest Mismatch Validation
    @Test
    @DisplayName("Should throw exception when record digest doesn't match batch digest")
    void testDigestMismatchThrowsException() {
        // Arrange
        String batchDigest = "xxh128:1111111111111111";
        String recordDigest = "xxh128:2222222222222222";
        RecordWarcUniversal record = createRecordWithDigest(recordDigest, "http://example.com");
        Set<RecordWarcUniversal> records = Set.of(record);
        Instant now = Instant.now();

        // Act & Assert
        assertThrows(IllegalArgumentException.class, () -> {
            new RecordBatch(batchDigest, records, now, now);
        });
    }

    // Test 7: Immutability - Records Set
    @Test
    @DisplayName("Should return immutable records set")
    void testRecordsSetIsImmutable() {
        // Arrange
        String digest = "xxh128:1234567890abcdef";
        RecordWarcUniversal record = createRecordWithDigest(digest, "http://example.com");
        Set<RecordWarcUniversal> records = new HashSet<>(Set.of(record));
        Instant now = Instant.now();

        RecordBatch batch = new RecordBatch(digest, records, now, now);

        // Act & Assert
        assertThrows(UnsupportedOperationException.class, () -> {
            batch.records().add(createRecordWithDigest(digest, "http://other.com"));
        });
    }

    // Test 8: Size Method
    @Test
    @DisplayName("Should return correct batch size")
    void testBatchSize() {
        String digest = "xxh128:1234567890abcdef";
        RecordWarcUniversal r1 = createRecordWithDigest(digest, "http://example.com/1");
        RecordWarcUniversal r2 = createRecordWithDigest(digest, "http://example.com/2");
        RecordWarcUniversal r3 = createRecordWithDigest(digest, "http://example.com/3");
        Set<RecordWarcUniversal> records = Set.of(r1, r2, r3);
        Instant now = Instant.now();

        RecordBatch batch = new RecordBatch(digest, records, now, now);

        assertEquals(3, batch.size());
    }

    // Test 9: Single Record Batch
    @Test
    @DisplayName("Should handle single-record batch")
    void testSingleRecordBatch() {
        String digest = "xxh128:1234567890abcdef";
        RecordWarcUniversal record = createRecordWithDigest(digest, "http://example.com");
        Set<RecordWarcUniversal> records = Set.of(record);
        Instant now = Instant.now();

        RecordBatch batch = new RecordBatch(digest, records, now, now);

        assertEquals(1, batch.size());
        assertEquals(digest, batch.sharedDigest());
    }

    // Test 10: Date Range Validation
    @Test
    @DisplayName("Should throw exception when minDate is after maxDate")
    void testDateRangeValidation() {
        // RecordBatch validates min < max
        String digest = "xxh128:1234567890abcdef";
        RecordWarcUniversal record = createRecordWithDigest(digest, "http://example.com");
        Set<RecordWarcUniversal> records = Set.of(record);

        Instant later = Instant.parse("2026-02-01T10:00:00Z");
        Instant earlier = Instant.parse("2026-01-01T10:00:00Z");

        // Should throw when minDate > maxDate
        assertThrows(IllegalArgumentException.class, () -> {
            new RecordBatch(digest, records, later, earlier);
        });
    }

    // Test 11: ActualDataSize Calculation
    @Test
    @DisplayName("Should calculate actualDataSize as sum of all records")
    void testActualDataSize() {
        String digest = "xxh128:1234567890abcdef";
        RecordWarcUniversal r1 = createRecordWithDigest(digest, "http://example.com/1");
        RecordWarcUniversal r2 = createRecordWithDigest(digest, "http://example.com/2");
        Set<RecordWarcUniversal> records = Set.of(r1, r2);
        Instant now = Instant.now();

        RecordBatch batch = new RecordBatch(digest, records, now, now);

        long expectedSize = r1.actualDataSize() + r2.actualDataSize();
        assertEquals(expectedSize, batch.actualDataSize());
    }

    // Test 12: Type Hierarchy
    @Test
    @DisplayName("Should implement RecordInMemory interface")
    void testTypeHierarchy() {
        String digest = "xxh128:1234567890abcdef";
        RecordWarcUniversal record = createRecordWithDigest(digest, "http://example.com");
        Set<RecordWarcUniversal> records = Set.of(record);
        Instant now = Instant.now();

        RecordBatch batch = new RecordBatch(digest, records, now, now);

        assertTrue(batch instanceof RecordInMemory);
        assertTrue(batch instanceof Record);
        assertEquals("RecordBatch", batch.typeName());
    }

    // Helper Methods
    private RecordWarcUniversal createRecordWithDigest(String digest, String uri) {
        Map<String, String> headers = new HashMap<>();
        headers.put("WARC-Type", "conversion");
        headers.put("WARC-Target-URI", uri);
        headers.put("WARC-Date", "2026-01-01T10:00:00Z");
        headers.put("WARC-Payload-Digest", digest);
        headers.put("Content-Type", "text/plain");

        byte[] content = "Test content".getBytes();

        return new RecordWarcUniversal(
            "conversion",
            headers,
            content
        );
    }
}
