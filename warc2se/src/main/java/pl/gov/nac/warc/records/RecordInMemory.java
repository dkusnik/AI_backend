package pl.gov.nac.warc.records;

import pl.gov.nac.warc.records.cdx.RecordCdx;
import pl.gov.nac.warc.records.warc.RecordWarc;

/**
 * Record with data fully in heap memory.
 * Provides actual size for accurate memory budgeting.
 * 
 * @see RecordWarc for WARC-based record types
 * @see RecordCdx for CDX index record types
 */
public non-sealed interface RecordInMemory extends Record {

    /**
     * Actual size of in-memory data in bytes.
     * This is the heap footprint of the record payload.
     * 
     * @return size in bytes
     */
    long actualDataSize();

    @Override
    default long declaredSize() {
        return actualDataSize();
    }
}
