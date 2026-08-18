package pl.gov.nac.warc.records;

/**
 * Base sealed interface for all record types in the pipeline.
 * 
 * @see InMemoryRecord for records with data in heap
 * @see ExternalRecord for records with data stored externally
 */
public sealed interface Record permits RecordInMemory, RecordExternal {

    /**
     * Declared size of the record data in bytes.
     * Used for metrics and progress estimation.
     * 
     * @return size in bytes if known, -1 if unknown
     */
    default long declaredSize() {
        return -1;
    }

    /**
     * Human-readable type name for logging and error messages.
     * 
     * @return short type name (e.g., "JwarcRecord", "WarcFileRecord")
     */
    String typeName();
}
