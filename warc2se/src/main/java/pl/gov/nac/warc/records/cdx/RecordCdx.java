package pl.gov.nac.warc.records.cdx;

import pl.gov.nac.warc.records.RecordInMemory;

/**
 * CDX index record types.
 * CDX is a capture index format used by web archives.
 * 
 * @see RecordCdxRaw for raw line (unparsed)
 * @see RecordCdxStructured for parsed/structured CDX entry
 */
public sealed interface RecordCdx extends RecordInMemory
        permits RecordCdxRaw, RecordCdxStructured {

    /**
     * Whether this is CDXJ format (JSON metadata) vs legacy CDX (space-delimited).
     * 
     * @return true if CDXJ format
     */
    boolean isCdxj();

    /**
     * The SURT (Sort-friendly URI Reordering Transform) key if available.
     * 
     * @return SURT key or null
     */
    String surtKey();

    /**
     * Timestamp in 14-digit format (YYYYMMDDhhmmss).
     * 
     * @return timestamp string
     */
    String timestamp();

    @Override
    default String typeName() {
        return "RecordCdx";
    }
}
