package pl.gov.nac.warc.records.warc;

import java.util.Map;

import pl.gov.nac.warc.records.RecordInMemory;

/**
 * Sealed interface for WARC-based in-memory records.
 * All WARC record types share common header access patterns.
 * 
 * @see RecordWarcJwarc for jwarc library wrapper
 * @see RecordWarcUniversal for library-agnostic representation
 * @see RecordWarcRawBytes for minimal parsing raw bytes
 */
public sealed interface RecordWarc extends RecordInMemory
        permits RecordWarcJwarc, RecordWarcUniversal, RecordWarcRawBytes {

    /**
     * WARC record type (e.g., "response", "request", "metadata", "resource").
     * 
     * @return WARC-Type header value
     */
    String warcType();

    /**
     * Target URI of the record.
     * 
     * @return WARC-Target-URI header value, or null if not applicable
     */
    String targetUri();

    /**
     * WARC-Date header value.
     * 
     * @return ISO 8601 timestamp string
     */
    String warcDate();

    /**
     * WARC-Record-ID header value (URI format).
     * 
     * @return unique record identifier
     */
    String recordId();

    /**
     * All WARC headers as a map.
     * 
     * @return immutable map of header name to value
     */
    Map<String, String> headers();

    /**
     * Raw serialized bytes of the complete WARC record.
     * Includes headers and payload.
     * 
     * @return raw bytes
     */
    byte[] rawBytes();

    @Override
    default long actualDataSize() {
        return rawBytes().length;
    }
}
