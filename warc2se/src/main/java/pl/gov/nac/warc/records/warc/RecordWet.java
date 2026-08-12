package pl.gov.nac.warc.records.warc;

import pl.gov.nac.warc.records.RecordInMemory;

import java.util.HashMap;
import java.util.Map;

/**
 * Record for WET (Web Extracted Text) processing pipeline.
 * Mutable intermediate record holding extracted text and metadata.
 * Replaces legacy WetRecord for proper type negotiation.
 */
public final class RecordWet implements RecordInMemory {

    private String targetUri;
    private String date;
    private String refersTo; // warc-record-id of source response
    private String text;
    private String contentType;
    private String language;
    private String digest;
    private byte[] bodyBytes; // temporary storage for HTTP body before text extraction
    private final Map<String, String> headers = new HashMap<>();

    public RecordWet() {
    }

    public RecordWet(String targetUri, String date, String refersTo) {
        this.targetUri = targetUri;
        this.date = date;
        this.refersTo = refersTo;
    }

    // Copy constructor for processors that need to modify
    public RecordWet(RecordWet other) {
        this.targetUri = other.targetUri;
        this.date = other.date;
        this.refersTo = other.refersTo;
        this.text = other.text;
        this.contentType = other.contentType;
        this.language = other.language;
        this.digest = other.digest;
        this.bodyBytes = other.bodyBytes;
        this.headers.putAll(other.headers);
    }

    @Override
    public String typeName() {
        return "WetRecord";
    }

    @Override
    public long actualDataSize() {
        long size = 0;
        if (text != null)
            size += text.length() * 2L; // chars are 2 bytes
        if (bodyBytes != null)
            size += bodyBytes.length;
        return size;
    }

    // Getters and setters
    public String targetUri() {
        return targetUri;
    }

    public RecordWet targetUri(String targetUri) {
        this.targetUri = targetUri;
        return this;
    }

    public String date() {
        return date;
    }

    public RecordWet date(String date) {
        this.date = date;
        return this;
    }

    public String refersTo() {
        return refersTo;
    }

    public RecordWet refersTo(String refersTo) {
        this.refersTo = refersTo;
        return this;
    }

    public String text() {
        return text;
    }

    public RecordWet text(String text) {
        this.text = text;
        return this;
    }

    public String contentType() {
        return contentType;
    }

    public RecordWet contentType(String contentType) {
        this.contentType = contentType;
        return this;
    }

    public String language() {
        return language;
    }

    public RecordWet language(String language) {
        this.language = language;
        return this;
    }

    public String digest() {
        return digest;
    }

    public RecordWet digest(String digest) {
        this.digest = digest;
        return this;
    }

    public byte[] bodyBytes() {
        return bodyBytes;
    }

    public RecordWet bodyBytes(byte[] bodyBytes) {
        this.bodyBytes = bodyBytes;
        return this;
    }

    public Map<String, String> headers() {
        return headers;
    }

    public RecordWet addHeader(String key, String value) {
        headers.put(key, value);
        return this;
    }
}
