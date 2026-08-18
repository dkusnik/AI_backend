package pl.gov.nac.warc.records.warc;

import java.util.Map;

/**
 * Library-agnostic WARC record representation.
 * Contains parsed headers and raw bytes without dependency on specific WARC
 * library.
 * Mutable to allow decoration/modification by processors.
 */
public non-sealed class RecordWarcUniversal implements RecordWarc {

  private String type;
  private Map<String, String> headers;
  private byte[] raw;
  private byte[] body;

  public RecordWarcUniversal(String type, Map<String, String> headers, byte[] raw) {
    this.type = type;
    this.headers = new java.util.TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    this.headers.putAll(headers);
    this.raw = raw;
    this.body = null; // Lazy or explicitly set
  }

  // Default constructor for builders
  public RecordWarcUniversal() {
    this.type = "unknown";
    this.headers = new java.util.TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    this.raw = new byte[0];
    this.body = null;
  }

  @Override
  public String typeName() {
    return "RecordWarcUniversal";
  }

  @Override
  public String warcType() {
    return type;
  }

  public RecordWarcUniversal warcType(String type) {
    this.type = type;
    this.headers.put("WARC-Type", type);
    return this;
  }

  @Override
  public String targetUri() {
    return headers.get("WARC-Target-URI");
  }

  public RecordWarcUniversal targetUri(String uri) {
    this.headers.put("WARC-Target-URI", uri);
    return this;
  }

  @Override
  public String warcDate() {
    return headers.get("WARC-Date");
  }

  public RecordWarcUniversal warcDate(String date) {
    this.headers.put("WARC-Date", date);
    return this;
  }

  @Override
  public String recordId() {
    return headers.get("WARC-Record-ID");
  }

  public RecordWarcUniversal recordId(String id) {
    this.headers.put("WARC-Record-ID", id);
    return this;
  }

  /**
   * Returns the live, mutable header map (case-insensitive keys).
   * Callers may add or modify headers directly; this is intentional to avoid
   * unnecessary copying in the hot path.
   */
  @Override
  public Map<String, String> headers() {
    return headers;
  }

  public RecordWarcUniversal headers(Map<String, String> headers) {
    this.headers = new java.util.TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    this.headers.putAll(headers);
    return this;
  }

  public RecordWarcUniversal addHeader(String key, String value) {
    this.headers.put(key, value);
    return this;
  }

  public String contentType() {
    return headers.get("Content-Type");
  }

  public RecordWarcUniversal contentType(String contentType) {
    this.headers.put("Content-Type", contentType);
    return this;
  }

  @Override
  public byte[] rawBytes() {
    return raw;
  }

  public RecordWarcUniversal rawBytes(byte[] raw) {
    this.raw = raw;
    return this;
  }

  /**
   * Create from raw bytes by parsing WARC headers.
   *
   * @param raw complete WARC record bytes
   * @return parsed RecordWarcUniversal
   */
  public static RecordWarcUniversal fromRaw(byte[] raw) {
    // Parse headers from raw bytes
    // Scan for the WARC header/body separator to avoid an arbitrary cap that
    // silently truncates records with many long headers.
    int headerLimit = raw.length;
    outer:
    for (int i = 0; i < raw.length - 1; i++) {
      if (raw[i] == '\r' && i + 3 < raw.length
          && raw[i+1] == '\n' && raw[i+2] == '\r' && raw[i+3] == '\n') {
        headerLimit = i + 4; break outer;
      }
      if (raw[i] == '\n' && raw[i+1] == '\n') {
        headerLimit = i + 2; break outer;
      }
    }
    String content = new String(raw, 0, headerLimit,
        java.nio.charset.StandardCharsets.ISO_8859_1);

    Map<String, String> headers = new java.util.TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    String type = "unknown";

    String[] lines = content.split("\r\n");
    for (String line : lines) {
      if (line.isEmpty())
        break; // End of headers
      int colon = line.indexOf(':');
      if (colon > 0) {
        String key = line.substring(0, colon).trim();
        String value = line.substring(colon + 1).trim();
        headers.put(key, value);
        if ("WARC-Type".equalsIgnoreCase(key)) {
          type = value;
        }
      }
    }

    return new RecordWarcUniversal(type, headers, raw);
  }

  public String digest() {
    String d = headers.get("WARC-Payload-Digest");
    if (d == null)
      d = headers.get("WARC-Block-Digest");
    return d;
  }

  public byte[] bodyBytes() {
    if (body != null)
      return body;
    // Basic body extraction: find \r\n\r\n
    for (int i = 0; i < raw.length - 3; i++) {
      if (raw[i] == '\r' && raw[i + 1] == '\n' && raw[i + 2] == '\r' && raw[i + 3] == '\n') {
        body = java.util.Arrays.copyOfRange(raw, i + 4, raw.length);
        return body;
      }
    }
    return new byte[0];
  }

  public RecordWarcUniversal bodyBytes(byte[] body) {
    this.body = body;
    return this;
  }
}
