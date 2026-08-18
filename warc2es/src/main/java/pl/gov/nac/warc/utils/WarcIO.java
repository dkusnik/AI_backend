package pl.gov.nac.warc.utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import org.netpreserve.jwarc.WarcReader;
import org.netpreserve.jwarc.WarcRecord;

import pl.gov.nac.warc.records.warc.RecordWarcUniversal;

public final class WarcIO {

  private WarcIO() {
  }

  public static byte[] getPayload(byte[] raw) {
    int end = findHeaderEnd(raw);
    if (end < 0)
      return raw;
    int start = end; // findHeaderEnd() returns first payload byte
    if (start >= raw.length)
      return new byte[0];

    // Try to respect Content-Length if present
    Map<String, String> headers = parseHeaders(raw);
    String clStr = headers.get("Content-Length");
    if (clStr != null) {
      try {
        int cl = Integer.parseInt(clStr.trim());
        if (start + cl <= raw.length) {
          return java.util.Arrays.copyOfRange(raw, start, start + cl);
        }
      } catch (NumberFormatException e) {
        // Ignore and take everything
      }
    }

    return java.util.Arrays.copyOfRange(raw, start, raw.length);
  }

  public static byte[] getHttpPayload(byte[] raw) {
    // First skip WARC headers
    byte[] warcPayload = getPayload(raw);
    // Then skip HTTP headers
    int end = findHeaderEnd(warcPayload);
    if (end < 0)
      return warcPayload;
    int start = end; // findHeaderEnd() returns first payload byte
    if (start >= warcPayload.length)
      return new byte[0];
    return java.util.Arrays.copyOfRange(warcPayload, start, warcPayload.length);
  }

  public static byte[] toWarcBytes(RecordWarcUniversal rec) {
    byte[] raw = rec.rawBytes();
    if (raw == null)
      raw = new byte[0];

    byte[] payload;
    // Check if raw already contains headers (starts with WARC/1.0 or WARC/1.1)
    if (raw.length >= 8 && new String(raw, 0, 5).equals("WARC/")) {
      payload = getPayload(raw);
    } else {
      payload = raw;
    }

    // Assemble headers + payload
    StringBuilder sb = new StringBuilder();
    String version = rec.headers().getOrDefault("WARC-Version", "1.0");
    if (!version.startsWith("WARC/")) {
      sb.append("WARC/").append(version).append("\r\n");
    } else {
      sb.append(version).append("\r\n");
    }

    for (Map.Entry<String, String> entry : rec.headers().entrySet()) {
      String key = entry.getKey();
      if ("Content-Length".equalsIgnoreCase(key) || "WARC-Version".equalsIgnoreCase(key))
        continue;
      sb.append(key).append(": ").append(entry.getValue()).append("\r\n");
    }
    sb.append("Content-Length: ").append(payload.length).append("\r\n");
    sb.append("\r\n");

    byte[] headerBytes = sb.toString().getBytes(StandardCharsets.ISO_8859_1);
    byte[] result = new byte[headerBytes.length + payload.length + 4]; // + \r\n\r\n at end
    System.arraycopy(headerBytes, 0, result, 0, headerBytes.length);
    System.arraycopy(payload, 0, result, headerBytes.length, payload.length);
    result[result.length - 4] = '\r';
    result[result.length - 3] = '\n';
    result[result.length - 2] = '\r';
    result[result.length - 1] = '\n';
    return result;
  }

  // OPT-C1: Streaming write - reduces memory allocation
  private static final byte[] DOUBLE_CRLF = "\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1);

  /**
   * OPT-C1: Write WARC record to OutputStream with reduced allocations.
   * Builds headers in StringBuilder (like original) but writes payload directly,
   * avoiding the large final byte[] allocation and copy.
   */
  public static void writeWarcRecord(RecordWarcUniversal rec, OutputStream out) throws IOException {
    byte[] raw = rec.rawBytes();
    if (raw == null)
      raw = new byte[0];

    int payloadOffset = 0;
    int payloadLength;

    // Check if raw already contains headers (starts with WARC/1.0 or WARC/1.1)
    if (raw.length >= 8 && raw[0] == 'W' && raw[1] == 'A' && raw[2] == 'R' && raw[3] == 'C' && raw[4] == '/') {
      // Find header end and extract payload inline
      int headerEnd = findHeaderEnd(raw);
      if (headerEnd >= 0) {
        payloadOffset = headerEnd; // findHeaderEnd() returns first payload byte
        payloadLength = raw.length - payloadOffset - 4; // Exclude trailing \r\n\r\n
        if (payloadLength < 0) payloadLength = 0;
      } else {
        payloadLength = raw.length;
      }
    } else {
      payloadLength = raw.length;
    }

    // Build headers with StringBuilder (like original - fast string concat)
    StringBuilder sb = new StringBuilder(256);
    String version = rec.headers().getOrDefault("WARC-Version", "1.0");
    if (!version.startsWith("WARC/")) {
      sb.append("WARC/");
    }
    sb.append(version).append("\r\n");

    for (Map.Entry<String, String> entry : rec.headers().entrySet()) {
      String key = entry.getKey();
      if ("Content-Length".equalsIgnoreCase(key) || "WARC-Version".equalsIgnoreCase(key))
        continue;
      sb.append(key).append(": ").append(entry.getValue()).append("\r\n");
    }
    sb.append("Content-Length: ").append(payloadLength).append("\r\n\r\n");

    // Write headers (single write for all headers)
    out.write(sb.toString().getBytes(StandardCharsets.ISO_8859_1));

    // Write payload directly from raw bytes (avoids copy)
    if (payloadOffset > 0) {
      out.write(raw, payloadOffset, payloadLength);
    } else {
      out.write(raw, 0, payloadLength);
    }

    // Trailing CRLFCRLF
    out.write(DOUBLE_CRLF);
  }

  public static byte[] serialize(WarcRecord rec) throws IOException {
    PooledBuffer target = BufferPool.INSTANCE.borrow();
    try {
      serialize(rec, target);
      byte[] result = new byte[target.length];
      System.arraycopy(target.array, 0, result, 0, target.length);
      return result;
    } finally {
      target.release();
    }
  }

  public static void serialize(WarcRecord rec, PooledBuffer target) throws IOException {
    // Fast path: avoid WarcWriter overhead for headers
    serializeFast(rec, target);
  }

  public static void serializeFast(WarcRecord rec, PooledBuffer target) throws IOException {
    // Write headers directly to buffer
    StringBuilder sb = new StringBuilder();
    sb.append(rec.version().toString()).append("\r\n");
    rec.headers().map().forEach((name, values) -> {
      for (String value : values) {
        sb.append(name).append(": ").append(value).append("\r\n");
      }
    });
    sb.append("\r\n");

    byte[] headerBytes = sb.toString().getBytes(StandardCharsets.ISO_8859_1);
    ensureCapacity(target, headerBytes.length);
    System.arraycopy(headerBytes, 0, target.array, target.length, headerBytes.length);
    target.length += headerBytes.length;

    // Write body
    OutputStream out = new OutputStream() {
      @Override
      public void write(int b) {
        ensureCapacity(target, 1);
        target.array[target.length++] = (byte) b;
      }

      @Override
      public void write(byte[] b, int off, int len) {
        ensureCapacity(target, len);
        System.arraycopy(b, off, target.array, target.length, len);
        target.length += len;
      }
    };

    rec.body().stream().transferTo(out);

    // Trailer CRLF
    ensureCapacity(target, 2);
    target.array[target.length++] = '\r';
    target.array[target.length++] = '\n';
  }

  private static void ensureCapacity(PooledBuffer target, int needed) {
    if (target.length + needed > target.array.length) {
      int newSize = Math.max(target.array.length * 2, target.length + needed);
      byte[] newArr = new byte[newSize];
      System.arraycopy(target.array, 0, newArr, 0, target.length);
      target.relocate(newArr, false);
    }
  }

  public static WarcRecord deserialize(byte[] raw) throws IOException {
    return deserialize(raw, raw.length);
  }

  public static WarcRecord deserialize(byte[] raw, int length) throws IOException {
    @SuppressWarnings("resource")
    WarcReader r = new WarcReader(new ByteArrayInputStream(raw, 0, length));
    return r.next().orElse(null);
  }

  public static WarcRecord deserialize(PooledBuffer pooled) throws IOException {
    return deserialize(pooled.array, pooled.length);
  }

  public static RecordWarcUniversal toUniversal(WarcRecord rec, byte[] raw) {
    return new RecordWarcUniversal(
        rec.type(),
        parseHeaders(raw),
        raw);
  }

  public static Map<String, String> parseHeaders(byte[] raw) {
    Map<String, String> headers = new LinkedHashMap<>();

    int end = findHeaderEnd(raw);
    if (end < 0)
      return headers;

    String block = new String(raw, 0, end, StandardCharsets.ISO_8859_1);
    String[] lines = block.split("\r?\n");

    // First line is status / request line; skip it
    for (int i = 1; i < lines.length; i++) {
      String line = lines[i].trim();
      if (line.isEmpty())
        continue;

      int colon = line.indexOf(':');
      if (colon <= 0)
        continue;

      String name = line.substring(0, colon).trim();
      String value = line.substring(colon + 1).trim();
      headers.put(name, value);
    }

    return headers;
  }

  /**
   * Returns the index of the first payload byte (i.e. the position immediately
   * after the header-terminating separator).
   *
   * <ul>
   *   <li>CRLF CRLF → returns {@code i + 4}</li>
   *   <li>LF LF fallback → returns {@code i + 2} (bare-LF, non-RFC-compliant)</li>
   * </ul>
   *
   * <p>Callers must use the returned value directly as the payload start
   * offset — do <em>not</em> add any additional offset.
   */
  private static int findHeaderEnd(byte[] raw) {
    // CRLF CRLF — standard RFC 2616 / WARC
    for (int i = 0; i < raw.length - 3; i++) {
      if (raw[i] == '\r' && raw[i + 1] == '\n'
          && raw[i + 2] == '\r' && raw[i + 3] == '\n') {
        return i + 4; // first payload byte
      }
    }
    // LF LF fallback — bare-LF (non-compliant but found in the wild)
    for (int i = 0; i < raw.length - 1; i++) {
      if (raw[i] == '\n' && raw[i + 1] == '\n') {
        return i + 2; // first payload byte
      }
    }
    return -1;
  }

  /**
   * Build a warcinfo record with NAC custom headers.
   *
   * @param derivativeType   NAC-WARC-derivative value (e.g., "wet", "doet",
   *                         "row"), null for standard WARC
   * @param lowercaseHeaders if true, sets NAC-LCH: true
   * @param recordOrder      NAC-record-order value (e.g., "digest-ascending"),
   *                         null for none
   * @param additionalFields additional warcinfo body fields (key=value format)
   * @return serialized warcinfo record bytes
   */
  public static byte[] buildWarcinfoRecord(String derivativeType, boolean lowercaseHeaders,
      String recordOrder, Map<String, String> additionalFields) {

    String recordId = "<urn:uuid:" + java.util.UUID.randomUUID() + ">";
    String date = java.time.Instant.now().toString();

    // Build warcinfo body content (application/warc-fields format)
    StringBuilder body = new StringBuilder();
    body.append("software: NAC WARC Pipeline 1.0\r\n");
    body.append("format: WARC File Format 1.1\r\n");
    body.append("NAC-Version: 1.0\r\n");

    if (lowercaseHeaders) {
      body.append("NAC-LCH: true\r\n");
    }

    if (derivativeType != null && !derivativeType.isBlank()) {
      body.append("NAC-WARC-derivative: ").append(derivativeType).append("\r\n");
    }

    if (recordOrder != null && !recordOrder.isBlank() && !"none".equalsIgnoreCase(recordOrder)) {
      body.append("NAC-record-order: ").append(recordOrder).append("\r\n");
    }

    if (additionalFields != null) {
      for (Map.Entry<String, String> entry : additionalFields.entrySet()) {
        body.append(entry.getKey()).append(": ").append(entry.getValue()).append("\r\n");
      }
    }

    byte[] bodyBytes = body.toString().getBytes(StandardCharsets.UTF_8);

    // Build WARC headers
    StringBuilder headers = new StringBuilder();
    headers.append("WARC/1.1\r\n");
    headers.append("WARC-Type: warcinfo\r\n");
    headers.append("WARC-Date: ").append(date).append("\r\n");
    headers.append("WARC-Record-ID: ").append(recordId).append("\r\n");
    headers.append("Content-Type: application/warc-fields\r\n");
    headers.append("Content-Length: ").append(bodyBytes.length).append("\r\n");
    headers.append("\r\n");

    byte[] headerBytes = headers.toString().getBytes(StandardCharsets.ISO_8859_1);

    // Combine: headers + body + trailing CRLFCRLF
    byte[] result = new byte[headerBytes.length + bodyBytes.length + 4];
    System.arraycopy(headerBytes, 0, result, 0, headerBytes.length);
    System.arraycopy(bodyBytes, 0, result, headerBytes.length, bodyBytes.length);
    result[result.length - 4] = '\r';
    result[result.length - 3] = '\n';
    result[result.length - 2] = '\r';
    result[result.length - 1] = '\n';

    return result;
  }
}
