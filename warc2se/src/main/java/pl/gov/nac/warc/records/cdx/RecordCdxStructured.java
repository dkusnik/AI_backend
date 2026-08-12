package pl.gov.nac.warc.records.cdx;

import java.util.Map;

/**
 * Structured CDX record with parsed fields.
 * For CDXJ, includes JSON metadata. For legacy CDX, includes field map.
 */
public final class RecordCdxStructured implements RecordCdx {

    private final String surtKey;
    private final String timestamp;
    private final String originalUrl;
    private final String mimeType;
    private final int statusCode;
    private final String digest;
    private final long offset;
    private final long length;
    private final String filename;
    private final Map<String, String> metadata;
    private final boolean isCdxj;

    public RecordCdxStructured(
            String surtKey,
            String timestamp,
            String originalUrl,
            String mimeType,
            int statusCode,
            String digest,
            long offset,
            long length,
            String filename,
            Map<String, String> metadata,
            boolean isCdxj) {
        this.surtKey = surtKey;
        this.timestamp = timestamp;
        this.originalUrl = originalUrl;
        this.mimeType = mimeType;
        this.statusCode = statusCode;
        this.digest = digest;
        this.offset = offset;
        this.length = length;
        this.filename = filename;
        this.metadata = metadata != null ? Map.copyOf(metadata) : Map.of();
        this.isCdxj = isCdxj;
    }

    @Override
    public String surtKey() {
        return surtKey;
    }

    @Override
    public String timestamp() {
        return timestamp;
    }

    @Override
    public boolean isCdxj() {
        return isCdxj;
    }

    public String originalUrl() {
        return originalUrl;
    }

    public String mimeType() {
        return mimeType;
    }

    public int statusCode() {
        return statusCode;
    }

    public String digest() {
        return digest;
    }

    public long offset() {
        return offset;
    }

    public long length() {
        return length;
    }

    public String filename() {
        return filename;
    }

    public Map<String, String> metadata() {
        return metadata;
    }

    @Override
    public String typeName() {
        return "RecordCdxStructured";
    }

    @Override
    public long actualDataSize() {
        // Approximate size: strings + map entries
        long size = 0;
        if (surtKey != null)
            size += surtKey.length() * 2L;
        if (timestamp != null)
            size += timestamp.length() * 2L;
        if (originalUrl != null)
            size += originalUrl.length() * 2L;
        if (mimeType != null)
            size += mimeType.length() * 2L;
        if (digest != null)
            size += digest.length() * 2L;
        if (filename != null)
            size += filename.length() * 2L;
        size += 8L + 8L + 4L; // offset, length, statusCode
        for (var entry : metadata.entrySet()) {
            size += entry.getKey().length() * 2L + entry.getValue().length() * 2L;
        }
        return size;
    }

    /**
     * Parse a raw CDX line into a structured record.
     * 
     * @param line raw CDX/CDXJ line
     * @return parsed record
     */
    public static RecordCdxStructured parse(String line) {
        boolean isCdxj = line.contains("{") && line.contains("}");

        if (isCdxj) {
            return parseCdxj(line);
        } else {
            return parseLegacyCdx(line);
        }
    }

    private static RecordCdxStructured parseCdxj(String line) {
        // CDXJ format: surt timestamp json
        String[] parts = line.split(" ", 3);
        if (parts.length < 3) {
            return new RecordCdxStructured(
                    parts.length > 0 ? parts[0] : null,
                    parts.length > 1 ? parts[1] : null,
                    null, null, 0, null, -1, -1, null, Map.of(), true);
        }

        String surt = parts[0];
        String ts = parts[1];
        String json = parts[2];

        // Simple JSON parsing (basic extraction)
        String url = extractJsonField(json, "url");
        String mime = extractJsonField(json, "mime");
        String status = extractJsonField(json, "status");
        String digest = extractJsonField(json, "digest");
        String offsetStr = extractJsonField(json, "offset");
        String lengthStr = extractJsonField(json, "length");
        String filename = extractJsonField(json, "filename");

        return new RecordCdxStructured(
                surt, ts, url, mime,
                status != null ? Integer.parseInt(status) : 0,
                digest,
                offsetStr != null ? Long.parseLong(offsetStr) : -1,
                lengthStr != null ? Long.parseLong(lengthStr) : -1,
                filename, Map.of(), true);
    }

    private static RecordCdxStructured parseLegacyCdx(String line) {
        // Legacy CDX: space-separated fields (varies by format)
        String[] parts = line.split(" ");
        // Common format: massaged-url date original mime status digest redirect robots
        // length offset filename
        return new RecordCdxStructured(
                parts.length > 0 ? parts[0] : null, // surt
                parts.length > 1 ? parts[1] : null, // timestamp
                parts.length > 2 ? parts[2] : null, // original url
                parts.length > 3 ? parts[3] : null, // mime
                parts.length > 4 ? parseIntSafe(parts[4]) : 0, // status
                parts.length > 5 ? parts[5] : null, // digest
                parts.length > 9 ? parseLongSafe(parts[9]) : -1, // offset
                parts.length > 8 ? parseLongSafe(parts[8]) : -1, // length
                parts.length > 10 ? parts[10] : null, // filename
                Map.of(), false);
    }

    private static String extractJsonField(String json, String field) {
        String pattern = "\"" + field + "\":";
        int idx = json.indexOf(pattern);
        if (idx < 0)
            return null;

        int start = idx + pattern.length();
        while (start < json.length() && (json.charAt(start) == ' ' || json.charAt(start) == '"')) {
            start++;
        }

        int end = start;
        boolean inQuotes = start > 0 && json.charAt(start - 1) == '"';
        if (inQuotes) {
            end = json.indexOf('"', start);
            if (end < 0)
                end = json.length();
        } else {
            while (end < json.length() && json.charAt(end) != ',' && json.charAt(end) != '}') {
                end++;
            }
        }

        return json.substring(start, end);
    }

    private static int parseIntSafe(String s) {
        try {
            return Integer.parseInt(s);
        } catch (Exception e) {
            return 0;
        }
    }

    private static long parseLongSafe(String s) {
        try {
            return Long.parseLong(s);
        } catch (Exception e) {
            return -1;
        }
    }
}
