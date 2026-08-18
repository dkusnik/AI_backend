package pl.gov.nac.warc.records.warc;

import java.util.Map;

/**
 * Raw byte WARC record with minimal parsing.
 * Used when full header parsing is not needed for performance.
 * Headers are parsed lazily on first access.
 */
public final class RecordWarcRawBytes implements RecordWarc {

    private final byte[] raw;
    private volatile Map<String, String> parsedHeaders;

    public RecordWarcRawBytes(byte[] raw) {
        this.raw = raw;
    }

    @Override
    public String typeName() {
        return "RecordWarcRawBytes";
    }

    @Override
    public String warcType() {
        return headers().getOrDefault("WARC-Type", "unknown");
    }

    @Override
    public String targetUri() {
        return headers().get("WARC-Target-URI");
    }

    @Override
    public String warcDate() {
        return headers().get("WARC-Date");
    }

    @Override
    public String recordId() {
        return headers().get("WARC-Record-ID");
    }

    @Override
    public Map<String, String> headers() {
        if (parsedHeaders == null) {
            synchronized (this) {
                if (parsedHeaders == null) {
                    parsedHeaders = parseHeaders();
                }
            }
        }
        return parsedHeaders;
    }

    @Override
    public byte[] rawBytes() {
        return raw;
    }

    private Map<String, String> parseHeaders() {
        String content = new String(raw, 0, Math.min(raw.length, 4096),
                java.nio.charset.StandardCharsets.ISO_8859_1);

        Map<String, String> headers = new java.util.HashMap<>();
        String[] lines = content.split("\r\n");
        for (String line : lines) {
            if (line.isEmpty())
                break;
            int colon = line.indexOf(':');
            if (colon > 0) {
                headers.put(line.substring(0, colon).trim(),
                        line.substring(colon + 1).trim());
            }
        }
        return Map.copyOf(headers);
    }
}
