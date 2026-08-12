package pl.gov.nac.warc.records.warc;

import java.util.Map;

/**
 * WARC record wrapping an org.netpreserve.jwarc.WarcRecord.
 * Provides type-safe access to jwarc library objects.
 */
public final class RecordWarcJwarc implements RecordWarc {

    private final org.netpreserve.jwarc.WarcRecord delegate;
    private final byte[] raw;
    private final Map<String, String> headerCache;

    public RecordWarcJwarc(org.netpreserve.jwarc.WarcRecord delegate, byte[] raw) {
        this.delegate = delegate;
        this.raw = raw;
        this.headerCache = extractHeaders(delegate);
    }

    /**
     * Access the underlying jwarc record for library-specific operations.
     * 
     * @return the wrapped jwarc record
     */
    public org.netpreserve.jwarc.WarcRecord delegate() {
        return delegate;
    }

    @Override
    public String typeName() {
        return "RecordWarcJwarc";
    }

    @Override
    public String warcType() {
        return delegate.type().toString();
    }

    @Override
    public String targetUri() {
        if (delegate instanceof org.netpreserve.jwarc.WarcTargetRecord tr) {
            return tr.targetURI().toString();
        }
        return null;
    }

    @Override
    public String warcDate() {
        return delegate.date().toString();
    }

    @Override
    public String recordId() {
        return delegate.id().toString();
    }

    @Override
    public Map<String, String> headers() {
        return headerCache;
    }

    @Override
    public byte[] rawBytes() {
        return raw;
    }

    private static Map<String, String> extractHeaders(org.netpreserve.jwarc.WarcRecord rec) {
        var builder = new java.util.HashMap<String, String>();
        rec.headers().map().forEach((k, v) -> {
            if (!v.isEmpty()) {
                builder.put(k, v.get(0));
            }
        });
        return Map.copyOf(builder);
    }
}
