package pl.gov.nac.warc.records.cdx;

/**
 * Raw CDX line record - contains unparsed line from CDX file.
 * Used for pass-through or when parsing is deferred.
 */
public final class RecordCdxRaw implements RecordCdx {

    private final String line;
    private final boolean isCdxj;

    public RecordCdxRaw(String line, boolean isCdxj) {
        this.line = line;
        this.isCdxj = isCdxj;
    }

    public RecordCdxRaw(String line) {
        this.line = line;
        // Detect CDXJ by JSON object presence
        this.isCdxj = line.contains("{") && line.contains("}");
    }

    /**
     * The raw CDX line.
     * 
     * @return raw line content
     */
    public String line() {
        return line;
    }

    @Override
    public boolean isCdxj() {
        return isCdxj;
    }

    @Override
    public String surtKey() {
        // Extract SURT from first field
        int space = line.indexOf(' ');
        return space > 0 ? line.substring(0, space) : null;
    }

    @Override
    public String timestamp() {
        // Extract timestamp from second field
        int first = line.indexOf(' ');
        if (first < 0)
            return null;
        int second = line.indexOf(' ', first + 1);
        return second > first ? line.substring(first + 1, second) : null;
    }

    @Override
    public String typeName() {
        return "RecordCdxRaw";
    }

    @Override
    public long actualDataSize() {
        return line.length() * 2L; // Java char = 2 bytes
    }
}
