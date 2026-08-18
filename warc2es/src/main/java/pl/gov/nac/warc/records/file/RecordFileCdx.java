package pl.gov.nac.warc.records.file;

import java.nio.file.Path;

/**
 * Reference to a CDX or CDXJ index file.
 */
public final class RecordFileCdx implements RecordFile {

    private final Path path;
    private final boolean isCdxj;

    public RecordFileCdx(Path path, boolean isCdxj) {
        this.path = path;
        this.isCdxj = isCdxj;
    }

    public RecordFileCdx(Path path) {
        this(path, path.toString().endsWith(".cdxj"));
    }

    @Override
    public String typeName() {
        return isCdxj ? "CdxjRecordFile" : "RecordFileCdx";
    }

    @Override
    public Path path() {
        return path;
    }

    /**
     * Whether this is CDXJ (JSON) format vs legacy CDX.
     * 
     * @return true if CDXJ format
     */
    public boolean isCdxj() {
        return isCdxj;
    }
}
