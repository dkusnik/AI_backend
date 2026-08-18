package pl.gov.nac.warc.records.file;

import java.nio.file.Path;

/**
 * Reference to a Dedup-On-Write (DOW) index file.
 */
public final class RecordFileDow implements RecordFile {

    private final Path path;

    public RecordFileDow(Path path) {
        this.path = path;
    }

    @Override
    public String typeName() {
        return "RecordFileDow";
    }

    @Override
    public Path path() {
        return path;
    }
}
