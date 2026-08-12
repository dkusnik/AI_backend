package pl.gov.nac.warc.records.file;

import java.nio.file.Path;
import java.util.Map;

/**
 * Reference to a WARC or WARC.GZ file.
 */
public final class RecordFileWarc implements RecordFile {

    private final Path path;
    private final boolean temporary;
    private final boolean handover;
    private final Map<String, Object> metadata;

    public RecordFileWarc(Path path) {
        this(path, false, false, Map.of());
    }

    public RecordFileWarc(Path path, boolean temporary, boolean handover, Map<String, Object> metadata) {
        this.path = path;
        this.temporary = temporary;
        this.handover = handover;
        this.metadata = Map.copyOf(metadata);
    }

    @Override
    public String typeName() {
        return "RecordFileWarc";
    }

    @Override
    public Path path() {
        return path;
    }

    @Override
    public boolean isTemporary() {
        return temporary;
    }

    @Override
    public boolean handoverOwnership() {
        return handover;
    }

    @Override
    public Map<String, Object> metadata() {
        return metadata;
    }
}
