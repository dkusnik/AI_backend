package pl.gov.nac.warc.records.file;

import java.nio.file.Path;
import java.util.Map;

import pl.gov.nac.warc.records.RecordExternal;

/**
 * Reference to a whole file.
 * Used for passing file handles between modules.
 * 
 * @see RecordFileWarc for .warc/.warc.gz files
 * @see RecordFileDow for dedup-on-write indexes
 */
public sealed interface RecordFile extends RecordExternal
        permits RecordFileWarc, RecordFileDow, RecordFileCdx {

    /**
     * Path to the file.
     * 
     * @return file path
     */
    Path path();

    /**
     * Whether this is a temporary file that should be deleted after use.
     * 
     * @return true if temporary
     */
    default boolean isTemporary() {
        return false;
    }

    /**
     * Optional extracted metadata if pre-parsed.
     * 
     * @return metadata map or empty
     */
    default Map<String, Object> metadata() {
        return Map.of();
    }

    @Override
    default long declaredSize() {
        try {
            return java.nio.file.Files.size(path());
        } catch (java.io.IOException _) {
            return -1;
        }
    }
}
