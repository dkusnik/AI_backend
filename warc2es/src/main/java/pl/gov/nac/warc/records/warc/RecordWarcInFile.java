package pl.gov.nac.warc.records.warc;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;

import pl.gov.nac.warc.records.RecordExternal;

/**
 * WARC record referenced by file path, offset, and length.
 * Supports lazy loading - bytes are only read when rawBytes() is called.
 * 
 * This is an external record (not in-memory) - it implements RecordExternal,
 * not RecordWarc, since data is not loaded by default.
 */
public final class RecordWarcInFile implements RecordExternal {

    private final Path file;
    private final long offset;
    private final long length;
    private final boolean compressed;

    // Cached raw bytes after first load
    private byte[] cachedBytes;

    /**
     * Create a reference to a WARC record in a file.
     * 
     * @param file       path to the WARC file
     * @param offset     byte offset of the record start
     * @param length     length of the record in bytes
     * @param compressed whether the file is GZIP-compressed
     */
    public RecordWarcInFile(Path file, long offset, long length, boolean compressed) {
        if (file == null) {
            throw new IllegalArgumentException("file cannot be null");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("offset cannot be negative");
        }
        this.file = file;
        this.offset = offset;
        this.length = length;
        this.compressed = compressed;
    }

    public Path file() {
        return file;
    }

    public long offset() {
        return offset;
    }

    public long length() {
        return length;
    }

    public boolean compressed() {
        return compressed;
    }

    @Override
    public String typeName() {
        return "RecordWarcInFile";
    }

    /**
     * Load and return the raw bytes of this record.
     * Results are cached for subsequent calls.
     * 
     * @return raw bytes of the WARC record
     * @throws IOException if reading fails
     */
    public byte[] rawBytes() throws IOException {
        if (cachedBytes != null) {
            return cachedBytes;
        }

        synchronized (this) {
            if (cachedBytes != null) {
                return cachedBytes;
            }

            cachedBytes = loadBytes();
            return cachedBytes;
        }
    }

    private byte[] loadBytes() throws IOException {
        if (compressed) {
            return loadCompressedBytes();
        } else {
            return loadUncompressedBytes();
        }
    }

    private byte[] loadUncompressedBytes() throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {
            raf.seek(offset);
            byte[] data = new byte[(int) length];
            int read = raf.read(data);
            if (read < length) {
                // Partial read - return what we got
                return java.util.Arrays.copyOf(data, read);
            }
            return data;
        }
    }

    private byte[] loadCompressedBytes() throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {
            raf.seek(offset);

            // Read compressed data
            byte[] compressedData = new byte[(int) length];
            raf.readFully(compressedData);

            // Decompress using GZIP
            try (InputStream bis = new java.io.ByteArrayInputStream(compressedData);
                    GZIPInputStream gzis = new GZIPInputStream(bis);
                    java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream()) {

                byte[] buffer = new byte[8192];
                int n;
                while ((n = gzis.read(buffer)) != -1) {
                    baos.write(buffer, 0, n);
                }
                return baos.toByteArray();
            }
        }
    }

    /**
     * Check if the bytes have been loaded into memory.
     */
    public boolean isLoaded() {
        return cachedBytes != null;
    }

    /**
     * Clear cached bytes to free memory.
     */
    public void clearCache() {
        cachedBytes = null;
    }
}
