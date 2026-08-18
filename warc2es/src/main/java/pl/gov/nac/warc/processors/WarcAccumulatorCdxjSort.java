package pl.gov.nac.warc.processors;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import pl.gov.nac.warc.reactive.Metrics;
import pl.gov.nac.warc.reactive.ReactiveInterfaces;
import pl.gov.nac.warc.records.Record;
import pl.gov.nac.warc.records.cdx.RecordCdx;
import pl.gov.nac.warc.records.cdx.RecordCdxStructured;

/**
 * Accumulates CDXJ records and emits them in sorted order (by SURT key).
 *
 * Uses in-memory accumulation until threshold, then spills to RocksDB.
 * On completion, emits all records sorted by SURT key + timestamp.
 *
 * This ensures CDXJ output is properly sorted for efficient lookups.
 */
public class WarcAccumulatorCdxjSort implements ReactiveInterfaces.ReactiveProcessor<Object, Object> {

    private static final Logger log = LogManager.getLogger(WarcAccumulatorCdxjSort.class);
    private static final String METRIC_KEY = "cdxj-sorter";

    static {
        RocksDB.loadLibrary();
    }

    private Flow.Subscriber<? super Object> downstream;
    private RocksDB db;
    private Options options;
    private WriteOptions writeOptions;

    // Configuration
    private int memoryThresholdMB = 100;
    private int maxRecordsInMemory = 10000;
    private String rocksdbPath = defaultRocksDbPath();

    // In-memory accumulation
    private final List<RecordCdxStructured> memoryBuffer = new ArrayList<>();
    private long memoryBytes = 0;
    private boolean useRocksDB = false;

    @Override
    public List<Class<? extends Record>> acceptedInputTypes() {
        return List.of(RecordCdx.class, RecordCdxStructured.class);
    }

    @Override
    public List<Class<? extends Record>> emittedOutputTypes() {
        return List.of(RecordCdxStructured.class);
    }

    @SuppressWarnings("resource")
    @Override
    public void configure(Map<String, Object> cfg) {
        Metrics.setModuleHeader(METRIC_KEY, "CDXJ Sorter");

        if (cfg.containsKey("memory-threshold-mb")) {
            this.memoryThresholdMB = Integer.parseInt(cfg.get("memory-threshold-mb").toString());
        }

        if (cfg.containsKey("max-records-in-memory")) {
            this.maxRecordsInMemory = Integer.parseInt(cfg.get("max-records-in-memory").toString());
        }

        if (cfg.containsKey("rocksdb-path")) {
            this.rocksdbPath = cfg.get("rocksdb-path").toString();
        }

        log.info("Configured: memoryThreshold={}MB, maxRecords={}, dbPath={}",
                memoryThresholdMB, maxRecordsInMemory, rocksdbPath);
    }

    private static String defaultRocksDbPath() {
        String explicit = System.getenv("WARC_CDXJ_SORT_DB_PATH");
        if (explicit != null && !explicit.isBlank()) {
            return explicit;
        }
        String tmpRoot = System.getenv("WARC_TMP_DIR");
        if (tmpRoot != null && !tmpRoot.isBlank()) {
            return Path.of(tmpRoot, "cdxj-sort-db").toString();
        }
        return Path.of(System.getProperty("user.dir"), "var", "db", "cdxj-sort-db").toString();
    }

    @Override
    public boolean beforeCheck(Map<String, Object> cfg) {
        return true;
    }

    @Override
    public int afterCheck(Map<String, Object> cfg) {
        return 0;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super Object> subscriber) {
        this.downstream = subscriber;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        downstream.onSubscribe(subscription);
    }

    @Override
    public void onNext(Object item) {
        RecordCdxStructured cdx = null;

        if (item instanceof RecordCdxStructured structured) {
            cdx = structured;
        } else if (item instanceof pl.gov.nac.warc.records.cdx.RecordCdxRaw raw) {
            // Parse raw CDX line to structured
            cdx = RecordCdxStructured.parse(raw.line());
        } else {
            // Pass through non-CDX records
            downstream.onNext(item);
            return;
        }

        try {
            accumulateRecord(cdx);
            Metrics.inc(METRIC_KEY, "recordsIn");
        } catch (Exception e) {
            log.error("Failed to accumulate CDX record: {}", e.getMessage(), e);
            Metrics.inc(METRIC_KEY, "errors");
        }
    }

    @Override
    public void onError(Throwable throwable) {
        cleanup();
        downstream.onError(throwable);
    }

    @Override
    public void onComplete() {
        try {
            log.info("Sorting and emitting {} total records (in-memory: {}, useRocksDB: {})",
                    Metrics.get(METRIC_KEY, "recordsIn"), memoryBuffer.size(), useRocksDB);

            if (useRocksDB) {
                // Flush remaining in-memory records to RocksDB
                flushMemoryToRocksDB();
                // Emit all records from RocksDB (sorted)
                emitFromRocksDB();
            } else {
                // Sort in-memory and emit
                sortAndEmitMemory();
            }

            log.info("Completed: {} records emitted", Metrics.get(METRIC_KEY, "recordsOut"));
        } catch (Exception e) {
            log.error("Error during completion: {}", e.getMessage(), e);
            downstream.onError(e);
            return;
        } finally {
            cleanup();
        }

        downstream.onComplete();
    }

    private void accumulateRecord(RecordCdxStructured cdx) throws Exception {
        // Add to memory buffer
        memoryBuffer.add(cdx);
        memoryBytes += cdx.actualDataSize();

        // Check thresholds
        if (memoryBuffer.size() >= maxRecordsInMemory
                || memoryBytes >= memoryThresholdMB * 1024L * 1024L) {
            log.info("Memory threshold exceeded (records: {}, bytes: {}MB), spilling to RocksDB",
                    memoryBuffer.size(), memoryBytes / (1024 * 1024));
            initRocksDBIfNeeded();
            flushMemoryToRocksDB();
        }
    }

    private void initRocksDBIfNeeded() throws Exception {
        if (db != null) {
            return; // Already initialized
        }

        java.nio.file.Path dbPathObj = java.nio.file.Path.of(rocksdbPath);

        // Clear existing database
        if (java.nio.file.Files.exists(dbPathObj)) {
            log.info("Clearing existing RocksDB: {}", rocksdbPath);
            deleteDirectory(dbPathObj.toFile());
        }

        java.nio.file.Files.createDirectories(dbPathObj);

        options = new Options()
                .setCreateIfMissing(true)
                .setCompressionType(org.rocksdb.CompressionType.LZ4_COMPRESSION);

        writeOptions = new WriteOptions()
                .setDisableWAL(true); // Faster writes, we can rebuild if crash

        db = RocksDB.open(options, rocksdbPath);
        useRocksDB = true;
        log.info("Initialized RocksDB at {}", rocksdbPath);
    }

    private void flushMemoryToRocksDB() throws RocksDBException {
        if (memoryBuffer.isEmpty()) {
            return;
        }

        log.info("Flushing {} records to RocksDB", memoryBuffer.size());

        try (WriteBatch batch = new WriteBatch()) {
            for (RecordCdxStructured cdx : memoryBuffer) {
                byte[] key = buildRocksDBKey(cdx);
                byte[] value = serializeCdx(cdx);
                batch.put(key, value);
            }
            db.write(writeOptions, batch);
        }

        Metrics.inc(METRIC_KEY, "rocksdbFlushes");
        memoryBuffer.clear();
        memoryBytes = 0;
    }

    private void sortAndEmitMemory() {
        log.info("Sorting {} records in memory", memoryBuffer.size());

        // Sort by SURT key + timestamp
        memoryBuffer.sort((a, b) -> {
            int cmp = compareSurtKeys(a.surtKey(), b.surtKey());
            if (cmp != 0)
                return cmp;
            return compareTimestamps(a.timestamp(), b.timestamp());
        });

        // Emit sorted records
        for (RecordCdxStructured cdx : memoryBuffer) {
            downstream.onNext(cdx);
            Metrics.inc(METRIC_KEY, "recordsOut");
        }
    }

    private void emitFromRocksDB() {
        log.info("Emitting sorted records from RocksDB");

        try (RocksIterator it = db.newIterator()) {
            it.seekToFirst();
            while (it.isValid()) {
                RecordCdxStructured cdx = deserializeCdx(it.value());
                downstream.onNext(cdx);
                Metrics.inc(METRIC_KEY, "recordsOut");
                it.next();
            }
        }
    }

    /**
     * Build RocksDB key: SURT + "|" + timestamp
     * This ensures lexicographic sorting by SURT, then by timestamp.
     */
    private byte[] buildRocksDBKey(RecordCdxStructured cdx) {
        String key = (cdx.surtKey() != null ? cdx.surtKey() : "")
                + "|"
                + (cdx.timestamp() != null ? cdx.timestamp() : "");
        return key.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Serialize RecordCdxStructured to bytes for RocksDB storage.
     *
     * <p>Uses a {@link ByteArrayOutputStream} that grows on demand so that records
     * with long URLs (encoded query strings, data URIs) are never silently dropped
     * due to a {@code BufferOverflowException} from a fixed-size buffer.
     */
    private byte[] serializeCdx(RecordCdxStructured cdx) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(512);
        DataOutputStream out = new DataOutputStream(baos);
        try {
            writeString(out, cdx.surtKey());
            writeString(out, cdx.timestamp());
            writeString(out, cdx.originalUrl());
            writeString(out, cdx.mimeType());
            out.writeInt(cdx.statusCode());
            writeString(out, cdx.digest());
            out.writeLong(cdx.offset());
            out.writeLong(cdx.length());
            writeString(out, cdx.filename());
            out.writeByte(cdx.isCdxj() ? 1 : 0);

            // Metadata map
            Map<String, String> metadata = cdx.metadata();
            out.writeInt(metadata.size());
            for (Map.Entry<String, String> entry : metadata.entrySet()) {
                writeString(out, entry.getKey());
                writeString(out, entry.getValue());
            }
        } catch (IOException e) {
            // ByteArrayOutputStream never throws IOException — this branch is unreachable.
            throw new RuntimeException("Unexpected serialization failure", e);
        }
        return baos.toByteArray();
    }

    /**
     * Deserialize bytes to RecordCdxStructured.
     */
    private RecordCdxStructured deserializeCdx(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data);

        String surtKey = readString(buf);
        String timestamp = readString(buf);
        String originalUrl = readString(buf);
        String mimeType = readString(buf);
        int statusCode = buf.getInt();
        String digest = readString(buf);
        long offset = buf.getLong();
        long length = buf.getLong();
        String filename = readString(buf);
        boolean isCdxj = buf.get() == 1;

        // Metadata map
        int metadataSize = buf.getInt();
        Map<String, String> metadata = new HashMap<>();
        for (int i = 0; i < metadataSize; i++) {
            String key = readString(buf);
            String value = readString(buf);
            metadata.put(key, value);
        }

        return new RecordCdxStructured(surtKey, timestamp, originalUrl, mimeType,
                statusCode, digest, offset, length, filename, metadata, isCdxj);
    }

    private void writeString(ByteBuffer buf, String s) {
        if (s == null) {
            buf.putInt(-1);
        } else {
            byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
            buf.putInt(bytes.length);
            buf.put(bytes);
        }
    }

    /** DataOutputStream overload used by {@link #serializeCdx}. */
    private void writeString(DataOutputStream out, String s) throws IOException {
        if (s == null) {
            out.writeInt(-1);
        } else {
            byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
            out.writeInt(bytes.length);
            out.write(bytes);
        }
    }

    private String readString(ByteBuffer buf) {
        int len = buf.getInt();
        if (len < 0) {
            return null;
        }
        byte[] bytes = new byte[len];
        buf.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Compare SURT keys lexicographically.
     * Null-safe: nulls sort last.
     */
    private int compareSurtKeys(String a, String b) {
        if (a == null && b == null)
            return 0;
        if (a == null)
            return 1;
        if (b == null)
            return -1;
        return a.compareTo(b);
    }

    /**
     * Compare timestamps chronologically.
     * Null-safe: nulls sort last.
     */
    private int compareTimestamps(String a, String b) {
        if (a == null && b == null)
            return 0;
        if (a == null)
            return 1;
        if (b == null)
            return -1;
        return a.compareTo(b);
    }

    private void cleanup() {
        memoryBuffer.clear();

        if (db != null) {
            try {
                db.close();
                db = null;
            } catch (Exception e) {
                log.error("Error closing RocksDB: {}", e.getMessage());
            }
        }

        if (options != null) {
            options.close();
            options = null;
        }

        if (writeOptions != null) {
            writeOptions.close();
            writeOptions = null;
        }

        // Clean up RocksDB directory
        try {
            java.nio.file.Path dbPath = java.nio.file.Path.of(rocksdbPath);
            if (java.nio.file.Files.exists(dbPath)) {
                deleteDirectory(dbPath.toFile());
            }
        } catch (Exception e) {
            log.warn("Failed to delete RocksDB directory: {}", e.getMessage());
        }
    }

    private void deleteDirectory(java.io.File dir) {
        if (dir.isDirectory()) {
            java.io.File[] children = dir.listFiles();
            if (children != null) {
                for (java.io.File child : children) {
                    deleteDirectory(child);
                }
            }
        }
        dir.delete();
    }
}
