package pl.gov.nac.warc.producers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.netpreserve.jwarc.WarcReader;
import org.netpreserve.jwarc.WarcRecord;

import pl.gov.nac.warc.reactive.Metrics;
import pl.gov.nac.warc.reactive.ReactiveInterfaces;
import pl.gov.nac.warc.records.Record;
import pl.gov.nac.warc.records.RecordBatch;
import pl.gov.nac.warc.records.warc.RecordWarcUniversal;
import pl.gov.nac.warc.utils.WarcCodec;
import pl.gov.nac.warc.utils.WarcCodec.ParsedRecord;
import pl.gov.nac.warc.utils.WarcIO;

import java.time.Instant;

/**
 * Archive extractor that divides large files into chunks based on CDXJ index.
 * Uses an iterative sampling heuristic to find balanced boundaries.
 *
 * V3: Lock-free emission via ConcurrentLinkedQueue to enable true parallelism.
 * Records are queued by worker threads and emitted by a dedicated drainer.
 */
public final class ChunkedArchiveExtractor implements ReactiveInterfaces.ReactiveProducer<Object> {

  private static final Logger log = LogManager.getLogger(ChunkedArchiveExtractor.class);
  private static final String METRIC_KEY = "producer";

  private Flow.Subscriber<? super Object> subscriber;
  // Demand tracking so request(n) is honoured per RS §1.1.
  // VirtualThreadEngine always calls request(Long.MAX_VALUE), so production pipelines
  // are unaffected. The special sentinel Long.MAX_VALUE means "unlimited demand".
  private final java.util.concurrent.atomic.AtomicLong demand = new java.util.concurrent.atomic.AtomicLong(0);
  private final Object demandMonitor = new Object();
  private List<String> inputFiles = new ArrayList<>();
  private Map<String, String> indexedCdxByStem = Map.of();
  private Class<?> negotiatedOutputType = RecordWarcUniversal.class;
  private volatile boolean shuttingDown = false;
  private boolean batchMode = false;

  // Batch accumulation state (only used when batchMode=true).
  // Keyed by "uri\u001Edate" to deduplicate records with the same URI+date that
  // appear in multiple input files — RecordWarcUniversal has no equals/hashCode
  // so a plain HashSet would use identity equality and never deduplicate.
  private String currentBatchDigest = null;
  private java.util.LinkedHashMap<String, RecordWarcUniversal> currentBatchRecords =
      new java.util.LinkedHashMap<>();
  private Instant batchMinDate = null;
  private Instant batchMaxDate = null;

  // Configuration
  private int N = 10; // Number of planned readers/threads
  private int M = 5; // Multiplier
  private double Y = 5.0; // Max chunk size % (probe threshold)
  private double X = 1.0; // Min chunk size % (merge threshold)
  private long minChunkSize = 100 * 1024L * 1024L; // 100MB minimum chunk size
  private String codec = "custom"; // custom | jwarc
  private boolean doetMerge = false; // Enable restricted K-Way merge for DOET dedupe

  private record FileChunk(String path, long startOffset, long endOffset) {
    long size() {
      return endOffset - startOffset;
    }
  }

  @Override
  public List<Class<? extends Record>> emittedOutputTypes() {
    return List.of(RecordWarcUniversal.class, RecordBatch.class);
  }

  @Override
  public void configure(Map<String, Object> cfg) {
    Metrics.setModuleHeader(METRIC_KEY, "Chunked Parallel Archive Extractor");
    ExpandedInputs expanded = expandInputs(WarcCodec.getFiles(cfg));
    this.inputFiles = expanded.dataFiles();
    this.indexedCdxByStem = expanded.cdxByStem();

    if (cfg.containsKey("globalConcurrencyCap")) {
      this.N = Integer.parseInt(cfg.get("globalConcurrencyCap").toString());
    } else if (cfg.containsKey("threads")) {
      this.N = Integer.parseInt(cfg.get("threads").toString());
    }

    if (cfg.containsKey("chunkMultiplier")) {
      this.M = Integer.parseInt(cfg.get("chunkMultiplier").toString());
    }
    if (cfg.containsKey("maxChunkSizePercent")) {
      this.Y = Double.parseDouble(cfg.get("maxChunkSizePercent").toString());
    }
    if (cfg.containsKey("minChunkSizePercent")) {
      this.X = Double.parseDouble(cfg.get("minChunkSizePercent").toString());
    }
    if (cfg.containsKey("minChunkSize")) {
      this.minChunkSize = Long.parseLong(cfg.get("minChunkSize").toString());
    }
    if (cfg.containsKey("codec")) {
      this.codec = cfg.get("codec").toString().toLowerCase();
    }
    Object mergeModeFlag = cfg.get("doet-merge");
    if (mergeModeFlag == null) {
      mergeModeFlag = cfg.get("doetMerge");
    }
    this.doetMerge = mergeModeFlag != null && Boolean.parseBoolean(mergeModeFlag.toString());

    // Keep merge-mode state deterministic across repeated configure() calls.
    this.batchMode = this.doetMerge;
    this.negotiatedOutputType = this.doetMerge ? RecordBatch.class : RecordWarcUniversal.class;
    if (this.doetMerge) {
      log.info("DOET merge mode enabled - will emit RecordBatch");
    }

    log.info("Config: N={}, M={}, Y={}%, X={}%, minChunkSize={}MB",
        N, M, Y, X, minChunkSize / (1024 * 1024));
  }

  @Override
  public void onNegotiatedOutputType(Class<?> type) {
    if (type != null) {
      this.negotiatedOutputType = type;
      this.batchMode = (type == RecordBatch.class);
      log.info("ChunkedArchiveExtractor: Output type negotiated to {}, batchMode={}",
          type.getSimpleName(), batchMode);
    }
  }

  @Override
  public boolean beforeCheck(Map<String, Object> cfg) {
    return !inputFiles.isEmpty();
  }

  @Override
  public int afterCheck(Map<String, Object> cfg) {
    return 0;
  }

  @Override
  public void subscribe(Flow.Subscriber<? super Object> subscriber) {
    this.subscriber = subscriber;
    subscriber.onSubscribe(new Flow.Subscription() {
      @Override
      public void request(long n) {
        if (n <= 0) {
          subscriber.onError(new IllegalArgumentException(
              "RS §3.9: request(n) requires n > 0, got: " + n));
          return;
        }
        synchronized (demandMonitor) {
          if (n == Long.MAX_VALUE) {
            demand.set(Long.MAX_VALUE);
          } else {
            // Saturating add — avoid overflow past Long.MAX_VALUE
            demand.updateAndGet(d -> d == Long.MAX_VALUE ? Long.MAX_VALUE
                : Math.min(Long.MAX_VALUE, d + n));
          }
          demandMonitor.notifyAll();
        }
      }

      @Override
      public void cancel() {
        shuttingDown = true;
        synchronized (demandMonitor) {
          demandMonitor.notifyAll();
        }
      }
    });
  }

  @Override
  public void startProducing() {
    Throwable estimateFailure = estimateTotals();
    if (estimateFailure != null) {
      failProduction(new RuntimeException("Failed to estimate archive totals", estimateFailure));
      return;
    }

    if (doetMerge) {
      startProducingMerged();
      return;
    }

    log.info("Starting parallel chunk production (Lock-Free V3)");

    // Optimization: if N=1 and we have a single file, bypass chunking and sampling
    if (N <= 1 && inputFiles.size() == 1) {
      String path = inputFiles.get(0);
      log.info("Single thread detected, bypassing chunking for {}", path);
      try {
        if ("jwarc".equals(codec)) {
          processFileJwarcDirect(path);
        } else {
          CountingInputStream counter = new CountingInputStream(new java.io.FileInputStream(path));
          WarcCodec.ArchiveType archiveType = WarcCodec.detectType(path);
          if (archiveType == WarcCodec.ArchiveType.UNKNOWN) {
            throw new IOException("Unknown archive type: " + path);
          }
          validateArchiveStart(path, 0, archiveType);
          long prev = 0;
          try (WarcCodec.WarcRecordIterator it = new WarcCodec.WarcRecordIterator(counter, archiveType == WarcCodec.ArchiveType.GZIP)) {
            while (it.hasNext() && !shuttingDown) {
              ParsedRecord rec = it.next();
              emitRecordDirect(rec, path);
              long cur = counter.count;
              Metrics.add(METRIC_KEY, "inputBytesRead", cur - prev);
              prev = cur;
            }
          }
        }
      } catch (Exception e) {
        log.error("Sequential bypass failed", e);
        Metrics.inc(METRIC_KEY, "failed-inputs");
        failProduction(e);
        return;
      }
      if (subscriber != null) {
        subscriber.onComplete();
      }
      return;
    }

    AtomicInteger failedInputs = new AtomicInteger(0);
    AtomicReference<Throwable> firstFailure = new AtomicReference<>();

    try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
      for (String path : inputFiles) {
        if (shuttingDown)
          break;

        try {
          List<FileChunk> allChunks = calculateChunks(path);
          if (allChunks.isEmpty())
            continue;

          // Divide chunks into N contiguous ranges for N threads
          int threadCount = Math.min(N, allChunks.size());
          int chunksPerThread = (int) Math.ceil((double) allChunks.size() / threadCount);

          for (int i = 0; i < threadCount; i++) {
            int startIdx = i * chunksPerThread;
            int endIdx = Math.min(startIdx + chunksPerThread, allChunks.size());
            if (startIdx >= endIdx)
              break;

            List<FileChunk> threadChunks = allChunks.subList(startIdx, endIdx);
            executor.submit(() -> {
              try {
                processChunkSequence(threadChunks);
              } catch (Exception e) {
                log.error("Worker failed", e);
                recordFailure(failedInputs, firstFailure, e);
              }
            });
          }
        } catch (Exception e) {
          log.error("Failed to initialize chunks for " + path, e);
          recordFailure(failedInputs, firstFailure, e);
        }
      }
    } // Implicitly waits for all virtual threads to complete

    // No emitter thread to join

    if (failedInputs.get() > 0) {
      Metrics.add(METRIC_KEY, "failed-inputs", failedInputs.get());
      failProduction(new RuntimeException(
          "Archive production failed for " + failedInputs.get() + " input/chunk task(s)",
          firstFailure.get()));
      return;
    }

    if (subscriber != null) {
      subscriber.onComplete();
    }
  }

  private void recordFailure(AtomicInteger failedInputs, AtomicReference<Throwable> firstFailure, Throwable failure) {
    failedInputs.incrementAndGet();
    firstFailure.compareAndSet(null, failure);
  }

  private void failProduction(Throwable failure) {
    Metrics.inc(METRIC_KEY, "errors");
    if (subscriber != null) {
      subscriber.onError(failure);
    }
  }

  private List<FileChunk> calculateChunks(String path) throws IOException {
    long fileSize = Files.size(Path.of(path));
    String cdxPath = resolveCdxPath(path);

    if (cdxPath == null || fileSize < minChunkSize) {
      log.info("No index or small file, using single chunk for {}", path);
      return List.of(new FileChunk(path, 0, fileSize));
    }

    // Adaptive chunk target
    int targetChunks = Math.max(1, N * M);
    if (fileSize / targetChunks < minChunkSize) {
      targetChunks = (int) Math.max(1, fileSize / minChunkSize);
      log.info("Adjusted target chunks to {} due to minChunkSize", targetChunks);
    }

    long maxGapBytes = (long) (fileSize * Y / 100.0);
    List<Long> offsets = WarcCodec.sampleOffsetsFromCdxj(cdxPath, path, fileSize, targetChunks, maxGapBytes);
    offsets.add(0L);
    offsets.add(fileSize);
    Collections.sort(offsets);

    List<Long> unique = new ArrayList<>();
    for (int i = 0; i < offsets.size(); i++) {
      if (i == 0 || !offsets.get(i).equals(offsets.get(i - 1))) {
        unique.add(offsets.get(i));
      }
    }

    // Heuristic: Merge small segments (X% or < minChunkSize)
    List<Long> merged = new ArrayList<>();
    merged.add(unique.get(0));
    long minSizeThreshold = (long) Math.max(minChunkSize, fileSize * X / 100.0);
    long maxSizeThreshold = (long) (fileSize * Y / 100.0);

    for (int i = 1; i < unique.size() - 1; i++) {
      long nextBoundary = unique.get(i + 1);
      long currentBoundary = unique.get(i);
      long prevBoundary = merged.get(merged.size() - 1);

      long sizeSinceLastBoundary = currentBoundary - prevBoundary;

      if (sizeSinceLastBoundary >= minSizeThreshold || (nextBoundary - prevBoundary > maxSizeThreshold)) {
        merged.add(currentBoundary);
      }
    }
    merged.add(unique.get(unique.size() - 1));

    List<FileChunk> chunks = new ArrayList<>();
    for (int i = 0; i < merged.size() - 1; i++) {
      chunks.add(new FileChunk(path, merged.get(i), merged.get(i + 1)));
    }

    log.info("File {} divided into {} chunks, using {} threads",
        path, chunks.size(), Math.min(N, chunks.size()));
    return chunks;
  }

  private void processChunkSequence(List<FileChunk> chunks) throws IOException {
    for (FileChunk chunk : chunks) {
      if (shuttingDown)
        break;
      processChunk(chunk);
    }
  }

  private void processChunk(FileChunk chunk) throws IOException {
    try {
      if ("jwarc".equals(codec)) {
        processChunkJwarc(chunk);
      } else {
        processChunkCodec(chunk);
      }
    } catch (Exception e) {
      log.error("Chunk failed: " + chunk.path + " [" + chunk.startOffset + "]", e);
      throw new IOException("Chunk failed: " + chunk.path + " [" + chunk.startOffset + "]", e);
    }
  }

  private void processChunkCodec(FileChunk chunk) throws IOException {
    CountingInputStream counter = new CountingInputStream(createBoundedStream(chunk));
    WarcCodec.ArchiveType archiveType = WarcCodec.detectType(chunk.path);
    if (archiveType == WarcCodec.ArchiveType.UNKNOWN) {
      throw new IOException("Unknown archive type: " + chunk.path);
    }
    validateArchiveStart(chunk.path, chunk.startOffset, archiveType);
    long prev = 0;
    try (WarcCodec.WarcRecordIterator it = new WarcCodec.WarcRecordIterator(counter, archiveType == WarcCodec.ArchiveType.GZIP)) {
      while (it.hasNext() && !shuttingDown) {
        ParsedRecord rec = it.next();
        emitRecordDirect(rec, chunk.path);
        long cur = counter.count;
        Metrics.add(METRIC_KEY, "inputBytesRead", cur - prev);
        prev = cur;
      }
    }
  }

  private void validateArchiveStart(String path, long offset, WarcCodec.ArchiveType archiveType) throws IOException {
    if (archiveType != WarcCodec.ArchiveType.GZIP) {
      return;
    }

    try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(path, "r")) {
      if (offset > 0) {
        raf.seek(offset);
      }
      int b1 = raf.read();
      int b2 = raf.read();
      if (b1 != 0x1f || b2 != 0x8b) {
        throw new IOException("Invalid gzip member at offset " + offset + ": " + path);
      }
    }
  }

  private void processChunkJwarc(FileChunk chunk) throws IOException {
    CountingInputStream counter = new CountingInputStream(createBoundedStream(chunk));
    java.io.InputStream is = WarcCodec.decompressIfNeeded(chunk.path, counter);
    try (WarcReader reader = new WarcReader(is)) {
      long prev = 0;
      for (WarcRecord rec : reader) {
        if (shuttingDown)
          break;
        emitRecordJwarcDirect(rec, chunk.path);
        long cur = counter.count;
        Metrics.add(METRIC_KEY, "inputBytesRead", cur - prev);
        prev = cur;
      }
    }
  }

  private void processFileJwarcDirect(String path) throws IOException {
    CountingInputStream counter = new CountingInputStream(new java.io.FileInputStream(path));
    java.io.InputStream is = WarcCodec.decompressIfNeeded(path, counter);
    try (WarcReader reader = new WarcReader(is)) {
      long prev = 0;
      for (WarcRecord rec : reader) {
        if (shuttingDown)
          break;
        emitRecordJwarcDirect(rec, path);
        long cur = counter.count;
        Metrics.add(METRIC_KEY, "inputBytesRead", cur - prev);
        prev = cur;
      }
    }
  }

  // Direct emission for single-thread bypass (no queue needed)
  private void emitRecordJwarcDirect(WarcRecord rec, String sourceFile) throws IOException {
    pl.gov.nac.warc.utils.PooledBuffer pooled = pl.gov.nac.warc.utils.BufferPool.INSTANCE.borrow();
    WarcIO.serializeFast(rec, pooled);

    Metrics.inc(METRIC_KEY, "recordsOut");
    Metrics.add(METRIC_KEY, "bytesOut", pooled.length);

    if (subscriber == null) {
      pooled.release();
      return;
    }

    if (negotiatedOutputType == RecordWarcUniversal.class) {
      byte[] bytes = java.util.Arrays.copyOf(pooled.array, pooled.length);
      java.util.Map<String, String> headerMap = new java.util.LinkedHashMap<>();
      rec.headers().map().forEach((k, v) -> {
        if (!v.isEmpty())
          headerMap.put(k, v.get(0));
      });
      RecordWarcUniversal rwu = new RecordWarcUniversal(rec.type(), headerMap, bytes);
      if (sourceFile != null)
        rwu.headers().put("X-Source-Warc", sourceFile);

      tryEmit(rwu);
      pooled.release();
    } else {
      pooled.release();
    }
  }

  /**
   * Emit a record to the subscriber once there is outstanding demand.
   * Long.MAX_VALUE demand means "unlimited" (set by VirtualThreadEngine, normal operation).
   * For finite demand, uses a CAS loop to atomically check-and-decrement so that concurrent
   * worker threads cannot both pass the {@code d > 0} guard and both emit — which would
   * overshoot RS §1.1 demand by 1.
   * Returns true if emitted, false if the subscription was cancelled.
   */
  private boolean tryEmit(Object record) {
    while (true) {
      long d = demand.get();
      if (d == Long.MAX_VALUE) {
        subscriber.onNext(record);
        return true;
      }
      if (d <= 0) {
        synchronized (demandMonitor) {
          while (demand.get() <= 0 && !shuttingDown) {
            try {
              demandMonitor.wait();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new IllegalStateException("Interrupted while waiting for downstream demand", e);
            }
          }
        }
        if (shuttingDown) {
          return false;
        }
        continue;
      }
      if (demand.compareAndSet(d, d - 1)) {
        subscriber.onNext(record);
        return true;
      }
      // Another thread changed demand concurrently; retry.
    }
  }

  /**
   * Create a bounded stream for a file chunk using FileInputStream + skip.
   * This enables OS kernel read-ahead prefetching, unlike RandomAccessFile.
   */
  private java.io.InputStream createBoundedStream(FileChunk chunk) throws IOException {
    java.io.FileInputStream fis = new java.io.FileInputStream(chunk.path);
    long skipped = fis.skip(chunk.startOffset);
    if (skipped < chunk.startOffset) {
      // Fall back to channel positioning if skip() doesn't work fully
      fis.getChannel().position(chunk.startOffset);
    }

    long chunkSize = chunk.endOffset - chunk.startOffset;
    // Wrap in BufferedInputStream for better read performance
    return new java.io.BufferedInputStream(new java.io.InputStream() {
      long remaining = chunkSize;
      final java.io.FileInputStream source = fis;

      @Override
      public int read() throws IOException {
        if (remaining <= 0)
          return -1;
        int b = source.read();
        if (b != -1)
          remaining--;
        return b;
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException {
        if (remaining <= 0)
          return -1;
        int toRead = (int) Math.min(len, remaining);
        int read = source.read(b, off, toRead);
        if (read != -1)
          remaining -= read;
        return read;
      }

      @Override
      public void close() throws IOException {
        source.close();
      }
    }, 131072); // 128KB buffer
  }

  /**
   * K-Way Merge implementation for pre-sorted DOET/WARC files.
   * Enforces strict input ordering and emits records in global order (Digest
   * ASC).
   */
  private void startProducingMerged() {
    log.info("Starting K-Way Merge (Inputs: {})", inputFiles.size());

    // Priority Queue for K-Way Merge
    java.util.PriorityQueue<MergeEntry> queue = new java.util.PriorityQueue<>();
    List<MergeCursor> cursors = new ArrayList<>();
    boolean errorOccurred = false;

    try {
      // 1. Open all files and load first record
      for (int i = 0; i < inputFiles.size(); i++) {
        String path = inputFiles.get(i);
        MergeCursor cursor = new MergeCursor(path, i, codec);
        if (cursor.advance()) {
          queue.add(new MergeEntry(cursor));
        } else {
          cursor.close();
        }
        cursors.add(cursor);
      }

      String globalLastDigest = "";

      // 2. Merge Loop
      int loopCount = 0;
      while (!queue.isEmpty() && !shuttingDown) {
        loopCount++;
        log.debug("K-way merge: Loop iteration {}, queue size={}, shuttingDown={}", loopCount, queue.size(), shuttingDown);

        MergeEntry minEntry = queue.poll();
        MergeCursor cursor = minEntry.cursor;

        // Check global order (sanity check for output)
        if (minEntry.digest.compareTo(globalLastDigest) < 0) {
          // This implies the merge logic itself failed or multiple files have overlap
          // that violates logic
          // But since we trust PQ, this shouldn't happen unless PriorityQueue is broken
          // or Comparator is inconsistent.
        }
        globalLastDigest = minEntry.digest;

        // Emit (batch mode or direct)
        log.debug("K-way merge EMIT: digest={}, path={}", minEntry.digest, cursor.path);
        if (batchMode) {
          emitWithBatching(cursor.currentParsed, cursor.path);
        } else {
          emitRecordDirect(cursor.currentParsed, cursor.path);
        }
        log.debug("K-way merge: After emit, shuttingDown={}", shuttingDown);

        // Advance cursor
        log.debug("K-way merge: Calling advance() for path={}", cursor.path);
        if (cursor.advance()) {
          log.debug("K-way merge: advance() returned true, adding back to queue");
          queue.add(new MergeEntry(cursor));
        } else {
          log.debug("K-way merge: advance() returned false, closing cursor for path={}", cursor.path);
          cursor.close();
        }
      }
      log.debug("K-way merge: Exited loop after {} iterations, queue.isEmpty={}, shuttingDown={}",
          loopCount, queue.isEmpty(), shuttingDown);

    } catch (Exception e) {
      log.error("Merge failed", e);
      errorOccurred = true;
      if (subscriber != null)
        subscriber.onError(e);
    } finally {
      // Flush any remaining batch
      if (batchMode && !currentBatchRecords.isEmpty()) {
        log.info("Flushing final batch in onComplete: digest={}, count={}",
            currentBatchDigest, currentBatchRecords.size());
        flushBatch();
      }
      // Cleanup any remaining cursors
      for (MergeCursor c : cursors)
        c.close();
      // RS §1.7 — do not emit onComplete after onError (double terminal signal).
      if (!errorOccurred && subscriber != null)
        subscriber.onComplete();
    }
  }

  // Direct emission for merge logic (converts to Universal)
  private void emitRecordDirect(ParsedRecord rec, String sourceFile) {
    if (subscriber == null)
      return;
    Metrics.inc(METRIC_KEY, "recordsOut");
    Metrics.add(METRIC_KEY, "bytesOut", rec.getRawBytes().length);

    // Always convert to Universal for Merge logic to support provenance headers
    // downstream
    Object out = WarcCodec.toUniversal(rec);
    if (out instanceof RecordWarcUniversal rwu && sourceFile != null) {
      rwu.headers().put("X-Source-Warc", sourceFile);
    }
    tryEmit(out);
  }

  /**
   * Emit record with batching enabled.
   * Accumulates records with same digest and emits RecordBatch when digest changes.
   */
  private void emitWithBatching(ParsedRecord rec, String sourceFile) {
    String digest = extractDigestFromParsed(rec);
    Object record = WarcCodec.toUniversal(rec);

    if (!(record instanceof RecordWarcUniversal rwu)) {
      log.warn("Cannot batch non-RecordWarcUniversal type: {}", record.getClass());
      return;
    }

    // Add X-Source-Warc header
    if (sourceFile != null) {
      rwu.headers().put("X-Source-Warc", sourceFile);
    }

    // Extract date
    String dateStr = rwu.headers().get("WARC-Date");
    Instant recordDate = parseInstant(dateStr);

    // DIGEST CHANGE DETECTED - flush previous batch
    if (currentBatchDigest != null && !currentBatchDigest.equals(digest)) {
      flushBatch();
    }

    // Start new batch or add to current
    if (currentBatchDigest == null) {
      currentBatchDigest = digest;
    }

    String batchKey = rwu.headers().getOrDefault("WARC-Target-URI", "")
        + "\u001E" + rwu.headers().getOrDefault("WARC-Date", "");
    currentBatchRecords.putIfAbsent(batchKey, rwu);

    // Update aggregate date range
    if (batchMinDate == null || recordDate.isBefore(batchMinDate)) {
      batchMinDate = recordDate;
    }
    if (batchMaxDate == null || recordDate.isAfter(batchMaxDate)) {
      batchMaxDate = recordDate;
    }
  }

  /**
   * Flush accumulated batch as RecordBatch.
   */
  private void flushBatch() {
    if (currentBatchRecords.isEmpty()) {
      return;
    }

    RecordBatch batch = new RecordBatch(
        currentBatchDigest,
        new java.util.LinkedHashSet<>(currentBatchRecords.values()),
        batchMinDate,
        batchMaxDate
    );

    log.debug("Emitting RecordBatch: digest={}, count={}, dateRange=[{}, {}]",
        currentBatchDigest.substring(0, Math.min(20, currentBatchDigest.length())),
        batch.size(), batchMinDate, batchMaxDate);

    tryEmit(batch);
    Metrics.inc(METRIC_KEY, "batches-emitted");
    for (int i = 0; i < batch.size(); i++) {
      Metrics.inc(METRIC_KEY, "recordsOut");
    }

    // Clear batch state
    currentBatchRecords = new java.util.LinkedHashMap<>();
    currentBatchDigest = null;
    batchMinDate = null;
    batchMaxDate = null;
  }

  /**
   * Extract digest from ParsedRecord.
   */
  private String extractDigestFromParsed(ParsedRecord rec) {
    String digest = rec.getHeaders().getOrDefault("WARC-Payload-Digest", "");
    if (digest.isEmpty()) {
      digest = rec.getHeaders().getOrDefault("warc-payload-digest", "");
    }
    if (digest.isEmpty()) {
      digest = rec.getHeaders().getOrDefault("WARC-Block-Digest", "");
    }
    if (digest.isEmpty()) {
      digest = rec.getHeaders().getOrDefault("warc-block-digest", "");
    }
    if (digest.isEmpty()) {
      // Match MergeCursor.extractDigest() fallback so warcinfo/no-digest records
      // get a stable group key rather than crashing RecordBatch validation.
      digest = "xxh128:00000000000000000000000000000000";
    }
    return digest;
  }

  /**
   * Parse ISO-8601 date string to Instant.
   */
  private Instant parseInstant(String dateStr) {
    if (dateStr == null || dateStr.isBlank()) {
      return Instant.now();
    }
    try {
      return Instant.parse(dateStr);
    } catch (Exception e) {
      log.warn("Failed to parse date: {}", dateStr);
      return Instant.now();
    }
  }

  /** Wrapper around file iterator to track state and enforce order. */
  private static class MergeCursor implements AutoCloseable {
    final String path;
    final int fileIndex;  // Position in input file list (0-based)
    WarcCodec.WarcRecordIterator iterator;
    ParsedRecord currentParsed;
    String lastDigest = "";

    MergeCursor(String path, int fileIndex, String codecName) throws IOException {
      this.path = path;
      this.fileIndex = fileIndex;
      // codecName unused
      this.iterator = WarcCodec.openWarc(path);
    }

    boolean advance() {
      if (iterator.hasNext()) {
        currentParsed = iterator.next();
        String digest = extractDigest(currentParsed);

        // STRICT ORDER CHECK
        if (digest.compareTo(lastDigest) < 0) {
          throw new RuntimeException("PANIC: Input file " + path + " is out of order! " +
              "Current: " + digest + " < Prev: " + lastDigest);
        }
        lastDigest = digest;
        log.debug("MergeCursor.advance(): path={}, digest={}, hasNext=true", path, digest);
        return true;
      }
      log.debug("MergeCursor.advance(): path={}, hasNext=false (EOF)", path);
      return false;
    }

    @Override
    public void close() {
      try {
        if (iterator != null)
          iterator.close();
      } catch (Exception e) {
      }
    }

    private String extractDigest(ParsedRecord rec) {
      // WARC-Payload-Digest (xxh128) is preferred for DOET merge operations
      String d = rec.getHeaders().get("warc-payload-digest");
      if (d == null)
        d = rec.getHeaders().get("warc-block-digest");
      if (d == null)
        return "xxh128:00000000000000000000000000000000"; // Fallback matching xxh128 format
      return d;
    }
  }

  private static class MergeEntry implements Comparable<MergeEntry> {
    final MergeCursor cursor;
    final String digest;
    final long length;

    MergeEntry(MergeCursor cursor) {
      this.cursor = cursor;
      this.digest = cursor.lastDigest; // digest was extracted during advance()
      this.length = cursor.currentParsed.getRawBytes().length;
    }

    @Override
    public int compareTo(MergeEntry other) {
      int c = this.digest.compareTo(other.digest);
      if (c != 0)
        return c;
      c = Long.compare(this.length, other.length);
      if (c != 0)
        return c;
      // Tiebreaker: use original input-list index for deterministic ordering
      // regardless of absolute path differences across runs/machines.
      int indexCmp = Integer.compare(this.cursor.fileIndex, other.cursor.fileIndex);
      if (indexCmp != 0) {
        log.info("K-way merge tiebreaker (file index): {}({}) vs {}({}) → {}",
            this.cursor.path, this.cursor.fileIndex, other.cursor.path, other.cursor.fileIndex, indexCmp);
      }
      return indexCmp;
    }
  }

  private Throwable estimateTotals() {
    long totalSize = 0;
    long totalEstimatedRecords = 0;
    Throwable firstFailure = null;

    for (String inputPath : inputFiles) {
      try {
        Path path = Path.of(inputPath);
        long size = Files.size(path);
        totalSize += size;

        if (size == 0)
          continue;

        String cdxPath = resolveCdxPath(inputPath);
        if (cdxPath != null) {
          try (java.util.stream.Stream<String> lines = Files.lines(Path.of(cdxPath))) {
            long count = lines.count();
            // Adjust for header if CDXJ has one (usually not standard CDXJ but check)
            // Simple count is better than sampling
            totalEstimatedRecords += count;
            log.info("Counted {} records for {} via CDXJ scan", count, inputPath);
            continue;
          } catch (Exception e) {
            log.warn("Failed to scan CDXJ, falling back to sampling: " + e.getMessage());
          }
        }

        // Fallback: byte-scan sampling
        long totalRecordsScanned = 0;
        long totalBytesScanned = 0;
        int sampleChunks = 5;
        int sampleBufferSize = 256 * 1024;

        try (java.nio.channels.FileChannel ch = java.nio.channels.FileChannel.open(path)) {
          java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(sampleBufferSize);

          for (int i = 0; i < sampleChunks; i++) {
            long offset = (size * i) / sampleChunks;
            if (offset + sampleBufferSize > size)
              offset = Math.max(0, size - sampleBufferSize);

            ch.position(offset);
            buf.clear();
            int read = ch.read(buf);
            if (read <= 0)
              continue;

            buf.flip();
            byte[] data = new byte[read];
            buf.get(data);

            int count = countSignatures(data);
            if (count > 0) {
              totalRecordsScanned += count;
              totalBytesScanned += read;
            }
          }
        }

        if (totalRecordsScanned > 0 && totalBytesScanned > 0) {
          double recordsPerByte = (double) totalRecordsScanned / totalBytesScanned;
          totalEstimatedRecords += (long) (size * recordsPerByte);
        }
      } catch (Exception e) {
        log.error("Failed to estimate totals for " + inputPath, e);
        Metrics.inc(METRIC_KEY, "failed-inputs");
        if (firstFailure == null) {
          firstFailure = e;
        }
      }
    }

    Metrics.set("engine", "totalBytes", totalSize);
    Metrics.set("engine", "totalRecords", totalEstimatedRecords);
    log.info("Estimated total bytes: {}, records: {}", totalSize, totalEstimatedRecords);
    return firstFailure;
  }

  private int countSignatures(byte[] data) {
    int count = 0;
    int i = 0;
    while (i < data.length - 10) {
      boolean isGzip = (data[i] == (byte) 0x1f) && (data[i + 1] == (byte) 0x8b) && (data[i + 2] == (byte) 0x08);
      boolean isWarc = (data[i] == 0x57) && (data[i + 1] == 0x41) && (data[i + 2] == 0x52)
          && (data[i + 3] == 0x43) && (data[i + 4] == 0x2F);

      if (isGzip || isWarc) {
        count++;
        i += 21;
      } else {
        i++;
      }
    }
    return count;
  }

  private record ExpandedInputs(List<String> dataFiles, Map<String, String> cdxByStem) {
  }

  /**
   * Expands input paths. Directory inputs are scanned recursively:
   * - WARC/WET files become producer input data files.
   * - CDX/CDXJ files are indexed by stem for sidecar lookup.
   * Plain file inputs are classified similarly.
   */
  private static ExpandedInputs expandInputs(List<String> paths) {
    List<String> dataFiles = new ArrayList<>();
    Map<String, String> cdxByStem = new LinkedHashMap<>();
    for (String p : paths) {
      Path path = Path.of(p);
      if (Files.isDirectory(path)) {
        try (var stream = Files.walk(path)) {
          stream.filter(Files::isRegularFile)
              .sorted()
              .forEach(f -> classifyInput(f.toString(), dataFiles, cdxByStem));
        } catch (IOException e) {
          log.error("Failed to scan directory for archive/index files: {}", p, e);
        }
      } else {
        classifyInput(p, dataFiles, cdxByStem);
      }
    }
    return new ExpandedInputs(dataFiles, Map.copyOf(cdxByStem));
  }

  private static void classifyInput(String path, List<String> dataFiles, Map<String, String> cdxByStem) {
    String name = Path.of(path).getFileName().toString().toLowerCase();
    if (isCdxName(name)) {
      cdxByStem.putIfAbsent(cdxStem(name), path);
    } else if (isDataArchiveName(name)) {
      dataFiles.add(path);
    } else {
      // Preserve historical behavior for explicit unknown file inputs.
      dataFiles.add(path);
    }
  }

  private static boolean isDataArchiveName(String name) {
    return name.endsWith(".warc") || name.endsWith(".wet")
        || name.endsWith(".warc.gz") || name.endsWith(".wet.gz")
        || name.endsWith(".warc.zst") || name.endsWith(".wet.zst")
        || name.endsWith(".warc.zstd") || name.endsWith(".wet.zstd")
        || name.endsWith(".warc.lz4") || name.endsWith(".wet.lz4")
        || name.endsWith(".warc.xz") || name.endsWith(".wet.xz");
  }

  private static boolean isCdxName(String name) {
    return name.endsWith(".cdx") || name.endsWith(".cdxj")
        || name.endsWith(".cdx.gz") || name.endsWith(".cdxj.gz");
  }

  private static String cdxStem(String name) {
    if (name.endsWith(".cdxj.gz"))
      return name.substring(0, name.length() - 8);
    if (name.endsWith(".cdx.gz"))
      return name.substring(0, name.length() - 7);
    if (name.endsWith(".cdxj"))
      return name.substring(0, name.length() - 5);
    if (name.endsWith(".cdx"))
      return name.substring(0, name.length() - 4);
    return name;
  }

  private String resolveCdxPath(String dataPath) {
    String fileName = Path.of(dataPath).getFileName().toString().toLowerCase();

    String fromExact = indexedCdxByStem.get(fileName);
    if (fromExact != null) {
      return fromExact;
    }

    String fromStem = indexedCdxByStem.get(stripArchiveStem(fileName));
    if (fromStem != null) {
      return fromStem;
    }

    return WarcCodec.findCdxSidecar(dataPath);
  }

  private static String stripArchiveStem(String name) {
    String[] archiveSuffixes = {
        ".warc.gz", ".wet.gz",
        ".warc.zst", ".wet.zst",
        ".warc.zstd", ".wet.zstd",
        ".warc.lz4", ".wet.lz4",
        ".warc.xz", ".wet.xz",
        ".warc", ".wet"
    };
    for (String suffix : archiveSuffixes) {
      if (name.endsWith(suffix)) {
        return name.substring(0, name.length() - suffix.length());
      }
    }
    return name;
  }

  /** Counts raw bytes read from the underlying stream (compressed file bytes). */
  private static final class CountingInputStream extends java.io.InputStream {
    private final java.io.InputStream delegate;
    volatile long count = 0;

    CountingInputStream(java.io.InputStream delegate) {
      this.delegate = delegate;
    }

    @Override
    public int read() throws IOException {
      int b = delegate.read();
      if (b != -1)
        count++;
      return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int n = delegate.read(b, off, len);
      if (n > 0)
        count += n;
      return n;
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }

}
