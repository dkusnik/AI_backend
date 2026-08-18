package pl.gov.nac.warc.consumers;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// removed FramedLZ4CompressorOutputStream
import com.github.luben.zstd.ZstdOutputStream;
import com.fasterxml.jackson.databind.ObjectMapper;

import pl.gov.nac.warc.reactive.Metrics;
import pl.gov.nac.warc.reactive.ReactiveInterfaces;
import pl.gov.nac.warc.records.Record;
import pl.gov.nac.warc.records.file.RecordFile;
import pl.gov.nac.warc.records.warc.RecordCompressed;
import pl.gov.nac.warc.records.warc.RecordWarcUniversal;
import pl.gov.nac.warc.utils.PooledBuffer;
import pl.gov.nac.warc.utils.WarcIO;

/**
 * Abstract base class for WARC consumers providing common functionality:
 * - Output format routing (WARC, WARC+CDX, CDX-only)
 * - Compression toggle
 * - Segment rotation with size limits
 * - CDX sidecar generation (CDXJ format for pywb)
 * - pywb filtering (response records only)
 * - Date ordering validation
 * - Copy mode with tagged tokens
 */
public abstract class ConsumerWarcBase implements ReactiveInterfaces.ReactiveConsumer<Object> {

  private static final Logger log = LogManager.getLogger(ConsumerWarcBase.class);
  private static final String METRIC_KEY = "consumer-warc-base";
  private static final String METRIC_ERRORS = "errors";
  private static final String WARC_DATE = "WARC-Date";
  private static final String WARC_EXT_PATTERN = "\\.w(arc|et)(\\.gz)?$";
  private static final String PUBLICATION_SCHEMA = "warc2es.output-publication/v1";
  private static final ObjectMapper JSON = new ObjectMapper();

  // Output format enum
  public enum OutputFormat {
    WARC, WARC_CDX, CDX_ONLY, ONE_WARC, MULTI_WARC
  }

  // Processing mode enum
  public enum ProcessingMode {
    MERGE, COPY
  }

  // Check order enum
  public enum CheckOrder {
    OFF, // No checking
    CONTEXT // Context-aware: order by digest, then URL, then date
  }

  // Compression type enum
  public enum CompressionType {
    NONE, GZIP, ZSTD, LZ4, XZ
  }

  // Configuration fields
  private static final String DOT_WARC = ".warc";
  private static final String RECORDS_OUT = "recordsOut";

  protected String outputFile;
  protected OutputFormat outputFormat = OutputFormat.WARC_CDX;
  protected ProcessingMode mode = ProcessingMode.MERGE;
  protected CheckOrder checkOrder = CheckOrder.CONTEXT;
  protected CompressionType compressionType = CompressionType.GZIP;
  protected boolean pywbOnly = false;
  protected long sizeLimit = 0;
  protected boolean skipIfNoChanges = false;
  protected boolean anyChanges = false;
  protected String derivativeType = null; // NAC-WARC-derivative value (wet, doet, row, etc.)
  protected String outputNameTemplate = null; // e.g. "{source}.processed.warc.gz"
  protected boolean splitProvenance = false;
  protected boolean parallelGzip = false;
  protected int compressionLevel = 6;
  protected String diffOutputFile = null; // NEW
  protected String recordOrder = null; // NAC-record-order value (e.g., "surt-ascending")
  protected boolean force = false;

  private Path publicationReport = null;
  private final List<PendingOutput> pendingOutputs = new ArrayList<>();
  private final Set<Path> pendingTargets = new HashSet<>();
  private final Map<Path, OutputArtifactStats> outputStats = new LinkedHashMap<>();
  private Path currentOutputTarget = null;
  private boolean skipPublication = false;
  private boolean publicationFinalized = false;

  // State tracking
  protected String lastSourceFile = null;
  protected final AtomicInteger segmentIndex = new AtomicInteger(0);
  protected final AtomicReference<CountingOutputStream> countStream = new AtomicReference<>();
  protected final AtomicReference<OutputStream> currentStream = new AtomicReference<>();
  protected final AtomicReference<PrintWriter> cdxWriter = new AtomicReference<>();
  protected volatile boolean warcinfoWrittenMain = false;

  // Split stream state (for merged/secondary records)
  protected final AtomicReference<CountingOutputStream> splitCountStream = new AtomicReference<>();
  protected final AtomicReference<OutputStream> splitStream = new AtomicReference<>();
  protected final AtomicReference<PrintWriter> splitCdxWriter = new AtomicReference<>();

  protected final List<String> generatedFiles = Collections.synchronizedList(new ArrayList<>());
  protected final List<String> generatedCdxFiles = Collections.synchronizedList(new ArrayList<>());

  private record PendingOutput(Path temporary, Path target) {
  }

  private static final class OutputArtifactStats {
    private long count;
    private long contentBytes;
    private final Map<String, Long> mimeTypes = new TreeMap<>();
    private final Map<String, Long> languages = new TreeMap<>();
    private long missingLanguage;
    private long missingMimetype;
    private Instant dateMin;
    private Instant dateMax;

    private void record(RecordWarcUniversal record) {
      if ("warcinfo".equalsIgnoreCase(record.warcType())) {
        return;
      }

      count++;
      contentBytes += emittedContentLength(record);

      String mime = normalizeMimetype(firstNonBlank(
          record.headers().get("WARC-Identified-Content-Type"),
          record.headers().get("WARC-Identified-Payload-Type")));
      if (mime == null) {
        missingMimetype++;
      } else {
        mimeTypes.merge(mime, 1L, Long::sum);
      }

      String language = normalizeLanguage(
          record.headers().get("WARC-Identified-Content-Language"));
      if (language == null) {
        missingLanguage++;
      } else {
        languages.merge(language, 1L, Long::sum);
      }

      String date = firstNonBlank(record.headers().get(WARC_DATE));
      if (date != null) {
        try {
          Instant instant = Instant.parse(date);
          if (dateMin == null || instant.isBefore(dateMin)) {
            dateMin = instant;
          }
          if (dateMax == null || instant.isAfter(dateMax)) {
            dateMax = instant;
          }
        } catch (DateTimeParseException _) {
          // Invalid record dates are handled by the processing pipeline. They do not
          // become misleading extrema in the output summary.
        }
      }
    }

    private void merge(OutputArtifactStats other) {
      count += other.count;
      contentBytes += other.contentBytes;
      other.mimeTypes.forEach((key, value) -> mimeTypes.merge(key, value, Long::sum));
      other.languages.forEach((key, value) -> languages.merge(key, value, Long::sum));
      missingLanguage += other.missingLanguage;
      missingMimetype += other.missingMimetype;
      if (other.dateMin != null && (dateMin == null || other.dateMin.isBefore(dateMin))) {
        dateMin = other.dateMin;
      }
      if (other.dateMax != null && (dateMax == null || other.dateMax.isAfter(dateMax))) {
        dateMax = other.dateMax;
      }
    }

    private Map<String, Object> asMap() {
      Map<String, Object> values = new LinkedHashMap<>();
      values.put("count", count);
      values.put("content_bytes", contentBytes);
      values.put("mime_types", new LinkedHashMap<>(mimeTypes));
      values.put("languages", new LinkedHashMap<>(languages));
      values.put("missing_language", missingLanguage);
      values.put("missing_mimetype", missingMimetype);
      values.put("date_min", dateMin == null ? null : dateMin.toString());
      values.put("date_max", dateMax == null ? null : dateMax.toString());
      return values;
    }

    private Map<String, Object> asArtifact(Path path) {
      Map<String, Object> values = new LinkedHashMap<>();
      values.put("path", path.toString());
      values.putAll(asMap());
      return values;
    }

    private static String normalizeMimetype(String value) {
      if (value == null) {
        return null;
      }
      String mediaType = value.split(";", 2)[0].trim().toLowerCase(Locale.ROOT);
      return mediaType.isEmpty() ? null : mediaType;
    }

    private static String normalizeLanguage(String value) {
      if (value == null) {
        return null;
      }
      String language = value.trim().toLowerCase(Locale.ROOT);
      return language.isEmpty() ? null : language;
    }

    private static long emittedContentLength(RecordWarcUniversal record) {
      String declared = record.headers().get("Content-Length");
      if (declared != null) {
        try {
          long length = Long.parseLong(declared);
          if (length >= 0) {
            return length;
          }
        } catch (NumberFormatException _) {
          // Fall through to the bytes that the codec will serialize.
        }
      }

      byte[] raw = record.rawBytes();
      if (raw == null) {
        return 0;
      }
      if (raw.length < 5 || raw[0] != 'W' || raw[1] != 'A' || raw[2] != 'R'
          || raw[3] != 'C' || raw[4] != '/') {
        return raw.length;
      }
      byte[] body = record.bodyBytes();
      int length = body.length;
      if (length >= 4 && body[length - 4] == '\r' && body[length - 3] == '\n'
          && body[length - 2] == '\r' && body[length - 1] == '\n') {
        length -= 4;
      }
      return length;
    }
  }

  // Context-aware order validation tracking.
  // These three fields are read and written exclusively from the single thread
  // that calls onNext() (VirtualThreadEngine serialises consumer delivery via
  // SerializedSubscriber). The volatile modifier provides cross-thread visibility
  // for diagnostic reads (e.g. from onComplete), but all write-then-compare
  // sequences are safe only because onNext() is called serially.
  protected volatile String lastDigestSeen = null;
  protected volatile String lastUrlSeen = null;
  protected volatile String lastSortDateSeen = null;
  protected final AtomicInteger dateOrderViolations = new AtomicInteger(0);

  protected abstract void writeRecordToStream(Object item, OutputStream stream) throws IOException;

  protected abstract void openWriter(OutputStream stream) throws IOException;

  protected abstract void closeWriter() throws IOException;

  protected abstract String getConsumerName();

  @Override
  public void configure(Map<String, Object> cfg) {
    Metrics.setModuleHeader(METRIC_KEY, getConsumerName());

    // Resolve output file with precedence: file (CLI) -> output -> outputFile
    outputFile = (String) cfg.getOrDefault("file", cfg.getOrDefault("output", cfg.get("outputFile")));
    force = Boolean.parseBoolean(String.valueOf(cfg.getOrDefault("force", "false")));
    Object report = cfg.get("publication-report");
    publicationReport = report == null || report.toString().isBlank()
        ? null
        : Path.of(report.toString()).toAbsolutePath().normalize();
    pendingOutputs.clear();
    pendingTargets.clear();
    outputStats.clear();
    currentOutputTarget = null;
    generatedFiles.clear();
    generatedCdxFiles.clear();
    skipPublication = false;
    publicationFinalized = false;
    anyChanges = false;

    String fmt = Objects
        .toString(cfg.getOrDefault("mode", cfg.getOrDefault("format", cfg.get("output-format"))), "warc+cdx")
        .toLowerCase();
    outputFormat = switch (fmt) {
      case "warc" -> OutputFormat.WARC;
      case "one-warc" -> OutputFormat.ONE_WARC;
      case "multi-warc" -> OutputFormat.MULTI_WARC;
      case "cdx", "index-only" -> OutputFormat.CDX_ONLY;
      case "wacz" -> throw new IllegalArgumentException("Unsupported output format: wacz");
      case "wet" -> {
        derivativeType = "wet";
        yield OutputFormat.WARC;
      }
      case "doet" -> {
        derivativeType = "doet";
        yield OutputFormat.WARC;
      }
      case "row" -> {
        derivativeType = "row";
        yield OutputFormat.WARC;
      }
      default -> OutputFormat.WARC_CDX;
    };

    mode = "copy".equals(cfg.get("mode")) ? ProcessingMode.COPY : ProcessingMode.MERGE;

    if (cfg.get("compress") != null || cfg.get("compression") != null) {
      Object c = cfg.getOrDefault("compression", cfg.get("compress"));
      if (c instanceof Boolean b) {
        compressionType = b ? CompressionType.GZIP : CompressionType.NONE;
      } else if (c instanceof String s) {
        compressionType = switch (s.toLowerCase()) {
          case "none" -> CompressionType.NONE;
          case "gzip" -> CompressionType.GZIP;
          case "zstd" -> CompressionType.ZSTD;
          case "lz4" -> CompressionType.LZ4;
          case "xz" -> CompressionType.XZ;
          case "mirror" -> CompressionType.GZIP;
          default -> CompressionType.GZIP;
        };
      }
    }

    // Handle CDX sidecar: default depends on output format (WARC_CDX implies true)
    Object cdxValue = cfg.getOrDefault("cdx-sidecar", cfg.get("cdx"));
    boolean defaultCdx = outputFormat == OutputFormat.WARC_CDX || outputFormat == OutputFormat.CDX_ONLY;
    boolean cdxSidecar = cdxValue != null ? Boolean.parseBoolean(String.valueOf(cdxValue)) : defaultCdx;

    // Downgrade WARC_CDX to WARC when explicitly disabled
    if (!cdxSidecar && outputFormat == OutputFormat.WARC_CDX) {
      outputFormat = OutputFormat.WARC;
    }
    // Upgrade WARC to WARC_CDX when enabled
    if (cdxSidecar && outputFormat == OutputFormat.WARC) {
      outputFormat = OutputFormat.WARC_CDX;
    }

    pywbOnly = Boolean.parseBoolean(String.valueOf(cfg.get("pywb-only")));

    Object limit = cfg.get("warc-size-limit");
    if (limit instanceof Number n)
      sizeLimit = n.longValue();
    else if (limit instanceof String s) {
      try {
        sizeLimit = Long.parseLong(s);
      } catch (NumberFormatException _) {
        // Ignore invalid number format, default is 0 (no limit)
      }
    }

    // Parse check-order option
    String orderStr = Objects.toString(cfg.get("check-order"), "context").toLowerCase();
    checkOrder = switch (orderStr) {
      case "off" -> CheckOrder.OFF;
      case "context" -> CheckOrder.CONTEXT;
      default -> CheckOrder.CONTEXT; // default to context-aware
    };

    outputNameTemplate = Objects.toString(cfg.getOrDefault("output-name-prefix", cfg.get("output-name-template")),
        null);

    skipIfNoChanges = Boolean.parseBoolean(String.valueOf(cfg.get("skip-if-no-changes")));
    splitProvenance = Boolean.parseBoolean(String.valueOf(cfg.get("split-provenance")));
    diffOutputFile = (String) cfg.get("diff-output"); // NEW

    if (splitProvenance && diffOutputFile == null && mode == ProcessingMode.MERGE) {
      throw new IllegalArgumentException("split-provenance requires diff-output parameter");
    }

    recordOrder = (String) cfg.get("record-order");
    parallelGzip = Boolean.parseBoolean(String.valueOf(cfg.getOrDefault("parallel-gzip", "false")));
    if (cfg.get("compression-level") != null) {
      compressionLevel = Integer.parseInt(String.valueOf(cfg.get("compression-level")));
    }

    log.info(() -> String.format(
        "Config: file=%s, format=%s, mode=%s, compression=%s, parallelGzip=%s, checkOrder=%s, sizeLimit=%d, splitProvenance=%s",
        outputFile, outputFormat, mode, compressionType, parallelGzip, checkOrder, sizeLimit,
        splitProvenance));
  }

  @Override
  public List<Class<? extends Record>> acceptedInputTypes() {
    // Preference for optimizing file transfers when possible
    return List.of(RecordFile.class, Record.class);
  }

  @Override
  public boolean beforeCheck(Map<String, Object> cfg) {
    try {
      if (outputFile == null || outputFile.isBlank()) {
        throw new IllegalArgumentException("Output file is required");
      }
      Path outPath = Path.of(outputFile);
      if (outputFormat == OutputFormat.MULTI_WARC) {
        // In multi-output mode outputFile points to a directory.
        Files.createDirectories(outPath);
      } else {
        Path p = outPath.getParent();
        if (p != null) {
          Files.createDirectories(p);
        }
        if (sizeLimit <= 0 && !force
            && Files.exists(outPath.toAbsolutePath().normalize(), LinkOption.NOFOLLOW_LINKS)) {
          throw new FileAlreadyExistsException(outPath.toString());
        }
      }
      return true;
    } catch (Exception e) {
      log.error("Cannot create output directory: {}", e.getMessage(), e);
      return false;
    }
  }

  @Override
  public int afterCheck(Map<String, Object> cfg) {
    long errors = Metrics.get(METRIC_KEY, METRIC_ERRORS);
    if (errors > 0) {
      log.error("Consumer completed with {} write/processing errors", errors);
      return 1;
    }
    return 0;
  }

  @Override
  public void onSubscribe(Flow.Subscription sub) {
    sub.request(Long.MAX_VALUE);
  }

  @Override
  public void onError(Throwable t) {
    log.error("onError: {}", t.getMessage(), t);
    Metrics.inc(METRIC_KEY, METRIC_ERRORS);
    cleanup();
  }

  @Override
  public void onComplete() {
    log.info("onComplete");
    closeAllStreams();
    if (checkOrder != CheckOrder.OFF && dateOrderViolations.get() > 0) {
      Metrics.set(METRIC_KEY, "dateViolations", dateOrderViolations.get());
    }

    handleSkipIfNoChanges();
  }

  private void handleSkipIfNoChanges() {
    if (skipIfNoChanges && !anyChanges) {
      log.info("No changes detected. Output temporaries will be discarded.");
      skipPublication = true;
    }
  }

  @Override
  public void startConsuming() {
    // MULTI_WARC must open per-source files lazily when first record arrives.
    if (mode == ProcessingMode.MERGE && outputFormat != OutputFormat.MULTI_WARC) {
      rotateSegment();
    }
  }

  @Override
  public void onNext(Object item) {
    try {
      Metrics.inc(METRIC_KEY, "recordsIn");

      if (mode == ProcessingMode.COPY && (item instanceof RecordFile || item instanceof Path)) {
        writeRecord(item);
        return;
      }

      String source = extractSource(item);
      rotateIfNecessary(source);

      if (pywbOnly && !isResponseRecord(item)) {
        Metrics.inc(METRIC_KEY, "pywbFiltered");
        return;
      }

      if (checkOrder != CheckOrder.OFF) {
        validateDateOrder(item);
      }

      writeRecord(item);
      Metrics.inc(METRIC_KEY, RECORDS_OUT);
      if (item instanceof RecordWarcUniversal record && currentOutputTarget != null) {
        outputStats.computeIfAbsent(currentOutputTarget, _ -> new OutputArtifactStats()).record(record);
      }
      if (item instanceof RecordWarcUniversal rec && rec.bodyBytes() != null) {
        Metrics.add(METRIC_KEY, "bytesOut", rec.bodyBytes().length);
      }
    } catch (Exception e) {
      Metrics.inc(METRIC_KEY, METRIC_ERRORS);
      log.error("Consumer execution failed", e);
      if (e instanceof IllegalStateException stateException) {
        throw stateException;
      }
      throw new IllegalStateException("Consumer execution failed", e);
    } finally {
      // releasePooledBuffer handles Object
      releasePooledBuffer(item);
    }
  }

  protected String extractSource(Object item) {
    if (item instanceof RecordWarcUniversal rec) {
      return rec.headers().get("X-Source-Warc");
    }
    return null;
  }

  protected void rotateIfNecessary(String currentSource) {
    boolean rotate = false;
    if (currentStream.get() == null) {
      rotate = true;
    } else if (outputFormat == OutputFormat.MULTI_WARC) {
      if (lastSourceFile != null && !lastSourceFile.equals(currentSource)) {
        rotate = true;
        segmentIndex.set(0); // Reset segments for new source
      }
    }

    if (!rotate && sizeLimit > 0 && countStream.get() != null && countStream.get().getCount() >= sizeLimit) {
      rotate = true;
    }

    if (rotate) {
      rotateSegment(currentSource);
    }
  }

  protected void rotateIfNecessary() {
    rotateIfNecessary(lastSourceFile);
  }

  protected void rotateSegment(String source) {
    closeCurrentSegment();
    lastSourceFile = source;

    String path = resolveOutputName(source);

    try {
      if (outputFormat != OutputFormat.CDX_ONLY) {
        PendingOutput pending = prepareOutput(path);
        OutputStream os = new BufferedOutputStream(Files.newOutputStream(pending.temporary()));
        countStream.set(new CountingOutputStream(os));
        currentStream.set(switch (compressionType) {
          case GZIP -> parallelGzip ? countStream.get() : new java.util.zip.GZIPOutputStream(countStream.get());
          case ZSTD -> new ZstdOutputStream(countStream.get());
          case LZ4 -> new net.jpountz.lz4.LZ4FrameOutputStream(countStream.get());
          case XZ ->
            new org.apache.commons.compress.compressors.xz.XZCompressorOutputStream(countStream.get());
          default -> countStream.get();
        });
        openWriter(currentStream.get());
        warcinfoWrittenMain = false;
        if (publicationReport != null) {
          currentOutputTarget = pending.target();
          outputStats.computeIfAbsent(currentOutputTarget, _ -> new OutputArtifactStats());
        }
        generatedFiles.add(pending.target().toString());
      }
      boolean needsCdx = outputFormat == OutputFormat.WARC_CDX || outputFormat == OutputFormat.CDX_ONLY;
      if (needsCdx) {
        String cp = path.replaceAll(WARC_EXT_PATTERN, "") + ".cdxj";
        PendingOutput pendingCdx = prepareOutput(cp);
        cdxWriter.set(new PrintWriter(new BufferedWriter(Files.newBufferedWriter(
            pendingCdx.temporary(), StandardCharsets.UTF_8))));
        generatedCdxFiles.add(pendingCdx.target().toString());
      }

      // Initialize Split Stream if enabled
      if (splitProvenance && outputFormat != OutputFormat.CDX_ONLY) {
        String splitPath;
        if (diffOutputFile != null) {
          splitPath = diffOutputFile;
          // rudimentary segmentation support for diff file if main rotates?
          // If sizeLimit > 0 and segmentIndex > 0 (already incremented for main), we
          // might collide.
          // For now, implementing exact path as requested.
        } else {
          splitPath = path.replace(".warc", "-merged.warc");
          if (splitPath.equals(path)) {
            splitPath = path + "-merged";
          }
        }

        PendingOutput pendingSplit = prepareOutput(splitPath);
        OutputStream sos = new BufferedOutputStream(Files.newOutputStream(pendingSplit.temporary()));
        splitCountStream.set(new CountingOutputStream(sos));
        splitStream.set(createCompressedStream(splitCountStream.get()));

        openWriter(splitStream.get());

        // Write warcinfo to split stream too
        writeWarcinfoRecordTo(splitStream.get());

        generatedFiles.add(pendingSplit.target().toString());

        if (needsCdx) {
          String scp = splitPath.replaceAll(WARC_EXT_PATTERN, "") + ".cdxj";
          PendingOutput pendingSplitCdx = prepareOutput(scp);
          splitCdxWriter.set(new PrintWriter(new BufferedWriter(Files.newBufferedWriter(
              pendingSplitCdx.temporary(), StandardCharsets.UTF_8))));
          generatedCdxFiles.add(pendingSplitCdx.target().toString());
        }
      }
    } catch (Exception e) {
      closeCurrentSegment();
      throw new IllegalStateException("Failed to rotate output segment: " + path, e);
    }
  }

  private OutputStream createCompressedStream(OutputStream out) throws IOException {
    return switch (compressionType) {
      case GZIP -> new java.util.zip.GZIPOutputStream(out);
      case ZSTD -> new ZstdOutputStream(out);
      case LZ4 -> new net.jpountz.lz4.LZ4FrameOutputStream(out);
      case XZ -> new org.apache.commons.compress.compressors.xz.XZCompressorOutputStream(out);
      default -> out;
    };
  }

  private void writeWarcinfoRecordTo(OutputStream stream) throws IOException {
    if (derivativeType == null)
      return;
    byte[] warcinfo = WarcIO.buildWarcinfoRecord(derivativeType, false, recordOrder, null);
    if (stream != null)
      stream.write(warcinfo);
  }

  protected void rotateSegment() {
    rotateSegment(lastSourceFile);
  }

  private String resolveOutputName(String source) {
    String base;
    if (outputFormat == OutputFormat.MULTI_WARC) {
      String sourceToken = source != null && !source.isBlank() ? source : "unknown-source";
      Path sourceName = Path.of(sourceToken).getFileName();
      String safeSourceToken = sourceName != null ? sourceName.toString() : "unknown-source";
      String baseName;
      if (outputNameTemplate != null) {
        String s = safeSourceToken.replaceFirst("\\.warc(?:\\.gz)?$", "");
        baseName = outputNameTemplate.replace("{source}", s);
      } else {
        baseName = safeSourceToken;
      }
      if (outputFile != null) {
        Path outputRoot = Path.of(outputFile).toAbsolutePath().normalize();
        Path resolved = outputRoot.resolve(baseName).normalize();
        if (!resolved.startsWith(outputRoot)) {
          throw new IllegalArgumentException("Resolved output path escapes output directory: " + baseName);
        }
        base = resolved.toString();
      } else {
        base = baseName;
      }
    } else {
      base = outputFile;
    }

    if (sizeLimit > 0) {
      int dot = base.lastIndexOf(DOT_WARC);
      if (dot > 0) {
        return String.format("%s.%05d%s", base.substring(0, dot), segmentIndex.getAndIncrement(),
            base.substring(dot));
      } else {
        return base + "." + segmentIndex.getAndIncrement();
      }
    }
    return base;
  }

  /** Write record to output stream and CDX sidecar. */
  private void writeRecord(Object item) throws IOException {
    if (item instanceof RecordFile recordFile) {
      handleRecordFile(recordFile);
      return;
    }
    if (item instanceof Path warcPath) {
      handleWarcPath(warcPath);
      return;
    }

    ensureWarcinfoForSegment(item);

    boolean needsCdx = outputFormat == OutputFormat.WARC_CDX || outputFormat == OutputFormat.CDX_ONLY;
    boolean isUpdateDual = false;

    String prov = extractProvenance(item);

    if (item instanceof RecordCompressed rc) {
      if (outputFormat != OutputFormat.CDX_ONLY) {
        long pre = countStream.get().getCount();
        currentStream.get().write(rc.compressedBytes());
        if (needsCdx) {
          writeCdxEntry(rc, pre, countStream.get().getCount() - pre);
        }
      } else {
        writeCdxEntry(rc, 0, 0);
      }
      anyChanges = true;
      return;
    }

    if (splitProvenance) {
      // Provenance routing for dual-output merge:
      // - "base-only": base only (default)
      // - "merged": both base and diff (metadata update)
      // - "new": both base and diff (new content)

      if ("merged".equalsIgnoreCase(prov) ||
          "new".equalsIgnoreCase(prov) ||
          "new-content".equalsIgnoreCase(prov) ||
          "uri-changed".equalsIgnoreCase(prov) ||
          "uri-reverted".equalsIgnoreCase(prov) ||
          "update".equalsIgnoreCase(prov)) {
        isUpdateDual = true; // Write to BOTH base and diff
      }
      // "base-only" and others: write to base only (default path)
    }

    if (outputFormat != OutputFormat.CDX_ONLY) {
      if (isUpdateDual && splitStream.get() != null) {
        // Write to BOTH streams
        long preMain = countStream.get() != null ? countStream.get().getCount() : 0;
        writeRecordToStream(item, currentStream.get());
        if (needsCdx) {
          writeCdxEntry(item, preMain, (countStream.get() != null ? countStream.get().getCount() : 0) - preMain);
        }

        long preSplit = splitCountStream.get().getCount();
        writeRecordToStream(item, splitStream.get());
        if (needsCdx) {
          writeCdxEntryTo(splitCdxWriter.get(), item, preSplit, splitCountStream.get().getCount() - preSplit);
        }
      } else {
        // Default: write only to MAIN stream
        long pre = countStream.get() != null ? countStream.get().getCount() : 0;
        writeRecordToStream(item, currentStream.get());
        if (needsCdx) {
          writeCdxEntry(item, pre, (countStream.get() != null ? countStream.get().getCount() : 0) - pre);
        }
      }
    } else {
      writeCdxEntry(item, 0, 0);
    }
    anyChanges = true;
  }

  private void ensureWarcinfoForSegment(Object item) {
    if (derivativeType == null || warcinfoWrittenMain || currentStream.get() == null) {
      return;
    }
    Map<String, String> extraHeaders = extractCrawlRangeHeaders(item);
    writeWarcinfoRecord(extraHeaders);
    warcinfoWrittenMain = true;
  }

  private Map<String, String> extractCrawlRangeHeaders(Object item) {
    if (!(item instanceof RecordWarcUniversal rec)) {
      return null;
    }
    String first = firstNonBlank(
        rec.headers().get("x-nac-crawl-first-date"),
        rec.headers().get("X-NAC-Crawl-First-Date"));
    String last = firstNonBlank(
        rec.headers().get("x-nac-crawl-last-date"),
        rec.headers().get("X-NAC-Crawl-Last-Date"));

    String urlId = firstNonBlank(rec.headers().get("X-NAC-URL-ID"));
    String crawlId = firstNonBlank(rec.headers().get("X-NAC-Crawl-ID"));

    if (first == null && last == null && urlId == null && crawlId == null) {
      return null;
    }

    Map<String, String> extra = new java.util.LinkedHashMap<>();
    if (urlId != null) {
      extra.put("X-NAC-URL-ID", urlId);
    }
    if (crawlId != null) {
      extra.put("X-NAC-Crawl-ID", crawlId);
    }
    if (first != null) {
      extra.put("x-nac-crawl-first-date", first);
    }
    if (last != null) {
      extra.put("x-nac-crawl-last-date", last);
    }
    return extra;
  }

  private static String firstNonBlank(String... values) {
    for (String v : values) {
      if (v != null && !v.isBlank()) {
        return v;
      }
    }
    return null;
  }

  private String extractProvenance(Object item) {
    if (item instanceof RecordCompressed rc) {
      return rc.provenance();
    }
    if (item instanceof RecordWarcUniversal u) {
      String p = u.headers().get("NAC-Merge-Result");
      if (p == null)
        p = u.headers().get("nac-merge-result");
      return p;
    }
    return null;
  }

  /** Release PooledBuffer if item is or contains one. */
  private void releasePooledBuffer(Object item) {
    if (item instanceof PooledBuffer pb) {
      pb.release();
    }
  }

  protected void handleRecordFile(RecordFile rf) throws IOException {
    Path sourcePath = rf.path();
    Path outDir = Optional.ofNullable(Path.of(outputFile).getParent()).orElse(Path.of("."));
    String outName = sourcePath.getFileName().toString().replaceAll(WARC_EXT_PATTERN, "") + "-copy"
        + (sourcePath.toString().endsWith(".gz") ? ".warc.gz" : DOT_WARC);
    Path outPath = outDir.resolve(outName);

    log.info("Optimized file copy: {} -> {}", sourcePath, outPath);
    PendingOutput pending = prepareOutput(outPath.toString());
    Files.copy(sourcePath, pending.temporary(), StandardCopyOption.REPLACE_EXISTING);
    generatedFiles.add(pending.target().toString());
    anyChanges = true;
    Metrics.inc(METRIC_KEY, RECORDS_OUT);
  }

  /** Handle Path objects from producer pass-through mode. */
  protected void handleWarcPath(Path warcPath) throws IOException {
    Path outDir = Optional.ofNullable(Path.of(outputFile).getParent()).orElse(Path.of("."));
    String outName = warcPath.getFileName().toString();
    Path outPath = outDir.resolve(outName);

    log.info("Copying {} -> {}", warcPath, outPath);
    PendingOutput pending = prepareOutput(outPath.toString());
    Files.copy(warcPath, pending.temporary(), StandardCopyOption.REPLACE_EXISTING);
    generatedFiles.add(pending.target().toString());
    anyChanges = true;
    Metrics.inc(METRIC_KEY, RECORDS_OUT);
  }

  /**
   * Write a warcinfo record with NAC custom headers at the start of each segment.
   * Only writes if derivativeType is set (wet, doet, row) or output format
   * requires it.
   */
  protected void writeWarcinfoRecord() {
    writeWarcinfoRecord(null);
  }

  protected void writeWarcinfoRecord(Map<String, String> segmentHeaders) {
    if (derivativeType == null) {
      return; // Standard WARC doesn't need custom warcinfo
    }

    try {
      Map<String, String> extraHeaders = segmentHeaders != null ? new java.util.HashMap<>(segmentHeaders) : null;
      if (splitProvenance && segmentIndex.get() <= 1 && diffOutputFile != null) {
        // Add baseline date for DoS detection
        if (extraHeaders == null) {
          extraHeaders = new java.util.HashMap<>();
        }
        extraHeaders.put("X-NAC-Baseline-Date", java.time.Instant.now().toString());
      }

      byte[] warcinfo = WarcIO.buildWarcinfoRecord(derivativeType, false, recordOrder, extraHeaders);
      if (parallelGzip && compressionType == CompressionType.GZIP) {
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        try (java.util.zip.GZIPOutputStream gzos = new java.util.zip.GZIPOutputStream(baos)) {
          gzos.write(warcinfo);
        }
        warcinfo = baos.toByteArray();
      }

      OutputStream stream = currentStream.get();
      if (stream != null) {
        stream.write(warcinfo);
        log.info("Wrote warcinfo with NAC-WARC-derivative: {}", derivativeType);
      }
    } catch (IOException e) {
      log.error("Failed to write warcinfo: {}", e.getMessage(), e);
      Metrics.inc(METRIC_KEY, METRIC_ERRORS);
    }
  }

  protected boolean isResponseRecord(Object item) {
    if (item instanceof RecordCompressed rc) {
      return "response".equalsIgnoreCase(rc.warcType());
    }
    if (item instanceof RecordWarcUniversal rec) {
      return "response".equalsIgnoreCase(rec.warcType());
    }
    return false;
  }

  protected void validateDateOrder(Object item) {
    if (checkOrder == CheckOrder.CONTEXT) {
      validateContextOrder(item);
    }
    // OFF mode does nothing
  }

  /**
   * Context-aware order validation: (crawlDate, digest) → URL.
   *
   * DOET files sort records by the compound key (crawlYmd, digest) because the
   * RocksDB dedup key is "crawlYmd|digest". At a crawl-date boundary the digest
   * resets to the start of the alphabet, which is correct for the compound key
   * but looks like a violation when only comparing bare digests. Using the full
   * compound key avoids false-positive warnings at date boundaries.
   */
  protected void validateContextOrder(Object item) {
    String digest = extractDigest(item);
    String url = extractUrl(item);
    String sortDate = extractSortDate(item);

    if (digest == null) {
      return; // Can't validate without digest
    }

    // Build compound sort key: (crawlDate YYYYMMDD, digest) — matches DOET sort order
    String compoundKey = (sortDate != null ? sortDate : "") + "|" + digest;
    String lastCompoundKey = (lastSortDateSeen != null ? lastSortDateSeen : "") + "|"
        + (lastDigestSeen != null ? lastDigestSeen : "");

    if (lastDigestSeen != null && compoundKey.compareTo(lastCompoundKey) < 0) {
      if (dateOrderViolations.incrementAndGet() <= 5) {
        log.warn("Sort order violation: {} < {}", compoundKey, lastCompoundKey);
      }
    }

    // Check URL order within same compound key (same crawlDate + same digest)
    if (lastDigestSeen != null && digest.equals(lastDigestSeen)
        && Objects.equals(sortDate, lastSortDateSeen)) {
      if (url != null && lastUrlSeen != null && url.compareTo(lastUrlSeen) < 0) {
        if (dateOrderViolations.incrementAndGet() <= 5) {
          log.warn("URL order violation within digest {}: {} < {}", digest, url, lastUrlSeen);
        }
      }
    }

    // Update tracking state
    lastDigestSeen = digest;
    lastUrlSeen = url;
    lastSortDateSeen = sortDate;
  }

  /**
   * Extracts the crawl date (YYYYMMDD) from the WARC-Date header for use as the
   * primary component of the compound sort key in DOET order validation.
   */
  protected String extractSortDate(Object item) {
    if (item instanceof RecordWarcUniversal rec) {
      String date = rec.headers().get(WARC_DATE);
      if (date == null) {
        date = rec.headers().get("warc-date");
      }
      if (date != null && date.length() >= 10) {
        // "2025-12-20T10:30:00Z" → "20251220"
        return date.substring(0, 10).replace("-", "");
      }
    }
    return null;
  }

  protected String extractDigest(Object item) {
    if (item instanceof RecordWarcUniversal rec) {
      String digest = rec.headers().get("WARC-Payload-Digest");
      if (digest == null) {
        digest = rec.headers().get("warc-payload-digest");
      }
      if (digest == null) {
        digest = rec.headers().get("WARC-Block-Digest");
      }
      if (digest == null) {
        digest = rec.headers().get("warc-block-digest");
      }
      return digest;
    }
    return null;
  }

  protected String extractUrl(Object item) {
    if (item instanceof RecordWarcUniversal rec) {
      String url = rec.headers().get("WARC-Target-URI");
      if (url == null) {
        url = rec.headers().get("warc-target-uri");
      }
      return url;
    }
    return null;
  }

  protected void writeCdxEntry(Object item, long off, long len) {
    if (cdxWriter.get() != null)
      writeCdxEntryTo(cdxWriter.get(), item, off, len);
  }

  protected void writeCdxEntryTo(PrintWriter w, Object item, long off, long len) {
    try {
      String uri = "-";
      String ts = "-";
      String mime = "-";
      String type = "-";
      String digest = "-";
      if (item instanceof RecordWarcUniversal u) {
        uri = u.headers().getOrDefault("WARC-Target-URI", "-");
        ts = u.headers().getOrDefault(WARC_DATE, "-");
        mime = u.headers().getOrDefault("Content-Type", "-");
        type = u.warcType();
        digest = u.headers().getOrDefault("WARC-Payload-Digest", "-");
      } else if (item instanceof RecordCompressed rc) {
        uri = rc.targetUri() != null ? rc.targetUri() : "-";
        ts = rc.warcDate() != null ? rc.warcDate() : "-";
        mime = rc.contentType() != null ? rc.contentType() : "-";
        type = rc.warcType() != null ? rc.warcType() : "-";
        digest = rc.digest() != null ? rc.digest() : "-";
      } else {
        byte[] data = extractByteArray(item);
        if (data.length > 0) {
          String h = new String(data, 0, Math.min(data.length, 2048));
          uri = extractHeader(h, "WARC-Target-URI");
          ts = extractHeader(h, WARC_DATE);
          type = extractHeader(h, "WARC-Type");
          digest = extractHeader(h, "WARC-Payload-Digest");
        }
      }
      String cTs = ts.replaceAll("\\D", "");
      if (cTs.length() > 14)
        cTs = cTs.substring(0, 14);
      w.println(toSurt(uri) + " " + cTs + " " + String.format(
          "{\"url\": \"%s\", \"mime\": \"%s\", \"status\": %d, \"digest\": \"%s\", \"offset\": %d, \"length\": %d, \"type\": \"%s\"}",
          esc(uri), esc(mime), 0, esc(digest), off, len, type));
    } catch (Exception e) {
      log.error("CDX error: {}", e.getMessage());
      Metrics.inc(METRIC_KEY, METRIC_ERRORS);
    }
  }

  /**
   * Extracts byte array from various item types.
   * Supports PooledBuffer and RecordWarc types.
   * Returns empty array if item is not a recognized byte container.
   */
  protected byte[] extractByteArray(Object item) {
    if (item instanceof PooledBuffer pb) {
      return java.util.Arrays.copyOf(pb.array, pb.length);
    } else if (item instanceof pl.gov.nac.warc.records.warc.RecordWarc rec) {
      return rec.rawBytes();
    } else if (item instanceof pl.gov.nac.warc.records.warc.RecordWarcInFile rec) {
      try {
        return rec.rawBytes();
      } catch (IOException e) {
        log.error("Failed to read RecordWarcInFile: {}", e.getMessage(), e);
        return new byte[0];
      }
    }
    return new byte[0];
  }

  private String extractHeader(String h, String n) {
    int i = h.indexOf(n + ":");
    if (i < 0)
      return "-";
    int e = h.indexOf("\r\n", i);
    return e > i ? h.substring(i + n.length() + 1, e).trim() : "-";
  }

  protected String toSurt(String uri) {
    if (uri == null || uri.equals("-") || !uri.contains("://"))
      return uri;
    try {
      String[] p = uri.split("://", 2);
      String u = p[1];
      int s = u.indexOf('/');
      String host = s > 0 ? u.substring(0, s) : u;
      String path = s > 0 ? u.substring(s) : "";
      String[] hp = host.split("\\.");
      StringBuilder sb = new StringBuilder();
      for (int i = hp.length - 1; i >= 0; i--) {
        sb.append(hp[i]).append(i > 0 ? "," : "");
      }
      return sb.toString() + ")" + path;
    } catch (Exception _) {
      return uri;
    }
  }

  private String esc(String s) {
    return s == null ? "" : s.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  private PendingOutput prepareOutput(String requestedTarget) throws IOException {
    if (publicationFinalized) {
      throw new IllegalStateException("Output publication has already been finalized");
    }
    Path target = Path.of(requestedTarget).toAbsolutePath().normalize();
    Path parent = target.getParent();
    if (parent == null) {
      throw new IOException("Output target has no parent: " + target);
    }
    Files.createDirectories(parent);
    if (!pendingTargets.add(target)) {
      throw new FileAlreadyExistsException("Output target was resolved more than once: " + target);
    }
    if (!force && Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
      pendingTargets.remove(target);
      throw new FileAlreadyExistsException(target.toString());
    }

    Path temporary;
    try {
      temporary = Files.createTempFile(parent, "." + target.getFileName() + ".", ".tmp");
    } catch (IOException e) {
      pendingTargets.remove(target);
      throw e;
    }
    PendingOutput pending = new PendingOutput(temporary, target);
    pendingOutputs.add(pending);
    return pending;
  }

  @Override
  public int publishOutputs() {
    if (publicationFinalized) {
      return 0;
    }
    closeAllStreams();

    List<PendingOutput> ordered = new ArrayList<>(pendingOutputs);
    ordered.sort((left, right) -> compareUtf8Bytes(left.target().toString(), right.target().toString()));
    if (skipPublication || Metrics.get(METRIC_KEY, METRIC_ERRORS) > 0) {
      cleanupPendingTemporaries();
      String status = skipPublication ? "discarded" : "error";
      boolean reported = writePublicationReport(status, ordered.size(), List.of());
      clearPendingOutputs();
      publicationFinalized = true;
      return skipPublication && reported ? 0 : 1;
    }

    List<Path> published = new ArrayList<>();
    for (PendingOutput pending : ordered) {
      try {
        moveOutput(pending.temporary(), pending.target(), force);
        published.add(pending.target());
        Metrics.inc(METRIC_KEY, "outputsPublished");
      } catch (IOException e) {
        log.error("Failed to publish output {}: {}", pending.target(), e.getMessage(), e);
        Metrics.inc(METRIC_KEY, METRIC_ERRORS);
        cleanupPendingTemporaries();
        String status = published.isEmpty() ? "error" : "partial";
        writePublicationReport(status, ordered.size(), published);
        clearPendingOutputs();
        publicationFinalized = true;
        return 1;
      }
    }

    boolean reported = writePublicationReport("published", ordered.size(), published);
    clearPendingOutputs();
    publicationFinalized = true;
    return reported ? 0 : 1;
  }

  @Override
  public void discardOutputs() {
    if (publicationFinalized) {
      return;
    }
    closeAllStreams();
    int planned = pendingOutputs.size();
    cleanupPendingTemporaries();
    writePublicationReport("discarded", planned, List.of());
    clearPendingOutputs();
    publicationFinalized = true;
  }

  /** Move one complete sibling temporary into its final name. */
  protected void moveOutput(Path temporary, Path target, boolean replace) throws IOException {
    if (!replace && Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
      throw new FileAlreadyExistsException(target.toString());
    }
    try {
      if (replace) {
        Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      } else {
        Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
      }
    } catch (AtomicMoveNotSupportedException e) {
      throw new IOException("Atomic output publication is not supported for " + target, e);
    }
  }

  private void cleanupPendingTemporaries() {
    for (PendingOutput pending : pendingOutputs) {
      try {
        Files.deleteIfExists(pending.temporary());
      } catch (IOException e) {
        log.warn("Failed to remove output temporary {}: {}", pending.temporary(), e.getMessage());
      }
    }
  }

  private void clearPendingOutputs() {
    pendingOutputs.clear();
    pendingTargets.clear();
  }

  private boolean writePublicationReport(String status, int planned, List<Path> published) {
    if (publicationReport == null) {
      return true;
    }
    try {
      Path parent = publicationReport.getParent();
      if (parent != null) {
        Files.createDirectories(parent);
      }
      Map<String, Object> report = new LinkedHashMap<>();
      report.put("schema", PUBLICATION_SCHEMA);
      report.put("status", status);
      report.put("planned", planned);
      report.put("published", published.stream().map(Path::toString).toList());
      Map<String, Object> stats = buildOutputStats(published);
      if (stats != null) {
        report.put("output_stats", stats);
      }
      Files.writeString(
          publicationReport,
          JSON.writeValueAsString(report) + System.lineSeparator(),
          StandardCharsets.UTF_8,
          StandardOpenOption.CREATE,
          StandardOpenOption.TRUNCATE_EXISTING,
          StandardOpenOption.WRITE);
      return true;
    } catch (IOException e) {
      log.error("Failed to write output publication report {}: {}", publicationReport, e.getMessage(), e);
      Metrics.inc(METRIC_KEY, METRIC_ERRORS);
      return false;
    }
  }

  private Map<String, Object> buildOutputStats(List<Path> published) {
    OutputArtifactStats aggregate = new OutputArtifactStats();
    List<Map<String, Object>> artifacts = new ArrayList<>();
    for (Path path : published) {
      OutputArtifactStats artifact = outputStats.get(path);
      if (artifact != null) {
        aggregate.merge(artifact);
        artifacts.add(artifact.asArtifact(path));
      }
    }
    if (artifacts.isEmpty()) {
      return null;
    }
    Map<String, Object> stats = aggregate.asMap();
    stats.put("artifacts", artifacts);
    return stats;
  }

  private static int compareUtf8Bytes(String left, String right) {
    byte[] leftBytes = left.getBytes(StandardCharsets.UTF_8);
    byte[] rightBytes = right.getBytes(StandardCharsets.UTF_8);
    int shared = Math.min(leftBytes.length, rightBytes.length);
    for (int i = 0; i < shared; i++) {
      int comparison = Integer.compare(Byte.toUnsignedInt(leftBytes[i]), Byte.toUnsignedInt(rightBytes[i]));
      if (comparison != 0) {
        return comparison;
      }
    }
    return Integer.compare(leftBytes.length, rightBytes.length);
  }

  protected void closeCurrentSegment() {
    try {
      closeWriter();
    } catch (IOException e) {
      log.error("Error closing writer: {}", e.getMessage(), e);
      Metrics.inc(METRIC_KEY, METRIC_ERRORS);
    }

    try {
      if (currentStream.get() != null) {
        currentStream.get().flush();
        currentStream.get().close();
      }
    } catch (IOException e) {
      log.error("Error closing currentStream: {}", e.getMessage(), e);
      Metrics.inc(METRIC_KEY, METRIC_ERRORS);
    }

    try {
      if (countStream.get() != null)
        countStream.get().close();
    } catch (IOException e) {
      log.error("Error closing countStream: {}", e.getMessage(), e);
      Metrics.inc(METRIC_KEY, METRIC_ERRORS);
    }

    try {
      if (cdxWriter.get() != null) {
        cdxWriter.get().flush();
        boolean failed = cdxWriter.get().checkError();
        cdxWriter.get().close();
        if (failed || cdxWriter.get().checkError()) {
          log.error("CDX writer reported an output failure");
          Metrics.inc(METRIC_KEY, METRIC_ERRORS);
        }
      }
    } catch (Exception e) {
      log.error("Error closing CDX writer: {}", e.getMessage(), e);
      Metrics.inc(METRIC_KEY, METRIC_ERRORS);
    }

    // Close split streams
    try {
      if (splitStream.get() != null) {
        splitStream.get().flush();
        splitStream.get().close();
      }
    } catch (IOException e) {
      log.error("Error closing splitStream: {}", e.getMessage(), e);
      Metrics.inc(METRIC_KEY, METRIC_ERRORS);
    }

    try {
      if (splitCountStream.get() != null)
        splitCountStream.get().close();
    } catch (IOException e) {
      // Log close failures so truncated split output files are not silently ignored.
      log.error("Failed to close splitCountStream — file may be truncated", e);
      Metrics.inc(METRIC_KEY, METRIC_ERRORS);
    }

    try {
      if (splitCdxWriter.get() != null) {
        splitCdxWriter.get().flush();
        boolean failed = splitCdxWriter.get().checkError();
        splitCdxWriter.get().close();
        if (failed || splitCdxWriter.get().checkError()) {
          log.error("Split CDX writer reported an output failure");
          Metrics.inc(METRIC_KEY, METRIC_ERRORS);
        }
      }
    } catch (Exception e) {
      log.error("Failed to close splitCdxWriter — file may be truncated", e);
      Metrics.inc(METRIC_KEY, METRIC_ERRORS);
    }

    currentStream.set(null);
    countStream.set(null);
    cdxWriter.set(null);
    splitStream.set(null);
    splitCountStream.set(null);
    splitCdxWriter.set(null);
    currentOutputTarget = null;
  }

  protected void closeAllStreams() {
    closeCurrentSegment();
  }

  protected void cleanup() {
    closeAllStreams();
  }

  protected static class CountingOutputStream extends FilterOutputStream {
    long bytesWritten = 0;

    public CountingOutputStream(OutputStream out) {
      super(out);
    }

    @Override
    public void write(int b) throws IOException {
      super.write(b);
      bytesWritten++;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      out.write(b, off, len);
      bytesWritten += len;
    }

    public long getCount() {
      return bytesWritten;
    }
  }
}
