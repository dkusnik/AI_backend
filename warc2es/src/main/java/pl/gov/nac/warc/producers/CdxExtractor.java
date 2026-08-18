package pl.gov.nac.warc.producers;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.SubmissionPublisher;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.netpreserve.jwarc.WarcDigest;
import org.netpreserve.jwarc.WarcReader;
import org.netpreserve.jwarc.WarcRecord;
import org.netpreserve.jwarc.WarcResponse;
import org.netpreserve.jwarc.WarcTargetRecord;

import pl.gov.nac.warc.reactive.Metrics;
import pl.gov.nac.warc.reactive.ReactiveInterfaces;
import pl.gov.nac.warc.records.Record;
import pl.gov.nac.warc.records.cdx.RecordCdxRaw;
import pl.gov.nac.warc.records.cdx.RecordCdxStructured;
import pl.gov.nac.warc.records.file.RecordFileCdx;

/**
 * Universal producer that identifies CDX, WARC, or WACZ files and emits
 * RecordCdxRaw or RecordCdxStructured entries depending on negotiated type.
 */
public final class CdxExtractor extends SubmissionPublisher<Object>
    implements ReactiveInterfaces.ReactiveProducer<Object> {

  private static final Logger log = LogManager.getLogger(CdxExtractor.class);
  private static final String METRIC_MODULE = "producer-universal";

  private List<String> inputFiles;
  private Class<? extends Record> negotiatedOutputType = RecordCdxRaw.class;

  /**
   * Priority order: file → raw → structured.
   * File-level types allow pass-through without processing individual records.
   */
  @Override
  public List<Class<? extends Record>> emittedOutputTypes() {
    return List.of(RecordFileCdx.class, RecordCdxRaw.class, RecordCdxStructured.class);
  }

  @Override
  public void onNegotiatedOutputType(Class<?> type) {
    if (type != null && Record.class.isAssignableFrom(type)) {
      @SuppressWarnings("unchecked")
      Class<? extends Record> recordType = (Class<? extends Record>) type;
      this.negotiatedOutputType = recordType;
    }
  }

  @Override
  public void configure(Map<String, Object> cfg) {
    Metrics.setModuleHeader(METRIC_MODULE, "Universal Index Producer");

    Object filesObj = cfg.get("files");
    if (filesObj instanceof List<?> list && !list.isEmpty()) {
      this.inputFiles = list.stream().map(Object::toString).toList();
    } else if (cfg.get("file") instanceof String s1) {
      this.inputFiles = List.of(s1);
    } else if (cfg.get("input") instanceof String s2) {
      this.inputFiles = List.of(s2);
    }

    log.info("Configured with {} files", (inputFiles != null ? inputFiles.size() : 0));
  }

  @Override
  public boolean beforeCheck(Map<String, Object> cfg) {
    if (inputFiles == null || inputFiles.isEmpty()) {
      log.error("No input files configured");
      return false;
    }
    for (String path : inputFiles) {
      if (!Files.exists(Path.of(path))) {
        log.error("Input file not found: {}", path);
        return false;
      }
    }
    return true;
  }

  @Override
  public int afterCheck(Map<String, Object> cfg) {
    return 0;
  }

  @Override
  public void startProducing() {
    for (String pathStr : inputFiles) {
      Path path = Path.of(pathStr);
      String name = path.getFileName().toString().toLowerCase();

      try {
        if (name.endsWith(".wacz")) {
          processWacz(path);
        } else if (name.endsWith(".warc") || name.endsWith(".warc.gz")) {
          processWarc(path);
        } else if (name.endsWith(".cdx") || name.endsWith(".cdxj") || name.endsWith(".cdx.gz")
            || name.endsWith(".cdxj.gz")) {
          processCdx(path);
        } else {
          log.error("Unsupported file format: {}", name);
        }
      } catch (Exception e) {
        log.error("Error processing " + name, e);
        Metrics.inc(METRIC_MODULE, "errors");
      }
    }
    close();
  }

  private void processCdx(Path path) throws IOException {
    log.info("Reading CDX/CDXJ: {}", path);
    try (InputStream is = new FileInputStream(path.toFile());
        InputStream maybeGzip = path.toString().endsWith(".gz") ? new GZIPInputStream(is) : is;
        BufferedReader reader = new BufferedReader(new InputStreamReader(maybeGzip, StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.trim().isEmpty() || line.startsWith(" CDXJ"))
          continue;
        emit(line);
      }
    }
  }

  private void processWarc(Path path) throws IOException {
    log.info("Indexing WARC on-the-fly: {}", path);
    String filename = path.getFileName().toString();
    boolean plainWarc = filename.toLowerCase().endsWith(".warc");

    if (plainWarc) {
      // Scan for record boundaries incrementally using a fixed buffer then re-open
      // the file for actual record parsing. CountingInputStream cannot be used here
      // because jwarc reads ahead and its internal buffer position does not align
      // with logical record boundaries.
      List<Long> starts = findRecordStartsIncremental(path);
      long fileSize = Files.size(path);
      try (WarcReader reader = new WarcReader(new FileInputStream(path.toFile()))) {
        int idx = 0;
        for (WarcRecord rec : reader) {
          long startOffset = (idx < starts.size()) ? starts.get(idx) : 0L;
          long nextOffset  = (idx + 1 < starts.size()) ? starts.get(idx + 1) : fileSize;
          long length = Math.max(0L, nextOffset - startOffset);
          emitRecord(rec, filename, startOffset, length);
          idx++;
        }
      }
      return;
    }

    try (CountingInputStream cis = new CountingInputStream(new FileInputStream(path.toFile()));
        WarcReader reader = new WarcReader(cis)) {
      long previousEnd = 0L;
      for (WarcRecord rec : reader) {
        long endOffset = cis.position();
        long startOffset = previousEnd;
        long length = Math.max(0L, endOffset - startOffset);
        emitRecord(rec, filename, startOffset, length);
        previousEnd = endOffset;
      }
    }
  }

  /**
   * Incrementally scan {@code path} for "WARC/" record boundaries using a sliding
   * window, returning the byte offset of every occurrence.  Avoids loading the
   * whole file into memory.
   */
  private List<Long> findRecordStartsIncremental(Path path) throws IOException {
    final byte[] marker = "WARC/".getBytes(StandardCharsets.US_ASCII);
    final int bufSize = 65536;
    List<Long> starts = new ArrayList<>();
    byte[] buf = new byte[bufSize + marker.length - 1];
    long fileOffset = 0;
    int carry = 0; // bytes retained from the previous buffer to detect splits across boundaries

    try (java.io.InputStream in = new java.io.BufferedInputStream(new FileInputStream(path.toFile()), bufSize)) {
      while (true) {
        int read = in.read(buf, carry, bufSize);
        if (read <= 0) break;
        int total = carry + read;
        // Scan buf[0..total-marker.length] for marker
        int scanTo = total - marker.length + 1;
        for (int i = 0; i < scanTo; i++) {
          boolean match = true;
          for (int j = 0; j < marker.length; j++) {
            if (buf[i + j] != marker[j]) { match = false; break; }
          }
          if (match) starts.add(fileOffset - carry + i);
        }
        // Retain the last (marker.length - 1) bytes for the next iteration
        carry = marker.length - 1;
        fileOffset += read;
        System.arraycopy(buf, total - carry, buf, 0, carry);
      }
    }
    return starts;
  }

  private void processWacz(Path path) throws IOException {
    log.info("Processing WACZ: {}", path);
    try (ZipFile zip = new ZipFile(path.toFile())) {
      ZipEntry indexEntry = findIndexEntry(zip);

      if (indexEntry != null) {
        processInternalIndex(zip, indexEntry);
      } else {
        log.info("No internal index found in WACZ, scanning archives...");
        scanWaczArchives(zip);
      }
    }
  }

  /** Find internal index entry in WACZ (indexes/index.cdxj or index.cdxj). */
  private ZipEntry findIndexEntry(ZipFile zip) {
    ZipEntry entry = zip.getEntry("indexes/index.cdxj");
    return entry != null ? entry : zip.getEntry("index.cdxj");
  }

  /** Process internal CDXJ index file. */
  private void processInternalIndex(ZipFile zip, ZipEntry indexEntry) throws IOException {
    log.info("Found internal index: {}", indexEntry.getName());
    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(zip.getInputStream(indexEntry), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (!line.trim().isEmpty() && !line.startsWith(" CDXJ")) {
          emit(line);
        }
      }
    }
  }

  /** Scan WACZ archive entries and generate CDXJ lines. */
  private void scanWaczArchives(ZipFile zip) throws IOException {
    java.util.Enumeration<? extends ZipEntry> entries = zip.entries();
    while (entries.hasMoreElements()) {
      ZipEntry ze = entries.nextElement();
      if (isWarcEntry(ze)) {
        processWarcEntry(zip, ze);
      }
    }
  }

  /** Check if zip entry is a WARC file in archive/ directory. */
  private boolean isWarcEntry(ZipEntry ze) {
    return ze.getName().startsWith("archive/")
        && (ze.getName().endsWith(".warc.gz") || ze.getName().endsWith(".warc"));
  }

  /** Process a single WARC entry from WACZ. */
  private void processWarcEntry(ZipFile zip, ZipEntry ze) throws IOException {
    String name = ze.getName().substring(ze.getName().lastIndexOf('/') + 1);
    try (CountingInputStream cis = new CountingInputStream(zip.getInputStream(ze));
        WarcReader reader = new WarcReader(cis)) {
      long startOffset = cis.position();
      for (WarcRecord rec : reader) {
        long endOffset = cis.position();
        long length = endOffset - startOffset;
        emitRecord(rec, name, startOffset, length);
        startOffset = endOffset;
      }
    }
  }

  private static final class CountingInputStream extends FilterInputStream {
    private long position;

    private CountingInputStream(InputStream in) {
      super(in);
      this.position = 0L;
    }

    long position() {
      return position;
    }

    @Override
    public int read() throws IOException {
      int b = super.read();
      if (b >= 0) {
        position++;
      }
      return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int n = super.read(b, off, len);
      if (n > 0) {
        position += n;
      }
      return n;
    }
  }

  /** Dispatch record based on negotiated output type. */
  private void emitRecord(WarcRecord rec, String filename, long offset, long length) {
    if (negotiatedOutputType == RecordCdxStructured.class) {
      RecordCdxStructured s = buildStructured(rec, filename, offset, length);
      if (s != null) {
        Metrics.inc(METRIC_MODULE, "linesOut");
        submit(s);
      }
    } else {
      String line = generateCdxjLine(rec, filename, offset);
      if (line != null)
        emit(line);
    }
  }

  /** Build a RecordCdxStructured from a WARC record. */
  private RecordCdxStructured buildStructured(WarcRecord rec, String filename, long offset, long length) {
    if (!(rec instanceof WarcTargetRecord target))
      return null;

    String url = target.targetURI().toString();
    String ts = formatTimestamp(target.date());
    String mime = target.contentType().base().toString();
    String digest = target.payloadDigest().map(WarcDigest::toString).orElse("");
    int statusCode = extractHttpStatus(rec);

    return new RecordCdxStructured(
        toSurt(url), ts, url, mime, statusCode,
        digest, offset, length, filename, Map.of(), true);
  }

  private String generateCdxjLine(WarcRecord rec, String filename, long offset) {
    if (!(rec instanceof WarcTargetRecord target))
      return null;

    String url = target.targetURI().toString();
    String ts = formatTimestamp(target.date());

    StringBuilder json = new StringBuilder();
    json.append("{");
    json.append("\"url\": \"").append(escapeJson(url)).append("\",");
    json.append("\"digest\": \"").append(target.payloadDigest().map(WarcDigest::toString).orElse("")).append("\",");
    json.append("\"mime\": \"").append(target.contentType().base()).append("\",");
    json.append("\"offset\": ").append(offset).append(",");
    json.append("\"filename\": \"").append(filename).append("\"");
    json.append("}");

    return url + " " + ts + " " + json.toString();
  }

  private String formatTimestamp(Instant instant) {
    String ts = instant.toString().replace("-", "").replace(":", "").replace("T", "").replace("Z", "");
    if (ts.length() > 14)
      ts = ts.substring(0, 14);
    return ts;
  }

  private int extractHttpStatus(WarcRecord rec) {
    if (rec instanceof WarcResponse resp) {
      try {
        return resp.http().status();
      } catch (Exception _) {
      }
    }
    return 0;
  }

  private String toSurt(String uri) {
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
      for (int i = hp.length - 1; i >= 0; i--)
        sb.append(hp[i]).append(i > 0 ? "," : "");
      return sb.toString() + ")" + path;
    } catch (Exception _) {
      return uri;
    }
  }

  private String escapeJson(String s) {
    return s.replace("\\", "\\\\").replace("\"", "\\\"");
  }


  private void emit(String line) {
    boolean isCdxj = line.contains("{") && line.contains("}");
    Metrics.inc(METRIC_MODULE, "linesOut");
    submit(new RecordCdxRaw(line, isCdxj));
  }
}
