package pl.gov.nac.warc.processors;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.Normalizer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tika.Tika;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import net.dankito.readability4j.Readability4J;
import net.dankito.readability4j.model.ReadabilityOptions;
import net.dankito.readability4j.processor.ArticleGrabber;
import net.dankito.readability4j.processor.MetadataParser;
import net.dankito.readability4j.processor.Postprocessor;
import net.dankito.readability4j.processor.Preprocessor;
import net.dankito.readability4j.util.RegExUtil;
import pl.gov.nac.warc.reactive.Metrics;
import pl.gov.nac.warc.reactive.ReactiveInterfaces;
import pl.gov.nac.warc.records.Record;
import pl.gov.nac.warc.records.warc.RecordWarcUniversal;
import pl.gov.nac.warc.utils.ReadabilityNative;
import pl.gov.nac.warc.utils.WarcIO;

/**
 * Extracts text from WARC records and decorates them with extracted text.
 * Uses Readability4J for HTML and Tika for other document types.
 *
 * - Sets WARC-Type to "conversion" if text is extracted
 * - Adds extracted text to the record body
 * - Does NOT change record class (decorator pattern)
 *
 * Input: RecordWarcUniversal (all WARC types)
 * Output: RecordWarcUniversal (same record, decorated with text)
 */
public class WarcDecoratorTextExtract
    implements ReactiveInterfaces.ReactiveProcessor<RecordWarcUniversal, RecordWarcUniversal> {

  private static final Logger log = LogManager.getLogger(WarcDecoratorTextExtract.class);
  private static final String METRIC_KEY = "text-decorator";
  private static final String HEADER_TITLE = "WARC-Extracted-Title";
  private static final int PDFTOTEXT_MAX_OUTPUT_BYTES = 50 * 1024 * 1024;
  // OPT-P3: Removed WHITESPACE_PATTERN - replaced with collapseWhitespace()
  // method
  // OPT-P2-04: ThreadLocal LinkedHashMap for header reuse (avoids allocation per
  // record)
  private static final ThreadLocal<java.util.LinkedHashMap<String, String>> HEADER_MAP_POOL = ThreadLocal
      .withInitial(java.util.LinkedHashMap::new);

  // OPT-P2-01: Regex scan for screen-reader text (avoids full Jsoup re-parse)
  private static final java.util.regex.Pattern SR_TEXT_PATTERN = java.util.regex.Pattern.compile(
      "<[a-zA-Z][^>]*\\bclass=[\"'][^\"']*(?:screen-reader-text|sr-only|visually-hidden)[^\"']*[\"'][^>]*>([^<]+)<",
      java.util.regex.Pattern.CASE_INSENSITIVE);

  // OPT-P2-23: RegExUtil singleton to avoid 15+ Pattern.compile() calls per
  // record.
  // RegExUtil is thread-safe as it only contains immutable Patterns.
  // We provide all 13 arguments to match the full Kotlin constructor from Java.
  private static final RegExUtil SHARED_REGEX = new RegExUtil(
      RegExUtil.UnlikelyCandidatesDefaultPattern,
      RegExUtil.OkMaybeItsACandidateDefaultPattern,
      RegExUtil.PositiveDefaultPattern,
      RegExUtil.NegativeDefaultPattern,
      RegExUtil.ExtraneousDefaultPattern,
      RegExUtil.BylineDefaultPattern,
      RegExUtil.ReplaceFontsDefaultPattern,
      RegExUtil.NormalizeDefaultPattern,
      RegExUtil.VideosDefaultPattern,
      RegExUtil.NextLinkDefaultPattern,
      RegExUtil.PrevLinkDefaultPattern,
      RegExUtil.WhitespaceDefaultPattern,
      RegExUtil.HasContentDefaultPattern);

  // OPT-P2-15: Increased threshold for skipping Readability4J on small HTML (was
  // 2KB)
  // Readability4J's sophisticated article extraction is overkill for small pages
  private static final int SMALL_HTML_THRESHOLD = 5120; // Skip Readability4J for HTML < 5KB
  private Flow.Subscriber<? super RecordWarcUniversal> downstream;
  private final Tika tika = new Tika();

  // Configuration
  private int minTextLength = 0;
  private int maxTextLength = 0; // 0 = no limit
  private boolean extractTitle = true;
  private boolean extractNormalize = true;
  private boolean dropOnFailure = true;
  private boolean hybridFallback = true;
  private int hybridThreshold = 200;
  private boolean preserveLinks = true;
  private boolean preserveHeadings = true;
  // Native Rust readability via Panama FFI (OPT-P2-JNI)
  private boolean useNativeReadability = ReadabilityNative.isAvailable();
  // pdftotext (Poppler) as PDF extractor instead of PDFBox/Tika
  private boolean usePdftotext = false;
  private String pdftotextPath = "pdftotext";
  private int pdftotextTimeoutSeconds = 60;
  private Semaphore pdftotextSemaphore = null; // null = unlimited

  @Override
  public List<Class<? extends Record>> acceptedInputTypes() {
    return List.of(RecordWarcUniversal.class);
  }

  @Override
  public List<Class<? extends Record>> emittedOutputTypes() {
    return List.of(RecordWarcUniversal.class);
  }

  @Override
  public boolean doesChangeRecordClass() {
    return false; // Decorator - same record type out
  }

  @Override
  public boolean isEnabled(Map<String, Object> cfg) {
    Object v = cfg.get("enabled");
    if (v instanceof Boolean b)
      return b;
    if (v instanceof String s)
      return Boolean.parseBoolean(s);
    return true; // Default to enabled if not specified
  }

  @Override
  public void configure(Map<String, Object> cfg) {
    Metrics.setModuleHeader(METRIC_KEY, "Text Extractor Decorator");
    tika.setMaxStringLength(50 * 1024 * 1024);

    minTextLength = getInt(cfg, "extract-min-text-length", 0);
    maxTextLength = getInt(cfg, "extract-max-text-length", 0);
    extractTitle = getBoolean(cfg, "extract-title", true);
    extractNormalize = getBoolean(cfg, "extract-normalize", true);
    dropOnFailure = getBoolean(cfg, "drop-on-failure", true);
    hybridFallback = getBoolean(cfg, "extract-hybrid-fallback", true);
    hybridThreshold = getInt(cfg, "extract-hybrid-threshold", 200);
    preserveLinks = getBoolean(cfg, "extract-preserve-links", true);
    preserveHeadings = getBoolean(cfg, "extract-preserve-headings", true);
    // OPT-P2-JNI: Native Rust readability (default: auto-enable when available)
    boolean cfgNative = getBoolean(cfg, "use-native-readability", true);
    useNativeReadability = cfgNative && ReadabilityNative.isAvailable();
    // pdftotext: optional Poppler-based PDF extractor (faster than PDFBox)
    usePdftotext = getBoolean(cfg, "use-pdftotext", false);
    pdftotextPath = getString(cfg, "pdftotext-path", "pdftotext");
    pdftotextTimeoutSeconds = Math.max(1, getInt(cfg, "pdftotext-timeout-seconds", 60));
    int maxConcurrent = getInt(cfg, "pdftotext-max-concurrent", 3);
    pdftotextSemaphore = maxConcurrent > 0 ? new Semaphore(maxConcurrent) : null;
    if (usePdftotext && !isPdftotextAvailable()) {
      log.warn("use-pdftotext=true but '{}' not found on PATH — falling back to Tika for PDFs", pdftotextPath);
      usePdftotext = false;
    }

    log.info(
        "Configured: minTextLength={}, maxTextLength={}, extractTitle={}, extractNormalize={}, dropOnFailure={}, hybridFallback={}, hybridThreshold={}, preserveLinks={}, preserveHeadings={}, useNativeReadability={} (available={}), usePdftotext={}, pdftotextMaxConcurrent={}",
        minTextLength, maxTextLength, extractTitle, extractNormalize, dropOnFailure, hybridFallback, hybridThreshold,
        preserveLinks, preserveHeadings, useNativeReadability, ReadabilityNative.isAvailable(), usePdftotext,
        maxConcurrent);

    // Reset timing accumulators so sequential pipeline runs in the same JVM don't
    // accumulate across runs.
    javaStringConvertNs = new java.util.concurrent.atomic.LongAdder();
    javaJsoupParse1Ns   = new java.util.concurrent.atomic.LongAdder();
    javaReadability4jNs = new java.util.concurrent.atomic.LongAdder();
    javaJsoupParse2Ns   = new java.util.concurrent.atomic.LongAdder();
    javaTotalNs         = new java.util.concurrent.atomic.LongAdder();
    javaCallCount       = new java.util.concurrent.atomic.LongAdder();

    // Pre-warm Readability4J and Tika to avoid ClassLoader contention during
    // parallel phase (Cycle 11)
    warmUp();
  }

  /**
   * Warm up Readability4J and Tika to ensure all required classes are loaded
   * and regexes are initialized before the high-concurrency phase starts.
   */
  private void warmUp() {
    log.info("Warming up text extraction components...");
    try {
      // 1. Warm up Readability4J dependencies
      String dummyHtml = "<html><body><article><h1>Warm-up</h1><p>Triggering class loading for Readability4J.</p></article></body></html>";
      Document doc = Jsoup.parse(dummyHtml, "http://example.com");
      ReadabilityOptions options = new ReadabilityOptions();
      Readability4J readability = new Readability4J(
          "http://example.com",
          doc,
          options,
          SHARED_REGEX,
          new Preprocessor(SHARED_REGEX),
          new MetadataParser(SHARED_REGEX),
          new ArticleGrabber(options, SHARED_REGEX),
          new Postprocessor());
      readability.parse();

      // 2. Warm up Tika to avoid mid-pipeline ServiceLoader class loading
      try (InputStream is = new ByteArrayInputStream("Warm-up".getBytes(StandardCharsets.UTF_8))) {
        tika.parseToString(is);
      }

      log.info("Warm-up phase completed successfully.");
    } catch (Exception e) {
      log.warn("Warm-up phase encountered an issue (non-critical): {}", e.getMessage());
    }
  }

  private int getInt(Map<String, Object> cfg, String key, int def) {
    Object v = cfg.get(key);
    if (v instanceof Number n)
      return n.intValue();
    if (v instanceof String s) {
      try {
        return Integer.parseInt(s);
      } catch (Exception ignored) {
      }
    }
    return def;
  }

  private boolean getBoolean(Map<String, Object> cfg, String key, boolean def) {
    Object v = cfg.get(key);
    if (v instanceof Boolean b)
      return b;
    if (v instanceof String s)
      return Boolean.parseBoolean(s);
    return def;
  }

  private String getString(Map<String, Object> cfg, String key, String def) {
    Object v = cfg.get(key);
    if (v instanceof String s)
      return s;
    if (v != null)
      return v.toString();
    return def;
  }

  @Override
  public void subscribe(Flow.Subscriber<? super RecordWarcUniversal> subscriber) {
    this.downstream = subscriber;
  }

  @Override
  public void onSubscribe(Flow.Subscription subscription) {
    downstream.onSubscribe(subscription);
  }

  @Override
  public void onNext(RecordWarcUniversal item) {
    try {
      String warcType = item.warcType();
      if ("warcinfo".equalsIgnoreCase(warcType) || "metadata".equalsIgnoreCase(warcType)) {
        downstream.onNext(item);
        return;
      }

      byte[] body = item.rawBytes();
      if (body == null || body.length == 0) {
        Metrics.inc(METRIC_KEY, "empty-body");
        downstream.onNext(item);
        return;
      }

      FullExtractionResult result = extractFullContent(item);
      if (result.text == null) {
        if (dropOnFailure) {
          Metrics.inc(METRIC_KEY, "dropped-on-failure");
        } else {
          downstream.onNext(item);
        }
        return;
      }

      RecordWarcUniversal decorated = decorateRecord(item, result);
      downstream.onNext(decorated);
    } catch (Exception e) {
      log.error("Error extracting", e);
      Metrics.inc(METRIC_KEY, "errors");
      downstream.onNext(item);
    }
  }

  private record FullExtractionResult(String text, String contentType, String title) {
  }

  private FullExtractionResult extractFullContent(RecordWarcUniversal item) {
    String warcType = item.warcType();
    byte[] raw = item.rawBytes();
    byte[] body = "response".equalsIgnoreCase(warcType) ? WarcIO.getHttpPayload(raw) : WarcIO.getPayload(raw);

    if (body == null || body.length == 0) {
      Metrics.inc(METRIC_KEY, "empty-payload");
      return new FullExtractionResult(null, null, null);
    }

    // Identify real Content-Type
    String contentType = item.contentType();
    if ("response".equalsIgnoreCase(warcType)) {
      String httpContentType = parseHttpContentType(raw);
      if (httpContentType != null && !httpContentType.isEmpty()) {
        contentType = httpContentType;
      }
    }
    if (contentType == null)
      contentType = "";
    contentType = contentType.toLowerCase();

    String text = null;
    String title = null;

    if (contentType.contains("text/html") || contentType.contains("application/xhtml+xml")) {
      ExtractResult htmlRes = extractHtml(item.targetUri(), body);
      text = htmlRes.text;
      title = htmlRes.title;
    } else if (contentType.contains("text/plain")) {
      text = new String(body, StandardCharsets.UTF_8);
    } else if (contentType.contains("application/json") || contentType.contains("text/json")) {
      Metrics.inc(METRIC_KEY, "skipped-json");
      return new FullExtractionResult(null, contentType, null);
    } else if (usePdftotext && contentType.contains("application/pdf")) {
      text = extractPdftotext(body);
      if (text == null) {
        Metrics.inc(METRIC_KEY, "pdftotext-fallback-tika");
        text = extractTika(body, contentType);
      }
    } else {
      text = extractTika(body, contentType);
    }

    if (extractNormalize && text != null && !text.isBlank()) {
      text = normalizeText(text);
    }

    if (text == null || text.isBlank()) {
      Metrics.inc(METRIC_KEY, "empty-text");
      return new FullExtractionResult(null, contentType, title);
    }

    if (text.length() < minTextLength) {
      Metrics.inc(METRIC_KEY, "text-too-short");
      return new FullExtractionResult(null, contentType, title);
    }

    if (maxTextLength > 0 && text.length() > maxTextLength) {
      text = text.substring(0, maxTextLength);
      Metrics.inc(METRIC_KEY, "text-truncated");
    }

    return new FullExtractionResult(text, contentType, title);
  }

  private RecordWarcUniversal decorateRecord(RecordWarcUniversal item, FullExtractionResult result) {
    Map<String, String> newHeaders = new java.util.LinkedHashMap<>(item.headers());

    // WARC core headers for conversion
    newHeaders.put("WARC-Type", "conversion");

    // Use cached/parsed Content-Type
    if (result.contentType != null) {
      newHeaders.put("WARC-Identified-Content-Type", result.contentType);
    }

    // Update content headers for the extracted text body
    newHeaders.put("Content-Type", "text/plain; charset=utf-8");

    // Remove digests and other headers that are no longer valid for the extracted
    // text
    newHeaders.remove("WARC-Block-Digest");
    newHeaders.remove("WARC-Payload-Digest");
    newHeaders.remove("WARC-Segment-Number");
    newHeaders.remove("WARC-Segment-Total-Length");
    newHeaders.remove("WARC-Segment-Origin-ID");

    // Title header
    if (extractTitle && result.title != null && !result.title.isBlank()) {
      newHeaders.put(HEADER_TITLE, result.title);
    } else {
      newHeaders.remove(HEADER_TITLE);
    }

    byte[] textBytes = result.text.getBytes(StandardCharsets.UTF_8);
    newHeaders.put("Content-Length", String.valueOf(textBytes.length));

    RecordWarcUniversal res = new RecordWarcUniversal("conversion", newHeaders, textBytes);

    Metrics.inc(METRIC_KEY, "extracted");
    return res;
  }

  @Override
  public void onError(Throwable throwable) {
    downstream.onError(throwable);
  }

  @Override
  public void onComplete() {
    printTimingSummary();
    downstream.onComplete();
  }

  private record ExtractResult(String text, String title) {
  }

  private ExtractResult extractHtml(String url, byte[] body) {
    // Proposal #9: Single Rust FFI call handles readability + fallback + screen-reader
    if (useNativeReadability) {
      try {
        ReadabilityNative.FullExtractResult r = ReadabilityNative.extractFull(
            body, url, preserveHeadings, preserveLinks);

        String text = r.textContent();
        if (text != null && !text.isBlank()) {
          String metricKey = r.method() == 0
              ? "native-readability-success"
              : "native-fallback-success";
          Metrics.inc(METRIC_KEY, metricKey);
          return new ExtractResult(text, r.title());
        }

        // Rust returned nothing for both paths — extremely rare
        Metrics.inc(METRIC_KEY, "native-both-empty");
      } catch (Exception e) {
        if (e instanceof ReadabilityNative.ReadabilityException
            && e.getMessage() != null
            && e.getMessage().contains("Invalid UTF-8")) {
          Metrics.inc(METRIC_KEY, "native-invalid-utf8-fallback");
          log.debug("Native full extraction rejected invalid UTF-8; falling back to Java");
        } else {
          log.warn("Native full extraction failed, falling back to Java: {}", e.getMessage());
        }
      }
    }

    // Java fallback (only when native unavailable or Rust both-paths failed)
    if (body.length < SMALL_HTML_THRESHOLD) {
      Metrics.inc(METRIC_KEY, "small-html-shortcut");
      return extractWithJsoupFallback(url, body);
    }
    return extractWithJavaReadability(url, body);
  }

  // Timing accumulators for Java fallback path (thread-safe, instance to reset between runs)
  private java.util.concurrent.atomic.LongAdder javaStringConvertNs = new java.util.concurrent.atomic.LongAdder();
  private java.util.concurrent.atomic.LongAdder javaJsoupParse1Ns = new java.util.concurrent.atomic.LongAdder();
  private java.util.concurrent.atomic.LongAdder javaReadability4jNs = new java.util.concurrent.atomic.LongAdder();
  private java.util.concurrent.atomic.LongAdder javaJsoupParse2Ns = new java.util.concurrent.atomic.LongAdder();
  private java.util.concurrent.atomic.LongAdder javaTotalNs = new java.util.concurrent.atomic.LongAdder();
  private java.util.concurrent.atomic.LongAdder javaCallCount = new java.util.concurrent.atomic.LongAdder();

  /** Print timing summary (call at end of pipeline) */
  public void printTimingSummary() {
    long jCount = javaCallCount.sum();

    if (jCount > 0) {
      log.info(
          "JAVA TIMING ({} calls): total={}ms, r4j={}ms ({}%), jsoup1={}ms ({}%), jsoup2={}ms ({}%), strconv={}ms ({}%) | per-call: {}ms",
          jCount,
          javaTotalNs.sum() / 1_000_000.0,
          javaReadability4jNs.sum() / 1_000_000.0,
          100.0 * javaReadability4jNs.sum() / javaTotalNs.sum(),
          javaJsoupParse1Ns.sum() / 1_000_000.0,
          100.0 * javaJsoupParse1Ns.sum() / javaTotalNs.sum(),
          javaJsoupParse2Ns.sum() / 1_000_000.0,
          100.0 * javaJsoupParse2Ns.sum() / javaTotalNs.sum(),
          javaStringConvertNs.sum() / 1_000_000.0,
          100.0 * javaStringConvertNs.sum() / javaTotalNs.sum(),
          javaTotalNs.sum() / 1_000_000.0 / jCount);
    }
  }

  /**
   * OPT-P2-01: Extract screen-reader text via regex instead of Jsoup parse.
   * Matches elements with class containing screen-reader-text, sr-only, or visually-hidden.
   */
  private static String extractScreenReaderText(String html) {
    // Fast pre-check: the vast majority of pages have none of these class names.
    // String.contains() uses SIMD intrinsics and is orders of magnitude faster than regex.
    if (!html.contains("screen-reader-text") && !html.contains("sr-only") && !html.contains("visually-hidden")) {
      return "";
    }
    java.util.regex.Matcher m = SR_TEXT_PATTERN.matcher(html);
    StringBuilder sb = new StringBuilder();
    while (m.find()) {
      String t = m.group(1).trim();
      if (!t.isEmpty()) {
        sb.append(t).append(' ');
      }
    }
    return sb.toString();
  }

  /**
   * Extract using Java Readability4J (fallback).
   */
  private ExtractResult extractWithJavaReadability(String url, byte[] body) {
    long t0 = System.nanoTime();
    try {
      // Phase 1: String conversion
      long t1 = System.nanoTime();
      String html = new String(body, StandardCharsets.UTF_8);
      long t2 = System.nanoTime();
      javaStringConvertNs.add(t2 - t1);

      // Phase 2: Jsoup parse #1 (full document)
      Document originalDoc = Jsoup.parse(html, url != null ? url : "");
      long t3 = System.nanoTime();
      javaJsoupParse1Ns.add(t3 - t2);

      // Extract screen-reader text from original HTML (before Readability4J strips
      // it)
      List<String> screenReaderFragments = new java.util.ArrayList<>();
      for (var elem : originalDoc.select(".screen-reader-text, [class*=sr-only], [class*=visually-hidden]")) {
        String text = elem.text();
        if (!text.isBlank()) {
          screenReaderFragments.add(text);
        }
      }

      // Phase 3: Readability4J parse
      // OPT-P2-23: Inject shared RegExUtil to avoid per-record compilation storm.
      // Call full 8-arg constructor to resolve Java interop ambiguity.
      ReadabilityOptions options = new ReadabilityOptions();
      Readability4J readability = new Readability4J(
          url != null ? url : "",
          originalDoc,
          options,
          SHARED_REGEX,
          new Preprocessor(SHARED_REGEX),
          new MetadataParser(SHARED_REGEX),
          new ArticleGrabber(options, SHARED_REGEX),
          new Postprocessor());
      var article = readability.parse();
      long t4 = System.nanoTime();
      javaReadability4jNs.add(t4 - t3);

      String title = article.getTitle();

      // Get article HTML content and post-process to catch any remaining nav
      String articleHtml = article.getContent();
      if (articleHtml == null || articleHtml.isBlank()) {
        // If Readability4J found nothing, return screen-reader text if available
        javaTotalNs.add(System.nanoTime() - t0);
        javaCallCount.increment();
        String accessibilityOnly = prependMissingFragments("", screenReaderFragments).trim();
        return new ExtractResult(accessibilityOnly.isEmpty() ? null : accessibilityOnly, title);
      }

      // Phase 4: Jsoup parse #2 (article content)
      Document articleDoc = Jsoup.parse(articleHtml);
      long t5 = System.nanoTime();
      javaJsoupParse2Ns.add(t5 - t4);

      articleDoc.select("nav, header, footer, aside, [class*=menu], [class*=nav], .skip-link").remove();
      String text = articleDoc.text();

      text = prependMissingFragments(text, screenReaderFragments);

      javaTotalNs.add(System.nanoTime() - t0);
      javaCallCount.increment();
      return new ExtractResult(text, title);
    } catch (Exception _) {
      return new ExtractResult(null, null);
    }
  }

  private ExtractResult extractWithJsoupFallback(String url, byte[] body) {
    try {
      String html = new String(body, StandardCharsets.UTF_8);
      Document doc = Jsoup.parse(html);

      // Extract screen-reader and accessibility text BEFORE removing elements
      List<String> accessibilityFragments = new java.util.ArrayList<>();
      for (var elem : doc.select(".screen-reader-text, [class*=sr-only], [class*=visually-hidden]")) {
        String text = elem.text();
        if (!text.isBlank()) {
          accessibilityFragments.add(text);
        }
      }

      // Remove obvious boilerplate (but NOT screen-reader elements - already
      // extracted)
      doc.select("script, style, nav, header, footer, aside, " +
          "[role=navigation], [role=banner], " +
          "[class*=menu], [class*=nav], [id*=menu], [id*=nav], " +
          ".skip-link, .cookie, .advertisement").remove();

      // Extract title
      String title = doc.select("title").text();
      if (title.isEmpty()) {
        var h1 = doc.select("h1").first();
        if (h1 != null) {
          title = h1.text();
        }
      }

      // Build text from semantic elements
      StringBuilder text = new StringBuilder();

      // Preserve headings if configured
      if (preserveHeadings) {
        for (var heading : doc.select("h1, h2, h3, h4, h5, h6")) {
          String headingText = heading.text();
          if (!headingText.isBlank()) {
            text.append(headingText).append("\n");
          }
        }
      }

      // Preserve meaningful links if configured
      if (preserveLinks) {
        for (var link : doc.select("a[href]")) {
          String linkText = link.text();
          if (linkText.length() > 2) { // Skip empty/icon links
            text.append(linkText).append(" ");
          }
        }
      }

      // Use the first semantic tier with substantial content. A combined selector
      // would append nested containers (for example main > article) more than once.
      String semanticText = "";
      for (String selector : List.of("main", "article", "[role=main]", ".content", ".entry-content")) {
        StringBuilder candidate = new StringBuilder();
        for (var main : doc.select(selector)) {
          String mainText = main.text();
          if (!mainText.isBlank()) {
            candidate.append(mainText).append(" ");
          }
        }
        if (candidate.toString().trim().length() >= 50) {
          semanticText = candidate.toString();
          break;
        }
      }

      if (semanticText.isBlank()) {
        semanticText = doc.body().text();
      }
      text.append(semanticText);

      return new ExtractResult(prependMissingFragments(text.toString().trim(), accessibilityFragments), title);
    } catch (Exception _) {
      return new ExtractResult(null, null);
    }
  }

  private String extractTika(byte[] body, String contentTypeHint) {
    try (InputStream is = new ByteArrayInputStream(body)) {
      if (log.isDebugEnabled()) {
        log.debug("Attempting Tika extraction for record body length: {}", body.length);
      }
      // OPT-P2-19: Pass content-type hint so Tika skips format detection
      if (contentTypeHint != null && !contentTypeHint.isBlank()) {
        org.apache.tika.metadata.Metadata metadata = new org.apache.tika.metadata.Metadata();
        metadata.set(org.apache.tika.metadata.HttpHeaders.CONTENT_TYPE, contentTypeHint);
        return tika.parseToString(is, metadata);
      }
      return tika.parseToString(is);
    } catch (Throwable e) {
      log.error("Tika extraction failed: {}", e.getMessage(), e);
      return null;
    }
  }

  private boolean isPdftotextAvailable() {
    Process process = null;
    try {
      process = new ProcessBuilder(pdftotextPath, "-v")
          .redirectErrorStream(true)
          .redirectOutput(ProcessBuilder.Redirect.DISCARD)
          .start();
      if (!process.waitFor(pdftotextTimeoutSeconds, TimeUnit.SECONDS)) {
        log.warn("pdftotext availability probe timed out after {}s", pdftotextTimeoutSeconds);
        return false;
      }
      return process.exitValue() == 0;
    } catch (InterruptedException e) {
      // Restore interrupt status so callers and shutdown machinery see the signal.
      Thread.currentThread().interrupt();
      return false;
    } catch (IOException e) {
      return false;
    } finally {
      terminateProcess(process);
    }
  }

  private String extractPdftotext(byte[] pdfBytes) {
    boolean permitAcquired = false;
    if (pdftotextSemaphore != null) {
      try {
        pdftotextSemaphore.acquire();
        permitAcquired = true;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return null;
      }
    }
    Path pdfTmp = null;
    Path outputTmp = null;
    Process process = null;
    try {
      pdfTmp = Files.createTempFile("warc-pdf-", ".pdf");
      outputTmp = Files.createTempFile("warc-pdftotext-", ".txt");
      Files.write(pdfTmp, pdfBytes);
      // Merge stderr into stdout so that reading stdout never deadlocks while
      // pdftotext tries to write >64 KB of diagnostics to a full stderr pipe buffer.
      // Redirecting to a file also prevents a full pipe from blocking the child.
      process = new ProcessBuilder(pdftotextPath, "-enc", "UTF-8", "-nopgbrk", pdfTmp.toString(), "-")
          .redirectErrorStream(true)
          .redirectOutput(outputTmp.toFile())
          .start();
      if (!process.waitFor(pdftotextTimeoutSeconds, TimeUnit.SECONDS)) {
        log.warn("pdftotext extraction timed out after {}s", pdftotextTimeoutSeconds);
        Metrics.inc(METRIC_KEY, "pdftotext-failed");
        return null;
      }

      if (process.exitValue() != 0) {
        Metrics.inc(METRIC_KEY, "pdftotext-failed");
        return null;
      }

      byte[] output;
      try (InputStream in = Files.newInputStream(outputTmp)) {
        output = in.readNBytes(PDFTOTEXT_MAX_OUTPUT_BYTES + 1);
      }
      if (output.length > PDFTOTEXT_MAX_OUTPUT_BYTES) {
        log.warn("pdftotext output exceeded {} bytes", PDFTOTEXT_MAX_OUTPUT_BYTES);
        Metrics.inc(METRIC_KEY, "pdftotext-failed");
        return null;
      }

      String text = new String(output, StandardCharsets.UTF_8);
      if (text.isBlank()) {
        Metrics.inc(METRIC_KEY, "pdftotext-failed");
        return null;
      }
      Metrics.inc(METRIC_KEY, "pdftotext-success");
      return text;
    } catch (IOException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      log.warn("pdftotext extraction failed: {}", e.getMessage());
      Metrics.inc(METRIC_KEY, "pdftotext-failed");
      return null;
    } finally {
      terminateProcess(process);
      if (pdfTmp != null) {
        try { Files.deleteIfExists(pdfTmp); } catch (IOException ignored) {}
      }
      if (outputTmp != null) {
        try { Files.deleteIfExists(outputTmp); } catch (IOException ignored) {}
      }
      if (permitAcquired) {
        pdftotextSemaphore.release();
      }
    }
  }

  private void terminateProcess(Process process) {
    if (process == null || !process.isAlive()) {
      return;
    }
    process.destroyForcibly();
    try {
      if (!process.waitFor(5, TimeUnit.SECONDS)) {
        log.warn("pdftotext child did not exit after forced destruction");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private String normalizeText(String text) {
    if (text == null)
      return null;
    String normalized = Normalizer.normalize(text, Normalizer.Form.NFKC);
    // OPT-P3: Replace regex with direct StringBuilder loop for whitespace collapse
    normalized = collapseWhitespace(normalized);
    return normalized;
  }

  /**
   * Collapse consecutive whitespace into single space, trim leading/trailing.
   * OPT-P3: ~5x faster than Pattern.matcher().replaceAll() for typical text.
   */
  private static String collapseWhitespace(String text) {
    int len = text.length();
    StringBuilder sb = new StringBuilder(len);
    boolean lastWasSpace = true; // Start true to trim leading whitespace
    for (int i = 0; i < len; i++) {
      char c = text.charAt(i);
      if (Character.isWhitespace(c)) {
        if (!lastWasSpace) {
          sb.append(' ');
          lastWasSpace = true;
        }
      } else {
        sb.append(c);
        lastWasSpace = false;
      }
    }
    // Trim trailing space if present
    int sbLen = sb.length();
    if (sbLen > 0 && sb.charAt(sbLen - 1) == ' ') {
      sb.setLength(sbLen - 1);
    }
    return sb.toString();
  }

  private static String prependMissingFragments(String text, List<String> fragments) {
    String normalizedAvailable = collapseWhitespace(text);
    StringBuilder prefix = new StringBuilder();
    for (String fragment : fragments) {
      String normalizedFragment = collapseWhitespace(fragment);
      if (normalizedFragment.isEmpty() || normalizedAvailable.contains(normalizedFragment)) {
        continue;
      }
      prefix.append(fragment.trim()).append(' ');
      normalizedAvailable = normalizedAvailable.isEmpty()
          ? normalizedFragment
          : normalizedAvailable + " " + normalizedFragment;
    }
    return prefix.append(text).toString();
  }

  /**
   * Extract HTTP Content-Type from inside a WARC response record's payload.
   * The payload contains HTTP status line + headers + body.
   */
  private String parseHttpContentType(byte[] raw) {
    // Find end of WARC headers (CRLF CRLF)
    int payloadStart = -1;
    for (int i = 0; i < raw.length - 3; i++) {
      if (raw[i] == '\r' && raw[i + 1] == '\n' && raw[i + 2] == '\r' && raw[i + 3] == '\n') {
        payloadStart = i + 4;
        break;
      }
    }
    if (payloadStart < 0 || payloadStart >= raw.length)
      return null;

    // Find end of HTTP headers within payload
    int httpHeaderEnd = -1;
    for (int i = payloadStart; i < raw.length - 3; i++) {
      if (raw[i] == '\r' && raw[i + 1] == '\n' && raw[i + 2] == '\r' && raw[i + 3] == '\n') {
        httpHeaderEnd = i;
        break;
      }
    }
    if (httpHeaderEnd < 0)
      return null;

    // Parse HTTP headers to find Content-Type

    // Skip Status Line
    int i = payloadStart;
    while (i < httpHeaderEnd && raw[i] != '\n') {
      i++;
    }
    if (i < httpHeaderEnd)
      i++; // skip \n

    // Scan headers
    while (i < httpHeaderEnd) {
      int lineStart = i;
      int lineEnd = -1;

      // Find end of line
      for (int k = i; k < httpHeaderEnd; k++) {
        if (raw[k] == '\n') {
          lineEnd = k;
          break;
        }
      }
      if (lineEnd == -1)
        lineEnd = httpHeaderEnd;

      // Move next line start
      i = lineEnd + 1;

      // Trim CR from lineEnd if present
      if (lineEnd > lineStart && raw[lineEnd - 1] == '\r') {
        lineEnd--;
      }

      // Check if starts with content-type:
      // "content-type:".length() == 13
      int len = lineEnd - lineStart;
      if (len > 13 && isContentType(raw, lineStart)) {
        // Extract value
        int valStart = lineStart + 13;
        // Trim leading spaces
        while (valStart < lineEnd && (raw[valStart] == ' ' || raw[valStart] == '\t')) {
          valStart++;
        }

        // Extract string
        String val = new String(raw, valStart, lineEnd - valStart, StandardCharsets.ISO_8859_1);

        // Process semicolon
        int semi = val.indexOf(';');
        if (semi > 0)
          val = val.substring(0, semi).trim();
        return val;
      }
    }
    return null;

  }

  private boolean isContentType(byte[] raw, int offset) {
    // "Content-Type:" or "content-type:" case insensitive check
    // C=67, c=99. Difference is 32.
    // We expect "content-type:"
    byte[] target = "content-type:".getBytes(StandardCharsets.US_ASCII);
    for (int j = 0; j < target.length; j++) {
      byte b = raw[offset + j];
      if (b >= 'A' && b <= 'Z')
        b += 32; // to lower
      if (b != target[j])
        return false;
    }
    return true;
  }
}
