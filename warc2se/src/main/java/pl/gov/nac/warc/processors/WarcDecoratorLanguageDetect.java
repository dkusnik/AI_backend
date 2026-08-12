package pl.gov.nac.warc.processors;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Flow;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tika.language.detect.LanguageDetector;
import org.apache.tika.language.detect.LanguageResult;

import pl.gov.nac.warc.reactive.Metrics;
import pl.gov.nac.warc.reactive.ReactiveInterfaces;
import pl.gov.nac.warc.records.Record;
import pl.gov.nac.warc.records.warc.RecordWarcUniversal;
import pl.gov.nac.warc.utils.WarcIO;

/**
 * Detects language of extracted text content using Apache Tika.
 * Decorates RecordWarcUniversal with language headers.
 *
 * - Adds WARC-Identified-Content-Language header
 * - Adds WARC-Language-Confidence header
 * - Passes through warcinfo and metadata records untouched
 *
 * Input: RecordWarcUniversal
 * Output: RecordWarcUniversal (decorated with language headers)
 */
public class WarcDecoratorLanguageDetect
    implements ReactiveInterfaces.ReactiveProcessor<RecordWarcUniversal, RecordWarcUniversal> {

  private static final Logger log = LogManager.getLogger(WarcDecoratorLanguageDetect.class);
  private static final String METRIC_KEY = "lang-decorator";
  private static final int DEFAULT_MIN_TEXT_LENGTH = 50;
  private static final float DEFAULT_CONFIDENCE_THRESHOLD = 0.8f;

  private Flow.Subscriber<? super RecordWarcUniversal> downstream;

  // ThreadLocal so each virtual thread gets its own LanguageDetector instance.
  // LanguageDetector.detect() mutates internal probability state and is not
  // documented as thread-safe; sharing a single instance across concurrent
  // virtual threads can produce wrong language results.
  private ThreadLocal<LanguageDetector> detectorLocal;

  private int minTextLength = DEFAULT_MIN_TEXT_LENGTH;
  private float confidenceThreshold = DEFAULT_CONFIDENCE_THRESHOLD;

  // fastText worker pool — N persistent processes, each handles one request at a time
  private record FasttextWorker(Process process, BufferedWriter stdin, BufferedReader stdout) {}
  private boolean useFasttext = false;
  private String fasttextPath = "fasttext";
  private String fasttextModelPath = "";
  private int fasttextProcessCount = 3;
  private BlockingQueue<FasttextWorker> workerPool;

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
  public void configure(Map<String, Object> cfg) {
    Metrics.setModuleHeader(METRIC_KEY, "Language Detector Decorator");

    minTextLength = getInt(cfg, "min-text-length", DEFAULT_MIN_TEXT_LENGTH);
    confidenceThreshold = getFloat(cfg, "confidence-threshold", DEFAULT_CONFIDENCE_THRESHOLD);
    useFasttext = getBoolean(cfg, "use-fasttext", false);
    fasttextPath = getString(cfg, "fasttext-path", "fasttext");
    fasttextModelPath = getString(cfg, "fasttext-model-path", "");

    fasttextProcessCount = getInt(cfg, "fasttext-process-count", 3);

    if (useFasttext) {
      if (fasttextModelPath.isBlank()) {
        log.warn("use-fasttext=true but fasttext-model-path is not set — falling back to Tika detector");
        useFasttext = false;
      } else {
        try {
          workerPool = new ArrayBlockingQueue<>(fasttextProcessCount);
          for (int i = 0; i < fasttextProcessCount; i++) {
            workerPool.put(startFasttextWorker());
          }
          log.info("fastText worker pool started: {} workers, model={}", fasttextProcessCount, fasttextModelPath);
        } catch (IOException | InterruptedException e) {
          log.warn("Failed to start fastText workers: {} — falling back to Tika detector", e.getMessage());
          shutdownWorkerPool();
          useFasttext = false;
        }
      }
    }

    if (!useFasttext) {
      // Initialise ThreadLocal and eagerly load models on this thread to move
      // the startup cost off the critical path.
      detectorLocal = ThreadLocal.withInitial(() -> {
        try {
          return LanguageDetector.getDefaultLanguageDetector().loadModels();
        } catch (IOException e) {
          throw new RuntimeException("Failed to load Tika language models", e);
        }
      });
      detectorLocal.get(); // eager load on configure() thread
      long start = System.currentTimeMillis();
      log.info("Language models loaded in {}ms. minTextLength={}, confidenceThreshold={}",
          (System.currentTimeMillis() - start), minTextLength, confidenceThreshold);
    }
  }

  private FasttextWorker startFasttextWorker() throws IOException {
    Process p = new ProcessBuilder(fasttextPath, "predict", fasttextModelPath, "-")
        .redirectErrorStream(false)
        .start();
    BufferedWriter in = new BufferedWriter(new OutputStreamWriter(p.getOutputStream(), StandardCharsets.UTF_8));
    BufferedReader out = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8));
    return new FasttextWorker(p, in, out);
  }

  private void shutdownWorkerPool() {
    if (workerPool == null) return;
    List<FasttextWorker> workers = new ArrayList<>();
    workerPool.drainTo(workers);
    for (FasttextWorker w : workers) {
      try { w.stdin().close(); } catch (IOException ignored) {}
      w.process().destroyForcibly();
    }
  }

  private int getInt(Map<String, Object> cfg, String key, int def) {
    Object v = cfg.get(key);
    if (v instanceof Number n)
      return n.intValue();
    if (v instanceof String s) {
      try {
        return Integer.parseInt(s);
      } catch (Exception _) {
        // Expected for non-numeric strings
      }
    }
    return def;
  }

  private float getFloat(Map<String, Object> cfg, String key, float def) {
    Object v = cfg.get(key);
    if (v instanceof Number n)
      return n.floatValue();
    if (v instanceof String s) {
      try {
        return Float.parseFloat(s);
      } catch (Exception _) {
        // Expected for non-numeric strings
      }
    }
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
      Metrics.inc(METRIC_KEY, "recordsIn");

      // Pass through warcinfo and metadata records untouched
      String warcType = item.warcType();
      if ("warcinfo".equalsIgnoreCase(warcType) || "metadata".equalsIgnoreCase(warcType)) {
        downstream.onNext(item);
        return;
      }

      byte[] raw = item.rawBytes();
      if (raw == null || raw.length == 0) {
        Metrics.inc(METRIC_KEY, "empty-body");
        downstream.onNext(item);
        return;
      }

      // Extract payload text based on record type
      // For conversion records (from extract-text), raw bytes IS the text content
      // For response records, need to strip HTTP headers
      // For other types, use generic payload extraction
      byte[] payload;
      if ("conversion".equalsIgnoreCase(warcType)) {
        payload = raw; // Already plain text from text extraction
      } else if ("response".equalsIgnoreCase(warcType)) {
        payload = WarcIO.getHttpPayload(raw);
      } else {
        payload = WarcIO.getPayload(raw);
      }

      if (payload == null || payload.length == 0) {
        Metrics.inc(METRIC_KEY, "empty-payload");
        downstream.onNext(item);
        return;
      }

      String text = new String(payload, StandardCharsets.UTF_8);

      if (text.length() < minTextLength) {
        Metrics.inc(METRIC_KEY, "text-too-short");
        // Still decorate with "und/short" to indicate the text was too short
        RecordWarcUniversal result = decorateWithLanguage(item, "und/short", "0.00");
        downstream.onNext(result);
        return;
      }

      // Detect language
      String langValue;
      String confValue;
      if (useFasttext) {
        String[] ft = detectWithFasttext(text);
        if (ft != null) {
          float confidence = Float.parseFloat(ft[1]);
          langValue = confidence >= confidenceThreshold ? ft[0] : "und/" + ft[0];
          confValue = ft[1];
        } else {
          Metrics.inc(METRIC_KEY, "fasttext-failed");
          langValue = "und";
          confValue = "0.00";
        }
      } else {
        LanguageResult langResult = detectorLocal.get().detect(text);
        String lang = langResult.getLanguage();
        float confidence = langResult.getRawScore();
        boolean certain = langResult.isReasonablyCertain() && confidence >= confidenceThreshold;
        langValue = certain ? lang : "und/" + lang;
        confValue = String.format("%.2f", confidence);
      }

      RecordWarcUniversal result = decorateWithLanguage(item, langValue, confValue);
      Metrics.inc(METRIC_KEY, langValue);
      downstream.onNext(result);

    } catch (Exception e) {
      log.error("Error detecting language", e);
      Metrics.inc(METRIC_KEY, "errors");
      downstream.onNext(item);
    }
  }

  private RecordWarcUniversal decorateWithLanguage(RecordWarcUniversal item, String lang, String confidence) {
    Map<String, String> newHeaders = new java.util.LinkedHashMap<>(item.headers());
    newHeaders.put("WARC-Identified-Content-Language", lang);
    newHeaders.put("WARC-Language-Confidence", confidence);

    Metrics.inc(METRIC_KEY, "decorated");
    return new RecordWarcUniversal(item.warcType(), newHeaders, item.rawBytes());
  }

  /**
   * Detect language using a pooled persistent fastText subprocess.
   * Acquires one worker from the pool, sends one line, reads one response, returns worker.
   *
   * @return [langCode, confidenceString] or null on failure
   */
  private String[] detectWithFasttext(String text) {
    FasttextWorker worker;
    try {
      worker = workerPool.take();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    }
    // Track whether the original 'worker' was removed from service (died or replaced).
    // If true, the finally block must NOT return it to the pool.
    boolean replaced = false;
    try {
      // fastText is line-oriented — collapse text to a single line
      String line = text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ');
      worker.stdin().write(line);
      worker.stdin().newLine();
      worker.stdin().flush();
      String response = worker.stdout().readLine();
      if (response == null) {
        // Worker process died — attempt replacement
        log.warn("fastText worker died, attempting replacement");
        try { worker.stdin().close(); } catch (IOException ignored) {}
        worker.process().destroyForcibly();
        replaced = true; // do not return dead worker regardless of what follows
        try {
          // offer() returns false when the queue is at capacity; in that case
          // the newly started worker must be killed explicitly, otherwise it leaks
          // (its stdin/stdout handles stay open and the OS process runs until JVM exit).
          FasttextWorker replacement = startFasttextWorker();
          if (!workerPool.offer(replacement)) {
            replacement.process().destroyForcibly();
          }
        } catch (IOException e2) {
          log.warn("Could not start replacement fastText worker: {} — pool shrinks by 1",
              e2.getMessage());
        }
        return null;
      }
      // Format: "__label__pl 0.999123"
      String[] parts = response.trim().split("\\s+", 2);
      if (parts.length < 2 || !parts[0].startsWith("__label__")) {
        log.warn("Unexpected fastText output: {}", response);
        return null;
      }
      String lang = parts[0].substring("__label__".length());
      String conf = String.format("%.2f", Float.parseFloat(parts[1]));
      Metrics.inc(METRIC_KEY, "fasttext-" + lang);
      return new String[]{lang, conf};
    } catch (IOException | NumberFormatException e) {
      log.warn("fastText detection failed: {}", e.getMessage());
      return null;
    } finally {
      if (!replaced) {
        workerPool.offer(worker);
      }
    }
  }

  private boolean getBoolean(Map<String, Object> cfg, String key, boolean def) {
    Object v = cfg.get(key);
    if (v instanceof Boolean b) return b;
    if (v instanceof String s) return Boolean.parseBoolean(s);
    return def;
  }

  private String getString(Map<String, Object> cfg, String key, String def) {
    Object v = cfg.get(key);
    if (v instanceof String s) return s;
    if (v != null) return v.toString();
    return def;
  }

  @Override
  public void onError(Throwable throwable) {
    shutdownWorkerPool();
    downstream.onError(throwable);
  }

  @Override
  public void onComplete() {
    shutdownWorkerPool();
    downstream.onComplete();
  }
}
