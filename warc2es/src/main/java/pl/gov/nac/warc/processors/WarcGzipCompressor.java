package pl.gov.nac.warc.processors;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;
import java.util.zip.GZIPOutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.gov.nac.warc.reactive.Metrics;
import pl.gov.nac.warc.reactive.ReactiveInterfaces;
import pl.gov.nac.warc.records.Record;
import pl.gov.nac.warc.records.warc.RecordCompressed;
import pl.gov.nac.warc.records.warc.RecordWarcUniversal;
import pl.gov.nac.warc.utils.WarcIO;

/**
 * Parallel GZIP compressor that converts RecordWarcUniversal into
 * RecordCompressed.
 * This shifts the compression work from the serial Consumer to parallel worker
 * threads.
 * Leveraging concatenated GZIP (RFC 1952) to scale compression.
 */
public class WarcGzipCompressor
    implements ReactiveInterfaces.ReactiveProcessor<RecordWarcUniversal, RecordCompressed> {

  private static final Logger log = LogManager.getLogger(WarcGzipCompressor.class);
  private static final String METRIC_KEY = "parallel-gzip";
  private Flow.Subscriber<? super RecordCompressed> downstream;

  private int compressionLevel = 6;

  private final ThreadLocal<ByteArrayOutputStream> threadLocalBaos = ThreadLocal
      .withInitial(() -> new ByteArrayOutputStream(64 * 1024));

  @Override
  public List<Class<? extends Record>> acceptedInputTypes() {
    return List.of(RecordWarcUniversal.class);
  }

  @Override
  public List<Class<? extends Record>> emittedOutputTypes() {
    return List.of(RecordCompressed.class);
  }

  @Override
  public boolean isEnabled(Map<String, Object> cfg) {
    Object v = cfg.get("parallel-gzip");
    if (v instanceof Boolean b)
      return b;
    if (v instanceof String s)
      return Boolean.parseBoolean(s);
    return false;
  }

  @Override
  public void configure(Map<String, Object> cfg) {
    Metrics.setModuleHeader(METRIC_KEY, "Parallel GZIP Compressor");
    Object level = cfg.get("compression-level");
    if (level instanceof Number n) {
      this.compressionLevel = n.intValue();
    } else if (level instanceof String s) {
      this.compressionLevel = Integer.parseInt(s);
    }
    log.debug("Parallel GZIP compression level: {}", compressionLevel);
  }

  @Override
  public void subscribe(Flow.Subscriber<? super RecordCompressed> subscriber) {
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

      ByteArrayOutputStream baos = threadLocalBaos.get();
      baos.reset();

      // Compress into a standalone GZIP member
      try (GZIPOutputStream gzos = new GZIPOutputStream(baos) {
        {
          def.setLevel(compressionLevel);
        }
      }) {
        WarcIO.writeWarcRecord(item, gzos);
        gzos.finish();
      }

      byte[] compressed = baos.toByteArray();

      // Extract metadata for CDX generation in Consumer
      String provenance = item.headers().get("NAC-Merge-Result");
      if (provenance == null)
        provenance = item.headers().get("nac-merge-result");

      String source = item.headers().get("X-Source-Warc");

      RecordCompressed result = new RecordCompressed(
          compressed,
          item.targetUri(),
          item.warcDate(),
          item.contentType(),
          item.warcType(),
          item.digest(),
          provenance,
          source);

      Metrics.inc(METRIC_KEY, "compressed");
      Metrics.add(METRIC_KEY, "bytesOut", compressed.length);
      downstream.onNext(result);

    } catch (IOException e) {
      Metrics.inc(METRIC_KEY, "errors");
      log.error("Error during parallel GZIP compression", e);
      onError(e);
    }
  }

  @Override
  public void onError(Throwable throwable) {
    downstream.onError(throwable);
  }

  @Override
  public void onComplete() {
    downstream.onComplete();
  }
}
