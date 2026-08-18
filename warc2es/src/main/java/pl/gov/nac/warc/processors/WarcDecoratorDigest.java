package pl.gov.nac.warc.processors;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;

import org.apache.commons.codec.digest.Blake3;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.openhft.hashing.LongTupleHashFunction;
import pl.gov.nac.warc.reactive.Metrics;
import pl.gov.nac.warc.reactive.ReactiveInterfaces;
import pl.gov.nac.warc.records.Record;
import pl.gov.nac.warc.records.warc.RecordWarcUniversal;
import pl.gov.nac.warc.utils.WarcIO;

/**
 * Computes digest of WARC record payload and adds it to headers.
 * Supports multiple algorithms: ssha1, sha256, sha512, blake3, xxh128.
 *
 * Input: RecordWarcUniversal
 * Output: RecordWarcUniversal (decorated with WARC-Payload-Digest)
 */
public class WarcDecoratorDigest
    implements ReactiveInterfaces.ReactiveProcessor<RecordWarcUniversal, RecordWarcUniversal> {

  private static final Logger log = LogManager.getLogger(WarcDecoratorDigest.class);
  private static final String METRIC_KEY = "digest-decorator";
  private Flow.Subscriber<? super RecordWarcUniversal> downstream;

  private static final String XXH128 = "xxh128";
  private String algorithm = XXH128;
  private ThreadLocal<MessageDigest> threadLocalDigest;

  @Override
  public List<Class<? extends Record>> acceptedInputTypes() {
    return List.of(RecordWarcUniversal.class);
  }

  @Override
  public List<Class<? extends Record>> emittedOutputTypes() {
    return List.of(RecordWarcUniversal.class);
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
    Metrics.setModuleHeader(METRIC_KEY, "WARC/WET Payload Digester");
    this.algorithm = getString(cfg, "digest", XXH128).toLowerCase();

    if (isJdkAlgorithm(algorithm)) {
      String jdkName = switch (algorithm) {
        case "sha1", "ssha1" -> "SHA-1";
        case "sha256" -> "SHA-256";
        case "sha512" -> "SHA-512";
        case "sha512_256", "sha512-256" -> "SHA-512/256";
        default -> "SHA-256";
      };
      threadLocalDigest = ThreadLocal.withInitial(() -> {
        try {
          return MessageDigest.getInstance(jdkName);
        } catch (NoSuchAlgorithmException e) {
          log.error(() -> jdkName + " not available", e);
          throw new IllegalStateException(jdkName + " not available", e);
        }
      });
    }
  }

  private boolean isJdkAlgorithm(String alg) {
    return List.of("sha1", "ssha1", "sha256", "sha512", "sha512_256", "sha512-256").contains(alg);
  }

  private String getString(Map<String, Object> cfg, String key, String def) {
    Object v = cfg.get(key);
    return v != null ? v.toString() : def;
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

      // Skip non-payload records if needed?
      // For now, digest everything that has a body.
      byte[] raw = item.rawBytes();
      if (raw == null || raw.length == 0) {
        Metrics.inc(METRIC_KEY, "skipped");
        downstream.onNext(item);
        return;
      }

      // Correctly extract payload for digest
      byte[] payload = "response".equalsIgnoreCase(item.warcType())
          ? WarcIO.getHttpPayload(raw)
          : WarcIO.getPayload(raw);

      if (payload.length == 0) {
        Metrics.inc(METRIC_KEY, "skipped");
        downstream.onNext(item);
        return;
      }

      String digestValue = computeDigest(payload);
      String headerName = "WARC-Payload-Digest";

      // Decorate record
      Map<String, String> newHeaders = new java.util.LinkedHashMap<>(item.headers());
      newHeaders.put(headerName, algorithm + ":" + digestValue);

      RecordWarcUniversal result = new RecordWarcUniversal(item.warcType(), newHeaders, item.rawBytes());

      Metrics.inc(METRIC_KEY, "digested");
      downstream.onNext(result);
    } catch (Exception e) {
      Metrics.inc(METRIC_KEY, "errors");
      log.error("Error calculating digest", e);
      downstream.onNext(item);
    }
  }

  private String computeDigest(byte[] data) {
    if (data == null) {
      return "";
    }
    if (isJdkAlgorithm(algorithm)) {
      byte[] hash = threadLocalDigest.get().digest(data);
      return HexFormat.of().formatHex(hash);
    } else if ("blake3".equals(algorithm)) {
      byte[] hash = Blake3.hash(data);
      return HexFormat.of().formatHex(hash);
    } else if (XXH128.equals(algorithm)) {
      // XX128 returns 16 bytes (2 longs)
      long[] hash = LongTupleHashFunction.xx128().hashBytes(data);
      return String.format("%016x%016x", hash[0], hash[1]);
    }
    return "";
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
