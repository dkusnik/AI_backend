package pl.gov.nac.warc.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;
import java.util.zip.GZIPInputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.netpreserve.jwarc.WarcReader;

import pl.gov.nac.warc.processors.WarcAccumulatorDeduplicateDoet;
import pl.gov.nac.warc.records.warc.RecordWarcUniversal;

class PerDaySplitTest {

  @TempDir
  Path tempDir;

  @Test
  void fixtureContainsRecordsFromTwoCaptureDates() throws Exception {
    Path fixture = Path.of("src/test/resources/multi-day.warc.gz");
    List<String> dates = new ArrayList<>();
    try (InputStream input = new GZIPInputStream(Files.newInputStream(fixture));
        WarcReader reader = new WarcReader(input)) {
      while (true) {
        var record = reader.next();
        if (record.isEmpty()) {
          break;
        }
        dates.add(record.orElseThrow().date().toString().substring(0, 10));
      }
    }
    assertEquals(List.of("2026-01-01", "2026-01-02"), dates);
  }

  @Test
  void perDayOptionEmitsOneRecordLevelDateBucketPerDay() {
    WarcAccumulatorDeduplicateDoet processor = new WarcAccumulatorDeduplicateDoet();
    processor.configure(Map.of(
        "rocksdb-path", tempDir.resolve("per-day-rocksdb").toString(),
        "bucket-prefix", "ignored-in-per-day-mode",
        "per-day", true));

    CollectingSubscriber subscriber = new CollectingSubscriber();
    processor.subscribe(subscriber);
    processor.onSubscribe(new NoOpSubscription());

    processor.onNext(record("sha256:first", "https://example.test/first",
        "2026-01-01T23:59:59Z", "first"));
    processor.onNext(record("sha256:second", "https://example.test/second",
        "2026-01-02T00:00:01Z", "second"));
    processor.onComplete();

    assertEquals(List.of("20260101", "20260102"), subscriber.items.stream()
        .map(record -> record.headers().get("X-Source-Warc"))
        .toList());
    assertEquals(List.of("2026-01-01", "2026-01-02"), subscriber.items.stream()
        .map(record -> record.headers().get("x-nac-crawl-first-date"))
        .toList());
    assertEquals(List.of("2026-01-01", "2026-01-02"), subscriber.items.stream()
        .map(record -> record.headers().get("x-nac-crawl-last-date"))
        .toList());
  }

  private static RecordWarcUniversal record(String digest, String uri, String date, String content) {
    Map<String, String> headers = new LinkedHashMap<>();
    headers.put("WARC-Type", "conversion");
    headers.put("WARC-Target-URI", uri);
    headers.put("WARC-Date", date);
    headers.put("WARC-Block-Digest", digest);
    headers.put("Content-Type", "text/plain; charset=utf-8");

    RecordWarcUniversal record = new RecordWarcUniversal(
        "conversion", headers, content.getBytes(StandardCharsets.UTF_8));
    return RecordWarcUniversal.fromRaw(pl.gov.nac.warc.utils.WarcIO.toWarcBytes(record));
  }

  private static final class CollectingSubscriber implements Flow.Subscriber<Object> {
    private final List<RecordWarcUniversal> items = new ArrayList<>();

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      subscription.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(Object item) {
      if (item instanceof RecordWarcUniversal record) {
        items.add(record);
      }
    }

    @Override
    public void onError(Throwable throwable) {
      throw new AssertionError(throwable);
    }

    @Override
    public void onComplete() {
    }
  }

  private static final class NoOpSubscription implements Flow.Subscription {
    @Override
    public void request(long n) {
    }

    @Override
    public void cancel() {
    }
  }
}
