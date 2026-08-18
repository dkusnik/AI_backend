package pl.gov.nac.warc.processors;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;

import org.junit.jupiter.api.Test;

import pl.gov.nac.warc.records.warc.RecordWarcUniversal;

public class WarcFilterTest {

  @Test
  public void testAllowDenyPassMode() {
    WarcFilter filter = new WarcFilter();
    filter.configure(Map.of(
        "mode", "allow-deny-pass",
        "allow-warc-types", "response,metadata",
        "deny-mime-types", "image/jpeg"));

    TestSubscriber subscriber = new TestSubscriber();
    filter.subscribe(subscriber);
    filter.onSubscribe(new NoOpSubscription());

    // 1. Matches allow, does not match deny -> KEEP
    filter.onNext(createRecord("response", "text/html", "http://test.com/1"));
    // 2. Matches allow, matches deny -> DROP
    filter.onNext(createRecord("response", "image/jpeg", "http://test.com/2"));
    // 3. Does not match allow -> DROP
    filter.onNext(createRecord("request", "text/html", "http://test.com/3"));
    // 4. Matches allow, no deny match -> KEEP
    filter.onNext(createRecord("metadata", "application/json", "http://test.com/4"));

    assertEquals(2, subscriber.items.size());
    assertEquals("http://test.com/1", subscriber.items.get(0).targetUri());
    assertEquals("http://test.com/4", subscriber.items.get(1).targetUri());
  }

  @Test
  public void testDenyAllowDropMode() {
    WarcFilter filter = new WarcFilter();
    filter.configure(Map.of(
        "mode", "deny-allow-drop",
        "deny-url-prefixes", "http://test.com/private",
        "allow-http-codes", "200-299"));

    TestSubscriber subscriber = new TestSubscriber();
    filter.subscribe(subscriber);
    filter.onSubscribe(new NoOpSubscription());

    // 1. No deny, matches allow -> KEEP
    filter.onNext(createRecord("response", "text/html", "http://test.com/public", 200));
    // 2. Matches deny -> DROP
    filter.onNext(createRecord("response", "text/html", "http://test.com/private/1", 200));
    // 3. No deny, no allow match -> DROP
    filter.onNext(createRecord("response", "text/html", "http://test.com/public/404", 404));

    assertEquals(1, subscriber.items.size());
    assertEquals("http://test.com/public", subscriber.items.get(0).targetUri());
  }

  @Test
  public void testHttpStatusRange() {
    WarcFilter filter = new WarcFilter();
    filter.configure(Map.of(
        "allow-http-codes", "200-201,404"));

    TestSubscriber subscriber = new TestSubscriber();
    filter.subscribe(subscriber);
    filter.onSubscribe(new NoOpSubscription());

    filter.onNext(createRecord("response", "text/html", "h1", 200));
    filter.onNext(createRecord("response", "text/html", "h2", 201));
    filter.onNext(createRecord("response", "text/html", "h3", 202));
    filter.onNext(createRecord("response", "text/html", "h4", 404));
    filter.onNext(createRecord("response", "text/html", "h5", 500));

    assertEquals(3, subscriber.items.size());
  }

  @Test
  public void testContentLength() {
    WarcFilter filter = new WarcFilter();
    filter.configure(Map.of(
        "allow-content-length-gt", 100,
        "allow-content-length-lt", 1000));

    TestSubscriber subscriber = new TestSubscriber();
    filter.subscribe(subscriber);
    filter.onSubscribe(new NoOpSubscription());

    filter.onNext(createRecordWithLen(50)); // DROP
    filter.onNext(createRecordWithLen(150)); // KEEP
    filter.onNext(createRecordWithLen(1050)); // DROP

    assertEquals(1, subscriber.items.size());
  }

  @Test
  public void testPagination() {
    WarcFilter filter = new WarcFilter();
    filter.configure(Map.of(
        "row-start", 2,
        "row-limit", 2));

    TestSubscriber subscriber = new TestSubscriber();
    filter.subscribe(subscriber);
    filter.onSubscribe(new NoOpSubscription());

    filter.onNext(createRecord("response", "t1", "u1")); // Row 1: SKIP
    filter.onNext(createRecord("response", "t2", "u2")); // Row 2: SKIP
    filter.onNext(createRecord("response", "t3", "u3")); // Row 3: EMIT (1/2)
    filter.onNext(createRecord("response", "t4", "u4")); // Row 4: EMIT (2/2)
    filter.onNext(createRecord("response", "t5", "u5")); // Row 5: LIMIT REACHED

    assertEquals(2, subscriber.items.size());
    assertEquals("u3", subscriber.items.get(0).targetUri());
    assertEquals("u4", subscriber.items.get(1).targetUri());
  }

  @Test
  public void testUrlRegex() {
    WarcFilter filter = new WarcFilter();
    filter.configure(Map.of(
        "allow-url-regexes", ".*\\.gov\\.pl/bip/.*,https://test\\.com/.*"));

    TestSubscriber subscriber = new TestSubscriber();
    filter.subscribe(subscriber);
    filter.onSubscribe(new NoOpSubscription());

    filter.onNext(createRecord("response", "t1", "https://test.com/index.html")); // KEEP
    filter.onNext(createRecord("response", "t1", "http://example.gov.pl/bip/info")); // KEEP
    filter.onNext(createRecord("response", "t1", "http://example.com/other")); // DROP

    assertEquals(2, subscriber.items.size());
  }

  @Test
  public void testHeaderFilters() {
    WarcFilter filter = new WarcFilter();
    filter.configure(Map.of(
        "allow-headers", "X-Custom=.*Valid.*,X-Required"));

    TestSubscriber subscriber = new TestSubscriber();
    filter.subscribe(subscriber);
    filter.onSubscribe(new NoOpSubscription());

    // 1. Valid regex match
    RecordWarcUniversal r1 = createRecord("response", "t1", "u1");
    r1.headers().put("X-Custom", "This is a Valid header");
    filter.onNext(r1);

    // 2. Presence match
    RecordWarcUniversal r2 = createRecord("response", "t1", "u2");
    r2.headers().put("X-Required", "anything");
    filter.onNext(r2);

    // 3. Regex mismatch
    RecordWarcUniversal r3 = createRecord("response", "t1", "u3");
    r3.headers().put("X-Custom", "invalid");
    filter.onNext(r3);

    assertEquals(2, subscriber.items.size());
  }

  @Test
  public void testDateFilters() {
    WarcFilter filter = new WarcFilter();
    filter.configure(Map.of(
        "allow-date-after", "2026-01-01T12:00:00Z",
        "allow-date-before", "2026-01-02T12:00:00Z"));

    TestSubscriber subscriber = new TestSubscriber();
    filter.subscribe(subscriber);
    filter.onSubscribe(new NoOpSubscription());

    filter.onNext(createRecordWithDate("2026-01-01T00:00:00Z")); // Too early
    filter.onNext(createRecordWithDate("2026-01-01T15:00:00Z")); // OK
    filter.onNext(createRecordWithDate("2026-01-02T15:00:00Z")); // Too late

    assertEquals(1, subscriber.items.size());
  }

  @Test
  public void testFilenameFilters() {
    WarcFilter filter = new WarcFilter();
    filter.configure(Map.of(
        "allow-filenames", "test1.warc,^test2.warc"));

    TestSubscriber subscriber = new TestSubscriber();
    filter.subscribe(subscriber);
    filter.onSubscribe(new NoOpSubscription());

    filter.onNext(createRecordWithFile("test1.warc")); // KEEP
    filter.onNext(createRecordWithFile("test2.warc")); // DROP (Excluded)
    filter.onNext(createRecordWithFile("other.warc")); // DROP (Not in includes)

    assertEquals(1, subscriber.items.size());
  }

  @Test
  public void testAllowGroupsAnd() {
    WarcFilter filter = new WarcFilter();
    filter.configure(Map.of(
        "allow-groups-and", true,
        "allow-warc-types", "response",
        "allow-mime-types", "text/html"));

    TestSubscriber subscriber = new TestSubscriber();
    filter.subscribe(subscriber);
    filter.onSubscribe(new NoOpSubscription());

    // 1. Both match -> KEEP
    filter.onNext(createRecord("response", "text/html", "u1"));
    // 2. Only one matches -> DROP
    filter.onNext(createRecord("metadata", "text/html", "u2"));
    // 3. Only the other matches -> DROP
    filter.onNext(createRecord("response", "application/json", "u3"));

    assertEquals(1, subscriber.items.size());
  }

  // --- Helpers ---

  private RecordWarcUniversal createRecordWithDate(String date) {
    RecordWarcUniversal r = createRecord("response", "text/plain", "u1");
    r.headers().put("WARC-Date", date);
    return r;
  }

  private RecordWarcUniversal createRecordWithFile(String filename) {
    RecordWarcUniversal r = createRecord("response", "text/plain", "u1");
    r.headers().put("X-Source-Warc", filename);
    return r;
  }

  private RecordWarcUniversal createRecord(String type, String mime, String url) {
    return createRecord(type, mime, url, 200);
  }

  private RecordWarcUniversal createRecord(String type, String mime, String url, int status) {
    Map<String, String> headers = new java.util.LinkedHashMap<>();
    headers.put("WARC-Type", type);
    headers.put("WARC-Target-URI", url);
    headers.put("Content-Type", mime);
    headers.put("WARC-Date", "2026-01-01T00:00:00Z");

    // Mock HTTP response for status parsing
    String payload = "HTTP/1.1 " + status + " OK\r\nContent-Type: " + mime + "\r\n\r\nHello";
    String raw = "WARC/1.0\r\n" +
        "WARC-Type: " + type + "\r\n" +
        "WARC-Target-URI: " + url + "\r\n" +
        "Content-Type: application/http; msgtype=response\r\n" +
        "\r\n" +
        payload + "\r\n\r\n";

    return new RecordWarcUniversal(type, headers, raw.getBytes(StandardCharsets.UTF_8));
  }

  private RecordWarcUniversal createRecordWithLen(long len) {
    Map<String, String> headers = new java.util.LinkedHashMap<>();
    headers.put("WARC-Type", "response");
    headers.put("Content-Length", String.valueOf(len));

    return new RecordWarcUniversal("response", headers, new byte[0]);
  }

  static class TestSubscriber implements Flow.Subscriber<RecordWarcUniversal> {
    List<RecordWarcUniversal> items = new ArrayList<>();

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
    }

    @Override
    public void onNext(RecordWarcUniversal item) {
      items.add(item);
    }

    @Override
    public void onError(Throwable throwable) {
    }

    @Override
    public void onComplete() {
    }
  }

  static class NoOpSubscription implements Flow.Subscription {
    @Override
    public void request(long n) {
    }

    @Override
    public void cancel() {
    }
  }
}
