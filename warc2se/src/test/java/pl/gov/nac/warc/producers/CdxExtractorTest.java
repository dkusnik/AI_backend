package pl.gov.nac.warc.producers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import pl.gov.nac.warc.records.cdx.RecordCdxRaw;
import pl.gov.nac.warc.records.cdx.RecordCdxStructured;

class CdxExtractorTest {

  @Test
  void testStructuredOutputMatchesGoldenFixture() throws Exception {
    Path warc = fixturePath("cdx-fixtures/sample-jwarc.warc");
    List<ExpectedRow> expected = readGoldenRows("cdx-fixtures/sample-expected-structured.tsv");

    CdxExtractor producer = new CdxExtractor();
    producer.configure(Map.of("file", warc.toString()));
    producer.onNegotiatedOutputType(RecordCdxStructured.class);

    TestSubscriber subscriber = new TestSubscriber();
    producer.subscribe(subscriber);
    producer.startProducing();
    subscriber.awaitComplete();

    assertEquals(expected.size(), subscriber.items.size(), "Structured output count must match golden rows");
    for (int i = 0; i < expected.size(); i++) {
      RecordCdxStructured actual = assertInstanceOf(RecordCdxStructured.class, subscriber.items.get(i));
      ExpectedRow exp = expected.get(i);
      assertEquals(exp.url, actual.originalUrl());
      assertEquals(exp.surt, actual.surtKey());
      assertEquals(exp.timestamp, actual.timestamp());
      assertEquals(exp.status, actual.statusCode());
    }
  }

  @Test
  void testRawOutputTypeProducesRawRecords() throws Exception {
    Path warc = fixturePath("cdx-fixtures/sample-jwarc.warc");

    CdxExtractor producer = new CdxExtractor();
    producer.configure(Map.of("file", warc.toString()));

    TestSubscriber subscriber = new TestSubscriber();
    producer.subscribe(subscriber);
    producer.startProducing();
    subscriber.awaitComplete();

    assertEquals(2, subscriber.items.size());
    RecordCdxRaw r1 = assertInstanceOf(RecordCdxRaw.class, subscriber.items.get(0));
    RecordCdxRaw r2 = assertInstanceOf(RecordCdxRaw.class, subscriber.items.get(1));
    assertTrue(r1.line().contains("\"url\": \"http://www.example.com/path\""));
    assertTrue(r2.line().contains("\"url\": \"http://www.example.com/other\""));
  }

  @Test
  void testStructuredOffsetsAreMonotonicAndStartBased() throws Exception {
    Path warc = fixturePath("cdx-fixtures/sample-jwarc.warc");
    long fileSize = Files.size(warc);

    CdxExtractor producer = new CdxExtractor();
    producer.configure(Map.of("file", warc.toString()));
    producer.onNegotiatedOutputType(RecordCdxStructured.class);

    TestSubscriber subscriber = new TestSubscriber();
    producer.subscribe(subscriber);
    producer.startProducing();
    subscriber.awaitComplete();

    RecordCdxStructured first = assertInstanceOf(RecordCdxStructured.class, subscriber.items.get(0));
    RecordCdxStructured second = assertInstanceOf(RecordCdxStructured.class, subscriber.items.get(1));

    assertEquals(0L, first.offset(), "First record offset should be start-based");
    assertTrue(first.length() >= 0, "First length should be non-negative");
    assertTrue(second.offset() >= 0, "Second offset should be non-negative");
    assertTrue(second.length() >= 0, "Second length should be non-negative");
    assertTrue(second.offset() + second.length() <= fileSize,
        "Last record range should not exceed file size");
  }

  @Test
  void testStructuredOffsetsMatchRecordStartPositionsExactly() throws Exception {
    Path warc = fixturePath("cdx-fixtures/sample-jwarc.warc");
    byte[] bytes = Files.readAllBytes(warc);
    List<Long> starts = findRecordStarts(bytes);
    assertTrue(starts.size() >= 2, "Fixture should contain at least two WARC records");

    CdxExtractor producer = new CdxExtractor();
    producer.configure(Map.of("file", warc.toString()));
    producer.onNegotiatedOutputType(RecordCdxStructured.class);

    TestSubscriber subscriber = new TestSubscriber();
    producer.subscribe(subscriber);
    producer.startProducing();
    subscriber.awaitComplete();

    assertEquals(2, subscriber.items.size());
    RecordCdxStructured first = assertInstanceOf(RecordCdxStructured.class, subscriber.items.get(0));
    RecordCdxStructured second = assertInstanceOf(RecordCdxStructured.class, subscriber.items.get(1));

    assertEquals(starts.get(0), first.offset(), "First structured offset must equal first record start");
    assertEquals(starts.get(1), second.offset(), "Second structured offset must equal second record start");
  }

  private Path fixturePath(String resourcePath) throws URISyntaxException {
    return Path.of(getClass().getClassLoader().getResource(resourcePath).toURI());
  }

  private List<ExpectedRow> readGoldenRows(String resourcePath) throws IOException, URISyntaxException {
    Path p = fixturePath(resourcePath);
    List<ExpectedRow> rows = new ArrayList<>();
    for (String line : Files.readAllLines(p)) {
      String trimmed = line.trim();
      if (trimmed.isEmpty() || trimmed.startsWith("#")) {
        continue;
      }
      String[] parts = trimmed.split("\\t");
      rows.add(new ExpectedRow(parts[0], parts[1], parts[2], Integer.parseInt(parts[3])));
    }
    return rows;
  }

  record ExpectedRow(String url, String surt, String timestamp, int status) {
  }

  private List<Long> findRecordStarts(byte[] bytes) {
    byte[] marker = "WARC/".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    List<Long> starts = new ArrayList<>();
    for (int i = 0; i <= bytes.length - marker.length; i++) {
      if (matchesAt(bytes, marker, i)) {
        starts.add((long) i);
      }
    }
    return starts;
  }

  private boolean matchesAt(byte[] bytes, byte[] marker, int offset) {
    return Arrays.equals(bytes, offset, offset + marker.length, marker, 0, marker.length);
  }

  static class TestSubscriber implements Flow.Subscriber<Object> {
    private final List<Object> items = new ArrayList<>();
    private final CountDownLatch done = new CountDownLatch(1);

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      subscription.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(Object item) {
      items.add(item);
    }

    @Override
    public void onError(Throwable throwable) {
      done.countDown();
      throw new RuntimeException(throwable);
    }

    @Override
    public void onComplete() {
      done.countDown();
    }

    void awaitComplete() throws InterruptedException {
      assertTrue(done.await(5, TimeUnit.SECONDS), "Timed out waiting for producer completion");
    }
  }
}
