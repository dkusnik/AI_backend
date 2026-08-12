package pl.gov.nac.warc.processors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.SubmissionPublisher;

import org.junit.jupiter.api.Test;

import pl.gov.nac.warc.recording.TestSubscriber;
import pl.gov.nac.warc.records.warc.RecordWarcUniversal;

public class WarcDecoratorDigestTest {

  @Test
  public void testComputeDigest_XXH128() throws Exception {
    WarcDecoratorDigest processor = new WarcDecoratorDigest();
    processor.configure(Map.of("digest", "xxh128"));

    byte[] payload = "Hello world".getBytes(StandardCharsets.UTF_8);
    RecordWarcUniversal input = new RecordWarcUniversal("resource", Map.of(), payload);

    TestSubscriber<RecordWarcUniversal> subscriber = new TestSubscriber<>();
    processor.subscribe(subscriber);

    SubmissionPublisher<RecordWarcUniversal> publisher = new SubmissionPublisher<>();
    publisher.subscribe(processor);

    publisher.submit(input);
    publisher.close();

    subscriber.awaitCompletion();
    List<RecordWarcUniversal> items = subscriber.getItems();
    assertEquals(1, items.size());

    RecordWarcUniversal result = items.get(0);
    String digestHeader = result.headers().get("WARC-Payload-Digest");
    assertNotNull(digestHeader);
    assertTrue(digestHeader.startsWith("xxh128:"));
    // Known XXH128 for "Hello world" (seed 0) is roughly predictable
    // But verifying format is good enough for unit test
  }

  @Test
  public void testComputeDigest_SHA256() throws Exception {
    WarcDecoratorDigest processor = new WarcDecoratorDigest();
    processor.configure(Map.of("digest", "sha256"));

    byte[] payload = "Hello world".getBytes(StandardCharsets.UTF_8);
    RecordWarcUniversal input = new RecordWarcUniversal("resource", Map.of(), payload);

    TestSubscriber<RecordWarcUniversal> subscriber = new TestSubscriber<>();
    processor.subscribe(subscriber);

    SubmissionPublisher<RecordWarcUniversal> publisher = new SubmissionPublisher<>();
    publisher.subscribe(processor);

    publisher.submit(input);
    publisher.close();

    subscriber.awaitCompletion();
    List<RecordWarcUniversal> items = subscriber.getItems();
    assertEquals(1, items.size());

    RecordWarcUniversal result = items.get(0);
    String digestHeader = result.headers().get("WARC-Payload-Digest");
    assertNotNull(digestHeader);
    assertTrue(digestHeader.startsWith("sha256:"));
    // SHA-256 of "Hello world" is 64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c
    assertTrue(digestHeader.contains("64ec88ca00b268"));
  }

  @Test
  public void testComputeDigest_SHA512() throws Exception {
    WarcDecoratorDigest processor = new WarcDecoratorDigest();
    processor.configure(Map.of("digest", "sha512"));

    byte[] payload = "Hello world".getBytes(StandardCharsets.UTF_8);
    RecordWarcUniversal input = new RecordWarcUniversal("resource", Map.of(), payload);

    TestSubscriber<RecordWarcUniversal> subscriber = new TestSubscriber<>();
    processor.subscribe(subscriber);

    SubmissionPublisher<RecordWarcUniversal> publisher = new SubmissionPublisher<>();
    publisher.subscribe(processor);

    publisher.submit(input);
    publisher.close();

    subscriber.awaitCompletion();
    List<RecordWarcUniversal> items = subscriber.getItems();
    assertEquals(1, items.size());

    RecordWarcUniversal result = items.get(0);
    String digestHeader = result.headers().get("WARC-Payload-Digest");
    assertNotNull(digestHeader);
    assertTrue(digestHeader.startsWith("sha512:"));
    // SHA-512 produces 128 hex characters (64 bytes)
    String hexPart = digestHeader.substring("sha512:".length());
    assertEquals(128, hexPart.length());
    // SHA-512 of "Hello world" starts with b7f783bae...
    assertTrue(digestHeader.contains("b7f783bae"));
  }

  @Test
  public void testComputeDigest_SHA512_256() throws Exception {
    WarcDecoratorDigest processor = new WarcDecoratorDigest();
    processor.configure(Map.of("digest", "sha512-256"));

    byte[] payload = "Hello world".getBytes(StandardCharsets.UTF_8);
    RecordWarcUniversal input = new RecordWarcUniversal("resource", Map.of(), payload);

    TestSubscriber<RecordWarcUniversal> subscriber = new TestSubscriber<>();
    processor.subscribe(subscriber);

    SubmissionPublisher<RecordWarcUniversal> publisher = new SubmissionPublisher<>();
    publisher.subscribe(processor);

    publisher.submit(input);
    publisher.close();

    subscriber.awaitCompletion();
    List<RecordWarcUniversal> items = subscriber.getItems();
    assertEquals(1, items.size());

    RecordWarcUniversal result = items.get(0);
    String digestHeader = result.headers().get("WARC-Payload-Digest");
    assertNotNull(digestHeader);
    assertTrue(digestHeader.startsWith("sha512-256:"));
    // SHA-512/256 produces 64 hex characters (32 bytes)
    String hexPart = digestHeader.substring("sha512-256:".length());
    assertEquals(64, hexPart.length());
  }

  @Test
  public void testComputeDigest_SHA512_256_UnderscoreVariant() throws Exception {
    // Test underscore variant (sha512_256)
    WarcDecoratorDigest processor = new WarcDecoratorDigest();
    processor.configure(Map.of("digest", "sha512_256"));

    byte[] payload = "Hello world".getBytes(StandardCharsets.UTF_8);
    RecordWarcUniversal input = new RecordWarcUniversal("resource", Map.of(), payload);

    TestSubscriber<RecordWarcUniversal> subscriber = new TestSubscriber<>();
    processor.subscribe(subscriber);

    SubmissionPublisher<RecordWarcUniversal> publisher = new SubmissionPublisher<>();
    publisher.subscribe(processor);

    publisher.submit(input);
    publisher.close();

    subscriber.awaitCompletion();
    List<RecordWarcUniversal> items = subscriber.getItems();
    assertEquals(1, items.size());

    RecordWarcUniversal result = items.get(0);
    String digestHeader = result.headers().get("WARC-Payload-Digest");
    assertNotNull(digestHeader);
    // Header should use underscore variant
    assertTrue(digestHeader.startsWith("sha512_256:"));
    String hexPart = digestHeader.substring("sha512_256:".length());
    assertEquals(64, hexPart.length());
  }

  @Test
  public void testComputeDigest_BLAKE3() throws Exception {
    WarcDecoratorDigest processor = new WarcDecoratorDigest();
    processor.configure(Map.of("digest", "blake3"));

    byte[] payload = "Hello world".getBytes(StandardCharsets.UTF_8);
    RecordWarcUniversal input = new RecordWarcUniversal("resource", Map.of(), payload);

    TestSubscriber<RecordWarcUniversal> subscriber = new TestSubscriber<>();
    processor.subscribe(subscriber);

    SubmissionPublisher<RecordWarcUniversal> publisher = new SubmissionPublisher<>();
    publisher.subscribe(processor);

    publisher.submit(input);
    publisher.close();

    subscriber.awaitCompletion();
    List<RecordWarcUniversal> items = subscriber.getItems();
    assertEquals(1, items.size());

    RecordWarcUniversal result = items.get(0);
    String digestHeader = result.headers().get("WARC-Payload-Digest");
    assertNotNull(digestHeader);
    assertTrue(digestHeader.startsWith("blake3:"));
    // BLAKE3 produces 64 hex characters (32 bytes)
    String hexPart = digestHeader.substring("blake3:".length());
    assertEquals(64, hexPart.length());
  }

  @Test
  public void testComputeDigest_SHA1() throws Exception {
    WarcDecoratorDigest processor = new WarcDecoratorDigest();
    processor.configure(Map.of("digest", "sha1"));

    byte[] payload = "Hello world".getBytes(StandardCharsets.UTF_8);
    RecordWarcUniversal input = new RecordWarcUniversal("resource", Map.of(), payload);

    TestSubscriber<RecordWarcUniversal> subscriber = new TestSubscriber<>();
    processor.subscribe(subscriber);

    SubmissionPublisher<RecordWarcUniversal> publisher = new SubmissionPublisher<>();
    publisher.subscribe(processor);

    publisher.submit(input);
    publisher.close();

    subscriber.awaitCompletion();
    List<RecordWarcUniversal> items = subscriber.getItems();
    assertEquals(1, items.size());

    RecordWarcUniversal result = items.get(0);
    String digestHeader = result.headers().get("WARC-Payload-Digest");
    assertNotNull(digestHeader);
    assertTrue(digestHeader.startsWith("sha1:"));
    // SHA-1 produces 40 hex characters (20 bytes)
    String hexPart = digestHeader.substring("sha1:".length());
    assertEquals(40, hexPart.length());
  }

  @Test
  public void testComputeDigest_AllAlgorithmsProduceDifferentResults() throws Exception {
    byte[] payload = "Test data for digest comparison".getBytes(StandardCharsets.UTF_8);

    String[] algorithms = {"xxh128", "sha1", "sha256", "sha512", "sha512-256", "blake3"};
    String[] digests = new String[algorithms.length];

    for (int i = 0; i < algorithms.length; i++) {
      WarcDecoratorDigest processor = new WarcDecoratorDigest();
      processor.configure(Map.of("digest", algorithms[i]));

      RecordWarcUniversal input = new RecordWarcUniversal("resource", Map.of(), payload);
      TestSubscriber<RecordWarcUniversal> subscriber = new TestSubscriber<>();
      processor.subscribe(subscriber);

      SubmissionPublisher<RecordWarcUniversal> publisher = new SubmissionPublisher<>();
      publisher.subscribe(processor);

      publisher.submit(input);
      publisher.close();

      subscriber.awaitCompletion();
      List<RecordWarcUniversal> items = subscriber.getItems();
      RecordWarcUniversal result = items.get(0);
      digests[i] = result.headers().get("WARC-Payload-Digest");
      assertNotNull(digests[i], "Digest for " + algorithms[i] + " should not be null");
    }

    // Verify all digests are different (different algorithms produce different hashes)
    for (int i = 0; i < digests.length; i++) {
      for (int j = i + 1; j < digests.length; j++) {
        assertTrue(!digests[i].equals(digests[j]),
            "Digests for " + algorithms[i] + " and " + algorithms[j] + " should be different");
      }
    }
  }

  @Test
  public void testComputeDigest_NullPayloadSafe() throws Exception {
    WarcDecoratorDigest processor = new WarcDecoratorDigest();
    processor.configure(Map.of("digest", "sha256"));

    // Simulate null bytes (though RecordWarcUniversal usually requires non-null)
    // We bypass the constructor check if we can, or just mock it,
    // but here we rely on the processor's safety if it ever encounters a record
    // where rawBytes() returns null.
    // Since the class is real, let's make a subclass or mock that returns null
    // rawBytes

    RecordWarcUniversal input = new RecordWarcUniversal("npe-test", Map.of(), new byte[0]) {
      @Override
      public byte[] rawBytes() {
        return null;
      }
    };

    TestSubscriber<RecordWarcUniversal> subscriber = new TestSubscriber<>();
    processor.subscribe(subscriber);

    SubmissionPublisher<RecordWarcUniversal> publisher = new SubmissionPublisher<>();
    publisher.subscribe(processor);

    publisher.submit(input);
    publisher.close();

    subscriber.awaitCompletion();
    List<RecordWarcUniversal> items = subscriber.getItems();
    assertEquals(1, items.size());

    // Should pass through without error and without digest header
    assertNull(items.get(0).headers().get("WARC-Payload-Digest"));
  }
}
