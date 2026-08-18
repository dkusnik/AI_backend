package pl.gov.nac.warc.consumers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static pl.gov.nac.warc.testutil.ExpectedLogSilencer.runWithLoggerMuted;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import pl.gov.nac.warc.reactive.Metrics;

class ConsumerWarcBaseTest {

  @TempDir
  Path tempDir;

  @BeforeEach
  void resetMetrics() {
    Metrics.reset();
  }

  @Test
  void failedSegmentRotationTerminatesConsumption() throws Exception {
    Path output = tempDir.resolve("output.warc");
    TestConsumer consumer = new TestConsumer();
    consumer.configure(Map.of(
        "file", output.toString(),
        "mode", "warc",
        "compression", "none",
        "cdx-sidecar", false,
        "check-order", "off",
        "warc-size-limit", 1));

    consumer.startConsuming();
    consumer.onNext(new Object());

    Files.createDirectory(tempDir.resolve("output.00001.warc"));

    runWithLoggerMuted(ConsumerWarcBase.class, () -> {
      assertThrows(IllegalStateException.class, () -> consumer.onNext(new Object()),
          "A failed rotation must terminate consumption instead of dropping this and later records");
      assertEquals(1, consumer.afterCheck(Map.of()),
          "The terminal write failure must produce a non-zero consumer status");
    });
  }

  @Test
  void failedStreamCloseProducesNonZeroStatus() {
    TestConsumer consumer = new TestConsumer();
    consumer.useCurrentStream(new FailingCloseOutputStream());

    runWithLoggerMuted(ConsumerWarcBase.class, () -> {
      consumer.onComplete();
      assertEquals(1, consumer.afterCheck(Map.of()),
          "A close failure can leave a truncated file and must produce a non-zero status");
    });
  }

  @Test
  void successfulOutputIsInvisibleUntilPublished() throws Exception {
    Path output = tempDir.resolve("output.warc");
    TestConsumer consumer = configuredConsumer(output, false);

    assertTrue(consumer.beforeCheck(Map.of()));
    consumer.startConsuming();
    consumer.onNext(new Object());

    assertFalse(Files.exists(output));
    assertEquals(1, siblingTemporaries().size());

    consumer.onComplete();
    assertFalse(Files.exists(output), "onComplete must only close the temporary");
    assertEquals(0, consumer.afterCheck(Map.of()));
    assertEquals(0, consumer.publishOutputs());

    assertEquals(1, Files.size(output));
    assertTrue(siblingTemporaries().isEmpty());
  }

  @Test
  void collisionFailsWithoutChangingExistingTarget() throws Exception {
    Path output = tempDir.resolve("output.warc");
    Files.writeString(output, "old");
    TestConsumer consumer = configuredConsumer(output, false);

    assertFalse(consumer.beforeCheck(Map.of()));
    consumer.discardOutputs();

    assertEquals("old", Files.readString(output));
    assertTrue(siblingTemporaries().isEmpty());
  }

  @Test
  void forceKeepsExistingTargetUntilPublication() throws Exception {
    Path output = tempDir.resolve("output.warc");
    Files.writeString(output, "old");
    TestConsumer consumer = configuredConsumer(output, true);

    assertTrue(consumer.beforeCheck(Map.of()));
    consumer.startConsuming();
    consumer.onNext(new Object());
    consumer.onComplete();

    assertEquals("old", Files.readString(output));
    assertEquals(0, consumer.afterCheck(Map.of()));
    assertEquals(0, consumer.publishOutputs());
    assertEquals(1, Files.size(output));
    assertTrue(siblingTemporaries().isEmpty());
  }

  @Test
  void discardRemovesTemporaryAndPreservesForcedTarget() throws Exception {
    Path output = tempDir.resolve("output.warc");
    Files.writeString(output, "old");
    TestConsumer consumer = configuredConsumer(output, true);

    consumer.startConsuming();
    consumer.onNext(new Object());
    consumer.onError(new IllegalStateException("injected processing failure"));
    consumer.discardOutputs();

    assertEquals("old", Files.readString(output));
    assertTrue(siblingTemporaries().isEmpty());
  }

  @Test
  void multiOutputPublishesInByteOrderAndRetainsCompletedPrefixOnFailure() throws Exception {
    Path outputDirectory = tempDir.resolve("multi");
    Path report = tempDir.resolve("publication.json");
    FailingSecondMoveConsumer consumer = new FailingSecondMoveConsumer();
    consumer.configure(Map.of(
        "file", outputDirectory.toString(),
        "mode", "multi-warc",
        "output-name-template", "{source}.warc",
        "compression", "none",
        "cdx-sidecar", false,
        "check-order", "off",
        "publication-report", report.toString()));

    assertTrue(consumer.beforeCheck(Map.of()));
    consumer.onNext("b");
    consumer.onNext("a");
    consumer.onComplete();

    assertEquals(1, consumer.publishOutputs());
    assertEquals(List.of("a.warc", "b.warc"), consumer.moveAttempts);
    assertTrue(Files.isRegularFile(outputDirectory.resolve("a.warc")));
    assertFalse(Files.exists(outputDirectory.resolve("b.warc")));
    assertTrue(siblingTemporaries(outputDirectory).isEmpty());
    JsonNode publication = new ObjectMapper().readTree(report.toFile());
    assertEquals("warc2es.output-publication/v1", publication.get("schema").asText());
    assertEquals("partial", publication.get("status").asText());
    assertEquals(2, publication.get("planned").asInt());
    assertEquals(outputDirectory.resolve("a.warc").toString(), publication.get("published").get(0).asText());
  }

  @Test
  void cdxAndSplitOutputsUseTheSamePublicationBoundary() throws Exception {
    Path output = tempDir.resolve("main.warc");
    Path split = tempDir.resolve("diff.warc");
    TestConsumer consumer = new TestConsumer();
    consumer.configure(Map.of(
        "file", output.toString(),
        "mode", "warc+cdx",
        "compression", "none",
        "cdx-sidecar", true,
        "check-order", "off",
        "split-provenance", true,
        "diff-output", split.toString()));

    assertTrue(consumer.beforeCheck(Map.of()));
    consumer.startConsuming();
    consumer.onNext(new Object());
    consumer.onComplete();

    Path cdx = tempDir.resolve("main.cdxj");
    Path splitCdx = tempDir.resolve("diff.cdxj");
    assertFalse(Files.exists(output));
    assertFalse(Files.exists(cdx));
    assertFalse(Files.exists(split));
    assertFalse(Files.exists(splitCdx));

    assertEquals(0, consumer.publishOutputs());
    assertTrue(Files.isRegularFile(output));
    assertTrue(Files.isRegularFile(cdx));
    assertTrue(Files.isRegularFile(split));
    assertTrue(Files.isRegularFile(splitCdx));
  }

  @Test
  void copyOutputUsesTheSamePublicationBoundary() throws Exception {
    Path sourceDirectory = tempDir.resolve("source");
    Files.createDirectories(sourceDirectory);
    Path source = sourceDirectory.resolve("copied.warc");
    Files.writeString(source, "complete-copy");
    Path target = tempDir.resolve("copied.warc");
    TestConsumer consumer = new TestConsumer();
    consumer.configure(Map.of(
        "file", tempDir.resolve("unused.warc").toString(),
        "mode", "copy",
        "force", false));

    assertTrue(consumer.beforeCheck(Map.of()));
    consumer.copyPath(source);

    assertFalse(Files.exists(target));
    assertEquals(1, siblingTemporaries().size());
    assertEquals(0, consumer.publishOutputs());
    assertEquals("complete-copy", Files.readString(target));
    assertFalse(Files.exists(tempDir.resolve("unused.warc")),
        "copy mode must not rotate or publish an unrelated main stream");
    assertTrue(siblingTemporaries().isEmpty());
  }

  @Test
  void publicationReportFailureCannotReturnSuccessAfterMove() throws Exception {
    Path output = tempDir.resolve("output.warc");
    Path reportDirectory = tempDir.resolve("report-is-a-directory");
    Files.createDirectory(reportDirectory);
    TestConsumer consumer = new TestConsumer();
    consumer.configure(Map.of(
        "file", output.toString(),
        "mode", "warc",
        "compression", "none",
        "cdx-sidecar", false,
        "check-order", "off",
        "publication-report", reportDirectory.toString()));

    consumer.startConsuming();
    consumer.onNext(new Object());
    consumer.onComplete();

    runWithLoggerMuted(ConsumerWarcBase.class, () -> assertEquals(1, consumer.publishOutputs()));
    assertTrue(Files.isRegularFile(output), "the already completed atomic move must not be rolled back");
  }

  @Test
  void closeFailureDiscardsTemporaryAndPreservesForcedTarget() throws Exception {
    Path output = tempDir.resolve("output.warc");
    Files.writeString(output, "old");
    TestConsumer consumer = new FailingWriterCloseConsumer();
    consumer.configure(Map.of(
        "file", output.toString(),
        "mode", "warc",
        "compression", "none",
        "cdx-sidecar", false,
        "check-order", "off",
        "force", true));

    consumer.startConsuming();
    consumer.onNext(new Object());
    runWithLoggerMuted(ConsumerWarcBase.class, () -> consumer.onComplete());

    assertEquals(1, consumer.afterCheck(Map.of()));
    consumer.discardOutputs();
    assertEquals("old", Files.readString(output));
    assertTrue(siblingTemporaries().isEmpty());
  }

  private TestConsumer configuredConsumer(Path output, boolean force) {
    TestConsumer consumer = new TestConsumer();
    consumer.configure(Map.of(
        "file", output.toString(),
        "mode", "warc",
        "compression", "none",
        "cdx-sidecar", false,
        "check-order", "off",
        "force", force));
    return consumer;
  }

  private List<Path> siblingTemporaries() throws IOException {
    return siblingTemporaries(tempDir);
  }

  private static List<Path> siblingTemporaries(Path directory) throws IOException {
    if (!Files.isDirectory(directory)) {
      return List.of();
    }
    try (var paths = Files.list(directory)) {
      return paths.filter(path -> path.getFileName().toString().endsWith(".tmp")).toList();
    }
  }

  private static class TestConsumer extends ConsumerWarcBase {

    void useCurrentStream(OutputStream stream) {
      currentStream.set(stream);
    }

    void copyPath(Path source) throws IOException {
      handleWarcPath(source);
    }

    @Override
    protected void writeRecordToStream(Object item, OutputStream stream) throws IOException {
      stream.write(1);
    }

    @Override
    protected void openWriter(OutputStream stream) {
    }

    @Override
    protected void closeWriter() throws IOException {
    }

    @Override
    protected String getConsumerName() {
      return "Test WARC Consumer";
    }
  }

  private static final class FailingWriterCloseConsumer extends TestConsumer {
    @Override
    protected void closeWriter() throws IOException {
      throw new IOException("injected writer close failure");
    }
  }

  private static final class FailingSecondMoveConsumer extends TestConsumer {
    private final List<String> moveAttempts = new ArrayList<>();

    @Override
    protected String extractSource(Object item) {
      return item.toString();
    }

    @Override
    protected void moveOutput(Path temporary, Path target, boolean replace) throws IOException {
      moveAttempts.add(target.getFileName().toString());
      if (moveAttempts.size() == 2) {
        throw new IOException("injected second move failure");
      }
      super.moveOutput(temporary, target, replace);
    }
  }

  private static final class FailingCloseOutputStream extends OutputStream {

    @Override
    public void write(int b) {
    }

    @Override
    public void close() throws IOException {
      throw new IOException("injected close failure");
    }
  }
}
