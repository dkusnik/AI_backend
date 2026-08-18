package pl.gov.nac.warc.producers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import pl.gov.nac.warc.reactive.Metrics;
import pl.gov.nac.warc.records.file.RecordFileWarc;
import pl.gov.nac.warc.records.warc.RecordWarcJwarc;
import pl.gov.nac.warc.records.warc.RecordWarcRawBytes;
import pl.gov.nac.warc.records.warc.RecordWarcUniversal;

class ArchiveExtractorJwarcTest {

  @TempDir
  Path tempDir;

  @AfterEach
  void resetMetrics() {
    Metrics.reset();
  }

  @Test
  void advertisesFileFirstAndHonorsConfiguredRecordFormats() throws Exception {
    Path input = Files.writeString(tempDir.resolve("input.warc"), "WARC/1.0\r\n");

    ArchiveExtractorJwarc defaults = new ArchiveExtractorJwarc();
    defaults.configure(Map.of("inputFiles", List.of(input.toString())));
    assertEquals(List.of(
        RecordFileWarc.class,
        RecordWarcRawBytes.class,
        RecordWarcJwarc.class,
        RecordWarcUniversal.class,
        pl.gov.nac.warc.records.warc.RecordWarcInFile.class),
        defaults.emittedOutputTypes());

    ArchiveExtractorJwarc universal = new ArchiveExtractorJwarc();
    universal.configure(Map.of("inputFiles", List.of(input.toString()), "output", "universal"));
    assertEquals(List.of(RecordWarcUniversal.class), universal.emittedOutputTypes());

    ArchiveExtractorJwarc bytes = new ArchiveExtractorJwarc();
    bytes.configure(Map.of("inputFiles", List.of(input.toString()), "output", "bytes"));
    assertEquals(List.of(RecordWarcRawBytes.class, RecordWarcUniversal.class), bytes.emittedOutputTypes());

    ArchiveExtractorJwarc nativeRecords = new ArchiveExtractorJwarc();
    nativeRecords.configure(Map.of("inputFiles", List.of(input.toString()), "output", "native"));
    assertEquals(List.of(RecordWarcJwarc.class, RecordWarcUniversal.class), nativeRecords.emittedOutputTypes());
  }

  @Test
  void beforeCheckRequiresAtLeastOneReadableRegularFile() throws Exception {
    ArchiveExtractorJwarc empty = new ArchiveExtractorJwarc();
    empty.configure(Map.of());
    assertFalse(empty.beforeCheck(Map.of()));

    ArchiveExtractorJwarc missing = new ArchiveExtractorJwarc();
    missing.configure(Map.of("inputFiles", List.of(tempDir.resolve("missing.warc").toString())));
    assertFalse(missing.beforeCheck(Map.of()));

    Path input = Files.writeString(tempDir.resolve("present.warc"), "WARC/1.0\r\n");
    ArchiveExtractorJwarc present = new ArchiveExtractorJwarc();
    present.configure(Map.of("inputFiles", List.of(input.toString())));
    assertTrue(present.beforeCheck(Map.of()));
  }

  @Test
  void negotiatedFileModePassesThroughEachWarcAndCompletes() throws Exception {
    Path first = Files.writeString(tempDir.resolve("first.warc"), "WARC/1.0\r\n");
    Path second = Files.writeString(tempDir.resolve("second.warc"), "WARC/1.0\r\n");
    ArchiveExtractorJwarc producer = new ArchiveExtractorJwarc();
    producer.configure(Map.of("inputFiles", List.of(first.toString(), second.toString())));
    producer.onNegotiatedOutputType(RecordFileWarc.class);

    CollectingSubscriber subscriber = new CollectingSubscriber();
    producer.subscribe(subscriber);
    producer.startProducing();

    assertNull(subscriber.failure);
    assertTrue(subscriber.completed);
    assertEquals(List.of(first, second), subscriber.items.stream()
        .map(RecordFileWarc.class::cast)
        .map(RecordFileWarc::path)
        .toList());
  }

  private static final class CollectingSubscriber implements Flow.Subscriber<Object> {
    private final List<Object> items = new ArrayList<>();
    private Throwable failure;
    private boolean completed;

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
      failure = throwable;
    }

    @Override
    public void onComplete() {
      completed = true;
    }
  }
}
