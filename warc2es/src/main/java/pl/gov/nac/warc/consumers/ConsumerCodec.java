package pl.gov.nac.warc.consumers;

import java.io.IOException;
import java.io.OutputStream;

import pl.gov.nac.warc.records.warc.RecordWarc;
import pl.gov.nac.warc.records.warc.RecordWarcInFile;
import pl.gov.nac.warc.records.warc.RecordWarcUniversal;
import pl.gov.nac.warc.utils.PooledBuffer;
import pl.gov.nac.warc.utils.WarcIO;

/**
 * Unified WARC consumer using custom codec (zero external WARC deps).
 * Extends ConsumerWarcBase with raw byte streaming.
 * Supports output formats: WARC, WARC+CDX, CDX.
 * Optimized for zero-copy paths and minimal heap allocations.
 *
 * Accepts: All RecordWarc subtypes, RecordWarcInFile, PooledBuffer
 */
public final class ConsumerCodec extends ConsumerWarcBase {

  @Override
  protected String getConsumerName() {
    return "Codec Archive writer";
  }

  @Override
  protected void openWriter(OutputStream stream) throws IOException {
    // Codec uses raw byte output - no external library writer
  }

  @Override
  protected void closeWriter() throws IOException {
    // No-op for raw byte output
  }

  @Override
  protected void writeRecordToStream(Object item, OutputStream stream) throws IOException {
    // Zero-copy path for PooledBuffer
    if (item instanceof PooledBuffer pb) {
      stream.write(pb.array, 0, pb.length);
      return;
    }

    // OPT-C1: Handle RecordWarcUniversal with streaming write (avoids byte[]
    // allocation)
    if (item instanceof RecordWarcUniversal rec) {
      WarcIO.writeWarcRecord(rec, stream);
      return;
    }

    // Handle all other RecordWarc types via rawBytes()
    if (item instanceof RecordWarc rec) {
      stream.write(rec.rawBytes());
      return;
    }

    // Handle RecordWarcInFile
    if (item instanceof RecordWarcInFile rec) {
      stream.write(rec.rawBytes());
      return;
    }

    throw new IOException("Cannot write record of type: " + item.getClass().getName());
  }
}
