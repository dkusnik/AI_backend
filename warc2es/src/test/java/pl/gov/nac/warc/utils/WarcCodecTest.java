package pl.gov.nac.warc.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import pl.gov.nac.warc.utils.WarcCodec.ParsedRecord;
import pl.gov.nac.warc.utils.WarcCodec.WarcRecordIterator;

public class WarcCodecTest {

  @Test
  public void testParseWarcRecord() throws IOException {
    String rawWarc = "WARC/1.0\r\n" +
        "WARC-Type: response\r\n" +
        "WARC-Target-URI: http://example.com\r\n" +
        "Content-Length: 5\r\n" +
        "My-Header: TestValue\r\n" +
        "\r\n" +
        "12345" +
        "\r\n\r\n";

    ByteArrayInputStream is = new ByteArrayInputStream(rawWarc.getBytes(StandardCharsets.UTF_8));
    // Use a small buffer to force multiple reads if possible, though strict testing
    // of vector requires more data
    try (WarcRecordIterator it = new WarcRecordIterator(is, false)) {
      assertTrue(it.hasNext());
      ParsedRecord record = it.next();

      assertEquals("WARC/1.0", record.getVersion());

      // Check if keys are lowercased (Required for WarcCodec internal logic)
      // If this fails, we need to fix WarcCodec or Ragel parser
      String type = record.getHeaders().get("warc-type");
      if (type == null) {
        // fallback to case-sensitive check to debug
        if (record.getHeaders().containsKey("WARC-Type")) {
          throw new RuntimeException("Headers are not lowercased! WarcCodec requires lowercased headers.");
        }
      }
      assertEquals("response", type);

      assertEquals("http://example.com", record.getHeaders().get("warc-target-uri"));
      assertEquals("5", record.getHeaders().get("content-length"));
      assertEquals("TestValue", record.getHeaders().get("my-header"));

      // Verify helper methods
      assertEquals(5, record.contentLength());
      assertEquals("response", record.type());

      assertEquals(5, record.getPayload().length);
      assertEquals("12345", new String(record.getPayload(), StandardCharsets.UTF_8));

      assertFalse(it.hasNext());
    }
  }

  @Test
  public void testHeaderSpacings() throws IOException {
    String rawWarc = "WARC/1.1\r\n" +
        "WARC-Type:resource\r\n" +
        "Content-Length:   10   \r\n" +
        "X-Custom-Header :  Value With Spaces  \r\n" +
        "\r\n" +
        "0123456789" +
        "\r\n\r\n";

    ByteArrayInputStream is = new ByteArrayInputStream(rawWarc.getBytes(StandardCharsets.UTF_8));
    try (WarcRecordIterator it = new WarcRecordIterator(is, false)) {
      assertTrue(it.hasNext());
      ParsedRecord record = it.next();

      assertEquals("WARC/1.1", record.getVersion());
      assertEquals("resource", record.type());
      assertEquals(10, record.contentLength());
      assertEquals("Value With Spaces", record.getHeaders().get("x-custom-header"));
    }
  }
}
