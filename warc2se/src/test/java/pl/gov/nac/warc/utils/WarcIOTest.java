package pl.gov.nac.warc.utils;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * Tests for WarcIO payload-extraction correctness.
 *
 * <p>H-7 (T-225): findHeaderEnd() off-by-two for bare-LF headers.
 * The bare-LF fallback returned index of first '\n' (pointing into the
 * separator itself). Callers then added +4 (correct for \r\n\r\n) which
 * silently discarded 2 payload bytes when headers used \n\n.
 */
public class WarcIOTest {

  // -------------------------------------------------------------------------
  // H-7: bare-LF off-by-two — RED TESTS (must fail before the fix)
  // -------------------------------------------------------------------------

  @Test
  void testGetPayloadBareLFDoesNotSkipTwoExtraBytes() {
    // Header block terminated with bare \n\n (no CR)
    byte[] raw = "HDR\n\nHello".getBytes(ISO_8859_1);
    byte[] payload = WarcIO.getPayload(raw);
    assertEquals("Hello", new String(payload, ISO_8859_1),
        "Payload must start immediately after \\n\\n, not 2 bytes later");
  }

  @Test
  void testGetPayloadWithBareLFHttpResponse() {
    // Bare-LF HTTP response (non-compliant but encountered in the wild)
    String raw = "HTTP/1.0 200 OK\n"
        + "Content-Type: text/plain\n"
        + "\n"
        + "Hello World";
    byte[] payload = WarcIO.getHttpPayload(raw.getBytes(ISO_8859_1));
    assertEquals("Hello World", new String(payload, ISO_8859_1),
        "HTTP payload must be fully intact after bare-LF header block");
  }

  // -------------------------------------------------------------------------
  // Regression guard: CRLF path must still work after the fix
  // -------------------------------------------------------------------------

  @Test
  void testGetPayloadCrlfUnchanged() {
    byte[] raw = "HDR\r\n\r\nHello".getBytes(ISO_8859_1);
    byte[] payload = WarcIO.getPayload(raw);
    assertEquals("Hello", new String(payload, ISO_8859_1),
        "Standard CRLF path must not regress after bare-LF fix");
  }

  @Test
  void testGetHttpPayloadCrlfUnchanged() {
    String raw = "HTTP/1.1 200 OK\r\n"
        + "Content-Type: text/plain\r\n"
        + "\r\n"
        + "World";
    byte[] payload = WarcIO.getHttpPayload(raw.getBytes(ISO_8859_1));
    assertEquals("World", new String(payload, ISO_8859_1),
        "Standard CRLF HTTP payload must not regress after bare-LF fix");
  }
}
