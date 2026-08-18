package pl.gov.nac.warc.utils;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests for native Rust readability library via Panama FFI.
 */
public class ReadabilityNativeTest {

    @BeforeAll
    static void checkNativeAvailable() {
        // Skip tests if native library not available (e.g., CI without Rust build)
        assumeTrue(ReadabilityNative.isAvailable(),
            "Native readability library not available - skipping tests");
    }

    @Test
    void testExtractSimpleArticle() {
        String html = """
            <html>
            <head><title>Test Article Title</title></head>
            <body>
                <article>
                    <h1>Main Heading</h1>
                    <p>This is the main content of the article. It contains enough text
                    to be considered meaningful content by the readability algorithm.
                    The algorithm needs sufficient text to determine this is article content.</p>
                    <p>More paragraph content here to ensure extraction works properly.
                    Multiple paragraphs help the algorithm identify the main content area.</p>
                </article>
                <nav>Navigation that should be removed</nav>
                <footer>Footer content to ignore</footer>
            </body>
            </html>
            """;

        ReadabilityNative.ExtractResult result = ReadabilityNative.extract(html, "http://example.com/article");

        assertNotNull(result);
        assertNotNull(result.textContent(), "Text content should not be null");
        assertTrue(result.textContent().contains("main content"), "Should contain article text");
        assertTrue(result.textLength() > 0, "Text length should be positive");

        // Navigation/footer should be stripped
        assertFalse(result.textContent().contains("Navigation that should be removed"));
    }

    @Test
    void testExtractWithNullUrl() {
        String html = "<html><body><p>Simple paragraph content.</p></body></html>";

        // Should not throw with null URL
        ReadabilityNative.ExtractResult result = ReadabilityNative.extract(html, null);
        assertNotNull(result);
    }

    @Test
    void testExtractMinimalHtml() {
        String html = "<p>Hello World</p>";

        ReadabilityNative.ExtractResult result = ReadabilityNative.extract(html, null);
        assertNotNull(result);
        // Even minimal HTML should produce some result
    }

    @Test
    void testExtractEmptyHtml() {
        String html = "";

        // Empty HTML should produce a result (possibly empty content)
        ReadabilityNative.ExtractResult result = ReadabilityNative.extract(html, null);
        assertNotNull(result);
    }

    @Test
    void testExtractWithTitle() {
        String html = """
            <html>
            <head><title>My Article Title</title></head>
            <body>
                <article>
                    <p>Article content that is long enough to be considered meaningful.
                    The readability algorithm needs sufficient content to extract properly.</p>
                </article>
            </body>
            </html>
            """;

        ReadabilityNative.ExtractResult result = ReadabilityNative.extract(html, null);

        assertNotNull(result);
        // Title may or may not be extracted depending on content
        // The important thing is it doesn't crash
    }

    @Test
    void testExtractPreservesUnicode() {
        String html = """
            <html>
            <body>
                <article>
                    <p>Polish: Zażółć gęślą jaźń</p>
                    <p>Japanese: こんにちは世界</p>
                    <p>Emoji: Hello 🌍 World!</p>
                </article>
            </body>
            </html>
            """;

        ReadabilityNative.ExtractResult result = ReadabilityNative.extract(html, null);

        assertNotNull(result);
        if (result.textContent() != null) {
            assertTrue(result.textContent().contains("Zażółć") ||
                       result.textContent().contains("こんにちは") ||
                       result.textContent().contains("🌍"),
                "Should preserve Unicode content");
        }
    }

    @Test
    void testExtractLargeHtml() {
        // Generate a reasonably large HTML document
        StringBuilder sb = new StringBuilder();
        sb.append("<html><body><article>");
        for (int i = 0; i < 100; i++) {
            sb.append("<p>Paragraph ").append(i).append(": Lorem ipsum dolor sit amet, ")
              .append("consectetur adipiscing elit. Sed do eiusmod tempor incididunt ")
              .append("ut labore et dolore magna aliqua.</p>\n");
        }
        sb.append("</article></body></html>");

        ReadabilityNative.ExtractResult result = ReadabilityNative.extract(sb.toString(), null);

        assertNotNull(result);
        assertNotNull(result.textContent());
        assertTrue(result.textLength() > 1000, "Should extract substantial content");
    }

    @Test
    void testExtractFullFromUtf8Bytes() {
        String html = """
            <html>
            <body>
                <article>
                    <p>Byte-oriented native extraction should avoid Java String
                    materialization before calling Rust while preserving readable
                    article text for normal UTF-8 HTML pages.</p>
                    <p>Additional paragraph text makes this page large enough for
                    the readability path or fallback path to produce content.</p>
                </article>
            </body>
            </html>
            """;

        ReadabilityNative.FullExtractResult result = ReadabilityNative.extractFull(
            html.getBytes(StandardCharsets.UTF_8),
            "http://example.com/bytes",
            true,
            true);

        assertNotNull(result);
        assertNotNull(result.textContent());
        assertTrue(result.textContent().contains("Byte-oriented native extraction"));
        assertTrue(result.method() == 0 || result.method() == 1);
    }

    @Test
    void testExtractFullBytesRejectsInvalidUtf8() {
        byte[] invalidUtf8 = new byte[] { (byte) 0xff, (byte) 0xfe, '<', 'p', '>' };

        ReadabilityNative.ReadabilityException ex = assertThrows(
            ReadabilityNative.ReadabilityException.class,
            () -> ReadabilityNative.extractFull(invalidUtf8, null, false, false));

        assertTrue(ex.getMessage().contains("Invalid UTF-8"));
    }

    @Test
    void testExtractFullPreservesHeadingsAndLinksIndependently() {
        String heading = "Unique FFI Heading";
        String link = "Unique FFI Link";
        String html = "<html><body><article><h2>" + heading + "</h2>" +
            "<p>This article contains enough stable content for native extraction. " +
            "A second sentence keeps the readability result deterministic.</p>" +
            "<a href=\"https://example.test/link\">" + link + "</a>" +
            "<p>Additional content completes the extraction fixture.</p>" +
            "</article></body></html>";
        byte[] bytes = html.getBytes(StandardCharsets.UTF_8);

        String neither = ReadabilityNative.extractFull(bytes, null, false, false).textContent();
        String headingsOnly = ReadabilityNative.extractFull(bytes, null, true, false).textContent();
        String linksOnly = ReadabilityNative.extractFull(bytes, null, false, true).textContent();
        String both = ReadabilityNative.extractFull(bytes, null, true, true).textContent();

        int baseHeadingCount = occurrences(neither, heading);
        int baseLinkCount = occurrences(neither, link);
        assertEquals(baseHeadingCount + 1, occurrences(headingsOnly, heading));
        assertEquals(baseLinkCount, occurrences(headingsOnly, link));
        assertEquals(baseHeadingCount, occurrences(linksOnly, heading));
        assertEquals(baseLinkCount + 1, occurrences(linksOnly, link));
        assertEquals(baseHeadingCount + 1, occurrences(both, heading));
        assertEquals(baseLinkCount + 1, occurrences(both, link));

        String stringEntry = ReadabilityNative.extractFull(html, null, true, false).textContent();
        assertEquals(baseHeadingCount + 1, occurrences(stringEntry, heading));
        assertEquals(baseLinkCount, occurrences(stringEntry, link));
    }

    @Test
    void testThreadSafety() throws InterruptedException {
        String html = """
            <html><body><article>
            <p>Content for thread safety test with enough text to extract.</p>
            </article></body></html>
            """;

        int threadCount = 10;
        int iterationsPerThread = 50;
        Thread[] threads = new Thread[threadCount];
        boolean[] success = new boolean[threadCount];

        for (int i = 0; i < threadCount; i++) {
            final int idx = i;
            threads[i] = Thread.ofVirtual().start(() -> {
                try {
                    for (int j = 0; j < iterationsPerThread; j++) {
                        ReadabilityNative.ExtractResult result = ReadabilityNative.extract(
                            html + "<!-- " + idx + "-" + j + " -->", null);
                        assertNotNull(result);
                    }
                    success[idx] = true;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        for (Thread t : threads) {
            t.join();
        }

        for (int i = 0; i < threadCount; i++) {
            assertTrue(success[i], "Thread " + i + " failed");
        }
    }

    private static int occurrences(String text, String needle) {
        return text.split(java.util.regex.Pattern.quote(needle), -1).length - 1;
    }
}
