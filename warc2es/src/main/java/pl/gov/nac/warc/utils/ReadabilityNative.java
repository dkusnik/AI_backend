package pl.gov.nac.warc.utils;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Panama FFI bridge to Rust readability library.
 *
 * This provides high-performance article extraction using Mozilla's Readability
 * algorithm implemented in Rust. Falls back gracefully if native library unavailable.
 *
 * <p>Thread-safety: All methods are thread-safe. Each extraction call is independent.</p>
 *
 * <p>Memory management: Native memory is automatically freed after each extraction.</p>
 */
public final class ReadabilityNative {

    private static final System.Logger LOG = System.getLogger(ReadabilityNative.class.getName());

    // Native library state
    private static final boolean AVAILABLE;
    private static final SymbolLookup LOOKUP;
    private static final Linker LINKER = Linker.nativeLinker();

    // Method handles for FFI calls
    private static final MethodHandle EXTRACT;
    private static final MethodHandle FREE_RESULT;
    private static final MethodHandle EXTRACT_FULL;
    private static final MethodHandle EXTRACT_FULL_BYTES;
    private static final MethodHandle FREE_FULL_RESULT;

    // Struct layout matching ExtractResult from readability.h
    // struct ExtractResult {
    //   char *title;        // offset 0
    //   char *content;      // offset 8
    //   char *text_content; // offset 16
    //   int32_t text_length;// offset 24
    //   char *error;        // offset 32 (after 4 bytes padding)
    // }
    private static final StructLayout EXTRACT_RESULT_LAYOUT = MemoryLayout.structLayout(
        ValueLayout.ADDRESS.withName("title"),
        ValueLayout.ADDRESS.withName("content"),
        ValueLayout.ADDRESS.withName("text_content"),
        ValueLayout.JAVA_INT.withName("text_length"),
        MemoryLayout.paddingLayout(4), // Padding for alignment
        ValueLayout.ADDRESS.withName("error")
    );

    private static final long TITLE_OFFSET = EXTRACT_RESULT_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("title"));
    private static final long CONTENT_OFFSET = EXTRACT_RESULT_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("content"));
    private static final long TEXT_CONTENT_OFFSET = EXTRACT_RESULT_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("text_content"));
    private static final long TEXT_LENGTH_OFFSET = EXTRACT_RESULT_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("text_length"));
    private static final long ERROR_OFFSET = EXTRACT_RESULT_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("error"));

    // Struct layout matching FullExtractResult from readability.h (Proposal #9)
    // Verified offsets via Rust offset_of! test: title=0, text_content=8, text_length=16, method=20, error=24, size=32
    private static final StructLayout FULL_RESULT_LAYOUT = MemoryLayout.structLayout(
        ValueLayout.ADDRESS.withName("title"),         // offset 0
        ValueLayout.ADDRESS.withName("text_content"),  // offset 8
        ValueLayout.JAVA_INT.withName("text_length"),  // offset 16
        ValueLayout.JAVA_INT.withName("method"),       // offset 20
        ValueLayout.ADDRESS.withName("error")          // offset 24
    );

    private static final long FULL_TITLE_OFFSET = 0;
    private static final long FULL_TEXT_OFFSET = 8;
    private static final long FULL_TEXT_LEN_OFFSET = 16;
    private static final long FULL_METHOD_OFFSET = 20;
    private static final long FULL_ERROR_OFFSET = 24;

    static {
        boolean available = false;
        SymbolLookup lookup = null;
        MethodHandle extract = null;
        MethodHandle freeResult = null;
        MethodHandle extractFull = null;
        MethodHandle extractFullBytes = null;
        MethodHandle freeFullResult = null;

        try {
            // Try to find native library
            Path libPath = findNativeLibrary();
            if (libPath != null) {
                lookup = SymbolLookup.libraryLookup(libPath, Arena.global());

                // Bind readability_extract(const char* html, const char* url) -> ExtractResult*
                extract = LINKER.downcallHandle(
                    lookup.find("readability_extract").orElseThrow(),
                    FunctionDescriptor.of(
                        ValueLayout.ADDRESS,  // returns ExtractResult*
                        ValueLayout.ADDRESS,  // html (const char*)
                        ValueLayout.ADDRESS   // url (const char*)
                    )
                );

                // Bind readability_free_result(ExtractResult* result)
                freeResult = LINKER.downcallHandle(
                    lookup.find("readability_free_result").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS)
                );

                // Bind readability_extract_full(html, url, preserve_headings, preserve_links)
                extractFull = LINKER.downcallHandle(
                    lookup.find("readability_extract_full").orElseThrow(),
                    FunctionDescriptor.of(
                        ValueLayout.ADDRESS,  // returns FullExtractResult*
                        ValueLayout.ADDRESS,  // html (const char*)
                        ValueLayout.ADDRESS,  // url (const char*)
                        ValueLayout.JAVA_BYTE, // preserve_headings (uint8_t)
                        ValueLayout.JAVA_BYTE // preserve_links (uint8_t)
                    )
                );

                // Bind readability_extract_full_bytes(html, len, url, preserve flags)
                extractFullBytes = LINKER.downcallHandle(
                    lookup.find("readability_extract_full_bytes").orElseThrow(),
                    FunctionDescriptor.of(
                        ValueLayout.ADDRESS,  // returns FullExtractResult*
                        ValueLayout.ADDRESS,  // html bytes (const uint8_t*)
                        ValueLayout.JAVA_LONG, // html_len (size_t on supported 64-bit Linux)
                        ValueLayout.ADDRESS,  // url (const char*)
                        ValueLayout.JAVA_BYTE, // preserve_headings (uint8_t)
                        ValueLayout.JAVA_BYTE // preserve_links (uint8_t)
                    )
                );

                // Bind readability_free_full_result(FullExtractResult* result)
                freeFullResult = LINKER.downcallHandle(
                    lookup.find("readability_free_full_result").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS)
                );

                available = true;
                LOG.log(System.Logger.Level.INFO, "Loaded native readability library from {0}", libPath);
            } else {
                LOG.log(System.Logger.Level.WARNING,
                    "Native readability library not found; falling back to Java extraction");
            }
        } catch (Exception e) {
            LOG.log(System.Logger.Level.WARNING, "Failed to load native readability library: {0}", e.getMessage());
        }

        AVAILABLE = available;
        LOOKUP = lookup;
        EXTRACT = extract;
        FREE_RESULT = freeResult;
        EXTRACT_FULL = extractFull;
        EXTRACT_FULL_BYTES = extractFullBytes;
        FREE_FULL_RESULT = freeFullResult;
    }

    /**
     * Check if native library is available.
     */
    public static boolean isAvailable() {
        return AVAILABLE;
    }

    /**
     * Extract article text from HTML using native Rust implementation.
     *
     * @param html HTML content to extract from
     * @param url Optional base URL for resolving relative links
     * @return Extraction result with title, content, and text
     * @throws IllegalStateException if native library is not available
     * @throws ReadabilityException if extraction fails
     */
    public static ExtractResult extract(String html, String url) {
        if (!AVAILABLE) {
            throw new IllegalStateException("Native readability library not available");
        }

        try (Arena arena = Arena.ofConfined()) {
            // Convert Java strings to C strings
            MemorySegment htmlPtr = arena.allocateFrom(html);
            MemorySegment urlPtr = (url != null && !url.isEmpty())
                ? arena.allocateFrom(url)
                : MemorySegment.NULL;

            // Call native function
            MemorySegment resultPtr = (MemorySegment) EXTRACT.invokeExact(htmlPtr, urlPtr);

            if (resultPtr.equals(MemorySegment.NULL)) {
                throw new ReadabilityException("Native extraction returned null");
            }

            try {
                // Reinterpret to full struct size for reading
                resultPtr = resultPtr.reinterpret(EXTRACT_RESULT_LAYOUT.byteSize());

                // Read error field first
                MemorySegment errorPtr = resultPtr.get(ValueLayout.ADDRESS, ERROR_OFFSET);
                if (!errorPtr.equals(MemorySegment.NULL)) {
                    String error = errorPtr.reinterpret(Integer.MAX_VALUE).getString(0);
                    throw new ReadabilityException(error);
                }

                // Read successful result
                String title = readCString(resultPtr, TITLE_OFFSET);
                String content = readCString(resultPtr, CONTENT_OFFSET);
                String textContent = readCString(resultPtr, TEXT_CONTENT_OFFSET);
                int textLength = resultPtr.get(ValueLayout.JAVA_INT, TEXT_LENGTH_OFFSET);

                return new ExtractResult(title, content, textContent, textLength);
            } finally {
                // Always free native memory
                FREE_RESULT.invokeExact(resultPtr);
            }
        } catch (ReadabilityException e) {
            throw e;
        } catch (Throwable t) {
            throw new ReadabilityException("Native extraction failed", t);
        }
    }

    /**
     * Read a null-terminated C string from a pointer field inside a struct.
     *
     * <p>The pointer returned by {@code struct.get(ADDRESS, offset)} has size 0 (unknown bounds).
     * We reinterpret it with {@code Integer.MAX_VALUE} to give the Panama API a large-enough
     * upper bound so that {@link MemorySegment#getString(long)} can scan for the NUL terminator.
     * Safety assumption: the Rust library guarantees that every non-null pointer in the result
     * struct points to a valid, NUL-terminated UTF-8 string whose length is well below 2 GB,
     * and that the string remains valid until {@code readability_free_result()} is called.
     * This method must therefore be invoked before the result struct is freed.
     */
    private static String readCString(MemorySegment struct, long offset) {
        MemorySegment ptr = struct.get(ValueLayout.ADDRESS, offset);
        if (ptr.equals(MemorySegment.NULL)) {
            return null;
        }
        return ptr.reinterpret(Integer.MAX_VALUE).getString(0);
    }

    /**
     * Find native library in common locations.
     */
    private static Path findNativeLibrary() {
        String libName = System.mapLibraryName("readability_jni");

        // Search paths in order of preference.
        // Note: pipeline-lib sets the "readability.native.path" system property
        // to the absolute path of the native library. If not set, we search
        // common relative paths from the project root or runtime environment.
        String[] searchPaths = {
            // 1. System property override (set by pipeline-lib / test runner)
            System.getProperty("readability.native.path"),
            // 2. Runtime: app/native/ (relative to out/ runtime root)
            "app/native/" + libName,
            // 3. Build output: target/dist/native/ (relative to warc2es/ project root)
            "target/dist/native/" + libName,
            // 4. Cargo output: src/native/target/release/ (relative to project root)
            "src/native/target/release/" + libName,
        };

        for (String pathStr : searchPaths) {
            if (pathStr == null) continue;
            Path path = Path.of(pathStr);
            if (path.toFile().exists()) {
                return path.toAbsolutePath();
            }
        }

        return null;
    }

    /**
     * Full extraction: readability + fallback + screen-reader in a single FFI call.
     * Eliminates all Java-side HTML parsing when native library is available.
     *
     * @param html HTML content to extract from
     * @param url Optional base URL for resolving relative links
     * @param preserveHeadings whether heading text is prepended
     * @param preserveLinks whether meaningful link text is prepended
     * @return Full extraction result with assembled text, title, and method indicator
     * @throws IllegalStateException if native library is not available
     * @throws ReadabilityException if extraction fails
     */
    public static FullExtractResult extractFull(
        String html, String url, boolean preserveHeadings, boolean preserveLinks) {
        if (!AVAILABLE) {
            throw new IllegalStateException("Native readability library not available");
        }

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment htmlPtr = arena.allocateFrom(html);
            MemorySegment urlPtr = (url != null && !url.isEmpty())
                ? arena.allocateFrom(url)
                : MemorySegment.NULL;

            MemorySegment resultPtr = (MemorySegment) EXTRACT_FULL.invokeExact(
                htmlPtr,
                urlPtr,
                (byte) (preserveHeadings ? 1 : 0),
                (byte) (preserveLinks ? 1 : 0));

            if (resultPtr.equals(MemorySegment.NULL)) {
                throw new ReadabilityException("Native full extraction returned null");
            }

            try {
                resultPtr = resultPtr.reinterpret(FULL_RESULT_LAYOUT.byteSize());

                // Check error first
                MemorySegment errorPtr = resultPtr.get(ValueLayout.ADDRESS, FULL_ERROR_OFFSET);
                if (!errorPtr.equals(MemorySegment.NULL)) {
                    String error = errorPtr.reinterpret(Integer.MAX_VALUE).getString(0);
                    throw new ReadabilityException(error);
                }

                String title = readCString(resultPtr, FULL_TITLE_OFFSET);
                String textContent = readCString(resultPtr, FULL_TEXT_OFFSET);
                int textLength = resultPtr.get(ValueLayout.JAVA_INT, FULL_TEXT_LEN_OFFSET);
                int method = resultPtr.get(ValueLayout.JAVA_INT, FULL_METHOD_OFFSET);

                return new FullExtractResult(title, textContent, textLength, method);
            } finally {
                FREE_FULL_RESULT.invokeExact(resultPtr);
            }
        } catch (ReadabilityException e) {
            throw e;
        } catch (Throwable t) {
            throw new ReadabilityException("Native full extraction failed", t);
        }
    }

    /**
     * Full extraction from a UTF-8 HTML byte buffer.
     *
     * <p>This avoids materializing a Java {@link String} before calling Rust. If
     * the bytes are not valid UTF-8 the Rust side returns an extraction error;
     * callers that process arbitrary web pages should catch it and use their
     * charset-tolerant Java fallback.</p>
     *
     * @param htmlUtf8 UTF-8 HTML bytes
     * @param url Optional base URL for resolving relative links
     * @param preserveHeadings whether heading text is prepended
     * @param preserveLinks whether meaningful link text is prepended
     * @return Full extraction result with assembled text, title, and method indicator
     * @throws IllegalStateException if native library is not available
     * @throws ReadabilityException if extraction fails
     */
    public static FullExtractResult extractFull(
        byte[] htmlUtf8, String url, boolean preserveHeadings, boolean preserveLinks) {
        if (!AVAILABLE) {
            throw new IllegalStateException("Native readability library not available");
        }
        Objects.requireNonNull(htmlUtf8, "htmlUtf8");

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment htmlPtr = htmlUtf8.length == 0
                ? MemorySegment.NULL
                : arena.allocateFrom(ValueLayout.JAVA_BYTE, htmlUtf8);
            MemorySegment urlPtr = (url != null && !url.isEmpty())
                ? arena.allocateFrom(url)
                : MemorySegment.NULL;

            MemorySegment resultPtr = (MemorySegment) EXTRACT_FULL_BYTES.invokeExact(
                htmlPtr,
                (long) htmlUtf8.length,
                urlPtr,
                (byte) (preserveHeadings ? 1 : 0),
                (byte) (preserveLinks ? 1 : 0));

            if (resultPtr.equals(MemorySegment.NULL)) {
                throw new ReadabilityException("Native full byte extraction returned null");
            }

            try {
                return readFullResult(resultPtr);
            } finally {
                FREE_FULL_RESULT.invokeExact(resultPtr);
            }
        } catch (ReadabilityException e) {
            throw e;
        } catch (Throwable t) {
            throw new ReadabilityException("Native full byte extraction failed", t);
        }
    }

    private static FullExtractResult readFullResult(MemorySegment resultPtr) {
        MemorySegment struct = resultPtr.reinterpret(FULL_RESULT_LAYOUT.byteSize());

        MemorySegment errorPtr = struct.get(ValueLayout.ADDRESS, FULL_ERROR_OFFSET);
        if (!errorPtr.equals(MemorySegment.NULL)) {
            String error = errorPtr.reinterpret(Integer.MAX_VALUE).getString(0);
            throw new ReadabilityException(error);
        }

        String title = readCString(struct, FULL_TITLE_OFFSET);
        String textContent = readCString(struct, FULL_TEXT_OFFSET);
        int textLength = struct.get(ValueLayout.JAVA_INT, FULL_TEXT_LEN_OFFSET);
        int method = struct.get(ValueLayout.JAVA_INT, FULL_METHOD_OFFSET);

        return new FullExtractResult(title, textContent, textLength, method);
    }

    /**
     * Result of article extraction.
     *
     * @param title Article title (may be null)
     * @param content Cleaned HTML content (may be null)
     * @param textContent Plain text content (may be null)
     * @param textLength Length of text content in bytes
     */
    public record ExtractResult(
        String title,
        String content,
        String textContent,
        int textLength
    ) {}

    /**
     * Result of full extraction (readability + fallback + screen-reader).
     *
     * @param title Article title (may be null)
     * @param textContent Assembled text: screen-reader text + primary text (may be null)
     * @param textLength Length of text content in bytes
     * @param method Extraction method: 0=readability, 1=fallback
     */
    public record FullExtractResult(
        String title,
        String textContent,
        int textLength,
        int method
    ) {}

    /**
     * Exception thrown when native extraction fails.
     */
    public static class ReadabilityException extends RuntimeException {
        public ReadabilityException(String message) {
            super(message);
        }

        public ReadabilityException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    // Prevent instantiation
    private ReadabilityNative() {}
}
