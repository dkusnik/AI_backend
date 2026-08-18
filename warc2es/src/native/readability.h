#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

/**
 * Result structure returned to caller via FFI
 * All strings are heap-allocated and must be freed with `readability_free_result`
 */
typedef struct ExtractResult {
  /**
   * Article title (may be null)
   */
  char *title;
  /**
   * Cleaned HTML content (may be null)
   */
  char *content;
  /**
   * Plain text content (may be null)
   */
  char *text_content;
  /**
   * Length of text_content in bytes
   */
  int32_t text_length;
  /**
   * Error message if extraction failed (null on success)
   */
  char *error;
} ExtractResult;

/**
 * Full extraction result: readability + fallback + screen-reader text.
 * All strings are heap-allocated and must be freed with `readability_free_full_result`.
 */
typedef struct FullExtractResult {
  /**
   * Article title (may be null)
   */
  char *title;
  /**
   * Final assembled text: sr_text + primary text (may be null)
   */
  char *text_content;
  /**
   * Length of text_content in bytes
   */
  int32_t text_length;
  /**
   * Extraction method: 0=readability, 1=fallback, -1=error
   */
  int32_t method;
  /**
   * Error message if extraction failed (null on success)
   */
  char *error;
} FullExtractResult;

/**
 * Extract article content from HTML
 *
 * # Arguments
 * * `html` - Null-terminated UTF-8 HTML string
 * * `url` - Null-terminated UTF-8 URL string (used for resolving relative links)
 *
 * # Returns
 * Pointer to ExtractResult. Caller must free with `readability_free_result`.
 *
 * # Safety
 * Both pointers must be valid null-terminated C strings or null.
 */
struct ExtractResult *readability_extract(const char *html, const char *url);

/**
 * Full extraction: readability + fallback + screen-reader text in a single call.
 *
 * When readability succeeds, returns readability text with method=0.
 * When readability returns empty, falls back to boilerplate-removal extraction (method=1).
 * Screen-reader text is prepended to the result when detected.
 * Non-zero preserve flags prepend the selected headings and meaningful link text.
 *
 * # Returns
 * Pointer to FullExtractResult. Caller must free with `readability_free_full_result`.
 *
 * # Safety
 * Both pointers must be valid null-terminated C strings or null.
 */
struct FullExtractResult *readability_extract_full(const char *html, const char *url,
                                                   uint8_t preserve_headings,
                                                   uint8_t preserve_links);

/**
 * Full extraction from a non-null-terminated UTF-8 byte buffer.
 *
 * # Returns
 * Pointer to FullExtractResult. Caller must free with `readability_free_full_result`.
 *
 * # Safety
 * html must point to at least html_len readable bytes unless html_len is zero.
 * url, when non-null, must be a valid null-terminated UTF-8 C string.
 */
struct FullExtractResult *readability_extract_full_bytes(const uint8_t *html, size_t html_len,
                                                         const char *url,
                                                         uint8_t preserve_headings,
                                                         uint8_t preserve_links);

/**
 * Free an ExtractResult allocated by `readability_extract`
 *
 * # Safety
 * Pointer must have been returned by `readability_extract` and not already freed.
 */
void readability_free_result(struct ExtractResult *result);

/**
 * Free a FullExtractResult allocated by `readability_extract_full`
 *
 * # Safety
 * Pointer must have been returned by `readability_extract_full` and not already freed.
 */
void readability_free_full_result(struct FullExtractResult *result);
