//! FFI bridge for Rust readability library
//!
//! Provides C-compatible functions for extracting article content from HTML.
//! Designed for use with Java Panama FFI or JNI.
//!
//! Two extraction APIs:
//! - `readability_extract` — readability-only extraction (original API)
//! - `readability_extract_full` — readability + fallback + screen-reader (Proposal #9)

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;

use kuchiki::traits::TendrilSink;
use readable_readability::Readability;

// ---------------------------------------------------------------------------
// Original ExtractResult (retained Java ABI)
// ---------------------------------------------------------------------------

/// Result structure returned to caller via FFI
/// All strings are heap-allocated and must be freed with `readability_free_result`
#[repr(C)]
pub struct ExtractResult {
    /// Article title (may be null)
    pub title: *mut c_char,
    /// Cleaned HTML content (may be null)
    pub content: *mut c_char,
    /// Plain text content (may be null)
    pub text_content: *mut c_char,
    /// Length of text_content in bytes
    pub text_length: i32,
    /// Error message if extraction failed (null on success)
    pub error: *mut c_char,
}

impl ExtractResult {
    fn success(title: Option<String>, content: Option<String>, text: Option<String>) -> *mut Self {
        let text_len = text.as_ref().map(|t| t.len() as i32).unwrap_or(0);

        let result = Box::new(ExtractResult {
            title: string_to_c(title),
            content: string_to_c(content),
            text_content: string_to_c(text),
            text_length: text_len,
            error: ptr::null_mut(),
        });

        Box::into_raw(result)
    }

    fn error(msg: String) -> *mut Self {
        let result = Box::new(ExtractResult {
            title: ptr::null_mut(),
            content: ptr::null_mut(),
            text_content: ptr::null_mut(),
            text_length: 0,
            error: string_to_c(Some(msg)),
        });

        Box::into_raw(result)
    }
}

// ---------------------------------------------------------------------------
// FullExtractResult (Proposal #9 — single-call full extraction)
// ---------------------------------------------------------------------------

/// Full extraction result: readability + fallback + screen-reader text.
/// All strings are heap-allocated and must be freed with `readability_free_full_result`.
#[repr(C)]
pub struct FullExtractResult {
    /// Article title (may be null)
    pub title: *mut c_char,
    /// Final assembled text: sr_text + primary text (may be null if both paths empty)
    pub text_content: *mut c_char,
    /// Length of text_content in bytes
    pub text_length: i32,
    /// Extraction method: 0=readability, 1=fallback
    pub method: i32,
    /// Error message if extraction failed (null on success)
    pub error: *mut c_char,
}

impl FullExtractResult {
    fn success(title: Option<String>, text: String, method: i32) -> *mut Self {
        let text_length = text.len() as i32;
        let text_opt = if text.is_empty() { None } else { Some(text) };

        let result = Box::new(FullExtractResult {
            title: string_to_c(title),
            text_content: string_to_c(text_opt),
            text_length,
            method,
            error: ptr::null_mut(),
        });
        Box::into_raw(result)
    }

    fn error(msg: String) -> *mut Self {
        let result = Box::new(FullExtractResult {
            title: ptr::null_mut(),
            text_content: ptr::null_mut(),
            text_length: 0,
            method: -1,
            error: string_to_c(Some(msg)),
        });
        Box::into_raw(result)
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Convert Option<String> to C string pointer (caller must free)
fn string_to_c(s: Option<String>) -> *mut c_char {
    match s {
        Some(mut string) => {
            string.retain(|c| c != '\0');
            CString::new(string)
                .map(|cs| cs.into_raw())
                .unwrap_or(ptr::null_mut())
        }
        None => ptr::null_mut(),
    }
}

/// Parse C string pointers into Rust string references.
/// Returns (html_str, url_option) or an error string.
unsafe fn parse_ffi_inputs<'a>(
    html: *const c_char,
    url: *const c_char,
) -> Result<(&'a str, Option<&'a str>), String> {
    if html.is_null() {
        return Err("html pointer is null".to_string());
    }

    let html_str = unsafe { CStr::from_ptr(html) }
        .to_str()
        .map_err(|e| format!("Invalid UTF-8 in html: {}", e))?;

    let url_str = if url.is_null() {
        None
    } else {
        match unsafe { CStr::from_ptr(url) }.to_str() {
            Ok(s) if !s.is_empty() => Some(s),
            _ => None,
        }
    };

    Ok((html_str, url_str))
}

/// Parse a byte buffer plus optional C URL into Rust string references.
/// The HTML buffer is not null-terminated; callers pass the exact byte length.
unsafe fn parse_ffi_bytes_inputs<'a>(
    html: *const u8,
    html_len: usize,
    url: *const c_char,
) -> Result<(&'a str, Option<&'a str>), String> {
    if html.is_null() {
        if html_len == 0 {
            let url_str = parse_ffi_url(url);
            return Ok(("", url_str));
        }
        return Err("html pointer is null".to_string());
    }

    let bytes = unsafe { std::slice::from_raw_parts(html, html_len) };
    let html_str =
        std::str::from_utf8(bytes).map_err(|e| format!("Invalid UTF-8 in html bytes: {}", e))?;

    let url_str = parse_ffi_url(url);
    Ok((html_str, url_str))
}

fn parse_ffi_url<'a>(url: *const c_char) -> Option<&'a str> {
    if url.is_null() {
        return None;
    }
    match unsafe { CStr::from_ptr(url) }.to_str() {
        Ok(s) if !s.is_empty() => Some(s),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Original FFI exports (unchanged)
// ---------------------------------------------------------------------------

fn panic_error(context: &str) -> String {
    format!("panic crossed Rust FFI boundary in {}", context)
}

/// Extract article content from HTML
///
/// # Safety
/// Both pointers must be valid null-terminated C strings or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn readability_extract(
    html: *const c_char,
    url: *const c_char,
) -> *mut ExtractResult {
    match catch_unwind(AssertUnwindSafe(|| unsafe {
        readability_extract_impl(html, url)
    })) {
        Ok(result) => result,
        Err(_) => ExtractResult::error(panic_error("readability_extract")),
    }
}

unsafe fn readability_extract_impl(html: *const c_char, url: *const c_char) -> *mut ExtractResult {
    let (html_str, url_str) = match unsafe { parse_ffi_inputs(html, url) } {
        Ok(v) => v,
        Err(e) => return ExtractResult::error(e),
    };

    match extract_article(html_str, url_str) {
        Ok((title, content, text)) => ExtractResult::success(title, content, text),
        Err(e) => ExtractResult::error(e),
    }
}

/// Free an ExtractResult allocated by `readability_extract`.
///
/// # Safety
/// `result` must be null or a live pointer returned by `readability_extract`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn readability_free_result(result: *mut ExtractResult) {
    let _ = catch_unwind(AssertUnwindSafe(|| unsafe {
        readability_free_result_impl(result)
    }));
}

unsafe fn readability_free_result_impl(result: *mut ExtractResult) {
    if result.is_null() {
        return;
    }

    let r = unsafe { Box::from_raw(result) };
    unsafe {
        free_c_string(r.title);
        free_c_string(r.content);
        free_c_string(r.text_content);
        free_c_string(r.error);
    }
}

// ---------------------------------------------------------------------------
// Proposal #9: Full extraction FFI exports
// ---------------------------------------------------------------------------

/// Full extraction: readability + fallback + screen-reader text in a single call.
///
/// When readability succeeds, returns readability text with method=0.
/// When readability returns empty, falls back to boilerplate-removal extraction (method=1).
/// Screen-reader text is prepended to the result when detected.
///
/// # Safety
/// Both pointers must be valid null-terminated C strings or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn readability_extract_full(
    html: *const c_char,
    url: *const c_char,
    preserve_headings: u8,
    preserve_links: u8,
) -> *mut FullExtractResult {
    match catch_unwind(AssertUnwindSafe(|| unsafe {
        readability_extract_full_impl(html, url, preserve_headings, preserve_links)
    })) {
        Ok(result) => result,
        Err(_) => FullExtractResult::error(panic_error("readability_extract_full")),
    }
}

unsafe fn readability_extract_full_impl(
    html: *const c_char,
    url: *const c_char,
    preserve_headings: u8,
    preserve_links: u8,
) -> *mut FullExtractResult {
    let (html_str, url_str) = match unsafe { parse_ffi_inputs(html, url) } {
        Ok(v) => v,
        Err(e) => return FullExtractResult::error(e),
    };

    let (title, text, method) = extract_full(
        html_str,
        url_str,
        preserve_headings != 0,
        preserve_links != 0,
    );
    FullExtractResult::success(title, text, method)
}

/// Full extraction from a non-null-terminated UTF-8 byte buffer.
///
/// # Safety
/// `html` must point to at least `html_len` readable bytes unless `html_len` is
/// zero. `url`, when non-null, must be a valid null-terminated UTF-8 C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn readability_extract_full_bytes(
    html: *const u8,
    html_len: usize,
    url: *const c_char,
    preserve_headings: u8,
    preserve_links: u8,
) -> *mut FullExtractResult {
    match catch_unwind(AssertUnwindSafe(|| unsafe {
        readability_extract_full_bytes_impl(html, html_len, url, preserve_headings, preserve_links)
    })) {
        Ok(result) => result,
        Err(_) => FullExtractResult::error(panic_error("readability_extract_full_bytes")),
    }
}

unsafe fn readability_extract_full_bytes_impl(
    html: *const u8,
    html_len: usize,
    url: *const c_char,
    preserve_headings: u8,
    preserve_links: u8,
) -> *mut FullExtractResult {
    let (html_str, url_str) = match unsafe { parse_ffi_bytes_inputs(html, html_len, url) } {
        Ok(v) => v,
        Err(e) => return FullExtractResult::error(e),
    };

    let (title, text, method) = extract_full(
        html_str,
        url_str,
        preserve_headings != 0,
        preserve_links != 0,
    );
    FullExtractResult::success(title, text, method)
}

/// Free a FullExtractResult allocated by `readability_extract_full`.
///
/// # Safety
/// `result` must be null or a live pointer returned by `readability_extract_full`
/// or `readability_extract_full_bytes`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn readability_free_full_result(result: *mut FullExtractResult) {
    let _ = catch_unwind(AssertUnwindSafe(|| unsafe {
        readability_free_full_result_impl(result)
    }));
}

unsafe fn readability_free_full_result_impl(result: *mut FullExtractResult) {
    if result.is_null() {
        return;
    }

    let r = unsafe { Box::from_raw(result) };
    unsafe {
        free_c_string(r.title);
        free_c_string(r.text_content);
        free_c_string(r.error);
    }
}

/// Helper to free a CString pointer if non-null
unsafe fn free_c_string(ptr: *mut c_char) {
    if !ptr.is_null() {
        let _ = unsafe { CString::from_raw(ptr) };
    }
}

// ---------------------------------------------------------------------------
// Internal extraction logic
// ---------------------------------------------------------------------------

type ArticleExtraction = (Option<String>, Option<String>, Option<String>);

/// Internal extraction using readability crate (unchanged from original)
fn extract_article(html: &str, url: Option<&str>) -> Result<ArticleExtraction, String> {
    let mut readability = Readability::new();
    readability
        .strip_unlikelys(true)
        .weight_classes(true)
        .clean_conditionally(true);

    if let Some(base_url) = url {
        if let Ok(parsed) = base_url.parse() {
            readability.base_url(Some(parsed));
        }
    }

    let (content_node, metadata) = readability.parse(html);

    let text_content = content_node.text_contents();
    let text = if text_content.trim().is_empty() {
        None
    } else {
        Some(text_content)
    };

    let html_content = content_node.to_string();
    let content = if html_content.trim().is_empty() {
        None
    } else {
        Some(html_content)
    };

    let title = metadata
        .article_title
        .filter(|t| !t.is_empty())
        .or_else(|| metadata.page_title.filter(|t| !t.is_empty()));

    Ok((title, content, text))
}

/// Full extraction: readability + fallback + screen-reader in one call.
fn extract_full(
    html: &str,
    url: Option<&str>,
    preserve_headings: bool,
    preserve_links: bool,
) -> (Option<String>, String, i32) {
    let screen_reader_fragments = extract_screen_reader_fragments(html);
    let preserved_text = extract_preserved_text(html, preserve_headings, preserve_links);

    // Phase 1: Try readability
    let readability_result = extract_article(html, url);

    // Phase 2: Determine primary text and method
    let (text, title, method) = match readability_result {
        Ok((title, _, Some(ref t))) if !t.trim().is_empty() => {
            (t.clone(), title, 0) // readability success
        }
        _ => {
            // Readability returned empty or failed — fallback extraction
            let (fallback_text, fallback_title) = extract_fallback(html);
            (fallback_text, fallback_title, 1)
        }
    };

    // Phase 3: Match the Java fallback's assembly order: headings, links,
    // primary text. Accessibility fragments remain the outermost prefix.
    let text_with_preserved = format!("{preserved_text}{text}");
    let final_text = prepend_missing_fragments(text_with_preserved, &screen_reader_fragments);

    (title, final_text, method)
}

fn extract_preserved_text(html: &str, preserve_headings: bool, preserve_links: bool) -> String {
    if !preserve_headings && !preserve_links {
        return String::new();
    }

    let document = kuchiki::parse_html().one(html);
    remove_boilerplate(&document);
    let mut text = String::new();

    if preserve_headings {
        if let Ok(matches) = document.select("h1, h2, h3, h4, h5, h6") {
            for heading in matches {
                let heading_text = heading.text_contents();
                let trimmed = heading_text.trim();
                if !trimmed.is_empty() {
                    text.push_str(trimmed);
                    text.push('\n');
                }
            }
        }
    }

    if preserve_links {
        if let Ok(matches) = document.select("a[href]") {
            for link in matches {
                let link_text = link.text_contents();
                let trimmed = link_text.trim();
                if trimmed.chars().count() > 2 {
                    text.push_str(trimmed);
                    text.push(' ');
                }
            }
        }
    }

    text
}

fn extract_screen_reader_fragments(html: &str) -> Vec<String> {
    if !html.contains("screen-reader-text")
        && !html.contains("sr-only")
        && !html.contains("visually-hidden")
    {
        return Vec::new();
    }

    let document = kuchiki::parse_html().one(html);
    let mut fragments = Vec::new();
    if let Ok(matches) =
        document.select(".screen-reader-text, [class*=sr-only], [class*=visually-hidden]")
    {
        for node in matches {
            let text: String = node.text_contents();
            let trimmed = text.trim();
            if !trimmed.is_empty() {
                fragments.push(trimmed.to_string());
            }
        }
    }
    fragments
}

fn prepend_missing_fragments(text: String, fragments: &[String]) -> String {
    let mut normalized_available = normalize_whitespace(&text);
    let mut prefix = String::new();

    for fragment in fragments {
        let normalized_fragment = normalize_whitespace(fragment);
        if normalized_fragment.is_empty() || normalized_available.contains(&normalized_fragment) {
            continue;
        }
        prefix.push_str(fragment.trim());
        prefix.push(' ');
        if !normalized_available.is_empty() {
            normalized_available.push(' ');
        }
        normalized_available.push_str(&normalized_fragment);
    }

    prefix.push_str(&text);
    prefix
}

fn normalize_whitespace(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn remove_boilerplate(document: &kuchiki::NodeRef) {
    let remove_selectors = [
        "script",
        "style",
        "nav",
        "header",
        "footer",
        "aside",
        "[role=navigation]",
        "[role=banner]",
        "[class*=menu]",
        "[class*=nav]",
        "[id*=menu]",
        "[id*=nav]",
        ".skip-link",
        ".cookie",
        ".advertisement",
    ];
    for selector in remove_selectors {
        if let Ok(matches) = document.select(selector) {
            let nodes: Vec<kuchiki::NodeDataRef<kuchiki::ElementData>> = matches.collect();
            for node in nodes {
                node.as_node().detach();
            }
        }
    }
}

/// Fallback extraction when readability returns empty.
/// Removes boilerplate elements, extracts from semantic containers,
/// falls back to full body text. Mirrors Java extractWithJsoupFallback().
fn extract_fallback(html: &str) -> (String, Option<String>) {
    let document = kuchiki::parse_html().one(html);

    // Remove boilerplate elements (mirrors Java Jsoup removal selectors)
    remove_boilerplate(&document);

    // Extract title: <title>, fallback to <h1>
    let title = document
        .select_first("title")
        .ok()
        .map(|el: kuchiki::NodeDataRef<kuchiki::ElementData>| el.text_contents().trim().to_string())
        .filter(|t: &String| !t.is_empty())
        .or_else(|| {
            document
                .select_first("h1")
                .ok()
                .map(|el: kuchiki::NodeDataRef<kuchiki::ElementData>| {
                    el.text_contents().trim().to_string()
                })
                .filter(|t: &String| !t.is_empty())
        });

    // Extract from semantic containers
    let semantic_selectors = [
        "main",
        "article",
        "[role=main]",
        ".content",
        ".entry-content",
    ];
    let mut text = String::new();
    for sel_str in &semantic_selectors {
        let mut candidate = String::new();
        if let Ok(matches) = document.select(sel_str) {
            for node in matches {
                let t: String = node.text_contents();
                let trimmed = t.trim();
                if !trimmed.is_empty() {
                    candidate.push_str(trimmed);
                    candidate.push(' ');
                }
            }
        }
        if candidate.trim().len() >= 50 {
            text = candidate;
            break;
        }
    }

    // Fallback to body text if semantic extraction found too little
    if text.len() < 50 {
        if let Ok(body) = document.select_first("body") {
            text = body.text_contents();
        }
    }

    (text.trim().to_string(), title)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    // -- Original API tests --

    #[test]
    fn test_extract_simple_html() {
        let html = r#"
            <html>
            <head><title>Test Article</title></head>
            <body>
                <article>
                    <h1>Main Heading</h1>
                    <p>This is the main content of the article. It should be extracted properly.</p>
                    <p>More content here with enough text to be considered meaningful content.</p>
                </article>
            </body>
            </html>
        "#;

        let html_c = CString::new(html).unwrap();
        let url_c = CString::new("http://example.com/article").unwrap();

        unsafe {
            let result = readability_extract(html_c.as_ptr(), url_c.as_ptr());
            assert!(!result.is_null());

            let r = &*result;
            assert!(r.error.is_null(), "Expected no error");

            readability_free_result(result);
        }
    }

    #[test]
    fn test_null_html() {
        unsafe {
            let result = readability_extract(ptr::null(), ptr::null());
            assert!(!result.is_null());

            let r = &*result;
            assert!(!r.error.is_null(), "Expected error for null input");

            readability_free_result(result);
        }
    }

    // -- Full extraction tests (Proposal #9) --

    #[test]
    fn test_full_readability_success() {
        let html = r#"<html><head><title>Article Title</title></head><body>
            <article>
                <h1>Main Heading</h1>
                <p>This is the main content of the article with enough text to pass
                the readability threshold. We need several sentences here to make sure
                that the readability algorithm considers this a real article and not
                just boilerplate navigation text.</p>
                <p>Second paragraph with additional meaningful content that helps
                establish this as a genuine article worth extracting.</p>
            </article>
            <nav>Navigation should be ignored</nav>
        </body></html>"#;

        let (title, text, method) = extract_full(html, Some("http://example.com"), false, false);
        assert_eq!(method, 0, "should use readability path");
        assert!(!text.is_empty(), "text should not be empty");
        assert!(
            !text.contains("Navigation should be ignored"),
            "nav should be stripped"
        );
        assert!(title.is_some(), "title should be extracted");
    }

    #[test]
    fn test_full_fallback_on_gallery_page() {
        // Simulates gallery pages where readability strips all content
        let html = r#"<html><head><title>Gallery</title></head><body>
            <div class="gallery">
                <figure><img src="1.jpg"><figcaption>Photo caption one</figcaption></figure>
                <figure><img src="2.jpg"><figcaption>Photo caption two</figcaption></figure>
            </div>
        </body></html>"#;

        let (_, text, method) = extract_full(html, None, false, false);
        assert_eq!(method, 1, "should use fallback path");
        assert!(
            text.contains("Photo caption") || !text.is_empty(),
            "fallback should extract body text"
        );
    }

    #[test]
    fn test_full_screen_reader_prepended() {
        let html = r#"<html><body>
            <span class="screen-reader-text">Skip to content</span>
            <article>
                <p>Main article text with enough meaningful content to pass the
                readability threshold and be considered a real article.</p>
                <p>More content for the readability algorithm to find.</p>
            </article>
        </body></html>"#;

        let (_, text, _) = extract_full(html, None, false, false);
        assert!(
            text.starts_with("Skip to content"),
            "sr text should be prepended, got: {}",
            &text[..text.len().min(60)]
        );
        assert_eq!(text.matches("Skip to content").count(), 1);
    }

    #[test]
    fn test_full_no_screen_reader_no_overhead() {
        let html = r#"<html><body>
            <article>
                <p>Normal page without any accessibility spans at all. This page
                has enough content for readability to extract something meaningful.</p>
                <p>Additional content paragraph here.</p>
            </article>
        </body></html>"#;

        let fragments = extract_screen_reader_fragments(html);
        assert!(fragments.is_empty(), "no sr text expected");
    }

    #[test]
    fn test_preserved_headings_and_links_are_independent() {
        let html = r#"<html><body><nav><h2>Ignored Navigation Heading</h2></nav>
            <article><h2>Unique Heading</h2>
            <a href="https://example.test/destination">Unique Link</a>
            <p>Primary article text with enough detail to produce a stable extraction result.</p>
            </article></body></html>"#;

        assert_eq!(extract_preserved_text(html, false, false), "");
        assert_eq!(
            extract_preserved_text(html, true, false),
            "Unique Heading\n"
        );
        assert_eq!(extract_preserved_text(html, false, true), "Unique Link ");
        assert_eq!(
            extract_preserved_text(html, true, true),
            "Unique Heading\nUnique Link "
        );
    }

    #[test]
    fn test_fallback_removes_boilerplate() {
        let html = r#"<html><body>
            <nav>Navigation menu items</nav>
            <main><p>Main content here that should be extracted by fallback.</p></main>
            <footer>Footer text should be removed</footer>
        </body></html>"#;

        let (text, _) = extract_fallback(html);
        assert!(text.contains("Main content"), "should have main content");
        assert!(!text.contains("Navigation menu"), "nav should be removed");
        assert!(!text.contains("Footer text"), "footer should be removed");
    }

    #[test]
    fn test_fallback_nested_semantic_content_is_emitted_once() {
        let unique =
            "Nested semantic content must be emitted exactly once by the fallback extractor.";
        let html =
            format!("<html><body><main><article><p>{unique}</p></article></main></body></html>");

        let (text, _) = extract_fallback(&html);
        assert_eq!(text.matches(unique).count(), 1, "got: {text}");
    }

    #[test]
    fn test_fallback_extracts_title() {
        let html = r#"<html><head><title>Page Title</title></head>
            <body><p>Some content</p></body></html>"#;

        let (_, title) = extract_fallback(html);
        assert_eq!(title, Some("Page Title".to_string()));
    }

    #[test]
    fn test_fallback_title_from_h1() {
        let html = r#"<html><body>
            <h1>Heading Title</h1>
            <p>Some content</p>
        </body></html>"#;

        let (_, title) = extract_fallback(html);
        assert_eq!(title, Some("Heading Title".to_string()));
    }

    #[test]
    fn test_fallback_body_text_when_no_semantic() {
        let html = r#"<html><body>
            <div><p>Just some body text without semantic containers but long enough
            to be considered real content by the fallback extractor.</p></div>
        </body></html>"#;

        let (text, _) = extract_fallback(html);
        assert!(text.contains("body text"), "should fall back to body text");
    }

    #[test]
    fn test_screen_reader_sr_only() {
        let html = r#"<html><body>
            <span class="sr-only">Screen reader only</span>
            <p>Visible text</p>
        </body></html>"#;

        let sr = extract_screen_reader_fragments(html).join(" ");
        assert!(
            sr.contains("Screen reader only"),
            "should extract sr-only text"
        );
    }

    #[test]
    fn test_screen_reader_visually_hidden() {
        let html = r#"<html><body>
            <div class="visually-hidden">Hidden text</div>
            <p>Visible text</p>
        </body></html>"#;

        let sr = extract_screen_reader_fragments(html).join(" ");
        assert!(sr.contains("Hidden text"), "should extract visually-hidden");
    }

    #[test]
    fn test_screen_reader_fragment_already_present_is_not_prepended() {
        let fragment = "Skip to content".to_string();
        let text = "Skip   to content Main article text".to_string();
        let result = prepend_missing_fragments(text, &[fragment]);

        assert_eq!(
            normalize_whitespace(&result)
                .matches("Skip to content")
                .count(),
            1
        );
    }

    #[test]
    fn test_screen_reader_fragment_absent_is_prepended_once() {
        let fragment = "Skip to content".to_string();
        let result = prepend_missing_fragments("Main article text".to_string(), &[fragment]);

        assert_eq!(result.matches("Skip to content").count(), 1);
        assert!(result.starts_with("Skip to content "));
    }

    #[test]
    fn test_full_ffi_roundtrip() {
        let html = r#"<html><body>
            <article>
                <p>Content for FFI roundtrip test with enough text to pass readability.</p>
                <p>More meaningful content here to ensure extraction works.</p>
            </article>
        </body></html>"#;
        let html_c = CString::new(html).unwrap();

        unsafe {
            let result = readability_extract_full(html_c.as_ptr(), ptr::null(), 0, 0);
            assert!(!result.is_null());

            let r = &*result;
            assert!(r.error.is_null(), "expected no error");
            assert!(r.method == 0 || r.method == 1, "method should be 0 or 1");

            readability_free_full_result(result);
        }
    }

    #[test]
    fn test_full_ffi_bytes_roundtrip() {
        let html = r#"<html><body>
            <article>
                <p>Content for byte FFI roundtrip test with enough text to pass readability.</p>
                <p>More meaningful content here to ensure extraction works.</p>
            </article>
        </body></html>"#;
        let bytes = html.as_bytes();

        unsafe {
            let result =
                readability_extract_full_bytes(bytes.as_ptr(), bytes.len(), ptr::null(), 0, 0);
            assert!(!result.is_null());

            let r = &*result;
            assert!(r.error.is_null(), "expected no error");
            assert!(r.method == 0 || r.method == 1, "method should be 0 or 1");

            readability_free_full_result(result);
        }
    }

    #[test]
    fn test_full_ffi_bytes_rejects_invalid_utf8() {
        let bytes = [0xff, 0xfe, b'<', b'p', b'>'];

        unsafe {
            let result =
                readability_extract_full_bytes(bytes.as_ptr(), bytes.len(), ptr::null(), 0, 0);
            assert!(!result.is_null());

            let r = &*result;
            assert!(!r.error.is_null(), "expected UTF-8 error");
            assert_eq!(r.method, -1);

            readability_free_full_result(result);
        }
    }

    #[test]
    fn test_full_ffi_null_html() {
        unsafe {
            let result = readability_extract_full(ptr::null(), ptr::null(), 0, 0);
            assert!(!result.is_null());

            let r = &*result;
            assert!(!r.error.is_null(), "expected error for null input");
            assert_eq!(r.method, -1);

            readability_free_full_result(result);
        }
    }

    #[test]
    fn test_pathological_html_ffi_entries_do_not_abort() {
        let samples = [
            "<html><body><article><p>unterminated",
            "<!doctype html><html><body><div><div><div><p>deep text</body>",
            "<html><body><script><<<<<</script><main>Visible text after malformed script</main></body></html>",
            "<html><body><a href=\"http://[invalid-url\">bad url</a><p>text</p></body></html>",
        ];

        for sample in samples {
            let html_c = CString::new(sample).unwrap();
            unsafe {
                let result = readability_extract(html_c.as_ptr(), ptr::null());
                assert!(!result.is_null());
                readability_free_result(result);

                let full = readability_extract_full(html_c.as_ptr(), ptr::null(), 0, 0);
                assert!(!full.is_null());
                readability_free_full_result(full);
            }
        }
    }

    #[test]
    fn test_ffi_abi_layout_and_signatures() {
        // Verify exported function signatures and result layouts match what Java expects.
        let _: unsafe extern "C" fn(*const c_char, *const c_char) -> *mut ExtractResult =
            readability_extract;
        let _: unsafe extern "C" fn(*mut ExtractResult) = readability_free_result;
        let _: unsafe extern "C" fn(
            *const c_char,
            *const c_char,
            u8,
            u8,
        ) -> *mut FullExtractResult = readability_extract_full;
        let _: unsafe extern "C" fn(
            *const u8,
            usize,
            *const c_char,
            u8,
            u8,
        ) -> *mut FullExtractResult = readability_extract_full_bytes;
        let _: unsafe extern "C" fn(*mut FullExtractResult) = readability_free_full_result;

        assert_eq!(
            std::mem::offset_of!(ExtractResult, title),
            0,
            "title offset"
        );
        assert_eq!(
            std::mem::offset_of!(ExtractResult, content),
            8,
            "content offset"
        );
        assert_eq!(
            std::mem::offset_of!(ExtractResult, text_content),
            16,
            "text_content offset"
        );
        assert_eq!(
            std::mem::offset_of!(ExtractResult, text_length),
            24,
            "text_length offset"
        );
        assert_eq!(
            std::mem::offset_of!(ExtractResult, error),
            32,
            "error offset"
        );
        assert_eq!(
            std::mem::size_of::<ExtractResult>(),
            40,
            "ExtractResult size"
        );
        assert_eq!(
            std::mem::align_of::<ExtractResult>(),
            8,
            "ExtractResult alignment"
        );
        assert_eq!(
            std::mem::offset_of!(FullExtractResult, title),
            0,
            "title offset"
        );
        assert_eq!(
            std::mem::offset_of!(FullExtractResult, text_content),
            8,
            "text_content offset"
        );
        assert_eq!(
            std::mem::offset_of!(FullExtractResult, text_length),
            16,
            "text_length offset"
        );
        assert_eq!(
            std::mem::offset_of!(FullExtractResult, method),
            20,
            "method offset"
        );
        assert_eq!(
            std::mem::offset_of!(FullExtractResult, error),
            24,
            "error offset"
        );
        assert_eq!(
            std::mem::size_of::<FullExtractResult>(),
            32,
            "FullExtractResult size"
        );
        assert_eq!(
            std::mem::align_of::<FullExtractResult>(),
            8,
            "FullExtractResult alignment"
        );
    }
}
