package pl.gov.nac.warc.processors;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Flow;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.gov.nac.warc.reactive.Metrics;
import pl.gov.nac.warc.reactive.ReactiveInterfaces;
import pl.gov.nac.warc.records.warc.RecordWarcUniversal;

/**
 * Flexible WARC record filter processor.
 * Supports filtering by URI, MIME, Size, Type, Headers, Date, HTTP Status, and
 * more.
 * Uses --allow-* / --deny-* naming convention with ^ prefix for value negation.
 *
 * Accepts: RecordWarcUniversal
 */
public class WarcFilter
    implements ReactiveInterfaces.ReactiveProcessor<RecordWarcUniversal, RecordWarcUniversal> {

  private static final Logger log = LogManager.getLogger(WarcFilter.class);
  private static final String METRIC_KEY = "warc-filter";

  // OPT-P2-18: Pool Matcher objects per thread per Pattern to avoid allocation on every record
  private static final ThreadLocal<java.util.IdentityHashMap<Pattern, java.util.regex.Matcher>> MATCHER_POOL =
      ThreadLocal.withInitial(java.util.IdentityHashMap::new);

  private static java.util.regex.Matcher pooledMatcher(Pattern p, CharSequence input) {
    java.util.IdentityHashMap<Pattern, java.util.regex.Matcher> pool = MATCHER_POOL.get();
    java.util.regex.Matcher m = pool.get(p);
    if (m == null) {
      m = p.matcher(input);
      pool.put(p, m);
    } else {
      m.reset(input);
    }
    return m;
  }

  private Flow.Subscriber<? super RecordWarcUniversal> subscriber;

  // Processing mode
  private enum FilterMode {
    ALLOW_DENY_PASS, DENY_ALLOW_DROP, ALLOW_DENY_COUNT, DENY_ALLOW_COUNT
  }

  private FilterMode mode = FilterMode.ALLOW_DENY_PASS;

  // Control flags
  private boolean allowGroupsAnd = false;
  private boolean denyGroupsAnd = false;
  private boolean allowWarcinfo = false;
  private boolean denyWarcinfo = false;

  // Pagination
  private long rowStart = 0;
  private long rowLimit = Long.MAX_VALUE;
  private long currentRow = 0;
  private long emittedCount = 0;

  // WARC types
  private ValueMatcher allowWarcTypes;
  private ValueMatcher denyWarcTypes;

  // MIME types
  private ValueMatcher allowMimeTypes;
  private ValueMatcher denyMimeTypes;
  private List<Pattern> allowMimeRegexes;
  private List<Pattern> denyMimeRegexes;
  private List<String> allowMimeContains;
  private List<String> denyMimeContains;

  // HTTP codes (with range support)
  private IntRangeMatcher allowHttpCodes;
  private IntRangeMatcher denyHttpCodes;

  // Content-length filters
  private Long allowContentLengthLt;
  private Long allowContentLengthGt;
  private Long denyContentLengthLt;
  private Long denyContentLengthGt;

  // URL filters
  private List<Pattern> allowUrlRegexes;
  private List<Pattern> denyUrlRegexes;
  private List<String> allowUrlPrefixes;
  private List<String> denyUrlPrefixes;
  private List<String> allowUrlContains;
  private List<String> denyUrlContains;

  // Header filters
  private List<HeaderFilter> allowHeaders;
  private List<HeaderFilter> denyHeaders;

  // Date filters
  private Instant allowDateBefore;
  private Instant allowDateAfter;
  private Instant denyDateBefore;
  private Instant denyDateAfter;

  // Row filters
  private Long allowRowBefore;
  private Long allowRowAfter;
  private Long denyRowBefore;
  private Long denyRowAfter;

  // Filename filters
  private ValueMatcher allowFilenames;
  private ValueMatcher denyFilenames;

  @Override
  public void configure(Map<String, Object> cfg) {
    Metrics.setModuleHeader(METRIC_KEY, "WARC/WET Filter");
    log.debug("Configuring WarcFilter...");

    // Processing mode
    String modeStr = getString(cfg, "mode", "allow-deny-pass");
    this.mode = switch (modeStr) {
      case "deny-allow-drop" -> FilterMode.DENY_ALLOW_DROP;
      case "allow-deny-count" -> FilterMode.ALLOW_DENY_COUNT;
      case "deny-allow-count" -> FilterMode.DENY_ALLOW_COUNT;
      default -> FilterMode.ALLOW_DENY_PASS;
    };

    // Control flags
    this.allowGroupsAnd = getBoolean(cfg, "allow-groups-and", false);
    this.denyGroupsAnd = getBoolean(cfg, "deny-groups-and", false);
    this.allowWarcinfo = getBoolean(cfg, "allow-warcinfo", false);
    this.denyWarcinfo = getBoolean(cfg, "deny-warcinfo", false);

    // Pagination
    // Reset counters on every configure() so that reusing this instance across
    // sequential pipeline runs does not carry over counts from the previous run.
    this.currentRow = 0;
    this.emittedCount = 0;
    this.rowStart = getLong(cfg, "row-start", 0L);
    this.rowLimit = getLong(cfg, "row-limit", Long.MAX_VALUE);

    // WARC types
    this.allowWarcTypes = new ValueMatcher(parseSet(cfg, "allow-warc-types"));
    this.denyWarcTypes = new ValueMatcher(parseSet(cfg, "deny-warc-types"));

    // MIME types
    this.allowMimeTypes = new ValueMatcher(parseSet(cfg, "allow-mime-types"));
    this.denyMimeTypes = new ValueMatcher(parseSet(cfg, "deny-mime-types"));
    this.allowMimeRegexes = parseRegexList(cfg, "allow-mime-regexes");
    this.denyMimeRegexes = parseRegexList(cfg, "deny-mime-regexes");
    this.allowMimeContains = parseSet(cfg, "allow-mime-contains");
    this.denyMimeContains = parseSet(cfg, "deny-mime-contains");

    // HTTP codes
    this.allowHttpCodes = new IntRangeMatcher(parseSet(cfg, "allow-http-codes"));
    this.denyHttpCodes = new IntRangeMatcher(parseSet(cfg, "deny-http-codes"));

    // Content-length
    this.allowContentLengthLt = getLongOrNull(cfg, "allow-content-length-lt");
    this.allowContentLengthGt = getLongOrNull(cfg, "allow-content-length-gt");
    this.denyContentLengthLt = getLongOrNull(cfg, "deny-content-length-lt");
    this.denyContentLengthGt = getLongOrNull(cfg, "deny-content-length-gt");

    // URLs
    this.allowUrlRegexes = parseRegexList(cfg, "allow-url-regexes");
    this.denyUrlRegexes = parseRegexList(cfg, "deny-url-regexes");
    this.allowUrlPrefixes = parseSet(cfg, "allow-url-prefixes");
    this.denyUrlPrefixes = parseSet(cfg, "deny-url-prefixes");
    this.allowUrlContains = parseSet(cfg, "allow-url-contains");
    this.denyUrlContains = parseSet(cfg, "deny-url-contains");

    // Headers
    this.allowHeaders = parseHeaderFilters(cfg, "allow-headers");
    this.denyHeaders = parseHeaderFilters(cfg, "deny-headers");

    // Dates
    this.allowDateBefore = parseDate(cfg, "allow-date-before");
    this.allowDateAfter = parseDate(cfg, "allow-date-after");
    this.denyDateBefore = parseDate(cfg, "deny-date-before");
    this.denyDateAfter = parseDate(cfg, "deny-date-after");

    // Rows
    this.allowRowBefore = getLongOrNull(cfg, "allow-row-before");
    this.allowRowAfter = getLongOrNull(cfg, "allow-row-after");
    this.denyRowBefore = getLongOrNull(cfg, "deny-row-before");
    this.denyRowAfter = getLongOrNull(cfg, "deny-row-after");

    // Filenames
    this.allowFilenames = new ValueMatcher(parseSet(cfg, "allow-filenames"));
    this.denyFilenames = new ValueMatcher(parseSet(cfg, "deny-filenames"));

    log.debug(() -> String.format(
        "Configured: Mode=%s, AllowTypes=%d, DenyTypes=%d, AllowMime=%d, DenyMime=%d, " +
            "AllowHTTP=%d, DenyHTTP=%d, RowStart=%d, RowLimit=%d",
        mode, allowWarcTypes.size(), denyWarcTypes.size(),
        allowMimeTypes.size(), denyMimeTypes.size(),
        allowHttpCodes.size(), denyHttpCodes.size(),
        rowStart, rowLimit));
  }

  @Override
  public List<Class<? extends pl.gov.nac.warc.records.Record>> acceptedInputTypes() {
    return List.of(RecordWarcUniversal.class);
  }

  @Override
  public List<Class<? extends pl.gov.nac.warc.records.Record>> emittedOutputTypes() {
    return List.of(RecordWarcUniversal.class);
  }

  @Override
  public boolean doesChangeRecordClass() {
    return false;
  }

  @Override
  public boolean isEnabled(java.util.Map<String, Object> cfg) {
    Object v = cfg.get("enabled");
    if (v instanceof Boolean b)
      return b;
    if (v instanceof String s)
      return Boolean.parseBoolean(s);
    return true; // Default to enabled if not specified
  }

  // -----------------------------------------------------------------
  // CONFIG PARSING HELPERS
  // -----------------------------------------------------------------

  private String getString(Map<String, Object> cfg, String key, String def) {
    Object v = cfg.get(key);
    return v != null ? v.toString() : def;
  }

  private boolean getBoolean(Map<String, Object> cfg, String key, boolean def) {
    Object v = cfg.get(key);
    if (v instanceof Boolean b)
      return b;
    if (v instanceof String s)
      return Boolean.parseBoolean(s);
    return def;
  }

  private long getLong(Map<String, Object> cfg, String key, long def) {
    Object v = cfg.get(key);
    if (v instanceof Number n)
      return n.longValue();
    if (v instanceof String s) {
      try {
        return Long.parseLong(s);
      } catch (Exception _) {
        return def;
      }
    }
    return def;
  }

  private Long getLongOrNull(Map<String, Object> cfg, String key) {
    Object v = cfg.get(key);
    if (v instanceof Number n)
      return n.longValue();
    if (v instanceof String s && !s.isBlank()) {
      try {
        return Long.parseLong(s);
      } catch (Exception _) {
        return null;
      }
    }
    return null;
  }

  private List<String> parseSet(Map<String, Object> cfg, String key) {
    Object val = cfg.get(key);
    if (val == null)
      return new ArrayList<>();
    if (val instanceof List<?> l) {
      return l.stream().map(Object::toString).toList();
    }
    if (val instanceof String s && !s.isBlank()) {
      // Strip brackets if present (CLI format: [value1,value2])
      s = s.trim();
      if (s.startsWith("[") && s.endsWith("]")) {
        s = s.substring(1, s.length() - 1);
      }
      if (s.isEmpty())
        return new ArrayList<>();
      return new ArrayList<>(Arrays.asList(s.split(",")));
    }
    return new ArrayList<>();
  }

  private List<Pattern> parseRegexList(Map<String, Object> cfg, String key) {
    List<String> regexStrings = parseSet(cfg, key);
    if (regexStrings.isEmpty())
      return List.of();

    // OPTIMIZATION: Combine multiple .* patterns into single alternation
    boolean allDotStar = regexStrings.stream()
        .allMatch(r -> r.startsWith(".*") && r.endsWith(".*"));

    if (allDotStar && regexStrings.size() > 1) {
      // Strip .* and combine with |
      String combined = regexStrings.stream()
          .map(r -> r.substring(2, r.length() - 2))
          .collect(Collectors.joining("|"));
      log.info("Optimized {} regex patterns into single pattern: {}",
          regexStrings.size(), combined);
      return List.of(Pattern.compile(combined));
    }

    // Otherwise, compile individually
    List<Pattern> out = new ArrayList<>();
    for (String r : regexStrings) {
      try {
        out.add(Pattern.compile(r));
      } catch (Exception _) {
        log.warn("Invalid regex: {}", r);
      }
    }
    return out;
  }

  private List<HeaderFilter> parseHeaderFilters(Map<String, Object> cfg, String key) {
    List<String> raw = parseSet(cfg, key);
    List<HeaderFilter> out = new ArrayList<>();
    for (String s : raw) {
      int eq = s.indexOf('=');
      if (eq > 0) {
        String name = s.substring(0, eq).trim();
        String p = s.substring(eq + 1);
        try {
          out.add(new HeaderFilter(name, Pattern.compile(p), false));
        } catch (Exception _) {
          log.warn("Invalid header regex: {}", s);
        }
      } else {
        out.add(new HeaderFilter(s.trim(), null, false));
      }
    }
    return out;
  }

  private Instant parseDate(Map<String, Object> cfg, String key) {
    Object val = cfg.get(key);
    if (val instanceof String s && !s.isBlank()) {
      try {
        return Instant.parse(s);
      } catch (DateTimeParseException e) {
        log.warn("Invalid {}: {}", key, s);
      }
    }
    return null;
  }

  // -----------------------------------------------------------------
  // REACTIVE INTERFACE
  // -----------------------------------------------------------------

  @Override
  public void subscribe(Flow.Subscriber<? super RecordWarcUniversal> subscriber) {
    this.subscriber = subscriber;
  }

  @Override
  public void onSubscribe(Flow.Subscription subscription) {
    subscriber.onSubscribe(subscription);
  }

  @Override
  public void onNext(RecordWarcUniversal item) {
    try {
      Metrics.inc(METRIC_KEY, "recordsIn");
      currentRow++;

      // Pagination: skip rows before start (rowStart=0 means no skip, rowStart>0
      // skips first N rows)
      if (rowStart > 0 && currentRow <= rowStart) {
        Metrics.inc(METRIC_KEY, "recordsSkipped");
        return;
      }

      // Pagination: stop after limit (disabled if rowLimit <= 0)
      if (rowLimit > 0 && emittedCount >= rowLimit) {
        Metrics.inc(METRIC_KEY, "recordsLimitReached");
        return;
      }

      RecordMeta meta = extractMetadata(item);
      boolean shouldEmit = evaluateFilters(meta);

      if (shouldEmit) {
        emittedCount++;
        Metrics.inc(METRIC_KEY, "recordsMatched");
        subscriber.onNext(item);
      } else {
        Metrics.inc(METRIC_KEY, "recordsDropped");
      }
    } catch (Exception e) {
      Metrics.inc(METRIC_KEY, "errors");
      log.error("Error processing record: {}", e.getMessage(), e);
    }
  }

  private boolean evaluateFilters(RecordMeta meta) {
    // Warcinfo special handling
    if ("warcinfo".equals(meta.type)) {
      if (allowWarcinfo)
        return true;
      if (denyWarcinfo)
        return false;
    }

    // Short-circuiting evaluation
    if (mode == FilterMode.DENY_ALLOW_DROP) {
      // First check deny - ANY match results in false
      if (checkAnyDenyMatch(meta)) {
        return false;
      }
      // Then check allow - MUST match at least one (if list not empty)
      if (isAllowConfigured()) {
        return checkAllAllowMatch(meta);
      }
      return false; // No allow configured = drop everything in this mode
    } else {
      // Default: ALLOW_DENY_PASS
      // First check allow
      if (isAllowConfigured()) {
        if (!checkAllAllowMatch(meta)) {
          return false;
        }
      }
      // Then check deny
      return !checkAnyDenyMatch(meta);
    }
  }

  private boolean isAllowConfigured() {
    return !allowWarcTypes.isEmpty() || !allowMimeTypes.isEmpty() || !allowMimeRegexes.isEmpty()
        || !allowMimeContains.isEmpty()
        || !allowHttpCodes.isEmpty() || allowContentLengthLt != null || allowContentLengthGt != null
        || !allowUrlRegexes.isEmpty() || !allowUrlPrefixes.isEmpty() || !allowUrlContains.isEmpty()
        || !allowHeaders.isEmpty() || allowDateBefore != null || allowDateAfter != null
        || allowRowBefore != null || allowRowAfter != null || !allowFilenames.isEmpty();
  }

  private boolean checkAllAllowMatch(RecordMeta meta) {
    List<Supplier<Boolean>> allowChecks = new ArrayList<>();
    if (!allowWarcTypes.isEmpty())
      allowChecks.add(() -> checkWarcTypes(meta, allowWarcTypes, true));
    if (!allowMimeTypes.isEmpty() || !allowMimeRegexes.isEmpty() || !allowMimeContains.isEmpty())
      allowChecks.add(() -> checkMimeTypes(meta, allowMimeTypes, allowMimeRegexes, allowMimeContains, true));
    if (!allowHttpCodes.isEmpty())
      allowChecks.add(() -> checkHttpCodes(meta, allowHttpCodes, true));
    if (allowContentLengthLt != null || allowContentLengthGt != null)
      allowChecks.add(() -> checkContentLength(meta, allowContentLengthLt, allowContentLengthGt, true));
    if (!allowUrlRegexes.isEmpty() || !allowUrlPrefixes.isEmpty() || !allowUrlContains.isEmpty())
      allowChecks.add(() -> checkUrls(meta, allowUrlRegexes, allowUrlPrefixes, allowUrlContains, true));
    if (!allowHeaders.isEmpty())
      allowChecks.add(() -> checkHeaders(meta, allowHeaders, true));
    if (allowDateBefore != null || allowDateAfter != null)
      allowChecks.add(() -> checkDates(meta, allowDateBefore, allowDateAfter, true));
    if (allowRowBefore != null || allowRowAfter != null)
      allowChecks.add(() -> checkRows(allowRowBefore, allowRowAfter, true));
    if (!allowFilenames.isEmpty())
      allowChecks.add(() -> checkFilenames(meta, allowFilenames, true));

    if (allowChecks.isEmpty())
      return true;

    if (allowGroupsAnd) {
      return allowChecks.stream().allMatch(Supplier::get);
    } else {
      return allowChecks.stream().anyMatch(Supplier::get);
    }
  }

  private boolean checkAnyDenyMatch(RecordMeta meta) {
    List<Supplier<Boolean>> denyChecks = new ArrayList<>();
    if (!denyWarcTypes.isEmpty())
      denyChecks.add(() -> checkWarcTypes(meta, denyWarcTypes, false));
    if (!denyMimeTypes.isEmpty() || !denyMimeRegexes.isEmpty() || !denyMimeContains.isEmpty())
      denyChecks.add(() -> checkMimeTypes(meta, denyMimeTypes, denyMimeRegexes, denyMimeContains, false));
    if (!denyHttpCodes.isEmpty())
      denyChecks.add(() -> checkHttpCodes(meta, denyHttpCodes, false));
    if (denyContentLengthLt != null || denyContentLengthGt != null)
      denyChecks.add(() -> checkContentLength(meta, denyContentLengthLt, denyContentLengthGt, false));
    if (!denyUrlRegexes.isEmpty() || !denyUrlPrefixes.isEmpty() || !denyUrlContains.isEmpty())
      denyChecks.add(() -> checkUrls(meta, denyUrlRegexes, denyUrlPrefixes, denyUrlContains, false));
    if (!denyHeaders.isEmpty())
      denyChecks.add(() -> checkHeaders(meta, denyHeaders, false));
    if (denyDateBefore != null || denyDateAfter != null)
      denyChecks.add(() -> checkDates(meta, denyDateBefore, denyDateAfter, false));
    if (denyRowBefore != null || denyRowAfter != null)
      denyChecks.add(() -> checkRows(denyRowBefore, denyRowAfter, false));
    if (!denyFilenames.isEmpty())
      denyChecks.add(() -> checkFilenames(meta, denyFilenames, false));

    if (denyChecks.isEmpty())
      return false;

    if (denyGroupsAnd) {
      return denyChecks.stream().allMatch(Supplier::get);
    } else {
      return denyChecks.stream().anyMatch(Supplier::get);
    }
  }

  // -----------------------------------------------------------------
  // INDIVIDUAL CHECKERS
  // -----------------------------------------------------------------

  private Boolean checkWarcTypes(RecordMeta meta, ValueMatcher matcher, boolean isAllow) {
    if (matcher.isEmpty())
      return null;
    return matcher.matches(meta.type);
  }

  private Boolean checkMimeTypes(RecordMeta meta, ValueMatcher matcher, List<Pattern> regexes, List<String> contains,
      boolean isAllow) {
    if (matcher.isEmpty() && regexes.isEmpty() && contains.isEmpty())
      return null;
    if (meta.mime == null || meta.mime.isEmpty())
      return !isAllow;

    if (!matcher.isEmpty() && matcher.matchesPrefix(meta.mime))
      return true;

    // Fast substring checks (10-20x faster than regex)
    for (String s : contains) {
      if (meta.mime.contains(s))
        return true;
    }

    for (Pattern p : regexes) {
      if (pooledMatcher(p, meta.mime).find())
        return true;
    }
    return false;
  }

  private Boolean checkHttpCodes(RecordMeta meta, IntRangeMatcher matcher, boolean isAllow) {
    if (matcher.isEmpty())
      return null;
    if (meta.httpStatus == 0)
      return !isAllow;
    return matcher.matches(meta.httpStatus);
  }

  private Boolean checkContentLength(RecordMeta meta, Long lt, Long gt, boolean isAllow) {
    if (lt == null && gt == null)
      return null;
    long len = meta.contentLength;
    if (lt != null && len >= lt)
      return false;
    if (gt != null && len <= gt)
      return false;
    return true;
  }

  private Boolean checkUrls(RecordMeta meta, List<Pattern> regexes, List<String> prefixes, List<String> contains,
      boolean isAllow) {
    if (regexes.isEmpty() && prefixes.isEmpty() && contains.isEmpty())
      return null;
    String url = meta.url != null ? meta.url : "";

    for (Pattern p : regexes) {
      if (pooledMatcher(p, url).find())
        return true;
    }
    for (String p : prefixes) {
      if (url.startsWith(p))
        return true;
    }
    for (String s : contains) {
      if (url.contains(s))
        return true;
    }
    return false;
  }

  private Boolean checkHeaders(RecordMeta meta, List<HeaderFilter> filters, boolean isAllow) {
    if (filters.isEmpty())
      return null;
    Map<String, String> headers = meta.originalRecord.headers();
    for (HeaderFilter f : filters) {
      String val = headers.get(f.name);
      if (val != null) {
        if (f.pattern == null)
          return true;
        if (pooledMatcher(f.pattern, val).find())
          return true;
      }
    }
    return false;
  }

  private Boolean checkDates(RecordMeta meta, Instant before, Instant after, boolean isAllow) {
    if (before == null && after == null)
      return null;
    if (meta.date == null)
      return !isAllow;
    if (after != null && !meta.date.isAfter(after))
      return false;
    if (before != null && !meta.date.isBefore(before))
      return false;
    return true;
  }

  private Boolean checkRows(Long before, Long after, boolean isAllow) {
    if (before == null && after == null)
      return null;
    if (before != null && currentRow >= before)
      return false;
    if (after != null && currentRow <= after)
      return false;
    return true;
  }

  private Boolean checkFilenames(RecordMeta meta, ValueMatcher matcher, boolean isAllow) {
    if (matcher.isEmpty())
      return null;
    if (meta.sourceWarc == null)
      return !isAllow;
    return matcher.matches(meta.sourceWarc);
  }

  // -----------------------------------------------------------------
  // METADATA EXTRACTION
  // -----------------------------------------------------------------

  private record RecordMeta(String type, String url, String mime, Instant date,
      String sourceWarc, int httpStatus, long contentLength, RecordWarcUniversal originalRecord) {
  }

  private RecordMeta extractMetadata(RecordWarcUniversal item) {
    String type = item.warcType();
    String url = item.targetUri();
    String dateStr = item.warcDate();
    Instant date = null;
    if (dateStr != null) {
      try {
        date = Instant.parse(dateStr);
      } catch (Exception _) {
      }
    }

    Map<String, String> h = item.headers();
    String mime = h.getOrDefault("WARC-Identified-Payload-Type",
        h.getOrDefault("Content-Type", ""));
    String sourceFile = h.get("X-Source-Warc");

    long contentLength = 0;
    try {
      String cl = h.get("Content-Length");
      if (cl != null)
        contentLength = Long.parseLong(cl);
    } catch (Exception _) {
    }

    int httpStatus = 0;
    // Skip expensive MIME parsing if no MIME filters configured
    boolean needMimeParsing = !allowMimeTypes.isEmpty() || !denyMimeTypes.isEmpty()
        || !allowMimeRegexes.isEmpty() || !denyMimeRegexes.isEmpty();
    if ("response".equals(type) && item.rawBytes() != null) {
      httpStatus = parseHttpStatus(item.rawBytes());
      if (needMimeParsing && mime.contains("application/http")) {
        String innerMime = parseInnerMime(item.rawBytes());
        if (innerMime != null)
          mime = innerMime;
      }
    }

    return new RecordMeta(type, url, mime, date, sourceFile, httpStatus, contentLength, item);
  }

  private String parseInnerMime(byte[] raw) {
    int i = findPayloadStart(raw);
    if (i < 0 || i >= raw.length)
      return null;

    // The payload of a response record starts with an HTTP response (status line +
    // headers)
    // We need to find the Content-Type header WITHIN the HTTP block
    int headerEnd = findHeaderEndInBlock(raw, i);
    if (headerEnd < 0)
      return null;

    String block = new String(raw, i, headerEnd - i, java.nio.charset.StandardCharsets.ISO_8859_1);
    String[] lines = block.split("\r?\n");
    // Start from line 1 (skip status line)
    for (int j = 1; j < lines.length; j++) {
      String line = lines[j].trim();
      if (line.toLowerCase().startsWith("content-type:")) {
        String val = line.substring("content-type:".length()).trim();
        // Strip parameters like ; charset=...
        int semi = val.indexOf(';');
        if (semi > 0)
          val = val.substring(0, semi).trim();
        return val;
      }
    }
    return null;
  }

  private int findPayloadStart(byte[] raw) {
    // Find CRLF CRLF in WARC record to find start of HTTP block
    for (int i = 0; i < raw.length - 3; i++) {
      if (raw[i] == '\r' && raw[i + 1] == '\n' && raw[i + 2] == '\r' && raw[i + 3] == '\n') {
        return i + 4;
      }
    }
    return -1;
  }

  private int findHeaderEndInBlock(byte[] raw, int start) {
    for (int i = start; i < raw.length - 3; i++) {
      if (raw[i] == '\r' && raw[i + 1] == '\n' && raw[i + 2] == '\r' && raw[i + 3] == '\n') {
        return i;
      }
    }
    return -1;
  }

  private int parseHttpStatus(byte[] raw) {
    int i = 0;
    boolean found = false;
    while (i < raw.length - 3) {
      if (raw[i] == '\r' && raw[i + 1] == '\n' && raw[i + 2] == '\r' && raw[i + 3] == '\n') {
        i += 4;
        found = true;
        break;
      }
      i++;
    }
    if (!found) {
      i = 0;
      while (i < raw.length - 1) {
        if (raw[i] == '\n' && raw[i + 1] == '\n') {
          i += 2;
          found = true;
          break;
        }
        i++;
      }
    }
    if (!found || i >= raw.length - 12)
      return 0;
    while (i < raw.length && raw[i] != ' ')
      i++;
    i++;
    if (i + 3 > raw.length)
      return 0;
    try {
      return Integer.parseInt(new String(raw, i, 3, java.nio.charset.StandardCharsets.US_ASCII));
    } catch (Exception _) {
      return 0;
    }
  }

  @Override
  public void onError(Throwable throwable) {
    subscriber.onError(throwable);
  }

  @Override
  public void onComplete() {
    subscriber.onComplete();
  }

  // -----------------------------------------------------------------
  // INNER CLASSES
  // -----------------------------------------------------------------

  private record HeaderFilter(String name, Pattern pattern, boolean negate) {
  }

  /** Matches string values with ^ prefix support for negation */
  private static class ValueMatcher {
    private final Set<String> includeExact = new HashSet<>();
    private final List<String> includePrefix = new ArrayList<>();
    private final Set<String> excludeExact = new HashSet<>();
    private final List<String> excludePrefix = new ArrayList<>();

    ValueMatcher(List<String> values) {
      for (String v : values) {
        if (v.startsWith("^")) {
          String s = v.substring(1);
          if (s.endsWith("*")) {
            excludePrefix.add(s.replace("*", ""));
          } else {
            excludeExact.add(s);
          }
        } else {
          if (v.endsWith("*")) {
            includePrefix.add(v.replace("*", ""));
          } else {
            includeExact.add(v);
          }
        }
      }
    }

    boolean isEmpty() {
      return includeExact.isEmpty() && includePrefix.isEmpty() && excludeExact.isEmpty() && excludePrefix.isEmpty();
    }

    int size() {
      return includeExact.size() + includePrefix.size() + excludeExact.size() + excludePrefix.size();
    }

    boolean matches(String value) {
      if (value == null)
        return false;

      // Check exact excludes first (O(1))
      if (excludeExact.contains(value))
        return false;

      // Check prefix excludes
      for (String ex : excludePrefix) {
        if (value.startsWith(ex))
          return false;
      }

      // Check exact includes (O(1))
      if (includeExact.contains(value))
        return true;

      // Check prefix includes
      for (String in : includePrefix) {
        if (value.startsWith(in))
          return true;
      }

      // Special case: if there are only excludes, and we didn't match any, then we
      // match.
      return (includeExact.isEmpty() && includePrefix.isEmpty())
          && (!excludeExact.isEmpty() || !excludePrefix.isEmpty());
    }

    // Keep matchesPrefix for backward compatibility if used, but optimize it
    boolean matchesPrefix(String value) {
      if (value == null)
        return false;

      // Check exact matches (which are also prefixes)
      if (excludeExact.contains(value))
        return false;
      if (includeExact.contains(value))
        return true;

      for (String ex : excludePrefix) {
        if (value.startsWith(ex))
          return false;
      }
      for (String in : includePrefix) {
        if (value.startsWith(in))
          return true;
      }
      return (includeExact.isEmpty() && includePrefix.isEmpty())
          && (!excludeExact.isEmpty() || !excludePrefix.isEmpty());
    }
  }

  /** Matches integer values with range (N-M) and ^ negation support */
  private static class IntRangeMatcher {
    private final List<int[]> includeRanges = new ArrayList<>();
    private final List<int[]> excludeRanges = new ArrayList<>();

    IntRangeMatcher(List<String> values) {
      for (String v : values) {
        boolean negate = v.startsWith("^");
        String s = negate ? v.substring(1) : v;
        int[] range = parseRange(s);
        if (range != null) {
          (negate ? excludeRanges : includeRanges).add(range);
        }
      }
    }

    private int[] parseRange(String s) {
      try {
        if (s.contains("-")) {
          String[] parts = s.split("-", 2);
          return new int[] { Integer.parseInt(parts[0]), Integer.parseInt(parts[1]) };
        } else {
          int val = Integer.parseInt(s);
          return new int[] { val, val };
        }
      } catch (Exception _) {
        return null;
      }
    }

    boolean isEmpty() {
      return includeRanges.isEmpty() && excludeRanges.isEmpty();
    }

    int size() {
      return includeRanges.size() + excludeRanges.size();
    }

    boolean matches(int value) {
      for (int[] r : excludeRanges) {
        if (value >= r[0] && value <= r[1])
          return false;
      }
      if (includeRanges.isEmpty())
        return true;
      for (int[] r : includeRanges) {
        if (value >= r[0] && value <= r[1])
          return true;
      }
      return false;
    }
  }
}
