package pl.gov.nac.warc.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses Google-like search queries into Elasticsearch Query DSL JSON.
 *
 * Supports:
 * - Simple terms: hello world
 * - Exact phrases: "hello world"
 * - OR operator: foo OR bar
 * - Exclusion: -unwanted
 * - Field filters: site:example.com, lang:pl, date:2024, uri:path, digest:sha1
 * - Filetype filter: filetype:pdf, filetype:html
 * - Date ranges: date:2024-01..2024-06
 * - Grouping: (foo bar) OR (baz qux)
 */
public final class GoogleQueryParser {

  private static final Pattern PHRASE_PATTERN = Pattern.compile("\"([^\"]+)\"");
  private static final Pattern FIELD_PATTERN = Pattern.compile("(site|lang|date|uri|digest|type|filetype):([^\\s\"]+)");
  private static final Pattern EXCLUDE_PATTERN = Pattern.compile("(?:^|\\s)-(\\S+)");
  private static final Pattern OR_PATTERN = Pattern.compile("\\s+OR\\s+", Pattern.CASE_INSENSITIVE);
  private static final Pattern DATE_RANGE_PATTERN = Pattern
      .compile("(\\d{4}(?:-\\d{2})?(?:-\\d{2})?)\\.\\.(\\d{4}(?:-\\d{2})?(?:-\\d{2})?)");

  private GoogleQueryParser() {
  }

  /**
   * Convert Google-like query string to Elasticsearch query JSON.
   *
   * @param query Google-like search query
   * @return Elasticsearch Query DSL JSON string
   */
  public static String toElasticsearchQuery(String query) {
    if (query == null || query.isBlank()) {
      return "{\"match_all\":{}}";
    }

    QueryBuilder builder = new QueryBuilder();
    parseQuery(query.trim(), builder);
    return builder.toJson();
  }

  /**
   * Convert to Elasticsearch query with highlights enabled.
   */
  public static String toElasticsearchQueryWithHighlights(String query, String... highlightFields) {
    String baseQuery = toElasticsearchQuery(query);

    if (highlightFields.length == 0) {
      highlightFields = new String[] { "content", "wet-content" };
    }

    StringBuilder highlight = new StringBuilder();
    highlight.append(",\"highlight\":{\"fields\":{");
    for (int i = 0; i < highlightFields.length; i++) {
      if (i > 0)
        highlight.append(",");
      highlight.append("\"").append(highlightFields[i]).append("\":{}");
    }
    highlight.append("}}");

    // Insert highlight before closing brace
    if (baseQuery.endsWith("}")) {
      return baseQuery.substring(0, baseQuery.length() - 1) + highlight + "}";
    }
    return baseQuery;
  }

  private static void parseQuery(String query, QueryBuilder builder) {
    // Check for OR groups at top level
    String[] orParts = OR_PATTERN.split(query);
    if (orParts.length > 1) {
      builder.startOr();
      for (String part : orParts) {
        parseClause(part.trim(), builder);
      }
      builder.endOr();
      return;
    }

    // Parse single clause
    parseClause(query, builder);
  }

  private static void parseClause(String clause, QueryBuilder builder) {
    String remaining = clause;

    // 1. Extract phrases
    Matcher phraseMatcher = PHRASE_PATTERN.matcher(remaining);
    while (phraseMatcher.find()) {
      builder.addPhrase(phraseMatcher.group(1));
    }
    remaining = PHRASE_PATTERN.matcher(remaining).replaceAll("");

    // 2. Extract field filters
    Matcher fieldMatcher = FIELD_PATTERN.matcher(remaining);
    while (fieldMatcher.find()) {
      builder.addFieldFilter(fieldMatcher.group(1), fieldMatcher.group(2));
    }
    remaining = FIELD_PATTERN.matcher(remaining).replaceAll("");

    // 3. Extract exclusions
    Matcher excludeMatcher = EXCLUDE_PATTERN.matcher(remaining);
    while (excludeMatcher.find()) {
      builder.addExclusion(excludeMatcher.group(1));
    }
    remaining = EXCLUDE_PATTERN.matcher(remaining).replaceAll(" ");

    // 4. Remaining are simple terms
    for (String term : remaining.trim().split("\\s+")) {
      if (!term.isBlank()) {
        builder.addTerm(term);
      }
    }
  }

  /**
   * Internal query builder that constructs ES bool queries.
   */
  private static class QueryBuilder {
    private final List<String> must = new ArrayList<>();
    private final List<String> mustNot = new ArrayList<>();
    private final List<String> should = new ArrayList<>();
    private boolean inOr = false;
    private QueryBuilder currentOrClause = null;

    void startOr() {
      inOr = true;
    }

    void endOr() {
      // Convert accumulated should clauses to proper structure
      inOr = false;
    }

    void addTerm(String term) {
      String clause = String.format("{\"match\":{\"content\":\"%s\"}}", escapeJson(term));
      addClause(clause);
    }

    void addPhrase(String phrase) {
      String clause = String.format("{\"match_phrase\":{\"content\":\"%s\"}}", escapeJson(phrase));
      addClause(clause);
    }

    void addExclusion(String term) {
      mustNot.add(String.format("{\"match\":{\"content\":\"%s\"}}", escapeJson(term)));
    }

    void addFieldFilter(String field, String value) {
      String clause = buildFieldFilter(field, value);
      if (clause != null) {
        addClause(clause);
      }
    }

    private String buildFieldFilter(String field, String value) {
      return switch (field.toLowerCase()) {
        case "site" -> String.format("{\"wildcard\":{\"warc-uri\":{\"value\":\"*%s*\"}}}", escapeJson(value));
        case "uri" -> String.format("{\"wildcard\":{\"warc-uri\":{\"value\":\"*%s*\"}}}", escapeJson(value));
        case "lang" -> String.format("{\"term\":{\"wet-lang\":\"%s\"}}", escapeJson(value));
        case "type" -> String.format("{\"term\":{\"warc-type\":\"%s\"}}", escapeJson(value));
        case "digest" -> String.format("{\"term\":{\"warc-digest\":\"%s\"}}", escapeJson(value));
        case "date" -> buildDateFilter(value);
        case "filetype" ->
          String.format("{\"wildcard\":{\"warc-uri\":{\"value\":\"*.%s\"}}}", escapeJson(value.toLowerCase()));
        default -> null;
      };
    }

    private String buildDateFilter(String value) {
      Matcher rangeMatcher = DATE_RANGE_PATTERN.matcher(value);
      if (rangeMatcher.matches()) {
        String from = rangeMatcher.group(1);
        String to = rangeMatcher.group(2);
        return String.format("{\"range\":{\"warc-date\":{\"gte\":\"%s\",\"lte\":\"%s\"}}}", from, to);
      }
      // Single date - prefix match
      return String.format("{\"prefix\":{\"warc-date\":\"%s\"}}", escapeJson(value));
    }

    private void addClause(String clause) {
      if (inOr) {
        should.add(clause);
      } else {
        must.add(clause);
      }
    }

    String toJson() {
      if (must.isEmpty() && should.isEmpty() && mustNot.isEmpty()) {
        return "{\"match_all\":{}}";
      }

      StringBuilder sb = new StringBuilder();
      sb.append("{\"bool\":{");

      boolean needsComma = false;

      if (!must.isEmpty()) {
        sb.append("\"must\":[");
        sb.append(String.join(",", must));
        sb.append("]");
        needsComma = true;
      }

      if (!should.isEmpty()) {
        if (needsComma)
          sb.append(",");
        sb.append("\"should\":[");
        sb.append(String.join(",", should));
        sb.append("],\"minimum_should_match\":1");
        needsComma = true;
      }

      if (!mustNot.isEmpty()) {
        if (needsComma)
          sb.append(",");
        sb.append("\"must_not\":[");
        sb.append(String.join(",", mustNot));
        sb.append("]");
      }

      sb.append("}}");
      return sb.toString();
    }

    private static String escapeJson(String s) {
      if (s == null)
        return "";
      return s.replace("\\", "\\\\")
          .replace("\"", "\\\"")
          .replace("\n", "\\n")
          .replace("\r", "\\r")
          .replace("\t", "\\t");
    }
  }
}
