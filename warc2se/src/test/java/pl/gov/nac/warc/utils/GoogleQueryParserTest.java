package pl.gov.nac.warc.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class GoogleQueryParserTest {

  @Test
  void blankInputUsesMatchAll() {
    assertEquals("{\"match_all\":{}}", GoogleQueryParser.toElasticsearchQuery(null));
    assertEquals("{\"match_all\":{}}", GoogleQueryParser.toElasticsearchQuery(" \t "));
  }

  @Test
  void emitsPhrasesFiltersExclusionsAndTermsInParserOrder() {
    assertEquals(
        "{\"bool\":{\"must\":["
            + "{\"match_phrase\":{\"content\":\"exact phrase\"}},"
            + "{\"wildcard\":{\"warc-uri\":{\"value\":\"*example.com*\"}}},"
            + "{\"term\":{\"wet-lang\":\"pl\"}},"
            + "{\"match\":{\"content\":\"alpha\"}}"
            + "],\"must_not\":[{\"match\":{\"content\":\"noise\"}}]}}",
        GoogleQueryParser.toElasticsearchQuery(
            "\"exact phrase\" site:example.com lang:pl -noise alpha"));
  }

  @Test
  void topLevelOrUsesShouldClausesAndMinimumOneMatch() {
    assertEquals(
        "{\"bool\":{\"should\":["
            + "{\"match\":{\"content\":\"alpha\"}},"
            + "{\"match\":{\"content\":\"beta\"}}"
            + "],\"minimum_should_match\":1}}",
        GoogleQueryParser.toElasticsearchQuery("alpha OR beta"));
  }

  @Test
  void dateRangesAndFileTypesUseTheirCurrentFields() {
    assertEquals(
        "{\"bool\":{\"must\":["
            + "{\"range\":{\"warc-date\":{\"gte\":\"2024-01\",\"lte\":\"2024-06\"}}},"
            + "{\"wildcard\":{\"warc-uri\":{\"value\":\"*.pdf\"}}}"
            + "]}}",
        GoogleQueryParser.toElasticsearchQuery("date:2024-01..2024-06 filetype:PDF"));
  }

  @Test
  void highlightsDefaultToContentAndWetContent() {
    assertEquals(
        "{\"bool\":{\"must\":[{\"match\":{\"content\":\"alpha\"}}]},"
            + "\"highlight\":{\"fields\":{\"content\":{},\"wet-content\":{}}}}",
        GoogleQueryParser.toElasticsearchQueryWithHighlights("alpha"));
    assertEquals(
        "{\"bool\":{\"must\":[{\"match\":{\"content\":\"alpha\"}}]},"
            + "\"highlight\":{\"fields\":{\"title\":{}}}}",
        GoogleQueryParser.toElasticsearchQueryWithHighlights("alpha", "title"));
  }
}
