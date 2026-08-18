package pl.gov.nac.warc.config;

/**
 * Final report output mode.
 */
public enum FinalReportMode {
  /** No final report (silent mode) */
  NONE,
  /** Single-line summary with key metrics */
  SUMMARY,
  /** Full report with all counters */
  FULL
}
