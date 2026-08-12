package pl.gov.nac.warc.config;

/**
 * Progress output mode for periodic reporter.
 */
public enum ProgressMode {
  /** No progress output (silent mode) */
  NONE,
  /** Single-line progress (default for non-interactive) */
  DEFAULT,
  /** Full progress block with sparklines */
  VERBOSE
}
