package pl.gov.nac.warc.config;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Global pipeline context providing shared state across modules.
 *
 * <p>
 * Exit codes:
 * <ul>
 * <li>0 - Success</li>
 * <li>1 - General error</li>
 * <li>10 - Pipeline negotiation failed (type mismatch)</li>
 * <li>11 - Required module not found</li>
 * <li>12 - Configuration error</li>
 * </ul>
 */
public final class PipelineContext {

  /** Exit code for successful execution */
  public static final int EXIT_SUCCESS = 0;
  /** Exit code for general errors */
  public static final int EXIT_ERROR = 1;
  /** Exit code for pipeline negotiation failures */
  public static final int EXIT_NEGOTIATION_FAILED = 10;
  /** Exit code for missing required module */
  public static final int EXIT_MODULE_NOT_FOUND = 11;
  /** Exit code for configuration errors */
  public static final int EXIT_CONFIG_ERROR = 12;

  private static final AtomicInteger exitCode = new AtomicInteger(EXIT_SUCCESS);

  private PipelineContext() {
    // Static utility class
  }

  /**
   * Sets the exit code, keeping the highest (most severe) code.
   * Thread-safe.
   *
   * @param code the exit code to set
   */
  public static void setExitCode(int code) {
    exitCode.updateAndGet(current -> Math.max(current, code));
  }

  /**
   * Gets the current exit code.
   *
   * @return the exit code
   */
  public static int getExitCode() {
    return exitCode.get();
  }

  /**
   * Resets the exit code to SUCCESS.
   * Should only be called at pipeline start.
   */
  public static void reset() {
    exitCode.set(EXIT_SUCCESS);
  }

  /**
   * Returns true if exit code indicates success.
   */
  public static boolean isSuccess() {
    return exitCode.get() == EXIT_SUCCESS;
  }
}
