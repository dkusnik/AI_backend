package pl.gov.nac.warc.reactive;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

/**
 * Manages reactive pipeline verbosity modes and their mapping to Log4j2 levels
 * and reporting behavior.
 */
public final class VerbosityManager {

  private static final Logger log = LogManager.getLogger(VerbosityManager.class);

  /**
   * Verbosity modes supported by the system.
   */
  public enum VerbosityMode {
    SILENT, // No console output, only errors in file
    BRIEF, // Single line summary
    BENCHMARK, // Stats + Timing, standard logging
    CLI, // Standard progress updates, INFO logging
    VERBOSE, // Detailed progress, INFO logging
    DEVELOPMENT, // DEBUG logging, detailed progress for development
    DEBUG // TRACE logging, exhaustive context
  }

  private VerbosityManager() {
  }

  /**
   * Applies the selected verbosity mode by configuring Log4j2 and internal
   * reporting flags.
   *
   * @param mode The selected verbosity mode.
   */
  public static void apply(VerbosityMode mode) {
    if (mode == null) {
      mode = VerbosityMode.CLI;
    }

    log.info("Applying verbosity mode: {}", mode);

    switch (mode) {
      case SILENT -> configureSilent();
      case BRIEF -> configureBrief();
      case BENCHMARK -> configureBenchmark();
      case CLI -> configureCli();
      case VERBOSE -> configureVerbose();
      case DEVELOPMENT -> configureDevelopment();
      case DEBUG -> configureDebug();
    }

    // Set system property that log4j2.xml can reference if needed
    System.setProperty("warc.verbosity", mode.name().toLowerCase());

    // Bridge java.util.logging (JUL) to match the console level
    java.util.logging.Level julLevel = switch (mode) {
      case SILENT -> java.util.logging.Level.OFF;
      case BRIEF, BENCHMARK -> java.util.logging.Level.WARNING;
      case CLI, VERBOSE -> java.util.logging.Level.INFO;
      case DEVELOPMENT -> java.util.logging.Level.FINE;
      case DEBUG -> java.util.logging.Level.ALL;
    };
    java.util.logging.Logger julRoot = java.util.logging.Logger.getLogger("");
    julRoot.setLevel(julLevel);
    for (var h : julRoot.getHandlers()) {
      h.setLevel(julLevel);
    }
  }

  private static void configureSilent() {
    System.setProperty("warc.logging.level", "ERROR");
    System.setProperty("warc.logging.console.level", "OFF");
    Configurator.reconfigure();
  }

  private static void configureBrief() {
    System.setProperty("warc.logging.level", "INFO");
    System.setProperty("warc.logging.console.level", "WARN");
    System.setProperty("warc.logging.pattern", "[%level] %msg%n");
    Configurator.reconfigure();
  }

  private static void configureBenchmark() {
    System.setProperty("warc.logging.level", "INFO");
    System.setProperty("warc.logging.console.level", "INFO");
    System.setProperty("warc.logging.pattern", "[%level] %msg%n");
    Configurator.reconfigure();
  }

  private static void configureCli() {
    System.setProperty("warc.logging.level", "INFO");
    System.setProperty("warc.logging.console.level", "INFO");
    System.setProperty("warc.logging.pattern", "[%d{HH:mm:ss}] [%level] [%logger{1}] %msg%n");
    Configurator.reconfigure();
  }

  private static void configureVerbose() {
    System.setProperty("warc.logging.level", "INFO");
    System.setProperty("warc.logging.console.level", "INFO");
    // More detailed pattern for verbose mode
    System.setProperty("warc.logging.pattern", "[%d{HH:mm:ss.SSS}] [%level] [%logger{1}] (%t) %msg%n");
    Configurator.reconfigure();
  }

  private static void configureDevelopment() {
    System.setProperty("warc.logging.level", "DEBUG");
    System.setProperty("warc.logging.console.level", "DEBUG");
    System.setProperty("warc.logging.pattern", "[%d{HH:mm:ss.SSS}] [%level] [%logger{1}:%L] %msg%n");
    Configurator.reconfigure();
  }

  private static void configureDebug() {
    System.setProperty("warc.logging.level", "TRACE");
    System.setProperty("warc.logging.console.level", "TRACE");
    System.setProperty("warc.logging.pattern", "[%d{HH:mm:ss.SSS}] [%level] [%C{1}.%M:%L] %msg%n");
    Configurator.reconfigure();
  }
}
