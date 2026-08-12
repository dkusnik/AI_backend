package pl.gov.nac.warc.testutil;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;

public final class ExpectedLogSilencer {

  private ExpectedLogSilencer() {
  }

  public static void runWithLoggerMuted(Class<?> loggerClass, Runnable action) {
    String loggerName = loggerClass.getName();
    Level previousLevel = LogManager.getLogger(loggerClass).getLevel();
    Configurator.setLevel(loggerName, Level.OFF);
    try {
      action.run();
    } finally {
      Configurator.setLevel(loggerName, previousLevel);
    }
  }
}
