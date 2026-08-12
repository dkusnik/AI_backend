package pl.gov.nac.warc;

import pl.gov.nac.warc.config.ArgParser;
import pl.gov.nac.warc.config.Config;
import pl.gov.nac.warc.config.LoadedConfig;
import pl.gov.nac.warc.consumers.ElasticsearchExporterVT;
import pl.gov.nac.warc.reactive.ProcessingResult;
import pl.gov.nac.warc.reactive.VirtualThreadEngine;

public final class Pipeline {

  private Pipeline() {
  }

  public static void main(String[] args) {
    System.exit(run(args));
  }

  public static int run(String[] args) {
    long started = System.nanoTime();
    String requestedFormat = ArgParser.detectResultFormat(args);
    boolean jsonRequested = "json".equalsIgnoreCase(requestedFormat);
    System.setProperty("warc.logging.console.target", jsonRequested ? "SYSTEM_ERR" : "SYSTEM_OUT");

    LoadedConfig loaded = null;
    try {
      loaded = Config.load(args);
    } catch (Config.CliSignalException e) {
      if (jsonRequested) {
        System.err.println(e.output());
        return emitResult(ProcessingResult.create(
            e.exitCode(), false, false, false, elapsedMillis(started),
            e.exitCode() == 0 ? null : ProcessingResult.INVALID_ARGUMENTS, e.output()));
      }
      System.out.println(e.output());
      return e.exitCode();
    } catch (Throwable t) {
      StartupFailure failure = classifyStartupFailure(t);
      System.err.println("ERROR: " + ProcessingResult.sanitizeMessage(messageOf(t)));
      if (jsonRequested) {
        return emitResult(ProcessingResult.create(
            failure.exitCode(), false, false, false, elapsedMillis(started),
            failure.errorCode(), messageOf(t)));
      }
      return failure.exitCode();
    }

    // CLI override via -Dwarc.engine takes precedence over config
    String engineType = System.getProperty("warc.engine", loaded.engineType);

    // ReactiveEngine was removed 2026-07-08 (R-11 #10): it produced no valid
    // output for current pipelines (R-10 M10: 0 MB/s, 7/290 records) and was
    // not selected by any shipped config. Fail fast instead of silently
    // running the wrong engine.
    if ("reactive".equalsIgnoreCase(engineType)) {
      String message = "engine type 'reactive' has been removed; use 'virtual' (default)";
      System.err.println("ERROR: " + message);
      if (loaded.isJsonResult()) {
        return emitResult(ProcessingResult.create(
            2, loaded.isDryRun, false, isElasticsearchActive(loaded), elapsedMillis(started),
            ProcessingResult.UNSUPPORTED_ENGINE, message));
      }
      return 2;
    }

    VirtualThreadEngine engine = new VirtualThreadEngine(loaded);
    int exitCode;
    String errorCode;
    String errorMessage;
    try {
      exitCode = engine.run();
      errorCode = engine.failureCode();
      errorMessage = engine.failureMessage();
    } catch (Throwable t) {
      exitCode = 1;
      errorCode = ProcessingResult.INTERNAL_ERROR;
      errorMessage = messageOf(t);
      System.err.println("ERROR: " + ProcessingResult.sanitizeMessage(errorMessage));
    }

    if (loaded.isJsonResult()) {
      return emitResult(ProcessingResult.create(
          exitCode,
          loaded.isDryRun,
          true,
          isElasticsearchActive(loaded),
          elapsedMillis(started),
          errorCode,
          errorMessage));
    }
    return exitCode;
  }

  private static int emitResult(ProcessingResult result) {
    return result.writeTo(System.out) ? result.exitCode() : 1;
  }

  private static boolean isElasticsearchActive(LoadedConfig loaded) {
    return loaded.consumer instanceof ElasticsearchExporterVT;
  }

  private static long elapsedMillis(long started) {
    return Math.max((System.nanoTime() - started) / 1_000_000L, 0L);
  }

  private static StartupFailure classifyStartupFailure(Throwable failure) {
    String message = messageOf(failure).toLowerCase(java.util.Locale.ROOT);
    if (hasCause(failure, ClassNotFoundException.class)
        || message.contains("unknown producer module")
        || message.contains("unknown processor module")
        || message.contains("unknown consumer module")
        || message.contains("unknown checker")) {
      return new StartupFailure(11, ProcessingResult.MODULE_NOT_FOUND);
    }
    if (failure instanceof IllegalArgumentException) {
      return new StartupFailure(2, ProcessingResult.INVALID_ARGUMENTS);
    }
    return new StartupFailure(12, ProcessingResult.CONFIGURATION_ERROR);
  }

  private static boolean hasCause(Throwable failure, Class<? extends Throwable> type) {
    for (Throwable current = failure; current != null; current = current.getCause()) {
      if (type.isInstance(current)) {
        return true;
      }
    }
    return false;
  }

  private static String messageOf(Throwable failure) {
    for (Throwable current = failure; current != null; current = current.getCause()) {
      if (current.getMessage() != null && !current.getMessage().isBlank()) {
        return current.getMessage();
      }
    }
    return failure.getClass().getSimpleName();
  }

  private record StartupFailure(int exitCode, String errorCode) {
  }
}
