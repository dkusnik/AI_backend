package pl.gov.nac.warc.reactive;

import java.io.PrintStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

import pl.gov.nac.warc.config.ProgressMode;

/**
 * PeriodicReporter
 *
 * Prints each second with elapsed time prefix on ALL lines (grep-friendly):
 *
 * [00:00:05] [producer] records: 150 | bytes: 12.5 MB
 * [00:00:05] [grep] in: 150 | matched: 45 | dropped: 105
 * [00:00:05] [memory] heap: 245 MB / 512 MB (peak: 312 MB)
 * [00:00:05] [throughput] ▂▃▅▇█▆▅▃
 */
public final class PeriodicReporter implements Runnable {

  private final AtomicBoolean running = new AtomicBoolean(false);
  private final List<String> moduleOrder;
  private Thread thread;

  // History for sparkline (last 20 seconds)
  private final LinkedList<Long> throughputHistory = new LinkedList<>();
  private static final int HISTORY_SIZE = 20;

  // For rate calculation
  private long lastBytes = 0;
  private long lastProgressBytes = 0; // compressed input bytes (for ETA / progress %)
  private long lastTime = System.currentTimeMillis();
  private int lastInlineLength = 0;
  private boolean inlineActive = false;

  // UTF-8 blocks for sparkline: ▂ ▃ ▄ ▅ ▆ ▇ █
  private static final char[] BLOCKS = new char[] { ' ', '▂', '▃', '▄', '▅', '▆', '▇', '█' };
  private static final String ENGINE_NS = "engine";

  private final boolean isBenchmark;
  private final ProgressMode progressMode;
  private final PrintStream console;

  public PeriodicReporter(List<String> moduleOrder, boolean isBenchmark, ProgressMode progressMode) {
    this(moduleOrder, isBenchmark, progressMode, false);
  }

  public PeriodicReporter(
      List<String> moduleOrder,
      boolean isBenchmark,
      ProgressMode progressMode,
      boolean outputToStderr) {
    this.moduleOrder = new ArrayList<>(moduleOrder != null ? moduleOrder : List.of());
    this.isBenchmark = isBenchmark;
    this.progressMode = progressMode != null ? progressMode : ProgressMode.DEFAULT;
    this.console = outputToStderr ? System.err : System.out;
  }

  public synchronized void start() {
    if (running.get())
      return;
    running.set(true);
    thread = new Thread(this, "metrics-reporter");
    thread.setDaemon(true);
    thread.start();
  }

  public synchronized void stop() {
    if (!running.get())
      return;
    running.set(false);
    if (thread != null) {
      thread.interrupt();
      try {
        thread.join(2000);
      } catch (InterruptedException _) {
        Thread.currentThread().interrupt();
      }
    }
    if (progressMode == ProgressMode.DEFAULT && inlineActive) {
      console.println();
      inlineActive = false;
      lastInlineLength = 0;
    }
  }

  /**
   * Print final progress line when processing completes.
   * Uses total-bytes / elapsed-time for throughput and forces ETA to 00:00.
   */
  public void printFinalLine() {
    if (progressMode == ProgressMode.NONE) {
      return;
    }
    printOnce(true);
  }

  @Override
  public void run() {
    while (running.get()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException _) {
        Thread.currentThread().interrupt();
        return;
      }
      if (!running.get())
        return;

      printOnce(false);
    }
  }

  private void printOnce() {
    printOnce(false);
  }

  private void printOnce(boolean isFinal) {
    if (progressMode == ProgressMode.NONE) {
      Metrics.updatePeakMemory();
      return;
    }

    // Update peak memory each second
    Metrics.updatePeakMemory();

    Map<Metrics.Key, LongAdder> snap = Metrics.snapshot();
    Duration d = Metrics.duration();
    String elapsed = Metrics.formatDuration(d);

    // -----------------------------------------------------------------
    // EXTRACT KEY METRICS
    // -----------------------------------------------------------------
    long rOut = Metrics.get("producer", "recordsOut");
    long bOut = Metrics.get("producer", "bytesOut");

    // bProgress: compressed bytes read from input file (same unit as bTotal=Files.size).
    // Falls back to bOut when the producer doesn't track inputBytesRead (e.g. WACZ, codec path).
    long bProgress = Metrics.get("producer", "inputBytesRead");
    if (bProgress == 0) {
      bProgress = bOut;
    }

    long rTotal = Metrics.get(ENGINE_NS, "totalRecords");
    long bTotal = Metrics.get(ENGINE_NS, "totalBytes");

    // THROUGHPUT CALC
    // - deltaOut  (decompressed) → MB/s display (pipeline throughput)
    // - deltaProgress (compressed) → sparkline history and ETA (matches bTotal unit)
    long now = System.currentTimeMillis();
    long deltaOut = bOut - lastBytes;
    long deltaProgress = bProgress - lastProgressBytes;
    long deltaTime = now - lastTime;
    if (deltaTime <= 0)
      deltaTime = 1;

    double outBytesPerSec = deltaOut * 1000.0 / deltaTime;
    double progressBytesPerSec = deltaProgress * 1000.0 / deltaTime;

    // Final line uses average throughput (total / elapsed); regular ticks use last-second rate
    long mbPerSec;
    if (isFinal) {
      long elapsedSecs = Math.max(1, d.toSeconds());
      mbPerSec = (bOut / (1024 * 1024)) / elapsedSecs;
    } else {
      mbPerSec = (long) (outBytesPerSec / (1024 * 1024));
    }

    lastBytes = bOut;
    lastProgressBytes = bProgress;
    lastTime = now;

    // UPDATE HISTORY with compressed-input rate (matches ETA/bTotal unit)
    if (throughputHistory.size() >= HISTORY_SIZE) {
      throughputHistory.removeFirst();
    }
    throughputHistory.add((long) progressBytesPerSec);

    // ETA CALC — rolling 10-second window average of compressed input rate
    String eta = "--:--";
    if (isFinal) {
      eta = "00:00";
    } else if (bTotal > 0) {
      int window = Math.min(10, throughputHistory.size());
      if (window > 0) {
        List<Long> recent = new ArrayList<>(throughputHistory)
            .subList(throughputHistory.size() - window, throughputHistory.size());
        double etaBytesPerSec = recent.stream().mapToLong(v -> v).average().orElse(0);
        if (etaBytesPerSec > 0) {
          long remaining = bTotal - bProgress;
          if (remaining > 0) {
            long seconds = (long) (remaining / etaBytesPerSec);
            eta = Metrics.formatDurationNoMillis(Duration.ofSeconds(seconds));
          } else {
            eta = "00:00";
          }
        }
      }
    }

    // PROGRESS % — use compressed progress vs compressed total (no unit mismatch)
    long rPct = (rTotal > 0) ? Math.min(100, rOut * 100 / rTotal) : 0;
    long bPct = (bTotal > 0) ? Math.min(100, bProgress * 100 / bTotal) : 0;

    // -----------------------------------------------------------------
    // LINE 1: Summary (with elapsed prefix)
    // -----------------------------------------------------------------
    String totalRStr = rTotal > 0 ? String.valueOf(rTotal) : "?";
    String totalBStr = bTotal > 0 ? String.valueOf(bTotal) : "?";

    String rPctStr = "--%";
    if (rTotal > 0) {
      rPctStr = String.format("%d%%", rPct);
    }
    String bPctStr = "--%";
    if (bTotal > 0) {
      bPctStr = String.format("%d%%", bPct);
    }

    if (progressMode == ProgressMode.DEFAULT) {
      // Format MB values — use compressed progress (bProgress) vs compressed total (bTotal)
      long bProgressMB = bProgress / (1024 * 1024);
      long bTotalMB = bTotal > 0 ? bTotal / (1024 * 1024) : 0;
      String totalBMBStr = bTotal > 0 ? String.valueOf(bTotalMB) : "?";
      String line = String.format("[%s] [main] %d/%s (%s) rec  %d/%s (%s) MB  %d MB/s  ETA %s",
          elapsed, rOut, totalRStr, rPctStr, bProgressMB, totalBMBStr, bPctStr, mbPerSec, eta);
      int pad = Math.max(0, lastInlineLength - line.length());
      if (isFinal) {
        // End the inline line permanently so subsequent log output starts on a new line
        console.println("\r" + line + " ".repeat(pad));
        inlineActive = false;
        lastInlineLength = 0;
      } else {
        console.print("\r" + line + " ".repeat(pad));
        console.flush();
        lastInlineLength = line.length();
        inlineActive = true;
      }
      return;
    }

    // Build one output buffer per tick to reduce synchronized console write overhead.
    StringBuilder out = new StringBuilder(2048);
    out.append(String.format("[%s] [main] %d/%s (%s) rec %d/%s (%s) bytes %d MB/s ETA %s%n", // NOSONAR
        elapsed,
        rOut, totalRStr, rPctStr,
        bProgress, totalBStr, bPctStr,
        mbPerSec,
        eta));

    // -----------------------------------------------------------------
    // LINE 2: Throughput graph (with elapsed prefix)
    // -----------------------------------------------------------------
    out.append(String.format("[%s] [throughput] %s%n", elapsed, renderSparkline(throughputHistory))); // NOSONAR

    // -----------------------------------------------------------------
    // LINE 3: Memory / Heap (with elapsed prefix)
    // -----------------------------------------------------------------
    Runtime rt = Runtime.getRuntime();
    long maxMB = rt.maxMemory() / (1024 * 1024);

    // In-flight tracking
    long rInFlight = Metrics.getRecordsInFlight();
    long rPeak = Metrics.getPeakRecordsInFlight();
    long rLimit = Metrics.getRecordsLimit();
    String rLimitStr = rLimit > 0 ? String.valueOf(rLimit) : "no-limit";

    long bInFlight = Metrics.getBytesInFlight();
    long bPeak = Metrics.getPeakBytesInFlight();
    long bLimit = Metrics.getBytesLimit();
    String bLimitStr = bLimit > 0 ? "limit " + Metrics.formatBytes(bLimit) : "no-limit";
    if (bLimit == 0)
      bLimitStr = "no-limit"; // redundant but clearer

    out.append(String.format("[%s] [memory]     in-flight: %d (peak: %d) / %s recs , %s (peak: %s) / %s / JVM %d MB%n", // NOSONAR
        elapsed,
        rInFlight, rPeak, rLimitStr,
        Metrics.formatBytes(bInFlight), Metrics.formatBytes(bPeak), bLimitStr, maxMB));

    // -----------------------------------------------------------------
    // MODULES (with elapsed prefix on each line)
    // -----------------------------------------------------------------
    Map<String, List<Map.Entry<Metrics.Key, LongAdder>>> groups = new TreeMap<>();
    for (var e : snap.entrySet()) {
      groups.computeIfAbsent(e.getKey().namespace, k -> new ArrayList<>()).add(e);
    }

    // Determine full order
    Set<String> order = new LinkedHashSet<>(moduleOrder);
    groups.keySet().forEach(order::add);

    for (String ns : order) {
      if (ns.startsWith(ENGINE_NS) || ns.equals("budget"))
        continue;

      List<Map.Entry<Metrics.Key, LongAdder>> list = groups.get(ns);
      if (list == null || list.isEmpty())
        continue;

      StringBuilder sb = new StringBuilder();
      list.sort(Comparator.comparing(e -> e.getKey().name));

      for (var e : list) {
        if (sb.length() > 0)
          sb.append(" | ");
        sb.append(e.getKey().name)
            .append(": ")
            .append(e.getValue().sum());
      }

      // Pad namespace for alignment
      String paddedNs = String.format("%-12s", ns);
      out.append(String.format("[%s] [%s] %s%n", elapsed, paddedNs, sb)); // NOSONAR
    }
    console.print(out);
  }

  private String renderSparkline(List<Long> history) {
    if (history.isEmpty())
      return "";

    long max = history.stream().mapToLong(v -> v).max().orElse(1);
    if (max == 0)
      max = 1;

    StringBuilder sb = new StringBuilder();
    for (Long val : history) {
      int index = (int) ((val * (BLOCKS.length - 1)) / max);
      if (index < 0)
        index = 0;
      if (index >= BLOCKS.length)
        index = BLOCKS.length - 1;
      sb.append(BLOCKS[index]);
    }
    return sb.toString();
  }
}
