package pl.gov.nac.warc.reactive;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public final class Metrics {

  public static final class Key {
    public final String namespace;
    public final String name;

    public Key(String namespace, String name) {
      this.namespace = namespace;
      this.name = name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (!(o instanceof Key))
        return false;
      Key k = (Key) o;
      return namespace.equals(k.namespace) && name.equals(k.name);
    }

    @Override
    public int hashCode() {
      return 31 * namespace.hashCode() + name.hashCode();
    }
  }

  private static final String ENGINE_NS = "engine";

  private static final Map<Key, LongAdder> counters = new ConcurrentHashMap<>();
  private static final Map<String, String> moduleHeaders = new ConcurrentHashMap<>();

  // Package-private — only VirtualThreadEngine and tests write this.
  static volatile boolean benchmarkMode = false;

  private static volatile Instant startTime;
  private static volatile Instant endTime;
  private static final AtomicLong peakMemoryBytes = new AtomicLong(0);
  private static volatile long startMemoryBytes = 0;

  // In-flight tracking
  private static final AtomicLong recordsInFlight = new AtomicLong(0);
  private static final AtomicLong peakRecordsInFlight = new AtomicLong(0);
  private static volatile long recordsLimit = 0;

  private static final AtomicLong bytesInFlight = new AtomicLong(0);
  private static final AtomicLong peakBytesInFlight = new AtomicLong(0);
  private static volatile long bytesLimit = 0;

  private Metrics() {
  }

  // ---------------------------------------------------------------------
  // Memory tracking
  // ---------------------------------------------------------------------
  public static void recordMemoryStart() {
    Runtime rt = Runtime.getRuntime();
    startMemoryBytes = rt.totalMemory() - rt.freeMemory();
    peakMemoryBytes.set(startMemoryBytes);
  }

  public static void updatePeakMemory() {
    Runtime rt = Runtime.getRuntime();
    long used = rt.totalMemory() - rt.freeMemory();
    peakMemoryBytes.updateAndGet(prev -> Math.max(prev, used));
  }

  public static long getPeakMemory() {
    return peakMemoryBytes.get();
  }

  public static long getPeakMemoryBytes() {
    return peakMemoryBytes.get();
  }

  public static long getStartMemoryBytes() {
    return startMemoryBytes;
  }

  public static String formatBytes(long bytes) {
    if (bytes < 1024)
      return bytes + " B";
    if (bytes < 1024 * 1024)
      return String.format("%.1f KB", bytes / 1024.0);
    if (bytes < 1024 * 1024 * 1024)
      return String.format("%.1f MB", bytes / (1024.0 * 1024));
    return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
  }

  // ---------------------------------------------------------------------
  // Counters
  // ---------------------------------------------------------------------
  public static LongAdder counter(String ns, String name) {
    return counters.computeIfAbsent(new Key(ns, name), k -> new LongAdder());
  }

  public static void inc(String ns, String name) {
    counter(ns, name).increment();
  }

  public static void add(String ns, String name, long delta) {
    counter(ns, name).add(delta);
  }

  public static long get(String ns, String name) {
    LongAdder c = counters.get(new Key(ns, name));
    return c == null ? 0 : c.sum();
  }

  public static Map<Key, LongAdder> snapshot() {
    return Map.copyOf(counters);
  }

  public static Map<String, Long> getSummary() {
    Map<String, Long> summary = new TreeMap<>();
    for (var e : counters.entrySet()) {
      summary.put(e.getKey().namespace + "." + e.getKey().name, e.getValue().sum());
    }
    summary.put("engine.timeElapsed", duration().toMillis());
    summary.put("engine.peakMemory", peakMemoryBytes.get());
    return summary;
  }

  public static void recordInFlight() {
    long current = recordsInFlight.incrementAndGet();
    peakRecordsInFlight.accumulateAndGet(current, Math::max);
  }

  public static void releaseRecord() {
    recordsInFlight.decrementAndGet();
  }

  public static long getRecordsInFlight() {
    return recordsInFlight.get();
  }

  public static long getPeakRecordsInFlight() {
    return peakRecordsInFlight.get();
  }

  public static void setRecordsLimit(long limit) {
    recordsLimit = limit;
  }

  public static long getRecordsLimit() {
    return recordsLimit;
  }

  public static void recordBytesInFlight(long delta) {
    long current = bytesInFlight.addAndGet(delta);
    peakBytesInFlight.accumulateAndGet(current, Math::max);
  }

  public static void recordRecordsInFlight(int delta) {
    long current = recordsInFlight.addAndGet(delta);
    peakRecordsInFlight.accumulateAndGet(current, Math::max);
  }

  public static void releaseRecords(int delta) {
    recordsInFlight.addAndGet(-delta);
  }

  public static void releaseBytes(long delta) {
    bytesInFlight.addAndGet(-delta);
  }

  public static long getBytesInFlight() {
    return bytesInFlight.get();
  }

  public static long getPeakBytesInFlight() {
    return peakBytesInFlight.get();
  }

  public static void setBytesLimit(long limit) {
    bytesLimit = limit;
  }

  public static long getBytesLimit() {
    return bytesLimit;
  }

  public static void reset() {
    benchmarkMode = false;
    counters.clear();
    moduleHeaders.clear();
    startTime = null;
    endTime = null;
    peakMemoryBytes.set(0);
    startMemoryBytes = 0;
    recordsInFlight.set(0);
    peakRecordsInFlight.set(0);
    recordsLimit = 0;
    bytesInFlight.set(0);
    peakBytesInFlight.set(0);
    bytesLimit = 0;
  }

  // ---------------------------------------------------------------------
  // Module headers
  // ---------------------------------------------------------------------
  public static void setModuleHeader(String ns, String text) {
    moduleHeaders.put(ns, text);
  }

  public static Optional<String> getModuleHeader(String ns) {
    return Optional.ofNullable(moduleHeaders.get(ns));
  }

  // ---------------------------------------------------------------------
  // Timing
  // ---------------------------------------------------------------------
  public static void markStart() {
    startTime = Instant.now();
    endTime = null;
  }

  public static void markEnd() {
    endTime = Instant.now();
  }

  public static Duration duration() {
    if (startTime == null)
      return Duration.ZERO;
    Instant end = endTime != null ? endTime : Instant.now();
    return Duration.between(startTime, end);
  }

  public static String formatDuration(Duration d) {
    long s = d.getSeconds();
    long m = (s % 3600) / 60;
    long sec = s % 60;
    long h = s / 3600;
    long millis = d.toMillis() % 1000;
    if (h > 0)
      return String.format("%02d:%02d:%02d.%03d", h, m, sec, millis);
    return String.format("%02d:%02d.%03d", m, sec, millis);
  }

  public static String formatDurationNoMillis(Duration d) {
    long s = d.getSeconds();
    long m = (s % 3600) / 60;
    long sec = s % 60;
    long h = s / 3600;
    if (h > 0)
      return String.format("%02d:%02d:%02d", h, m, sec);
    return String.format("%02d:%02d", m, sec);
  }

  // ---------------------------------------------------------------------
  // FINAL REPORT
  // ---------------------------------------------------------------------
  public static void set(String ns, String name, long value) {
    LongAdder c = counter(ns, name);
    c.reset();
    c.add(value);
  }

  // ---------------------------------------------------------------------
  // FINAL REPORT
  // ---------------------------------------------------------------------
  public static String buildFinalReport(List<String> moduleOrder) {
    StringBuilder sb = new StringBuilder();

    Map<Key, LongAdder> snap = snapshot();

    // Collect ALL values including engine time for width calculation
    List<String> allValues = new ArrayList<>();

    for (var e : snap.values()) {
      allValues.add(String.valueOf(e.sum()));
    }

    // Include engine time
    allValues.add(formatDuration(duration()));

    // Compute max width across ALL left-column values
    int maxValLen = allValues.stream()
        .mapToInt(String::length)
        .max()
        .orElse(1);

    // Group metrics by namespace
    Map<String, List<Map.Entry<Key, LongAdder>>> groups = new TreeMap<>();
    for (var e : snap.entrySet()) {
      groups.computeIfAbsent(e.getKey().namespace, k -> new ArrayList<>()).add(e);
    }

    // -----------------------------------------------------------------
    // MODULE BLOCKS
    // -----------------------------------------------------------------
    // Use LinkedHashSet to preserve order and avoid duplicates if "processor" is
    // repeated
    Set<String> order = new LinkedHashSet<>(moduleOrder);
    // Add any other namespaces found in snap that weren't in order
    groups.keySet().forEach(order::add);

    for (String ns : order) {
      if (ns.startsWith(ENGINE_NS))
        continue; // Handled separately

      List<Map.Entry<Key, LongAdder>> list = groups.get(ns);
      if (list == null || list.isEmpty())
        continue;

      sb.append("[").append(ns).append("]\n");

      getModuleHeader(ns).ifPresent(h -> sb.append(h).append("\n"));

      list.sort(Comparator.comparing(e -> e.getKey().name));

      for (var e : list) {
        String val = String.valueOf(e.getValue().sum());
        String metricName = e.getKey().name;
        sb.append(String.format("%" + maxValLen + "s  %s%n", val, metricName));
      }
    }

    // -----------------------------------------------------------------
    // ENGINE BLOCK
    // -----------------------------------------------------------------
    sb.append("[engine]\n");

    String elapsed = formatDuration(duration());
    sb.append(String.format("%" + maxValLen + "s  timeElapsed%n", elapsed));

    // Throughput (Average IO) via Producer BytesOut + Consumer BytesIn (if any,
    // usually produced->out)
    // Simplification: Using Producer BytesOut as "Processed Bytes"
    long totalBytes = get("producer", "bytesOut");
    long durationMillis = duration().toMillis();
    if (durationMillis > 0) {
      double avgMbPerSec = (double) totalBytes / 1024 / 1024 / (durationMillis / 1000.0);
      sb.append(String.format("%" + maxValLen + ".2f  MB/s (avg)%n", avgMbPerSec));
    }

    // Only print recordsIn/Out if they exist or are non-zero to avoid clutter,
    // OR better: print them if they were used.
    // The user request shows them as 0, so we print them.
    long recordsIn = get(ENGINE_NS, "recordsIn");
    long recordsOut = get(ENGINE_NS, "recordsOut");

    sb.append(String.format("%" + maxValLen + "d  recordsIn%n", recordsIn));
    sb.append(String.format("%" + maxValLen + "d  recordsOut%n", recordsOut));

    // Memory stats
    sb.append(String.format("%" + maxValLen + "s  memoryStart%n", formatBytes(getStartMemoryBytes())));
    sb.append(String.format("%" + maxValLen + "s  memoryPeak%n", formatBytes(getPeakMemoryBytes())));

    return sb.toString();
  }

  // ---------------------------------------------------------------------
  // SUMMARY REPORT (Single Line)
  // ---------------------------------------------------------------------

  /**
   * Build a single-line summary report suitable for benchmarks.
   * Format: "612.53 MB/s (avg) 00:01.234 timeElapsed 328.0 MB memoryPeak"
   */
  public static String buildFinalReportSummary(List<String> moduleOrder) {
    Duration d = duration();
    long totalBytes = get("producer", "bytesOut");
    long durationMillis = d.toMillis();

    double avgMbPerSec = 0;
    if (durationMillis > 0) {
      avgMbPerSec = (double) totalBytes / (1024 * 1024) / (durationMillis / 1000.0);
    }

    return String.format("%.2f MB/s (avg)  %s timeElapsed  %s memoryPeak",
        avgMbPerSec,
        formatDuration(d),
        formatBytes(getPeakMemoryBytes()));
  }
}
