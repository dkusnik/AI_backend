package pl.gov.nac.warc.reactive;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.gov.nac.warc.config.FinalReportMode;
import pl.gov.nac.warc.config.LoadedConfig;
import pl.gov.nac.warc.config.PipelineNegotiator;
import pl.gov.nac.warc.processors.WarcGzipCompressor;
import pl.gov.nac.warc.utils.gzip.IsalDecompressor;

/**
 * High-performance engine using Virtual Threads with decoupled
 * producer/processor.
 *
 * Architecture:
 *
 * <pre>
 * Producer Thread(s)
 *        ↓
 * [Bounded Queue] (backpressure via queue capacity)
 *        ↓
 * Worker Virtual Threads (N workers, each processes through full chain)
 *        ↓
 * Processor Chain (parallel execution)
 *        ↓
 * SerializedSubscriber (serializes concurrent writes)
 *        ↓
 * Consumer
 * </pre>
 *
 * This decouples I/O-bound production from CPU-bound processing, enabling
 * true parallel text extraction across multiple cores.
 */
public final class VirtualThreadEngine {

  private static final Logger log = LogManager.getLogger(VirtualThreadEngine.class);
  private static final Object POISON_PILL = new Object();

  private final LoadedConfig cfg;
  private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);
  private volatile String failureCode;
  private volatile String failureMessage;

  public VirtualThreadEngine(LoadedConfig cfg) {
    this.cfg = cfg;
  }

  public int run() {
    log.info("Starting pipeline (VirtualThreadEngine): {}", cfg.pipelineDef.name);

    Metrics.reset();
    Metrics.recordMemoryStart();
    Metrics.markStart();

    Thread shutdownHook = new Thread(() -> {
      shutdownRequested.set(true);
      log.info("Shutdown requested (Ctrl-C), terminating gracefully...");
    });
    Runtime.getRuntime().addShutdownHook(shutdownHook);

    List<String> moduleOrder = buildModuleOrder();

    PeriodicReporter reporter = new PeriodicReporter(
        moduleOrder, cfg.isBenchmark, cfg.progressMode, cfg.isJsonResult());
    printConfigLine();
    reporter.start();

    try {
      List<ReactiveInterfaces.ReactiveProcessor<?, ?>> activeProcessors = getActiveProcessors();
      List<Map<String, Object>> activeProcessorConfigs = getActiveProcessorConfigs();

      try {
        negotiatePipeline(activeProcessors, activeProcessorConfigs);
      } catch (Throwable t) {
        log.error("Pipeline type negotiation failed", t);
        discardConsumerOutputs();
        return fail(10, ProcessingResult.PIPELINE_NEGOTIATION_FAILED, messageOf(t));
      }

      if (cfg.isDryRun) {
        log.info("Dry run completed. Pipeline shape validated.");
        return 0;
      }

      try {
        if (!runBeforeChecks()) {
          log.error("Pipeline failed before-checkers");
          discardConsumerOutputs();
          return fail(1, ProcessingResult.BEFORE_CHECK_FAILED, "pipeline before-check failed");
        }
      } catch (Throwable t) {
        log.error("Pipeline before-check failed", t);
        discardConsumerOutputs();
        return fail(1, ProcessingResult.BEFORE_CHECK_FAILED, messageOf(t));
      }

      int pipelineExit = runPipeline(activeProcessors);
      int afterExit;
      try {
        afterExit = runAfterChecks();
      } catch (Throwable t) {
        log.error("Pipeline after-check failed", t);
        afterExit = fail(1, ProcessingResult.AFTER_CHECK_FAILED, messageOf(t));
      }
      int prerequisiteExit = Math.max(pipelineExit, afterExit);
      if (afterExit != 0 && pipelineExit == 0 && failureCode == null) {
        fail(afterExit, ProcessingResult.AFTER_CHECK_FAILED, "pipeline after-check failed");
      }

      int publicationExit = 0;
      if (prerequisiteExit == 0) {
        try {
          publicationExit = cfg.consumer.publishOutputs();
        } catch (Throwable t) {
          log.error("Output publication failed", t);
          publicationExit = fail(1, ProcessingResult.AFTER_CHECK_FAILED, messageOf(t));
          discardConsumerOutputs();
        }
        if (publicationExit != 0 && failureCode == null) {
          fail(publicationExit, ProcessingResult.AFTER_CHECK_FAILED, "output publication failed");
        }
      } else {
        discardConsumerOutputs();
      }
      int finalExit = Math.max(prerequisiteExit, publicationExit);

      reporter.printFinalLine();
      reporter.stop();

      if (!cfg.logCliNone) {
        log.info("Pipeline finished with exit code: {}", finalExit);
      }

      // Handle final report based on mode
      if (cfg.finalReportMode != FinalReportMode.NONE) {
        String report = (cfg.finalReportMode == FinalReportMode.SUMMARY)
            ? Metrics.buildFinalReportSummary(moduleOrder)
            : Metrics.buildFinalReport(moduleOrder);

        java.io.PrintStream out = (cfg.isJsonResult() || cfg.finalReportToStderr) ? System.err : System.out;
        out.println(report);
      }

      return finalExit;
    } catch (Throwable t) {
      log.error("Unhandled pipeline failure", t);
      discardConsumerOutputs();
      return fail(1, ProcessingResult.INTERNAL_ERROR, messageOf(t));
    } finally {
      Metrics.markEnd();
      reporter.stop();
      try {
        if (!shutdownRequested.get()) {
          Runtime.getRuntime().removeShutdownHook(shutdownHook);
        }
      } catch (IllegalStateException ignored) {
      }
    }
  }

  private int runPipeline(List<ReactiveInterfaces.ReactiveProcessor<?, ?>> activeProcessors) {
    Instant start = Instant.now();
    CountDownLatch latch = new CountDownLatch(1);
    AtomicBoolean terminalError = new AtomicBoolean(false);

    try {
      wireAndStart(latch, activeProcessors, terminalError);
    } catch (Throwable t) {
      terminalError.set(true);
      log.error("Failed to start pipeline", t);
      return fail(1, ProcessingResult.PROCESSING_FAILED, messageOf(t));
    }

    try {
      boolean ok = latch.await(cfg.shutdownTimeoutSeconds, TimeUnit.SECONDS);
      if (!ok) {
        log.error("Pipeline timed out after {} seconds", cfg.shutdownTimeoutSeconds);
        return fail(1, ProcessingResult.TIMED_OUT,
            "pipeline timed out after " + cfg.shutdownTimeoutSeconds + " seconds");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.error("Interrupted while waiting for pipeline completion", e);
      return fail(1, ProcessingResult.INTERRUPTED, "interrupted while waiting for pipeline completion");
    }

    Duration elapsed = Duration.between(start, Instant.now());
    log.info("Pipeline execution time: {}", (Object) formatDuration(elapsed));
    return terminalError.get()
        ? fail(1, ProcessingResult.PROCESSING_FAILED, "pipeline terminated with a processing error")
        : 0;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private void wireAndStart(CountDownLatch latch, List<ReactiveInterfaces.ReactiveProcessor<?, ?>> activeProcessors,
      AtomicBoolean terminalError) {

    // Set minimal metrics limits for UI
    Metrics.setRecordsLimit(cfg.maxRecords);
    Metrics.benchmarkMode = cfg.isBenchmark;

    // Determine worker count from concurrency setting
    int workerCount = cfg.concurrency();
    if (workerCount <= 0) {
      workerCount = Runtime.getRuntime().availableProcessors();
    }
    // Cap at reasonable limit
    workerCount = Math.min(workerCount, 200);

    // When parallel GZIP is enabled, limit workers to maxRecords to bound
    // concurrent compression buffer memory
    IsalDecompressor.setEnabled(cfg.isalEnabled);

    if (cfg.parallelGzip) {
      workerCount = Math.min(workerCount, cfg.maxRecords);
      log.info("Parallel GZIP: limiting workers to {} (maxRecords) to bound compression memory", workerCount);
    }

    // Use maxRecords for queue capacity (backpressure)
    int queueCapacity = Math.max(cfg.maxRecords, workerCount * 2);
    // JCTools requirement: power of 2 capacity
    if (Integer.bitCount(queueCapacity) != 1) {
      queueCapacity = Integer.highestOneBit(queueCapacity - 1) << 1;
    }

    log.info("Parallel mode: {} workers, queue capacity {}", workerCount, queueCapacity);

    // 1. Consumer Setup
    Flow.Subscriber consumer = cfg.consumer;

    // Wrap consumer with SerializedSubscriber for thread-safe access
    // Use a capacity proportional to worker count to balance backpressure
    // and throughput. Too small (e.g., 1) causes excessive blocking at higher
    // core counts; too large bypasses backpressure.
    // Use a larger capacity for the result serializer to avoid backpressure
    // deadlocks
    // between many workers and the single consumer.
    int serializerCapacity = Math.max(1024, workerCount * 32);
    if (Integer.bitCount(serializerCapacity) != 1) {
      serializerCapacity = Integer.highestOneBit(serializerCapacity - 1) << 1;
    }

    consumer = new SerializedSubscriber<>(CompositeSubscriber.wrap(consumer)
        .withLatch(latch)
        .withErrorFlag(terminalError)
        .withMemoryTracking()
        .build(), serializerCapacity);

    Flow.Subscriber tail = consumer;

    // 2. Metrics (Out)
    MetricCountingProcessor<Object> outCounter = new MetricCountingProcessor<>("engine", "recordsOut", null);
    outCounter.subscribe(tail);
    tail = outCounter;

    // 3. Processors (Reverse wiring)
    // If parallel GZIP is enabled, it MUST be the last processor before the
    // consumer
    if (cfg.parallelGzip) {
      WarcGzipCompressor compressor = new WarcGzipCompressor();
      Map<String, Object> compCfg = new java.util.HashMap<>();
      compCfg.put("parallel-gzip", true);
      compCfg.put("compression-level", cfg.parallelGzipLevel);
      compressor.configure(compCfg);

      compressor.subscribe(tail);
      tail = compressor;
    }

    for (int i = activeProcessors.size() - 1; i >= 0; i--) {
      Flow.Processor p = activeProcessors.get(i);
      p.subscribe(tail);
      tail = p;
    }

    // 4. Create parallel dispatcher between producer and processor chain
    final Flow.Subscriber processorHead = tail;
    ParallelDispatcher dispatcher = new ParallelDispatcher(
        processorHead,
        workerCount,
        queueCapacity,
        shutdownRequested,
        cfg);

    // 5. Metrics (In) - wraps dispatcher
    Flow.Publisher producer = cfg.producer;
    MetricCountingProcessor<Object> inCounter = new MetricCountingProcessor<>("engine", "recordsIn", producer);
    inCounter.subscribe(dispatcher);

    // 6. Start
    tryInvokeStartConsuming(cfg.consumer);
    dispatcher.start();
    try {
      cfg.producer.startProducing();
    } catch (Throwable t) {
      terminalError.set(true);
      dispatcher.onError(t);
    }
    log.info("Producer started (Parallel Dispatch), awaiting pipeline completion...");
  }

  /**
   * Parallel dispatcher that decouples producer from processor threads.
   * Uses a bounded queue and virtual thread workers for parallel processing.
   */
  private static class ParallelDispatcher implements Flow.Subscriber<Object> {
    private final Flow.Subscriber<Object> downstream;
    private final int workerCount;
    private final java.util.concurrent.BlockingQueue<Object> queue;
    private final ExecutorService executor;
    private final AtomicBoolean completed = new AtomicBoolean(false);
    private final AtomicBoolean terminalSent = new AtomicBoolean(false);
    private final ReentrantReadWriteLock downstreamSignals = new ReentrantReadWriteLock();
    private final CountDownLatch workersDoneLatch;
    private final LoadedConfig cfg;
    private final int queueCapacity;
    private final long lowWatermark;
    private final AtomicLong remainingDemandBudget;
    private final AtomicLong pendingDemand = new AtomicLong(0);
    private volatile Flow.Subscription upstream;
    private final AtomicBoolean shutdown;

    @SuppressWarnings("unchecked")
    ParallelDispatcher(Flow.Subscriber<?> downstream, int workerCount, int queueCapacity, AtomicBoolean shutdown,
        LoadedConfig cfg) {
      this.downstream = (Flow.Subscriber<Object>) downstream;
      this.workerCount = workerCount;
      this.cfg = cfg;
      this.queueCapacity = queueCapacity;
      this.queue = new java.util.concurrent.ArrayBlockingQueue<>(queueCapacity);
      this.executor = Executors.newVirtualThreadPerTaskExecutor();
      this.workersDoneLatch = new CountDownLatch(workerCount);
      this.shutdown = shutdown;
      this.lowWatermark = Math.max(1, Math.min(queueCapacity / 2L, workerCount * 2L));
      // Queue capacity is the backpressure mechanism: queue.put() blocks the producer
      // when full, and maybeRefillDemand() creates a sliding window as workers drain it.
      // A fixed budget (cfg.maxRecords) is a one-shot limit that would starve the producer
      // after the first maxRecords items when producers respect demand.
      this.remainingDemandBudget = null;
    }

    void start() {
      // Dispatcher is a Subscriber, so it must be wired to upstream via subscribe()
      // This is handled in wireAndStart where inCounter.subscribe(dispatcher) is
      // called.
      // But ParallelDispatcher needs to start its workers.
      for (int i = 0; i < workerCount; i++) {
        final int workerId = i;
        executor.submit(() -> workerLoop(workerId));
      }
    }

    private void workerLoop(int workerId) {
      boolean alreadyCounted = false;
      try {
        while (!shutdown.get()) {
          Object item;
          try {
            item = queue.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
          }
          if (item == null) {
            if (completed.get() && queue.isEmpty()) {
              break;
            }
            continue;
          }

          if (item == POISON_PILL) {
            // Count down first, then check — avoids TOCTOU race where two workers
            // both see getCount()>1 and both re-offer POISON_PILL, causing stray
            // re-entries into the loop.
            workersDoneLatch.countDown();
            if (workersDoneLatch.getCount() > 0) {
              queue.offer(POISON_PILL);
            }
            // Skip the countDown() in finally since we already called it above.
            alreadyCounted = true;
            break;
          }

          try {
            downstreamSignals.readLock().lock();
            try {
              if (!terminalSent.get()) {
                downstream.onNext(item);
              }
            } finally {
              downstreamSignals.readLock().unlock();
            }
          } catch (Throwable t) {
            log.error("Worker {} error processing record", workerId, t);
            signalError(t);
            break;
          }
        }
      } finally {
        if (!alreadyCounted) {
          workersDoneLatch.countDown();
        }
        // Last worker to finish sends onComplete
        if (workersDoneLatch.getCount() == 0) {
          signalComplete();
          executor.shutdown();
        }
      }
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      this.upstream = subscription;
      requestFromUpstream(queueCapacity);
    }

    @Override
    public void onNext(Object item) {
      if (shutdown.get() || completed.get() || terminalSent.get()) {
        return;
      }
      pendingDemand.updateAndGet(v -> v > 0 ? v - 1 : 0);
      try {
        queue.put(item);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
      maybeRefillDemand();
    }

    @Override
    public void onError(Throwable throwable) {
      signalError(throwable);
    }

    private void signalError(Throwable throwable) {
      completed.set(true);
      boolean deliverError = terminalSent.compareAndSet(false, true);
      queue.offer(POISON_PILL);
      if (deliverError) {
        downstreamSignals.writeLock().lock();
        try {
          downstream.onError(throwable);
        } finally {
          downstreamSignals.writeLock().unlock();
        }
      }
    }

    private void signalComplete() {
      if (terminalSent.compareAndSet(false, true)) {
        downstreamSignals.writeLock().lock();
        try {
          downstream.onComplete();
        } finally {
          downstreamSignals.writeLock().unlock();
        }
      }
    }

    @Override
    public void onComplete() {
      completed.set(true);
      // Add poison pill to signal workers
      queue.offer(POISON_PILL);
    }

    private void requestFromUpstream(long n) {
      if (n <= 0 || completed.get()) {
        return;
      }
      Flow.Subscription sub = upstream;
      if (sub == null) {
        return;
      }
      AtomicLong budget = remainingDemandBudget;
      if (budget == null) {
        pendingDemand.addAndGet(n);
        sub.request(n);
        return;
      }
      while (true) {
        long remaining = budget.get();
        if (remaining <= 0) {
          return;
        }
        long toRequest = Math.min(remaining, n);
        if (budget.compareAndSet(remaining, remaining - toRequest)) {
          pendingDemand.addAndGet(toRequest);
          sub.request(toRequest);
          return;
        }
      }
    }

    private void maybeRefillDemand() {
      long inFlight = pendingDemand.get();
      if (inFlight > lowWatermark) {
        return;
      }
      long target = Math.max(1, queueCapacity - queue.size());
      long missing = target - inFlight;
      if (missing > 0) {
        requestFromUpstream(missing);
      }
    }

  }

  // --- Helper methods ---

  private List<ReactiveInterfaces.ReactiveProcessor<?, ?>> getActiveProcessors() {
    List<ReactiveInterfaces.ReactiveProcessor<?, ?>> active = new java.util.ArrayList<>();
    for (int i = 0; i < cfg.processors.size(); i++) {
      if (cfg.processors.get(i).isEnabled(cfg.processorConfigs.get(i))) {
        active.add(cfg.processors.get(i));
      }
    }
    return active;
  }

  private List<Map<String, Object>> getActiveProcessorConfigs() {
    List<Map<String, Object>> active = new java.util.ArrayList<>();
    for (int i = 0; i < cfg.processors.size(); i++) {
      if (cfg.processors.get(i).isEnabled(cfg.processorConfigs.get(i))) {
        active.add(cfg.processorConfigs.get(i));
      }
    }
    return active;
  }

  private List<String> buildModuleOrder() {
    java.util.List<String> order = new java.util.ArrayList<>();
    order.add("producer");
    if (cfg.resolvedStages != null)
      order.addAll(cfg.resolvedStages);
    order.add("processor");
    order.add("consumer");
    return order;
  }

  private boolean runBeforeChecks() {

    if (!cfg.producer.beforeCheck(cfg.producerConfig))
      return false;
    for (int i = 0; i < cfg.processors.size(); i++) {
      if (!cfg.processors.get(i).beforeCheck(cfg.processorConfigs.get(i)))
        return false;
    }
    if (!cfg.consumer.beforeCheck(cfg.consumerConfig))
      return false;
    return true;
  }

  private int runAfterChecks() {
    int code = 0;
    code = Math.max(code, cfg.producer.afterCheck(cfg.producerConfig));
    for (int i = 0; i < cfg.processors.size(); i++) {
      code = Math.max(code, cfg.processors.get(i).afterCheck(cfg.processorConfigs.get(i)));
    }
    code = Math.max(code, cfg.consumer.afterCheck(cfg.consumerConfig));
    for (int i = 0; i < cfg.afterCheckers.size(); i++) {
      code = Math.max(code, cfg.afterCheckers.get(i).afterCheck(cfg.afterCheckerConfigs.get(i)));
    }
    return code;
  }

  private void discardConsumerOutputs() {
    try {
      cfg.consumer.discardOutputs();
    } catch (Throwable t) {
      log.error("Failed to discard unpublished consumer outputs", t);
    }
  }

  private static void tryInvokeStartConsuming(ReactiveInterfaces.ReactiveConsumer<?> consumer) {
    consumer.startConsuming();
  }

  private static String formatDuration(Duration d) {
    long s = d.getSeconds();
    long abs = Math.abs(s);
    return String.format("%02d:%02d:%02d", abs / 3600, (abs % 3600) / 60, abs % 60);
  }

  private void negotiatePipeline(List<ReactiveInterfaces.ReactiveProcessor<?, ?>> activeProcessors,
      List<Map<String, Object>> activeConfigs) {
    negotiateChain(cfg.producer, activeProcessors, cfg.consumer);
  }

  /**
   * Walk the full module chain and notify each module of its negotiated
   * input/output type. Throws {@link IllegalStateException} if
   * adjacent modules have incompatible types, so misconfigured pipelines fail
   * fast at startup rather than with a ClassCastException at runtime.
   *
   * <p>Package-private for unit testing.
   */
  static void negotiateChain(
      ReactiveInterfaces.ReactiveProducer<?> producer,
      List<ReactiveInterfaces.ReactiveProcessor<?, ?>> processors,
      ReactiveInterfaces.ReactiveConsumer<?> consumer) {

    // Build full module list: producer + processors + consumer
    List<ReactiveInterfaces.ReactiveModule> chain = new java.util.ArrayList<>();
    chain.add(producer);
    chain.addAll(processors);
    chain.add(consumer);

    List<Map<String, Object>> configs = new java.util.ArrayList<>();
    for (int i = 0; i < chain.size(); i++) configs.add(Map.of());

    PipelineNegotiator.NegotiationResult result =
        PipelineNegotiator.negotiate(chain, configs);

    if (!result.isSuccess()) {
      String errors = String.join("; ", result.messages());
      throw new IllegalStateException("Pipeline type negotiation failed: " + errors);
    }
  }

  // --- Metrics Processor Wrapper ---
  private static class MetricCountingProcessor<T> implements Flow.Processor<T, T> {
    private final String ns;
    private final String name;
    private final Flow.Publisher<T> upstreamPublisher;
    private Flow.Subscriber<? super T> downstream;

    public MetricCountingProcessor(String ns, String name, Flow.Publisher<T> upstreamPublisher) {
      this.ns = ns;
      this.name = name;
      this.upstreamPublisher = upstreamPublisher;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super T> subscriber) {
      this.downstream = subscriber;
      if (upstreamPublisher != null)
        upstreamPublisher.subscribe(this);
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      if (downstream != null)
        downstream.onSubscribe(subscription);
    }

    @Override
    public void onNext(T item) {
      Metrics.inc(ns, name);
      if (downstream != null)
        downstream.onNext(item);
    }

    @Override
    public void onError(Throwable throwable) {
      if (downstream != null)
        downstream.onError(throwable);
    }

    @Override
    public void onComplete() {
      if (downstream != null)
        downstream.onComplete();
    }
  }

  /**
   * Prints a one-line configuration summary to stdout before the progress reporter
   * starts. Shown even when log output is suppressed so the operator always knows
   * what settings are active.
   *
   * Example: [config] cores:4  vthreads:50  maxheap:1G  output:daily
   */
  private void printConfigLine() {
    if (cfg.progressMode == pl.gov.nac.warc.config.ProgressMode.NONE) {
      return;
    }
    Runtime rt = Runtime.getRuntime();
    int cores = rt.availableProcessors();
    int vthreads = cfg.globalConcurrencyCap > 0 ? cfg.globalConcurrencyCap : cores;
    long maxHeapMB = rt.maxMemory() / (1024 * 1024);
    String heap = maxHeapMB >= 1024 ? (maxHeapMB / 1024) + "G" : maxHeapMB + "M";

    // Determine output mode: daily = YYYYMMDD.ext  vs  single = timestamped name
    String outputMode = "single";
    if (cfg.consumerConfig != null) {
      Object f = cfg.consumerConfig.get("file");
      if (f instanceof String path && !path.isEmpty()) {
        String basename = java.nio.file.Path.of(path).getFileName().toString();
        if (basename.matches("\\d{8}\\..*")) {
          outputMode = "daily";
        }
      }
    }

    java.io.PrintStream out = cfg.isJsonResult() ? System.err : System.out;
    out.printf("[config] cores:%d  vthreads:%d  maxheap:%s  output:%s%n",
        cores, vthreads, heap, outputMode);
  }

  public String failureCode() {
    return failureCode;
  }

  public String failureMessage() {
    return failureMessage;
  }

  private int fail(int exitCode, String errorCode, String message) {
    if (failureCode == null) {
      failureCode = errorCode;
      failureMessage = message;
    }
    return exitCode;
  }

  private static String messageOf(Throwable failure) {
    return failure.getMessage() == null || failure.getMessage().isBlank()
        ? failure.getClass().getSimpleName()
        : failure.getMessage();
  }

}
