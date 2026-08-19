# Performance Architecture

This document separates reproduced measurements from implementation facts and
from historical tuning claims. It is not a promise that one profile or one
throughput figure applies to another host, filesystem, corpus, or pipeline.

## Evidence policy

The only measurement sources used here are the tracked S0-005 artifacts:

- **Results:** `warc2es-plans/baseline/performance-results.md`
- **JFR:** `warc2es-plans/baseline/performance-jfr-summary.txt`

The results artifact contains the repeatable benchmark command, environment,
sample distribution, and record-parity check. The JFR artifact contains a
separate representative extraction and textual JDK profile. Source inspection
is used to describe mechanisms, never to manufacture performance numbers.

Older session figures, YAML comments, and untracked recordings are history, not
evidence. They must be reproduced under the discipline below before they become
recommendations.

## Reproduced baseline

S0-005 measured B1 text extraction with global deduplication on tmpfs. The
accepted run used the Płock input with ISA-L gzip decompression, gzip DOET
output, a pinned one-gigabyte heap, one active processor, and 21 samples.

| samples | minimum | mean | maximum | population standard deviation | output parity |
|---:|---:|---:|---:|---:|---:|
| 21 | 185.35 MB/s | 206.02 MB/s | 271.43 MB/s | 16.03 MB/s | 290 records |

Source: **Results**, sections “Run metadata” and “Result”. The 86.08 MB/s
minimum-to-maximum spread is 41.8% of the mean, so this baseline cannot support
small optimization claims. The population standard deviation is 7.8% of the
mean; use that run-derived value as a screening floor, not the old hard-coded
noise threshold. A candidate still needs another complete, matched run rather
than one favorable sample. These derived percentages use only the distribution
published in **Results**.

The companion JFR extraction produced the same 290-record parity result at
256.31 MB/s over 6.825 seconds and reported a 994.0 MB peak. It is one profile,
not an additional benchmark distribution. Source: **JFR**, “Result”.

No Java method dominated the sampled profile: the largest reproduced hot-method
row was 3.87%. Allocation samples were dominated by `byte[]` at 78.13%, followed
by `String` at 10.73%. The recording contained no CPU-time samples, so its
CPU-time view is empty and cannot be treated as a second CPU profile. Source:
**JFR**, “Hot methods”, “Allocation pressure”, and “Event counts”. This is a
flat sampled profile with concentrated allocation types; the next gain is more
likely to require a structural reduction in copying or materialization than a
micro-optimization of one sampled method.

## Which controls reach the running engine

The public tuning surface and the implementation do not line up uniformly:

- `engine.concurrency` is live. `VirtualThreadEngine` uses it as the dispatcher
  worker count.
- `engine.maxRecords` is live. It contributes to `ArrayBlockingQueue` capacity
  and caps worker count when the optional parallel-gzip path is active.

The accepted S0-005 run pinned the JVM envelope but did not sweep these controls,
so it supplies no causal throughput result for changing them. In particular,
the repository's thread/core annotations and earlier M3–M9 comparisons have no
tracked supporting artifact. Historical claims about scaling across core counts
must therefore be rerun; they are not copied here as findings. The shipped
parallel-oriented profiles also request a different processor envelope, but
the operator `--profile` path does not currently apply the embedded YAML profile
blocks. Until that configuration path is wired and measured, profile comments
are intent rather than reproduced tuning guidance.

## Fixed per-record and materialization costs

Several implementation costs scale with record count, not just input bytes:

- For each selected PDF, the `pdftotext` path starts a child process and drains
  its output. Process lifetime is bounded and concurrency is semaphore-limited,
  but process creation and pipe/file handling remain fixed per-PDF work. The JFR
  artifact does not isolate a `pdftotext` CPU share, so none is claimed.
- A native-readability extraction creates a confined foreign-memory arena and
  crosses the FFI boundary for its result. The JFR artifact samples foreign
  downcalls but does not attribute a complete cost to readability, so it does
  not justify a percentage claim.
- `PooledBuffer` and `BufferPool` exist, but the pooled array remains publicly
  mutable and the pool cap uses a separate check followed by insertion. Their
  presence is not proof that allocation is bounded or reuse is reliable.
- The working record model is a mutable `RecordWarcUniversal` holding complete
  payload bytes on heap. The allocation profile's `byte[]` and `String`
  concentration is consistent with this shape, but does not assign all those
  allocations to one class. Source: **JFR**, “Allocation pressure”.
- Non-merge deduplication writes records to RocksDB, then reconstructs every
  unique record into a heap `HashMap`, copies the values into a list, and sorts
  that list before emission. That second materialization weakens the memory
  advantage expected from an external store. The reported 994.0 MB peak is a
  whole-pipeline observation and must not be attributed solely to this step.
  Source for the peak: **JFR**, “Result”.

These are architectural pressure points, not pre-approved rewrite tasks. A
change needs an isolated hypothesis, record-parity checks, and a matched
measurement before it earns a performance claim.

## I/O strategy is deployment-specific

The tracked baseline used resident tmpfs input and output on a verified `tmpfs`
mount. Source: **Results**, “Run metadata” and “Command”. It contains no HDD,
network filesystem, or ordinary disk comparison. It therefore supports neither
an HDD/tmpfs multiplier nor a general label of “I/O-bound” or “CPU-bound”.

No current profile selects an I/O strategy; profiles choose JVM and engine
resources. The appropriate bottleneck depends on storage latency, page-cache
state, compression ratio, decompressor, output codec, record mix, and extraction
tools. Measure those properties on the deployment being tuned. A `dd` run from
`/dev/zero` is not a substitute for reading and writing the actual corpus,
especially on a compressed or sparse filesystem.

## Compression: serial default, optional parallel path

The shipped base configuration leaves `parallelGzip` disabled. In that default
path, `ConsumerWarcBase` writes gzip through `GZIPOutputStream`; the tracked JFR
run explicitly used this serial output path. Source: **JFR**, final paragraph.

An optional parallel implementation exists. When explicitly enabled,
`VirtualThreadEngine` installs `WarcGzipCompressor`, worker threads produce
`RecordCompressed`, and the consumer writes those compressed members. The
parallel-oriented YAML profiles request it, but the operator profile selector
does not yet activate those embedded blocks. S0-005 did not execute or compare
the parallel path, so this document makes no speedup, CPU-share, or reliability
claim for it.

Published WET remains gzip-only under the current product contract. A different
codec is a contract decision as well as a benchmark variable; it is not a
performance-only substitution.

## Measurement and acceptance discipline

Use all of the following gates for a performance change:

1. Pin the product commit, Java version, heap, active processor count, input
   bytes and SHA-256, codec, filesystem type, and exact command.
2. Use the staged benchmark runner and record its effective sample count. The
   accepted baseline used 21 samples. Source: **Results**, “Run metadata”.
3. Report minimum, mean, maximum, and population standard deviation. For this
   baseline, one standard deviation is 16.03 MB/s, or 7.8% of the mean; treat a
   smaller apparent gain as noise until a matched repeated run proves otherwise.
   Source and derivation: **Results**, “Result”.
4. Require record parity as a co-equal gate. The B1 corpus expectation in the
   tracked run is 290 records. Source: **Results**, “Result”. A faster run with
   fewer records is a correctness failure, not an optimization.
5. Keep benchmark failure accounting enabled. C0-005 removed the old behavior
   that suppressed exit-code counters in benchmark mode, and its regression
   test now protects the current behavior.
6. Change one hypothesis at a time, retain raw summaries, and reject runs with
   a different resource envelope or overlapping workload.

A2-010 separately classified the extraction rebaseline as intentional
screen-reader-fragment normalization and retained sort-only parity. Those counts
are not part of the S0-005 performance artifacts, so they are not reused as this
benchmark's parity value.

## Rejected measurements and unverified history

One rejected run is preserved in **Results**, “Discarded pre-run”. Its 21
samples reported 344.78/378.12/419.97 MB/s, but it inherited a two-gigabyte heap
and ran under unacceptable memory pressure on a no-swap host. It is useful as an
example of rejection discipline, not as a competing baseline.

The following remain unverified leads because no surviving artifact reproduces
them:

- earlier headline throughput and JFR percentage claims;
- M3–M9 thread, queue, and memory-budget comparisons;
- core-scaling claims and profile comments that quote “optimal” settings;
- HDD/tmpfs multipliers;
- the YAML comment describing a rejected parallel-gzip experiment;
- CPU-share claims for `pdftotext`, native readability, or compression.

Do not average, compare, or use those claims to choose defaults. Recover their
artifacts or rerun them with the measurement gates above.

## Reproducing the evidence

Use the complete command and environment recorded in **Results**. The runnable
script is the assembled
`target/dist/testing/scripts/integration/bench-optimization.sh`; the source-tree
copy does not resolve the assembled distribution by itself. The tracked JFR
artifact records the separate profiling command and the exact `jfr view`
commands. The binary JFR file is intentionally not part of the repository.
