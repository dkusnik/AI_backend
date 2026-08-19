# Java Architecture

This document records the Java structure that exists today and three directions
for later refactoring. It is descriptive: it does not authorize runtime,
configuration, CLI, format, or packaged-layout changes.

## Runtime structure

`Pipeline` is the sole Java entrypoint. It asks `Config.load()` to turn the YAML
definition plus command-line overrides into a `LoadedConfig`, rejects the
removed `reactive` engine choice, and runs `VirtualThreadEngine`.

The configured data path is:

```text
producer -> zero or more processors -> consumer
```

Modules are selected from YAML and instantiated reflectively. Before execution,
`PipelineNegotiator` checks adjacent record types. `VirtualThreadEngine` then
wires the modules around an `ArrayBlockingQueue`, dispatches work on virtual
threads, serializes consumer delivery, and owns terminal signalling, metrics,
and reporting. This is the running architecture and the only pipeline-level
backpressure mechanism.

## Source census

The census was refreshed on 2026-08-02 after the C3 removals. Counts include
comments and blank lines because they come directly from `wc -l`.

| package | files | lines | current role |
|---|---:|---:|---|
| `processors` | 9 | 4,539 | Pipeline transforms and accumulators; the largest and least uniform package. |
| `config` | 16 | 3,079 | YAML loading, CLI parsing, three-tier overrides, validation, negotiation, and reflective module instantiation. |
| `utils` | 7 | 2,779 | Active WARC codec and I/O, Elasticsearch HTTP, query parsing, buffers, and native readability. |
| `reactive` | 8 | 2,178 | The virtual-thread engine, metrics, reporting, and live subscriber adapters. |
| `producers` | 4 | 2,069 | Archive and CDX input modules. |
| `consumers` | 4 | 1,980 | WARC-family output and Elasticsearch export modules. |
| `records` + `records/*` | 18 | 1,510 | Record interfaces and the WARC, CDX, and file-backed record hierarchy. |
| `checkers` | 5 | 627 | Pipeline pre- and post-validation modules. |
| `utils/gzip` | 5 | 553 | Gzip implementation selection and ISA-L acceleration. |
| package root | 1 | 141 | `Pipeline`, the process entrypoint. |
| **total** | **77** | **19,455** | Compared with 40 test files / 8,500 lines, a test-to-main line ratio of 0.44. |

The baseline `util` / `utils` one-letter split was not an intentional boundary.
The singular package belonged only to an unconfigured duplicate producer path;
C3-002 removed that path. Active producers use `utils/WarcCodec`. The old split
was residue from two WARC implementations, not a package pattern to restore.

### Concentration

Five files contain 5,370 lines: 28% of all main Java lines in 6% of the files.

| file | lines |
|---|---:|
| `processors/WarcAccumulatorDeduplicateDoet.java` | 1,367 |
| `producers/ChunkedArchiveExtractor.java` | 1,097 |
| `consumers/ConsumerWarcBase.java` | 1,043 |
| `processors/WarcDecoratorTextExtract.java` | 936 |
| `processors/WarcFilter.java` | 927 |

## Refactoring directions

These are themes and end states, not an implementation queue.

### 1. One reactive engine and one backpressure mechanism

`VirtualThreadEngine` owns a bounded `ArrayBlockingQueue`. The queue capacity is
derived from `maxRecords`; `queue.put()` blocks its producer when full, while
worker consumption refills upstream Flow demand. `CompositeSubscriber` adds
only completion, memory-tracking, and error-propagation behavior.

C3-005 removed the unwired token/semaphore design and its unused
`CompositeSubscriber` hook. Live queueing, serialization, terminal signalling,
metrics, and reporting remain. Semaphores local to Elasticsearch batch limits,
external-process limits, or producer demand are independent resource controls,
not a second pipeline backpressure architecture.

### 2. Split the large modules at pure-logic seams first

The five concentrated files mix unrelated concerns:

- `WarcAccumulatorDeduplicateDoet` combines configuration and RocksDB lifecycle,
  record adaptation, key/value encoding, calendar grouping, provenance state,
  and emission.
- `ChunkedArchiveExtractor` combines input discovery, index pairing, chunk
  planning, codec reads, ordered merge, and batch construction.
- `ConsumerWarcBase` combines output naming and containment, rotation,
  compression, WARC/CDXJ writing, provenance routing, and order validation.
- `WarcDecoratorTextExtract` combines format dispatch, native readability,
  Tika, external `pdftotext` process control, and text normalization.
- `WarcFilter` combines configuration parsing, header interpretation, predicate
  construction, and filtering.

The end state is small, deterministic logic with focused unit tests around the
stateful orchestration. Extract pure seams first: calendar calculations,
deduplication key/value encoding, and CDXJ serialization are safer starting
points than moving concurrent state machines. Stateful extraction should follow
only after the existing behavior is characterized, so concurrency defects are
not merely distributed across more classes.

### 3. Make declared contracts enforceable

The record hierarchy and configuration loader both state guarantees that the
compiler cannot currently check.

`Record` is sealed only at its first level: `RecordInMemory` and
`RecordExternal` are non-sealed, as is `RecordWarcUniversal` farther down the
hierarchy. Production dispatch contains more than fifty record-type `instanceof` checks and
no type-pattern switch. `RecordWet`, `RecordFileCdx`, and `RecordWarcInFile` are
advertised by type negotiation but are not constructed by production code;
`RecordFileDow` is permitted and documented but is not advertised or
constructed.

At the same time, the two-argument `Config.load()` is 418 lines and reads or
writes the global `CliOverrides` map at 21 sites. Its precedence contract is
encoded in statement order rather than in an explicit sequence of phases.

The end state is honest and checkable. Either close the record hierarchy and
use exhaustive dispatch, or remove the `sealed` claim and model supported
negotiated types directly. Express configuration loading as explicit ordered
phases whose precedence can be tested as data. Existing characterization tests
for `ArgParser`, `OverrideResolver`, and `ConfigResolver` are the prerequisite
safety net; this direction does not change those contracts.

## Reproducing the census

Run from the product root:

```bash
find src/main/java/pl/gov/nac/warc -name '*.java' -print0 \
  | sort -z \
  | xargs -0 wc -l

find src/test/java -name '*.java' -print0 \
  | sort -z \
  | xargs -0 wc -l
```

The package rows are derived from the first file list by grouping the path
component immediately below `pl/gov/nac/warc`, with `records/*` combined and
`utils/gzip` reported separately. A changed count is evidence that this record
must be updated, not a reason to preserve the table unchanged.
