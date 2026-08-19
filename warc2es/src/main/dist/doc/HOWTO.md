# WARC2ES HOWTO

End-user guide for building and operating `warc2es`.

## Table of Contents

- [1. What This Tool Does](#1-what-this-tool-does)
- [2. Installation](#2-installation)
- [3. First End-to-End Run](#3-first-end-to-end-run)
- [4. Text Extraction Guide (`warc-cli extract-text`)](#4-text-extraction-guide-warc-cli-extract-text)
- [5. DOET Merge Guide (`warc-cli merge`)](#5-doet-merge-guide-warc-cli-merge)
- [6. Elasticsearch Ingestion Guide (`es-cli`)](#6-elasticsearch-ingestion-guide-es-cli)
- [7. CLI Parameters (Practical Reference)](#7-cli-parameters-practical-reference)
- [8. YAML Configuration Summary](#8-yaml-configuration-summary)
- [9. Performance Profiles Summary](#9-performance-profiles-summary)
- [10. Recommended Operating Patterns](#10-recommended-operating-patterns)
- [11. Troubleshooting](#11-troubleshooting)
- [12. Command Cheat Sheet](#12-command-cheat-sheet)

## 1. What This Tool Does

`warc-cli` and `es-cli` form a pipeline for web archive processing:

1. Read WARC inputs.
2. Extract textual content into date-bucketed outputs.
3. Merge old and new crawl outputs with provenance metadata.
4. Load merged results into Elasticsearch.

Core files you produce:

- Per-crawl extracted files: `*.doet.gz`
- Merged baseline file: `baseline.wet.gz` (or `.doet.gz`, if you choose)
- Incremental file: `diff.doet.gz`

## 2. Installation

### 2.1 Prerequisites

- Linux/macOS shell
- Java 25+
- `bash`, `gzip`, `zcat`
- Optional but recommended: Docker for local Elasticsearch

Check Java:

```bash
java -version
```

### 2.2 Build Distribution

From `warc2es/`:

```bash
make
```

Direct CLI tools and test launcher:

- `target/dist/bin/warc-cli`
- `target/dist/bin/es-cli`
- `target/dist/bin/test-cli`

Operator runtime scripts:

- `out/warc2wet.sh`
- `out/wet-merge.sh`
- `out/es-upsert.sh`
- `out/es-delete.sh`
- `out/es-reinit.sh`

Re-running the operator `es-upsert.sh` transaction is safe because it refreshes
the exact stream, then deletes the provenance pair before loading. The refresh
is a pre-delete correctness barrier for previously accepted writes, not a
post-write search-visibility promise, and requires `maintenance` (or broader
`manage`) index privilege. This is different from a direct data-stream
`es-cli load-stream`: it uses create-by-ID, and a duplicate ID is a hard HTTP
409 conflict. Regular-index uploads use index-by-ID and overwrite the existing
document.

Native extraction library produced by packaging:

- `target/dist/native/libreadability_jni.so`
- `out/app/native/libreadability_jni.so`

If this library is missing or incompatible, extraction falls back to Java Readability4J.
To verify native loading:

```bash
warc-cli --verbose extract-text data/input.warc.gz --output-dir=work/out --output-prefix=check 2>&1 | grep -i readability
```

Optional `PATH` setup:

```bash
export PATH="$PWD/target/dist/bin:$PATH"
```

### 2.3 Start Local Elasticsearch (Optional)

```bash
docker compose -f elasticsearch-single/docker-compose.yml up -d
```

Check health:

```bash
curl -fsS http://localhost:9200/_cluster/health?pretty
es-cli check-health
```

Initialize a stream by its exact Elasticsearch name:

```bash
es-cli init nac-data-v1
```

## 3. First End-to-End Run

Prepare folders:

```bash
mkdir -p data work/crawl1 work/crawl2 work/merge
```

Extract two crawls (see Section 4.2 for all extraction options and defaults):

```bash
warc-cli extract-text data/crawl1.warc.gz \
  --output-dir=work/crawl1 \
  --output-prefix=crawl1

warc-cli extract-text data/crawl2.warc.gz \
  --output-dir=work/crawl2 \
  --output-prefix=crawl2
```

Merge baseline + new crawl:

```bash
warc-cli merge \
  --output-base=work/merge/baseline.wet.gz \
  --output-diff=work/merge/diff.doet.gz \
  work/crawl1 work/crawl2
```

Load to Elasticsearch:

```bash
es-cli check-health
es-cli init nac-data-v1
es-cli load work/merge/baseline.wet.gz
es-cli load work/merge/diff.doet.gz
```

Verify:

```bash
es-cli list-indices
es-cli search "*"
```

## 4. Text Extraction Guide (`warc-cli extract-text`)

Usage:

```bash
warc-cli extract-text <input...> [output] [options...]
```

### 4.1 Input modes

- Single file input
- Multiple files in one command
- Directory input (recursive file discovery)

Examples:

```bash
# single file
warc-cli extract-text data/crawl1.warc.gz --output-dir=work/crawl1 --output-prefix=crawl1

# multiple files
warc-cli extract-text data/p1.warc.gz data/p2.warc.gz --output-dir=work/crawl1 --output-prefix=crawl1

# directory
warc-cli extract-text data/crawl1/ --output-dir=work/crawl1 --output-prefix=crawl1
```

### 4.2 Important extraction options and defaults

Defaults are important:

- `--output-dir=<dir>` default: `./var/extracted-crawls`
- `--output-prefix=<name>` default: `extract`
- `--deduplicate-scope=<scope>` default: `sort-only` (no deduplication)

Common extraction options:

- `--deduplicate-scope=<sort-only|global|url|none>`
- `--processor.extract-text.extract-min-text-length=<N>`
- `--processor.extract-text.extract-title=<bool>`
- `--processor.lang-detect.lang-filter=<lang>`
- `--no-cdx-sidecar`

### 4.3 Output naming, WET vs DOET, and sidecars

Generated files are date-bucketed, for example:

- `crawl1-20251220.doet.gz`
- `crawl1-20251221.doet.gz`

Format note:

- WET (`.wet.gz`) is the plain extracted-text stream format.
- DOET (`.doet.gz`) is the deduplication-oriented output format produced by this pipeline.
- In this toolchain, `extract-text` writes `.doet.gz` outputs by default.

CDX sidecars:

- A `.cdxj` sidecar is created next to each output unless `--no-cdx-sidecar` is used.
- These sidecars are used for fast URL/dedup lookups in later merge operations.

### 4.4 Other `warc-cli` commands (quick overview)

- `warc-cli validate <file...>` validate decode/headers/magic.
- `warc-cli grep <input> <output> [options]` filter by MIME/URL/status/type.
- `warc-cli info <file>` print file statistics and integrity summary.
- `warc-cli regen-cdxj <file...>` regenerate CDX indexes.
- `warc-cli regen-zip <input> <output>` recompress archive output.
- `warc-cli regen-digests <input> <output>` recompute digests.
- `warc-cli dedupe <input> <output>` deduplicate existing WET/DOET files.
- `warc-cli extract-merge-baseline --output=<file> <input...>` extract and baseline-merge in one command.

## 5. DOET Merge Guide (`warc-cli merge`)

Usage:

```bash
warc-cli merge --output-base=<base> --output-diff=<diff> <baseline|folder> <scan|folder...> [options]
```

Important positional rule:

- The first positional input is always baseline (historical state).
- All following positional inputs are new scan inputs to compare against baseline.

Required options:

- `--output-base=<file>`
- `--output-diff=<file>` (required for `merge`; use `baseline` for single-output workflows)

### 5.1 Merge semantics

Baseline input is treated as known state. Scan input(s) are compared against it.

Key provenance values written to merged output:

- `base-only`
- `merged`
- `new`
- `uri-changed`
- `uri-reverted`

### 5.2 Deduplication scope in merge

- `global` (default): same content digest treated as same record regardless of URL
- `url`: same digest at different URLs kept as separate records

Example:

```bash
warc-cli merge \
  --output-base=work/merge/baseline.wet.gz \
  --output-diff=work/merge/diff.doet.gz \
  --deduplicate-scope=url \
  work/crawl1 work/crawl2
```

### 5.3 Baseline-only workflow

When diff output is not needed:

```bash
warc-cli baseline --output=work/merge/baseline.wet.gz work/crawl1 work/crawl2
```

Notes:

- `warc-cli baseline` examples use `.wet.gz` in CLI help; this guide follows that convention.
- `.doet.gz` also works in downstream flow if you keep naming consistent.
- If `warc-cli baseline` no-arg help text looks mixed with merge help in your shell, prefer this HOWTO syntax.

## 6. Elasticsearch Ingestion Guide (`es-cli`)

Core commands for extraction/merge ingestion:

- `es-cli check-health`
- `es-cli init [<exact-stream-name>]`
- `es-cli purge [<exact-stream-name>]`
- `es-cli load <file>`
- `es-cli load-index <file> <index>`
- `es-cli load-stream <file> <stream>`
- `es-cli search "<query>"`
- `es-cli list-indices`
- `es-cli batch-delete <target> <query_json> [url_params]`
  Delete documents matching the JSON query. Uses `conflicts=proceed` by default to handle version conflicts gracefully. An optional parameter (e.g. `?refresh=true` or `refresh=true`) can be passed to force an immediate search index refresh.

The low-level `init` and `purge` commands take exact Elasticsearch stream names
and never add a prefix. For example, use `nac-data-v1`, not `v1`. Omitting the
stream name from either command targets `nac-data-default`.

### 6.1 Typical ingestion sequence

```bash
es-cli check-health
es-cli init nac-data-v1
es-cli load work/merge/baseline.wet.gz
es-cli load work/merge/diff.doet.gz
es-cli search "*"
```

### 6.2 Full reload vs incremental load

- Full reload: load a complete baseline file.
- Incremental update: load only `diff.doet.gz` after each new crawl merge.

For full command coverage (maintenance, diagnostics, shard tools, URL discovery), run `es-cli --help` (or just `es-cli` in this build):

```bash
es-cli
```

## 7. CLI Parameters (Practical Reference)

### 7.1 Common `warc-cli` parameters

- `--profile=<light|light-optimized|light-parallel|parallel>`
- `--engine=<virtual|reactive>`
- `-t`, `--threads=<N>`
- `-r`, `--max-records=<N>`
- `-o`, `--output=<file>`
- `--no-cdx-sidecar`
- `--output-size-limit=<MB>`
- `-v`, `--verbose`
- `-s`, `--silent`

### 7.2 Parameter quick map by stage

Extraction quality control:

- `--processor.extract-text.extract-min-text-length`
- `--processor.extract-text.extract-title`
- `--processor.lang-detect.lang-filter`

Merge behavior:

- `--deduplicate-scope`
- `--output-base`
- `--output-diff`

Engine/backpressure tuning:

- `--threads`
- `--max-records` (queue capacity floor and parallel-GZIP worker cap)

### 7.3 Environment variables

| Variable | Accepted values | Effect |
|---|---|---|
| `WARC_CLI_PROFILE` | `light`, `light-optimized`, `light-parallel`, `parallel` | Default profile used by CLI wrappers when `--profile` is not provided. |
| `JAVA_OPTS` | Standard JVM args (e.g. `-Xms1g -Xmx2g -XX:+UseZGC`) | Overrides/extends JVM runtime flags for both `warc-cli` and `es-cli`. |
| `WARC_JFR_ENABLED` | `true` or unset | Enables Java Flight Recorder output when `true`. |
| `WARC_JFR_PATH` | Absolute or relative filesystem path | Target JFR file path when JFR is enabled. If unset, defaults under the active runtime distribution's `recordings/` directory. |

## 8. YAML Configuration Summary

Primary config file:

- `target/dist/conf/config.yaml`
- `out/app/conf/config.yaml`

Top-level sections:

- `logging`: verbosity, progress, final report behavior
- `global.jvm`: heap/cpu/gc defaults
- `global.engine`: concurrency and backpressure controls
- `global.args`: CLI flag mapping to config keys
- `profiles`: named overrides for `global.*`
- `pipelines`: command wiring and module overrides

### 8.1 Keys most users adjust

- `global.engine.concurrency`
- `global.engine.maxRecords`
- `global.engine.recordSizeThresholdMB`
- `logging.verbosity`

Note on naming:

- `--engine.maxRecords` and `--engine.recordSizeThresholdMB` are config-style override keys.
- They are not top-level short CLI flags like `-r` or `--size-threshold-mb`.

## 9. Performance Profiles Summary

Profiles are defined in `target/dist/conf/config.yaml` for direct CLI runs and
`out/app/conf/config.yaml` for operator runtime scripts. They are reflected in
`warc-cli` help.

| Profile | Heap | CPU Count | Concurrency | maxRecords | Typical Use |
|---|---:|---:|---:|---:|---|
| `light` (default) | 1g | 1 | 10 | 5 | safe default, low-resource environments |
| `light-optimized` | 1g | 3 | 50 | 5 | balanced local throughput |
| `light-parallel` | 1g | 4 | 25 | 10 | 1GB memory with higher core usage |
| `parallel` | 4g | 4 | 100 | 20 | higher-throughput dedicated hosts |

Profile selection:

```bash
warc-cli --profile=light-optimized extract-text ...
```

or:

```bash
export WARC_CLI_PROFILE=light-optimized
```

## 10. Recommended Operating Patterns

### 10.1 Multi-crawl incremental workflow (3-crawl example)

Step 1: initial crawl extract + baseline merge.

```bash
warc-cli extract-text data/crawl1.warc.gz --output-dir=work/crawl1 --output-prefix=crawl1
warc-cli baseline --output=work/merge/baseline-c1.wet.gz work/crawl1
es-cli load work/merge/baseline-c1.wet.gz
```

Step 2: second crawl extract + incremental merge.

```bash
warc-cli extract-text data/crawl2.warc.gz --output-dir=work/crawl2 --output-prefix=crawl2
warc-cli merge \
  --output-base=work/merge/baseline-c2.wet.gz \
  --output-diff=work/merge/diff-c2.doet.gz \
  work/merge/baseline-c1.wet.gz work/crawl2
es-cli load work/merge/diff-c2.doet.gz
```

Step 3: third crawl extract + incremental merge.

```bash
warc-cli extract-text data/crawl3.warc.gz --output-dir=work/crawl3 --output-prefix=crawl3
warc-cli merge \
  --output-base=work/merge/baseline-c3.wet.gz \
  --output-diff=work/merge/diff-c3.doet.gz \
  work/merge/baseline-c2.wet.gz work/crawl3
es-cli load work/merge/diff-c3.doet.gz
```

Single-command alternative for initial baseline:

```bash
warc-cli extract-merge-baseline --output=work/merge/baseline-c1.wet.gz data/crawl1.warc.gz
```

### 10.2 Data hygiene

- Keep each crawl in separate `work/crawl-<date>/` folder.
- Keep merge outputs versioned by date.
- Keep ingestion command logs for repeatability.

## 11. Troubleshooting

### 11.1 Elasticsearch unreachable

```bash
es-cli check-health
```

If failing:

- check `localhost:9200` reachability
- confirm container/service is running
- run `es-cli init <exact-stream-name>` after first cluster startup

### 11.2 Merge output empty or unexpectedly small

- verify extracted `*.doet.gz` files are non-empty
- run `warc-cli info` on source inputs
- retry without strict text/language filters

### 11.3 Lower-than-expected extraction volume

- reduce `--processor.extract-text.extract-min-text-length`
- remove `--processor.lang-detect.lang-filter`
- check whether `--deduplicate-scope` is `global`/`url` vs default `sort-only`

### 11.4 Resource pressure (memory or long runtime)

- switch to `--profile=light` for safer memory usage
- reduce `--threads`
- reduce `--max-records` to lower the queue floor and parallel-GZIP worker cap

## 12. Command Cheat Sheet

```bash
# Build
make

# Extract
warc-cli extract-text <input...> --output-dir=<dir> --output-prefix=<name>

# Dedupe existing extracted output
warc-cli dedupe <input.wet.gz> <deduped.wet.gz>

# Merge baseline + scans (diff required)
warc-cli merge --output-base=<base.wet.gz> --output-diff=<diff.doet.gz> <baseline> <scan...>

# Baseline only
warc-cli baseline --output=<baseline.wet.gz> <input...>

# Extract + merge baseline in one step
warc-cli extract-merge-baseline --output=<baseline.wet.gz> <input...>

# Validation, filtering, and info
warc-cli validate <file...>
warc-cli grep <input> <output> [options]
warc-cli info <file>

# Regeneration utilities
warc-cli regen-cdxj <file...>
warc-cli regen-zip <input> <output>
warc-cli regen-digests <input> <output>

# ES ingest/search
es-cli check-health
es-cli init <exact-stream-name>
es-cli load <file.doet.gz>
es-cli search "<query>"
es-cli list-indices
```
