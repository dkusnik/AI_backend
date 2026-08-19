# Shell Architecture

This document explains why each file in `lib/scripts/` exists, which files are
part of the operator runtime, and which files are scheduled to leave the
packaged runtime. It deliberately separates the measured baseline from the
approved end state: the layout tasks following this document change locations,
but not operator behavior.

## Baseline census

The baseline was measured on 2026-08-01 from parent commit
`0d659d9f005edf8619aa29e292c10dad71e9a248`. The S0-001 change adds only this
document, so its product sources are the same as the eventual documentation
commit. A normal assembly copies all seven source files below into
`out/app/lib/scripts/` unchanged.

Sizes are exact bytes from `stat`, not rounded estimates.

| file | bytes | runtime reachability | rationale and approved disposition |
|---|---:|---|---|
| `warc2wet.sh` | 8,284 | Invoked in the packaged runtime by the 149-byte top-level `warc2wet.sh` wrapper. Tests also invoke the implementation directly. | WARC-to-WET operator workflow. Promote its content to the top-level command and remove the hidden copy in S2-001. |
| `es-upsert.sh` | 6,895 | Invoked in the packaged runtime by the 150-byte top-level `es-upsert.sh` wrapper. Tests also invoke the implementation directly. | Elasticsearch-ingest and archive workflow. Promote its content to the top-level command and remove the hidden copy in S2-001. |
| `wet-merge.sh` | 4,142 | Invoked in the packaged runtime by the 150-byte top-level `wet-merge.sh` wrapper. The packaging guard also names the implementation. | WET-to-DOET merge workflow. Promote its content to the top-level command and remove the hidden copy in S2-001. |
| `runtime-lib.sh` | 2,906 | Sourced by all five operator implementations: `warc2wet.sh`, `es-upsert.sh`, `wet-merge.sh`, `es-delete.sh`, and `es-reinit.sh`. | Retain. It is the shared operator-workflow library for runtime layout, paths, profiles, stream names, identifiers, and `es-cli` invocation. |
| `pipeline-lib` | 6,251 | Sourced or resolved by `bin/warc-cli`, `bin/es-cli`, raw `pipeline`, and `warc2wet.sh`. | Retain. It is the shared JVM-invocation library for classpath/config/JVM option assembly and `run_pipeline`. S0-002 documents this boundary in both libraries. |
| `pipeline` | 149 | No command in `out/` invokes it. Thirteen test scripts invoke the copy in `target/dist/lib/scripts/`; `bin/test-cli` also parses raw-pipeline references for reporting. | It is a direct JVM test entrypoint, not an operator command. S1-002 excludes it from `out/`; S1-004 renames the target-only entrypoint to `pipeline-direct`. |
| `warc-validate-impl` | 2,990 | No invoker. Before this architecture record was added, the search outside build outputs found only its own header and the packaging guard's allowlist; later searches also find this document. | Delete in S1-001. Validation remains owned by `warc-cli validate` and the Java `warc-validate` pipeline. |

The three top-level wrappers are four lines each. Their only operation after
strict-mode setup and locating their directory is to `exec` the matching hidden
implementation. The other two operator commands already contain their complete
implementations at the top level: `es-delete.sh` is 5,758 bytes and
`es-reinit.sh` is 3,284 bytes.

### Raw-pipeline test consumers

The thirteen consumers are test infrastructure, not packaged-runtime callers:

- `testing/scripts/bench-optimization.sh`
- `testing/scripts/benchmark/throughput-001.sh`
- `testing/scripts/functional/config-nested-overrides-001.sh`
- `testing/scripts/functional/config-positional-args-001.sh`
- `testing/scripts/functional/config-validation-001.sh`
- `testing/scripts/functional/dry-run-inputs-outputs-001.sh`
- `testing/scripts/functional/dry-run-json-format-001.sh`
- `testing/scripts/smoke/dry-run-001.sh`
- `testing/scripts/smoke/help-flag-001.sh`
- `testing/scripts/smoke/invalid-pipeline-001.sh`
- `testing/scripts/smoke/silent-flag-001.sh`
- `testing/scripts/smoke/verbose-flag-001.sh`
- `testing/scripts/smoke/version-flag-001.sh`

They deliberately bypass the curated `warc-cli` command layer to test raw
pipeline names and module-level configuration overrides. That is a valid test
facility, but not a reason to expose it under `out/`.

## Approved end-state inventory

The following state is approved by S-D2, S-D4, S-D5, and S-D7. Those decisions
authorize only the stated packaged-layout and test-entrypoint changes. The five
top-level operator filenames, their accepted arguments, output, ordering, exit
status, and valid-installation runtime behavior remain unchanged.

### Operator package: `out/app/lib/scripts/`

| file | reason retained |
|---|---|
| `runtime-lib.sh` | Shared operator-workflow behavior used by the five top-level commands. |
| `pipeline-lib` | Shared JVM launch behavior used by the admin CLIs and `warc2wet.sh`. |

The full implementations of `warc2wet.sh`, `es-upsert.sh`, and `wet-merge.sh`
move to their existing top-level paths in `out/`. Their hidden copies disappear.
`warc-validate-impl` and raw `pipeline` also disappear from `out/app/lib/scripts/`.

### Build/test distribution: `target/dist/lib/scripts/`

| file | reason retained |
|---|---|
| `runtime-lib.sh` | Shared library staged for source/build-layout execution. |
| `pipeline-lib` | Shared JVM launch library. |
| `pipeline-direct` | Test-only direct JVM entrypoint, renamed from `pipeline`; explicitly excluded from `out/`. |

There is consequently one implementation per operator command and no
unsupported validator or raw launcher in the operator package. The two shared
libraries remain separate because they have different responsibilities; they
are not duplicate entrypoints.

## The Java utility package

`pl.gov.nac.warc.utils` contains the active general WARC codec, I/O,
Elasticsearch, query, buffer, and native-readability helpers. The former
one-letter-singular package and its unconfigured duplicate producer path were
removed in C3-002; they were residue from a parallel WARC implementation, not
an architectural boundary or a naming convention to restore.

## Reproducing the census

Run these commands from the product root (`warc2es-rc/`) and record changed
counts as findings:

```bash
stat -c '%n %s' src/main/dist/lib/scripts/*
stat -c '%n %s' \
  src/main/dist/{warc2wet.sh,es-upsert.sh,wet-merge.sh,es-delete.sh,es-reinit.sh}

rg -l 'lib/scripts/pipeline' src/main/dist/testing | LC_ALL=C sort

rg -n 'warc-validate-impl' . \
  --glob '!target/**' --glob '!out/**' \
  --glob '!src/main/dist/doc/ARCHITECTURE-SHELL.md'

for script in warc2wet.sh es-upsert.sh wet-merge.sh; do
  rg -n "(?:app/)?lib/scripts/$script" . \
    --glob '!target/**' --glob '!out/**' \
done
```
