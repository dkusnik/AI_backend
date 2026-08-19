# warc2es operator guide

This directory is a deployable `warc2es` Release 1 runtime. It converts WARC
archives to searchable text derivatives and publishes them to Elasticsearch.
Run commands from this directory and use the top-level scripts as the stable
operator interface.

## Requirements

- Linux on the same architecture as `app/native/libreadability_jni.so`
- JDK 25 available as `java`
- Elasticsearch 9.4.x
- `curl`, `gzip`, `jq`, `perl`, GNU core utilities, and util-linux `flock`
- ISA-L runtime library (`libisal2` on Debian/Ubuntu) for accelerated GZIP
- Poppler `pdftotext` for the default PDF extraction path
- Elasticsearch ICU analysis plugin and the configured Hunspell dictionaries

The application JARs, native library, scripts, Elasticsearch templates, and
runtime configuration are already included under `app/`.

## Configure Elasticsearch

The scripts read connection settings from the environment and then from
`.profile`:

| Variable | Meaning | Default |
|---|---|---|
| `ES_URL` | Elasticsearch REST endpoint | `http://localhost:9200` |
| `ES_USER` | Basic-auth user | `elastic` |
| `ES_PASS` | Basic-auth password | value of `ELASTIC_PASSWORD`, or empty |
| `ELASTIC_PASSWORD` | Deployment password alias | empty |

Release archives contain `.profile.example`, never a live `.profile`. Either
export connection settings in the operator session, inject them through the
deployment secret mechanism, or create a local profile before use:

```bash
cp .profile.example .profile
${EDITOR:-vi} .profile
```

Do not store a password in a transferred archive.

Check connectivity before conversion or ingestion:

```bash
./app/bin/es-cli check-health
```

The command succeeds for green or yellow cluster health and fails for red
health or a connection/authentication error.

## Directory layout

| Path | Contents |
|---|---|
| `in/` | Source WARC files |
| `wet/<url-id>/<crawl-id>/<source>.wet.gz` | Managed WET staging produced by `warc2wet.sh` |
| `doet/` | Optional merged/deduplicated DOET output |
| `all/wet/<url-id>/<crawl-id>/<sha256>.wet.gz` | Published WET sets available for replay |
| `out/` | Reserved operator output directory |
| `app/` | Runtime binaries, libraries, configuration, native code, and logs |
| `.profile` | Local Elasticsearch connection defaults; never put it in source control with secrets |

The small top-level launchers delegate to implementations in
`app/lib/scripts/`. Keep both locations together when transferring the runtime.
The lower-level CLIs remain in `app/bin/` by design.

`warc2wet.sh` and `es-upsert.sh` emit compact JSON results by default. Pass
`--result-format=human` explicitly when interactive progress output is wanted.

## Standard workflow

Pass one or more WARC files or directories explicitly:

```bash
./warc2wet.sh in/crawl.warc.gz \
  --url-id=example_org \
  --crawl-id=2026_07
```

Single-output mode writes:

```text
wet/example_org/2026_07/crawl.wet.gz
```

Publish that derivative to a data stream:

```bash
./es-reinit.sh --stream=release1 --yes  # once, when creating a new empty stream
./es-upsert.sh --url-id=example_org \
  --crawl-id=2026_07 \
  --stream=release1
```

Operator `--stream` options accept either shorthand such as `release1` or an
exact Elasticsearch name such as `nac-data-release1`. Low-level
`./app/bin/es-cli init` and `purge` commands accept exact names and never add a
prefix; with no stream argument, both target `nac-data-default`.

This targets `nac-data-release1`. With no positional path, `es-upsert.sh` loads
all files staged under the selected pair together with its published set. A
positional file or directory instead defines the complete replacement set. The
invocation validates every selected record, refreshes the exact stream so
previously accepted writes are visible to pair deletion, deletes the complete
provenance pair, and loads the transaction in one JVM. This pre-delete barrier
requires `maintenance` (or broader `manage`) index privilege; it does not make
the replacement writes immediately searchable. After successful Elasticsearch
processing, managed files below `wet/` move into the content-addressed
published path; external explicit inputs are copied there and retained.

To replay every published provenance set:

```bash
./es-upsert-all.sh --stream=release1
```

Direct `es-upsert.sh` emits one JSON operator result by default. Archive replay
emits NDJSON: one delegated result per provenance directory and one summary line
last.

## Optional per-day merge

Use per-day extraction when a crawl should be merged into consecutive date
groups before ingest:

```bash
./warc2wet.sh in/ --url-id=example_org --crawl-id=2026_07 --per-day
./wet-merge.sh wet/example_org/2026_07 doet/
```

`wet-merge.sh` considers only `YYYYMMDD.wet.gz` files. It creates one DOET file
per consecutive date range and uses a temporary RocksDB database for global
deduplication. That database is job scratch state and is removed after use.
DOET ingestion/publication is not part of the current operator workflow.

## Delete and restore

Delete one provenance set from Elasticsearch and then remove its exact
published-WET directory contents:

```bash
./es-delete.sh \
  --url-id=example_org \
  --crawl-id=2026_07 \
  --stream=release1
```

Preview the query and ordered cleanup candidates with `--dry-run`. It makes no
Elasticsearch calls and does not create lock files. Both identifiers are
required and empty sentinels are rejected. Whole-stream deletion is a separate,
explicit scope:

```bash
./es-delete.sh --stream=release1 --all-documents
```

`es-delete.sh` has no confirmation or `--yes` shortcut. Elasticsearch deletion
always happens first. If it fails, published WETs remain untouched; if an
individual WET removal fails, cleanup continues and the command returns a
nonzero `partial` result. Use `--result-format=json` for one compact operator
object with the exact target and cleanup counts.

Purge and recreate an entire stream:

```bash
./es-reinit.sh --stream=release1 --yes
```

This is destructive. It preserves the shared template and ILM policy, deletes
`nac-data-release1`, recreates it empty, preserves every published WET, and does
not replay them. It retains its `--yes`/TTY confirmation contract. Its
`--dry-run` also makes no Elasticsearch or filesystem changes. Do not run it
against a production stream during integration testing.

## Acceptance testing

Integration testing has four independent gates:

1. validate the fixture and runtime;
2. convert WARC to WET;
3. initialize an isolated stream and ingest the WET;
4. verify count, provenance fields, date mapping, and searchable content, then
   remove the isolated stream.

### Fixture contract

Fixtures are external test data and are not shipped in this runtime. Keep each
fixture with a small manifest containing:

- filename, byte size, and SHA-256 checksum;
- source and permission to use it;
- WARC version and expected record count;
- one stable search token and expected minimum hit count;
- expected extracted/indexed document count;
- expected `nac-url-id`, `nac-crawl-id`, and date range.

A useful smoke fixture is a small concatenated-GZIP WARC containing at least
one HTML response with a unique token, `WARC-Target-URI`, `WARC-Date`, and valid
payload/block metadata. A full acceptance fixture should also exercise
representative PDFs, character encodings, languages, malformed pages, and
duplicate payloads.
Do not use a zero-byte or arbitrary gzip file as a positive fixture.

The acceptance corpus uses two external Plock crawls:

| Crawl | Source filename | Size | Expected indexed documents |
|---|---|---:|---:|
| December | `plock.ap.gov.pl.warc.gz` | 1,775,273,688 bytes | 290 |
| January | `plock.ap.gov.pl-2026-01-30-ace2d026-00000.warc.gz` | 1,775,610,652 bytes | 300 |

These counts are fixture assertions, not general WARC-to-document ratios:
non-response records and content rejected by extraction filters do not become
search documents. Verify fixture checksums from the corpus manifest before
using these numbers.

### Test against Elasticsearch on 127.0.0.1

Use a unique stream ID and fixture provenance. The following example assumes a
fixture with the stable search token `soczewka`:

```bash
export ES_URL=http://127.0.0.1:9200
export ES_USER=elastic
# Export ES_PASS only when the local cluster requires authentication.

STREAM_ID=test_release1_it_local
URL_ID=integration_fixture
CRAWL_ID=release1_local

./app/bin/es-cli check-health
./es-reinit.sh --stream="$STREAM_ID" --yes
./warc2wet.sh in/fixture.warc.gz \
  --url-id="$URL_ID" \
  --crawl-id="$CRAWL_ID"

WET_FILE="wet/${URL_ID}/${CRAWL_ID}/fixture.wet.gz"
test -n "$WET_FILE" && test -f "$WET_FILE"

./es-upsert.sh --url-id="$URL_ID" --crawl-id="$CRAWL_ID" \
  --stream="$STREAM_ID"
./app/bin/es-cli refresh "nac-data-$STREAM_ID"
./app/bin/es-cli get-stream "nac-data-$STREAM_ID"
./app/bin/es-cli search soczewka --stream="nac-data-$STREAM_ID"
```

Inspect at least one document and the effective mapping:

```bash
AUTH=()
test -z "${ES_PASS:-}" || AUTH=(-u "${ES_USER:-elastic}:$ES_PASS")
curl -fsS "${AUTH[@]}" \
  "$ES_URL/nac-data-$STREAM_ID/_count?pretty"
curl -fsS "${AUTH[@]}" \
  "$ES_URL/nac-data-$STREAM_ID/_mapping?pretty"
curl -fsS "${AUTH[@]}" \
  "$ES_URL/nac-data-$STREAM_ID/_search?size=1&pretty"
```

Confirm that the count matches the fixture manifest, the document has the
expected `nac-url-id` and `nac-crawl-id`, `warc-date` parses as a date, and the
known token is searchable. Then remove the isolated stream:

```bash
./app/bin/es-cli delete-stream "nac-data-$STREAM_ID"
```

### Test against a Puppet-managed environment

Run from a host that can resolve and reach the Puppet inventory names. The
dedicated ingest endpoint is exposed by the front-management node; the current
inventory name is `ai-prd-frntdb04`:

```bash
export ES_URL=http://ai-prd-frntdb04:9200
export ES_USER=elastic
read -rsp 'Elasticsearch password: ' ELASTIC_PASSWORD
export ELASTIC_PASSWORD
printf '\n'

./app/bin/es-cli check-health
AUTH=(-u "${ES_USER}:${ELASTIC_PASSWORD}")
curl -fsS "${AUTH[@]}" "$ES_URL?filter_path=version.number,cluster_name&pretty"
```

Before ingest, verify that the reported cluster version is 9.4.x and that
health is green or yellow. Release 1 does not claim compatibility with an older
Puppet Elasticsearch image. Obtain the password through the deployment secret
mechanism; do not copy it into `.profile`, shell history, fixture manifests, or
test reports.

Use a collision-resistant non-production stream, for example:

```bash
STREAM_ID="test_release1_it_$(date +%Y%m%d_%H%M%S)"
URL_ID=integration_fixture
CRAWL_ID=release1_puppet

./es-reinit.sh --stream="$STREAM_ID" --yes
./warc2wet.sh in/fixture.warc.gz \
  --url-id="$URL_ID" \
  --crawl-id="$CRAWL_ID"
WET_FILE="wet/${URL_ID}/${CRAWL_ID}/fixture.wet.gz"
test -n "$WET_FILE" && test -f "$WET_FILE"
./es-upsert.sh --url-id="$URL_ID" --crawl-id="$CRAWL_ID" \
  --stream="$STREAM_ID"
./app/bin/es-cli refresh "nac-data-$STREAM_ID"
./app/bin/es-cli search soczewka --stream="nac-data-$STREAM_ID"
```

Perform the same count, mapping, provenance, date, and content assertions as in
the localhost procedure. Always clean up the exact stream created by this run,
even after a failed assertion:

```bash
./app/bin/es-cli delete-stream "nac-data-$STREAM_ID"
```

Never use `default`, a release stream, an alias, or `nac-data-*` as the test
target. Never use `es-reinit.sh` merely to clean selected production documents.

### Search-client acceptance

The deployed runtime can verify Elasticsearch through `es-cli search`. The
consumer-facing acceptance suite belongs to the separately deployed search
client. Point that suite at the isolated stream and verify:

- health and authentication;
- exact total and pagination behavior;
- full-text and phrase search;
- filters for domain, language, extension, date, URL ID, and crawl ID;
- ascending and descending sort on `warc-date`;
- JSON result fields and schema names.

Do not declare the ingest integration complete from document count alone.

## Current Elasticsearch schema

The shipped index template applies to the `nac-data` data stream and streams
matching `nac-data-*`. Its principal fields are:

| Field | Type | Purpose |
|---|---|---|
| `warc-id` | keyword | WARC record ID |
| `warc-uri` | keyword | Original target URI |
| `warc-date` | date | Capture timestamp |
| `warc-digest` | keyword | Stable content/document digest |
| `wet-lang` | keyword | Detected language |
| `nac-url-id` | keyword | Normalized source identifier |
| `nac-crawl-id` | keyword | Normalized crawl identifier |
| `nac-first-seen`, `nac-last-seen` | date | Merge history bounds |
| `nac-missing-count`, `nac-revisit-count`, `nac-chain-length` | integer | Merge/revisit counters |
| `nac-status`, `nac-merge-result`, `merge-provenance` | keyword | Merge state and provenance |
| `nac-deduplicated`, `nac-primary-uri`, `nac-previous-uri` | keyword | Deduplication/URI chain metadata |
| `metadata.source_file`, `metadata.session_id`, `metadata.ingest_operator`, `metadata.gen` | keyword | Ingest metadata |
| `content` | text | Searchable extracted text |

`content` uses the `multi_lang_search` analyzer and has language-specific
subfields for Polish, Lithuanian, Belarusian, Ukrainian, English, German,
Russian, and French. The schema authority is
`app/conf/elasticsearch/templates/nac-data-template.json`. Integration clients
must use `warc-date`; there is no supported `date` alias.

## Supported inputs and operational notes

- The top-level extraction workflow accepts case-sensitive `.warc` and `.warc.gz`.
  Concatenated-GZIP `.warc.gz` is the interoperability baseline.
- Input directories are scanned recursively in bytewise relative-path order.
- Relative input paths resolve from the caller's working directory.
- `warc2wet.sh` requires both identifiers and preserves values matching
  `[A-Za-z0-9._-]{1,128}` exactly.
- Extraction paths are deterministic under `wet/<url-id>/<crawl-id>/`; source
  WARC files are retained.
- `es-upsert.sh` accepts one explicit `.wet.gz` file or directory and requires
  the same identifier pair. Normal directories must not be empty. The retired
  `--all` mode has no compatibility fallback; use `es-upsert-all.sh`.
- Pair mutation is fail-fast under same-host advisory locks. Elasticsearch and
  published-WET updates are recoverable by retry but are not one atomic commit.
- Archive replay validates the filename digest and record provenance before
  Elasticsearch mutation and never rewrites anything below `all/wet/`.
- WET and DOET are derived data. Preserve the source WARC and the fixture or
  ingest manifest independently.
