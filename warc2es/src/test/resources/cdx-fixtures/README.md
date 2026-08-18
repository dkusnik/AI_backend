# CDX Extractor Fixtures

This directory contains reproducible fixtures for `CdxExtractor` tests.

- `sample-jwarc.warc`: deterministic WARC with 2 response records.
- `sample-expected-structured.tsv`: golden expectations used by tests.
- `sample-jwarc.warc.sha256`: checksum for fixture integrity.
- `generate-jwarc-fixture.sh`: regenerates fixture and checksum using `jwarc`.

Regeneration:

```bash
src/test/resources/cdx-fixtures/generate-jwarc-fixture.sh
```

Notes:

- The fixture is generated with fixed UUIDs and timestamps.
- Golden expectations focus on stable semantics (URL, SURT, status, timestamp).
- Offsets/lengths are asserted in tests as structural invariants (monotonic and positive).
