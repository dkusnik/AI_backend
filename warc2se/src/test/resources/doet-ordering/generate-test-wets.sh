#!/bin/bash
# Generate synthetic WET files for DOET ordering tests
# Tests K-Way merge, provenance tracking, and ordering validation

set -e

OUTPUT_DIR="."
echo "=== Generating Synthetic WET Test Files ==="
echo "Output directory: $OUTPUT_DIR"

# Helper: Create WET record
create_wet_record() {
    local digest="$1"
    local uri="$2"
    local date="$3"
    local content="$4"
    local content_length="${#content}"

    cat <<EOF
WARC/1.0
WARC-Type: conversion
WARC-Date: $date
WARC-Record-ID: <urn:uuid:$(uuidgen)>
WARC-Target-URI: $uri
Content-Type: text/plain; charset=utf-8
Content-Length: $content_length
WARC-Block-Digest: $digest

$content

EOF
}

# Helper: Create warcinfo record
create_warcinfo() {
    local date="$1"
    local filename="$2"

    cat <<EOF
WARC/1.0
WARC-Type: warcinfo
WARC-Date: $date
WARC-Record-ID: <urn:uuid:$(uuidgen)>
Content-Type: application/warc-fields
Content-Length: 100

software: warc-cli test-generator
format: WET
filename: $filename


EOF
}

# ============================================================================
# FILE 1: baseline-2026-01-15.wet (Baseline with 3 records)
# ============================================================================
echo "Generating File 1: baseline-2026-01-15.wet"
{
    create_warcinfo "2026-01-15T09:00:00Z" "baseline-2026-01-15.wet"

    # Record 1: Content A (digest aaa...)
    create_wet_record \
        "sha256:aaa1111111111111111111111111111111111111111111111111111111111111" \
        "http://test.gov.pl/content-a" \
        "2026-01-15T10:00:00Z" \
        "Content A - This is the first baseline content with some text for testing."

    # Record 2: Content B (digest bbb...)
    create_wet_record \
        "sha256:bbb2222222222222222222222222222222222222222222222222222222222222" \
        "http://test.gov.pl/content-b" \
        "2026-01-15T10:05:00Z" \
        "Content B - Second baseline record with different digest for merge testing."

    # Record 3: Content C (digest ccc...)
    create_wet_record \
        "sha256:ccc3333333333333333333333333333333333333333333333333333333333333" \
        "http://test.gov.pl/content-c" \
        "2026-01-15T10:10:00Z" \
        "Content C - Third baseline content to validate multi-record ordering in output."

} > "$OUTPUT_DIR/baseline-2026-01-15.wet"

# ============================================================================
# FILE 2: scan-2026-01-20.wet (Refresh + New Content)
# ============================================================================
echo "Generating File 2: scan-2026-01-20.wet"
{
    create_warcinfo "2026-01-20T09:00:00Z" "scan-2026-01-20.wet"

    # Record 1: Content A refreshed (same digest, same URI, newer date)
    create_wet_record \
        "sha256:aaa1111111111111111111111111111111111111111111111111111111111111" \
        "http://test.gov.pl/content-a" \
        "2026-01-20T11:00:00Z" \
        "Content A - This is the first baseline content with some text for testing."

    # Record 2: Content D (new digest)
    create_wet_record \
        "sha256:ddd4444444444444444444444444444444444444444444444444444444444444" \
        "http://test.gov.pl/content-d" \
        "2026-01-20T11:05:00Z" \
        "Content D - New content discovered in second scan, should get new-content provenance."

} > "$OUTPUT_DIR/scan-2026-01-20.wet"

# ============================================================================
# FILE 3: scan-2026-01-25.wet (URI Change + New Content)
# ============================================================================
echo "Generating File 3: scan-2026-01-25.wet"
{
    create_warcinfo "2026-01-25T09:00:00Z" "scan-2026-01-25.wet"

    # Record 1: Content A with URI change (same digest, different URI)
    create_wet_record \
        "sha256:aaa1111111111111111111111111111111111111111111111111111111111111" \
        "http://test.gov.pl/archive/content-a" \
        "2026-01-25T12:00:00Z" \
        "Content A - This is the first baseline content with some text for testing."

    # Record 2: Content E (new)
    create_wet_record \
        "sha256:eee5555555555555555555555555555555555555555555555555555555555555" \
        "http://test.gov.pl/content-e" \
        "2026-01-25T12:05:00Z" \
        "Content E - Additional new content for third scan file testing merge capabilities."

} > "$OUTPUT_DIR/scan-2026-01-25.wet"

# ============================================================================
# FILE 4: scan-2026-01-30.wet (Refresh + New Content)
# ============================================================================
echo "Generating File 4: scan-2026-01-30.wet"
{
    create_warcinfo "2026-01-30T09:00:00Z" "scan-2026-01-30.wet"

    # Record 1: Content B refreshed (same digest, same URI)
    create_wet_record \
        "sha256:bbb2222222222222222222222222222222222222222222222222222222222222" \
        "http://test.gov.pl/content-b" \
        "2026-01-30T13:00:00Z" \
        "Content B - Second baseline record with different digest for merge testing."

    # Record 2: Content F (new)
    create_wet_record \
        "sha256:fff6666666666666666666666666666666666666666666666666666666666666" \
        "http://test.gov.pl/content-f" \
        "2026-01-30T13:05:00Z" \
        "Content F - Final new content record to complete the test dataset for validation."

} > "$OUTPUT_DIR/scan-2026-01-30.wet"

# ============================================================================
# FILE 5: out-of-order.wet (Intentionally broken for error testing)
# ============================================================================
echo "Generating File 5: out-of-order.wet (INVALID - for error testing)"
{
    create_warcinfo "2026-02-01T09:00:00Z" "out-of-order.wet"

    # Record 1: Digest zzz (late in alphabet)
    create_wet_record \
        "sha256:zzz9999999999999999999999999999999999999999999999999999999999999" \
        "http://test.gov.pl/content-z" \
        "2026-02-01T14:00:00Z" \
        "Content Z - This record appears first but has a late digest."

    # Record 2: Digest aaa (early in alphabet) - ORDERING VIOLATION!
    create_wet_record \
        "sha256:aaa1111111111111111111111111111111111111111111111111111111111111" \
        "http://test.gov.pl/content-a-late" \
        "2026-02-01T14:05:00Z" \
        "Content A - This is the first baseline content with some text for testing."

} > "$OUTPUT_DIR/out-of-order.wet"

# ============================================================================
# Generate README with expected results
# ============================================================================
cat > "$OUTPUT_DIR/README.md" <<'EOFREADME'
# DOET Ordering Test Data

## Generated Files

1. **baseline-2026-01-15.wet** (3 records: A, B, C)
   - Baseline file with `aaa`, `bbb`, `ccc` digests
   - Oldest dates (2026-01-15)

2. **scan-2026-01-20.wet** (2 records: A refreshed, D new)
   - Content A: baseline-refresh (same URI)
   - Content D: new-content

3. **scan-2026-01-25.wet** (2 records: A URI changed, E new)
   - Content A: uri-changed (URI: /archive/content-a)
   - Content E: new-content

4. **scan-2026-01-30.wet** (2 records: B refreshed, F new)
   - Content B: baseline-refresh
   - Content F: new-content

5. **out-of-order.wet** (INVALID)
   - Intentionally broken: zzz appears before aaa
   - Should trigger RuntimeException

## Expected Merge Output (Files 1-4)

### Ordering (by digest ascending, date ascending within digest)
```
1. sha256:aaa... | 2026-01-15T10:00:00Z | http://test.gov.pl/content-a          | baseline-primary
2. sha256:aaa... | 2026-01-20T11:00:00Z | http://test.gov.pl/content-a          | baseline-refresh
3. sha256:aaa... | 2026-01-25T12:00:00Z | http://test.gov.pl/archive/content-a  | uri-changed
4. sha256:bbb... | 2026-01-15T10:05:00Z | http://test.gov.pl/content-b          | baseline-primary
5. sha256:bbb... | 2026-01-30T13:00:00Z | http://test.gov.pl/content-b          | baseline-refresh
6. sha256:ccc... | 2026-01-15T10:10:00Z | http://test.gov.pl/content-c          | baseline-primary
7. sha256:ddd... | 2026-01-20T11:05:00Z | http://test.gov.pl/content-d          | new-content
8. sha256:eee... | 2026-01-25T12:05:00Z | http://test.gov.pl/content-e          | new-content
9. sha256:fff... | 2026-01-30T13:05:00Z | http://test.gov.pl/content-f          | new-content
```

### Provenance Distribution
- baseline-primary: 3 (A, B, C from baseline file)
- baseline-refresh: 2 (A, B refreshed in later scans)
- uri-changed: 1 (A with new URI)
- new-content: 3 (D, E, F)

## Test Commands

### Test 1: Valid 4-file merge
```bash
cd /home/newton/Desktop/warc-workspace/pipeline

./dist/bin/warc-cli text-extract \
  --doet-merge \
  --primary-file "baseline-2026-01-15.wet" \
  -o tmp/test-merged-output.doet.gz \
  src/test/resources/doet-ordering/baseline-2026-01-15.wet \
  src/test/resources/doet-ordering/scan-2026-01-20.wet \
  src/test/resources/doet-ordering/scan-2026-01-25.wet \
  src/test/resources/doet-ordering/scan-2026-01-30.wet

# Validate ordering
zcat tmp/test-merged-output.doet.gz | \
  grep "WARC-Block-Digest:" | \
  awk '{print $2}' | \
  sort -c && echo "✅ ORDERING VALID" || echo "❌ ORDERING BROKEN"
```

### Test 2: Out-of-order detection (should fail)
```bash
./dist/bin/warc-cli text-extract \
  --doet-merge \
  -o tmp/test-error-output.doet.gz \
  src/test/resources/doet-ordering/out-of-order.wet

# Expected: RuntimeException with "PANIC: Input file...out of order!"
```

### Test 3: Provenance validation
```bash
# Extract digest + provenance pairs
zcat tmp/test-merged-output.doet.gz | \
  grep -E "(WARC-Block-Digest|NAC-Provenance)" | \
  paste - - | \
  awk '{print $2, $4}' | \
  column -t > tmp/provenance-report.txt

cat tmp/provenance-report.txt
```

### Test 4: Date ordering within digest groups
```bash
# Check chronological order for digest aaa
zcat tmp/test-merged-output.doet.gz | \
  grep -A1 "sha256:aaa" | \
  grep "WARC-Date:" | \
  awk '{print $2}'
# Expected: 2026-01-15 → 2026-01-20 → 2026-01-25 (ascending)
```
EOFREADME

echo ""
echo "=== Generation Complete ==="
echo "Files created:"
ls -lh "$OUTPUT_DIR"/*.wet
echo ""
echo "Read README.md for test commands and expected results"
