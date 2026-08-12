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
