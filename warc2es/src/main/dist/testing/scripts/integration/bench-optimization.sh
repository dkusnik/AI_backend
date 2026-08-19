#!/bin/bash
# bench-optimization.sh - Optimization benchmark suite
# 7 runs per scenario, statistics: min, avg, max, stddev
# @timeout: 3600
# @runs: 1
#
# Usage:
#   ./bench-optimization.sh              # Run all scenarios
#   ./bench-optimization.sh A1           # Run only A1 (text extraction HDD)
#   ./bench-optimization.sh A1 A2        # Run A1 and A2
#   ./bench-optimization.sh B            # Run all B scenarios (tmpfs)
#   ./bench-optimization.sh --list       # List available scenarios
#
# Scenarios:
#   A1 - Text Extraction (HDD)      ~8 min
#   A2 - Producer Only (HDD)        ~4 min
#   B1 - Text Extraction (tmpfs)    ~6 min
#   B2 - Producer Only (tmpfs)      ~2 min
#   B3 - Text Extraction PDFBox/Tika (tmpfs) ~6 min  (pdftotext=false; compare vs B1 to isolate PDF impact)
#   C1 - Text Extraction (small)    ~1 min
#   C2 - Producer Only (small)      ~0.5 min

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"
while [[ "$PROJECT_ROOT" != "/" && ! -f "$PROJECT_ROOT/pom.xml" ]]; do
    PROJECT_ROOT="$(dirname "$PROJECT_ROOT")"
done
if [[ ! -f "$PROJECT_ROOT/pom.xml" ]]; then
    echo "ERROR: could not locate project root from $SCRIPT_DIR" >&2
    exit 1
fi

export JAVA_HOME="${JAVA_HOME:-}"
DIST_ROOT="${DIST_ROOT:-$PROJECT_ROOT/target/dist}"
WARC_CLI="$DIST_ROOT/bin/warc-cli"
PIPELINE_JAR="$DIST_ROOT/lib/pipeline.jar"
BENCH_ISAL_ENABLED="${BENCH_ISAL_ENABLED:-true}"
read -r -a BENCH_ENGINE_ARGS_ARR <<< "${BENCH_ENGINE_ARGS:-}"

# Use JAVA_OPTS if set (e.g., from bench-b1-b2-parallel.sh), otherwise use defaults
DEFAULT_JAVA_OPTS="-Xms1G -Xmx1G -XX:+UseZGC -XX:ActiveProcessorCount=1"
EFFECTIVE_JAVA_OPTS="${JAVA_OPTS:-$DEFAULT_JAVA_OPTS}"
JAVA_BIN="java"
if [ -n "$JAVA_HOME" ]; then
    JAVA_BIN="$JAVA_HOME/bin/java"
fi
read -r -a JAVA_OPTS_ARR <<< "$EFFECTIVE_JAVA_OPTS"
JAVA_CMD=( "$JAVA_BIN" "${JAVA_OPTS_ARR[@]}" --add-modules jdk.incubator.vector -jar "$PIPELINE_JAR" )
# pipeline-lib sets WARC_DIST_DIR; when invoking the jar directly we must set it ourselves
export WARC_DIST_DIR="$DIST_ROOT"

# Test files
TEST_DATA_ROOT="${TEST_DATA_DIR:-$PROJECT_ROOT/shared}"
if [[ ! -d "$TEST_DATA_ROOT" && -d "$PROJECT_ROOT/../shared" ]]; then
    TEST_DATA_ROOT="$PROJECT_ROOT/../shared"
fi
PLOCK_HDD="${PLOCK_HDD:-$TEST_DATA_ROOT/plock.ap.gov.pl.warc.gz}"
SMALL_FILE="${SMALL_FILE:-$TEST_DATA_ROOT/example.com.warc.gz}"
RAMDISK_DIR="${RAMDISK_DIR:-$PROJECT_ROOT/target/testing/tmp/ramdisk}"

resolve_isal_library() {
    local configured="${WARC_ISAL_LIBRARY:-}"
    if [[ -n "$configured" && -f "$configured" ]]; then
        echo "$configured"
        return 0
    fi
    local candidate
    for candidate in \
        "/usr/lib/x86_64-linux-gnu/libisal.so.2" \
        "/usr/lib/libisal.so.2" \
        "/usr/local/lib/libisal.so.2" \
        "/lib/x86_64-linux-gnu/libisal.so.2"; do
        if [[ -f "$candidate" ]]; then
            echo "$candidate"
            return 0
        fi
    done
    return 1
}

ISAL_LIBRARY_PATH="$(resolve_isal_library || true)"
ACTIVE_GZIP_DECOMPRESSOR="commons"
if [[ "$BENCH_ISAL_ENABLED" == "true" && -n "$ISAL_LIBRARY_PATH" ]]; then
    ACTIVE_GZIP_DECOMPRESSOR="isal"
fi

# Output directory
BENCH_DIR="$PROJECT_ROOT/target/testing/benchmark_results"
mkdir -p "$BENCH_DIR"

TIMESTAMP=$(date +%Y%m%d-%H%M%S)
RESULTS_FILE="$BENCH_DIR/bench_${TIMESTAMP}.md"
TMP_OUTPUT="$PROJECT_ROOT/target/testing/tmp/bench-output-$$"

cleanup_benchmark_outputs() {
    local path
    for path in "$TMP_OUTPUT" "${TMP_OUTPUT_RAM:-}" "${TMP_OUTPUT_SMALL:-}"; do
        if [[ -n "$path" ]]; then
            rm -rf -- "$path"
        fi
    done
}
trap cleanup_benchmark_outputs EXIT

# Number of runs per scenario
RUNS=21

# Quality check expected value (global dedup, Tika 3.2.3 + PDFBox 3.0.5 + ISA-L baseline)
# Baseline (2026-03-06, d3e4d27, N=21): B1=198.41 MB/s, B2=584.14 MB/s
EXPECTED_RECORDS=290

# Parse command line arguments or environment variable
if [ -n "$BENCH_SCENARIOS" ]; then
    read -ra SELECTED_SCENARIOS <<< "$BENCH_SCENARIOS"
else
    SELECTED_SCENARIOS=("$@")
fi

# Handle --list flag
if [[ " ${SELECTED_SCENARIOS[*]} " =~ " --list " ]] || [[ " ${SELECTED_SCENARIOS[*]} " =~ " -l " ]]; then
    echo "Available scenarios:"
    echo "  A1 - Text Extraction (HDD)      ~8 min"
    echo "  A2 - Producer Only (HDD)        ~4 min"
    echo "  B1 - Text Extraction (tmpfs)    ~6 min"
    echo "  B2 - Producer Only (tmpfs)      ~2 min"
    echo "  B3 - Text Extraction PDFBox/Tika (tmpfs)  ~6 min  (pdftotext=false; isolates PDF extraction cost)"
    echo "  C1 - Text Extraction (small)    ~1 min"
    echo "  C2 - Producer Only (small)      ~0.5 min"
    echo ""
    echo "Usage:"
    echo "  ./bench-optimization.sh          # Run all"
    echo "  ./bench-optimization.sh A1       # Run only A1"
    echo "  ./bench-optimization.sh A        # Run A1 and A2"
    echo "  ./bench-optimization.sh A1 B1    # Run A1 and B1"
    exit 0
fi

should_run_scenario() {
    local scenario="$1"
    local group="${scenario:0:1}"  # First character (A, B, C)

    # If no scenarios specified, run all
    if [ ${#SELECTED_SCENARIOS[@]} -eq 0 ]; then
        return 0
    fi

    for sel in "${SELECTED_SCENARIOS[@]}"; do
        # Exact match (A1, B1, etc.)
        if [ "$sel" = "$scenario" ]; then
            return 0
        fi
        # Group match (A, B, C)
        if [ "$sel" = "$group" ]; then
            return 0
        fi
    done
    return 1
}

echo "=== WARC Pipeline Optimization Benchmark ==="
echo "Date: $(date)"
echo "Branch: $(git -C "$PROJECT_ROOT" branch --show-current 2>/dev/null || echo 'unknown')"
echo "Commit: $(git -C "$PROJECT_ROOT" rev-parse --short HEAD 2>/dev/null || echo 'unknown')"
echo "WARC CLI: $WARC_CLI"
echo "ISA-L enabled: $BENCH_ISAL_ENABLED"
echo "ISA-L library: ${ISAL_LIBRARY_PATH:-not-found}"
echo "GZIP decompressor: $ACTIVE_GZIP_DECOMPRESSOR"
echo "Results: $RESULTS_FILE"
if [ ${#SELECTED_SCENARIOS[@]} -gt 0 ]; then
    echo "Selected: ${SELECTED_SCENARIOS[*]}"
fi
echo ""

# Initialize results file
cat > "$RESULTS_FILE" << EOF
# Optimization Benchmark Results

| Field | Value |
|-------|-------|
| Date | $(date) |
| Branch | $(git -C "$PROJECT_ROOT" branch --show-current 2>/dev/null || echo 'unknown') |
| Commit | $(git -C "$PROJECT_ROOT" rev-parse --short HEAD 2>/dev/null || echo 'unknown') |
| Java | $JAVA_HOME |
| Host | $(hostname) |
| ISA-L enabled | $BENCH_ISAL_ENABLED |
| ISA-L library | ${ISAL_LIBRARY_PATH:-not-found} |
| GZIP decompressor | $ACTIVE_GZIP_DECOMPRESSOR |
| Runs | $RUNS |
| Scenarios | ${SELECTED_SCENARIOS[*]:-ALL} |

EOF

calc_stats() {
    local -n arr=$1
    local n=${#arr[@]}

    if [ "$n" -eq 0 ]; then
        MIN="0"; AVG="0"; MAX="0"; STDDEV="0"
        return
    fi

    local sum=0
    MIN=${arr[0]}
    MAX=${arr[0]}

    for val in "${arr[@]}"; do
        sum=$(echo "$sum + $val" | bc -l)
        if (( $(echo "$val < $MIN" | bc -l) )); then MIN=$val; fi
        if (( $(echo "$val > $MAX" | bc -l) )); then MAX=$val; fi
    done

    AVG=$(echo "scale=2; $sum / $n" | bc)

    # Standard deviation
    local sumsq=0
    for val in "${arr[@]}"; do
        local diff=$(echo "$val - $AVG" | bc -l)
        sumsq=$(echo "$sumsq + ($diff * $diff)" | bc -l)
    done
    STDDEV=$(echo "scale=2; sqrt($sumsq / $n)" | bc)
}

run_bench_series() {
    local name="$1"
    shift
    local cleanup_path="$1"
    shift
    local -a cmd=( "$@" )
    local runs="$RUNS"
    local results=()

    echo "----------------------------------------"
    echo "Running: $name ($runs iterations)"

    for i in $(seq 1 $runs); do
        echo -n "  Run $i/$runs... "

        # Clean up the scenario's previous output, if it produces one.
        if [[ -n "$cleanup_path" ]]; then
            rm -rf -- "$cleanup_path"
        fi

        # Run benchmark and capture output
        local output
        output=$("${cmd[@]}" 2>&1) || {
            echo "FAILED"
            echo "$output" | tail -5
            return 1
        }

        # Extract throughput from final report (format: "XX.XX MB/s")
        local throughput
        throughput=$(echo "$output" | grep -oP '\d+\.\d+ MB/s' | head -1 | awk '{print $1}')

        if [ -z "$throughput" ] || [ "$throughput" = "0" ]; then
            # Try alternative format
            throughput=$(echo "$output" | grep -oP 'throughput[: ]+\K\d+\.\d+' | head -1)
        fi

        if [ -z "$throughput" ]; then
            echo "No throughput found"
            return 1
        else
            echo "$throughput MB/s"
            results+=("$throughput")
        fi
    done

    # Calculate statistics
    calc_stats results

    echo "  Stats: Min=$MIN, Avg=$AVG, Max=$MAX, StdDev=$STDDEV"
    echo "| $name | $MIN | $AVG | $MAX | $STDDEV |" >> "$RESULTS_FILE"
}

count_records_in_file() {
    local path="$1"
    local count

    count=$(gzip -cd "$path" 2>/dev/null | grep -aic '^warc/1\.[01]' || true)
    if [[ "${count:-0}" -eq 0 ]]; then
        count=$(gzip -cd "$path" 2>/dev/null | gzip -cd 2>/dev/null \
            | grep -aic '^warc/1\.[01]' || true)
    fi
    echo "${count:-0}"
}

count_records_in_output() {
    local path="$1"

    if [ -f "$path" ]; then
        count_records_in_file "$path"
        return 0
    fi

    if [ -d "$path" ]; then
        local count=0
        local f
        shopt -s nullglob
        for f in "$path"/*.wet.gz "$path"/*.doet.gz; do
            local n
            n=$(count_records_in_file "$f")
            count=$((count + ${n:-0}))
        done
        shopt -u nullglob
        echo "$count"
        return 0
    fi

    echo "0"
    return 1
}

quality_check() {
    local file="$1"
    local expected="${2:-$EXPECTED_RECORDS}"

    echo -n "Quality check: "
    if [ ! -e "$file" ]; then
        echo "FAIL (file not found)"
        return 1
    fi

    local count
    count=$(count_records_in_output "$file")

    if [ "$count" -eq "$expected" ]; then
        echo "PASS ($count records)"
        return 0
    else
        echo "FAIL (expected $expected, got $count)"
        return 1
    fi
}

# ========================================
# Scenario Set A: HDD Baseline
# ========================================

RAN_A=false

if [ -f "$PLOCK_HDD" ]; then
    if should_run_scenario "A1" || should_run_scenario "A2"; then
        echo "" >> "$RESULTS_FILE"
        echo "## Scenario Set A: HDD Baseline" >> "$RESULTS_FILE"
        echo "" >> "$RESULTS_FILE"
        echo "| Scenario | Min (MB/s) | Avg (MB/s) | Max (MB/s) | StdDev |" >> "$RESULTS_FILE"
        echo "|----------|------------|------------|------------|--------|" >> "$RESULTS_FILE"
        RAN_A=true
    fi

    # A1: Text Extraction
    if should_run_scenario "A1"; then
        run_bench_series "A1: Text Extraction (HDD)" \
            "$TMP_OUTPUT" \
            "$WARC_CLI" extract-text "$PLOCK_HDD" --output-dir="$TMP_OUTPUT" --output-prefix=bench \
            --progress-none --final-report-summary --consumer.*.warc-size-limit=0 \
            --isal-enabled="$BENCH_ISAL_ENABLED" "${BENCH_ENGINE_ARGS_ARR[@]}"

        # Quality check
        quality_check "$TMP_OUTPUT"
        echo "" >> "$RESULTS_FILE"
        echo "Quality: $(count_records_in_output "$TMP_OUTPUT") records (expected: $EXPECTED_RECORDS)" >> "$RESULTS_FILE"
        echo "" >> "$RESULTS_FILE"
    fi

    # A2: Producer Only (chunked extractor with noop consumer)
    if should_run_scenario "A2"; then
        run_bench_series "A2: Producer Only (HDD)" \
            "" \
            "${JAVA_CMD[@]}" warc-grep-chunked "$PLOCK_HDD" --progress-none --final-report-summary \
            --isal-enabled="$BENCH_ISAL_ENABLED" "${BENCH_ENGINE_ARGS_ARR[@]}"
        echo "" >> "$RESULTS_FILE"
    fi
else
    echo "WARNING: HDD test file not found at $PLOCK_HDD"
fi

# ========================================
# Scenario Set B: tmpfs (if available)
# ========================================

PLOCK_RAM="$RAMDISK_DIR/plock.ap.gov.pl.warc.gz"
RAN_B=false

if [ -d "$RAMDISK_DIR" ] && [ -f "$PLOCK_RAM" ]; then
    if should_run_scenario "B1" || should_run_scenario "B2" || should_run_scenario "B3"; then
        echo "" >> "$RESULTS_FILE"
        echo "## Scenario Set B: tmpfs (I/O Eliminated)" >> "$RESULTS_FILE"
        echo "" >> "$RESULTS_FILE"
        echo "| Scenario | Min (MB/s) | Avg (MB/s) | Max (MB/s) | StdDev |" >> "$RESULTS_FILE"
        echo "|----------|------------|------------|------------|--------|" >> "$RESULTS_FILE"
        RAN_B=true
    fi

    TMP_OUTPUT_RAM="$RAMDISK_DIR/bench-output-$$"

    # B1: Text Extraction (matches production warc2wet pipeline)
    if should_run_scenario "B1"; then
        run_bench_series "B1: Text Extraction (tmpfs)" \
            "$TMP_OUTPUT_RAM" \
            "$WARC_CLI" extract-text "$PLOCK_RAM" --output-dir="$TMP_OUTPUT_RAM" --output-prefix=bench \
            --progress-none --final-report-summary --consumer.*.warc-size-limit=0 \
            --deduplicate-scope=global --isal-enabled="$BENCH_ISAL_ENABLED" "${BENCH_ENGINE_ARGS_ARR[@]}"

        # Quality check
        quality_check "$TMP_OUTPUT_RAM"
        echo "" >> "$RESULTS_FILE"
        echo "Quality: $(count_records_in_output "$TMP_OUTPUT_RAM") records (expected: $EXPECTED_RECORDS)" >> "$RESULTS_FILE"
        echo "" >> "$RESULTS_FILE"
    fi

    # B2: Producer Only (chunked extractor with noop consumer)
    if should_run_scenario "B2"; then
        run_bench_series "B2: Producer Only (tmpfs)" \
            "" \
            "${JAVA_CMD[@]}" warc-grep-chunked "$PLOCK_RAM" --progress-none --final-report-summary \
            --isal-enabled="$BENCH_ISAL_ENABLED" "${BENCH_ENGINE_ARGS_ARR[@]}"
        echo "" >> "$RESULTS_FILE"
    fi

    # B3: Text Extraction with PDFBox/Tika (pdftotext disabled — compare vs B1 to isolate PDF extraction cost)
    if should_run_scenario "B3"; then
        run_bench_series "B3: Text Extraction PDFBox/Tika (tmpfs)" \
            "$TMP_OUTPUT_RAM" \
            "$WARC_CLI" extract-text "$PLOCK_RAM" --output-dir="$TMP_OUTPUT_RAM" --output-prefix=bench \
            --progress-none --final-report-summary --consumer.*.warc-size-limit=0 \
            --deduplicate-scope=global --isal-enabled="$BENCH_ISAL_ENABLED" \
            --processor.extract-text.use-pdftotext=false "${BENCH_ENGINE_ARGS_ARR[@]}"

        # Quality check
        quality_check "$TMP_OUTPUT_RAM"
        echo "" >> "$RESULTS_FILE"
        echo "Quality: $(count_records_in_output "$TMP_OUTPUT_RAM") records (expected: $EXPECTED_RECORDS)" >> "$RESULTS_FILE"
        echo "" >> "$RESULTS_FILE"
    fi

elif (should_run_scenario "B1" || should_run_scenario "B2" || should_run_scenario "B3"); then
    if [ -d "$RAMDISK_DIR" ]; then
        echo ""
        echo "tmpfs available but test file not found. Run:"
        echo "  cp '$PLOCK_HDD' '$PLOCK_RAM'"
    else
        echo ""
        echo "tmpfs not available at $RAMDISK_DIR. Run:"
        echo "  sudo mkdir -p $RAMDISK_DIR"
        echo "  sudo mount -t tmpfs -o size=4G tmpfs $RAMDISK_DIR"
        echo "  cp '$PLOCK_HDD' '$RAMDISK_DIR/'"
    fi
    exit 1
fi

# ========================================
# Scenario Set C: Quick Iteration
# ========================================

RAN_C=false

if [ -f "$SMALL_FILE" ]; then
    if should_run_scenario "C1" || should_run_scenario "C2"; then
        echo "" >> "$RESULTS_FILE"
        echo "## Scenario Set C: Quick Iteration (8.5MB file)" >> "$RESULTS_FILE"
        echo "" >> "$RESULTS_FILE"
        echo "| Scenario | Min (MB/s) | Avg (MB/s) | Max (MB/s) | StdDev |" >> "$RESULTS_FILE"
        echo "|----------|------------|------------|------------|--------|" >> "$RESULTS_FILE"
        RAN_C=true
    fi

    TMP_OUTPUT_SMALL="$PROJECT_ROOT/target/testing/tmp/bench-small-$$"

    # C1: Text Extraction (small file)
    if should_run_scenario "C1"; then
        run_bench_series "C1: Text Extraction (small)" \
            "$TMP_OUTPUT_SMALL" \
            "$WARC_CLI" extract-text "$SMALL_FILE" --output-dir="$TMP_OUTPUT_SMALL" --output-prefix=bench \
            --progress-none --final-report-summary --consumer.*.warc-size-limit=0 \
            --isal-enabled="$BENCH_ISAL_ENABLED" "${BENCH_ENGINE_ARGS_ARR[@]}"
    fi

    # C2: Producer Only (small file, chunked extractor with noop consumer)
    if should_run_scenario "C2"; then
        run_bench_series "C2: Producer Only (small)" \
            "" \
            "${JAVA_CMD[@]}" warc-grep-chunked "$SMALL_FILE" --progress-none --final-report-summary \
            --isal-enabled="$BENCH_ISAL_ENABLED" "${BENCH_ENGINE_ARGS_ARR[@]}"
    fi

    echo "" >> "$RESULTS_FILE"
fi

# ========================================
# Cleanup and Summary
# ========================================

echo ""
echo "=== Benchmark Complete ==="
echo ""
echo "Results saved to: $RESULTS_FILE"
echo ""
cat "$RESULTS_FILE"
