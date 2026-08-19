#!/bin/bash
# bench-matrix-suite.sh
# Usage: ./bench-matrix-suite.sh [output_report.md] [--multiplier N]
#
# Runs a Multi-Dimensional Benchmark Matrix:
# 1. Cores: 1..5
# 2. Heap:  5 distinct sizes per core count (Optimization Search)
# 3. Buffer: Auto-scaled based on Heap size
#
# Options:
#   --multiplier N : Phase 2 - Run with N copies of data (via concatenation)

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../../../" && pwd)"
JAR_FILE="${DIST_ROOT:-$PROJECT_ROOT/target/dist}/lib/pipeline.jar"
RAMDISK_DIR="${RAMDISK_DIR:-$PROJECT_ROOT/target/testing/tmp/ramdisk}"
BASE_INPUT="${BASE_INPUT:-$RAMDISK_DIR/plock.ap.gov.pl.warc.gz}"
OUTPUT_DIR="$PROJECT_ROOT/target/testing/tmp/bench-matrix"
REPORT_FILE="${1:-$PROJECT_ROOT/target/testing/benchmark_results/matrix_report_$(date +%Y%m%d-%H%M%S).md}"

# Phase 2: Data Volume Multiplier
MULTIPLIER=1
if [[ "$*" == *"--multiplier"* ]]; then
    # Simple argument parsing
    for i in "$@"; do
        if [[ $i == --multiplier=* ]]; then
            MULTIPLIER="${i#*=}"
        fi
    done
fi

# Prepare Input Data
INPUT_FILE="$BASE_INPUT"
if [ "$MULTIPLIER" -gt 1 ]; then
    echo "Preparing Phase 2 Data: ${MULTIPLIER}x Volume..."
    INPUT_FILE="$RAMDISK_DIR/plock_x${MULTIPLIER}.warc.gz"
    if [ ! -f "$INPUT_FILE" ]; then
        echo "Generating $INPUT_FILE..."
        # Create empty file
        > "$INPUT_FILE"
        for ((i=1; i<=MULTIPLIER; i++)); do
            cat "$BASE_INPUT" >> "$INPUT_FILE"
        done
    fi
fi

RUNS=5

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

mkdir -p "$OUTPUT_DIR"
mkdir -p "$(dirname "$REPORT_FILE")"

# Header
echo "# Matrix Benchmark Report (Volume: ${MULTIPLIER}x)" > "$REPORT_FILE"
echo "**Date**: $(date)" >> "$REPORT_FILE"
echo "**System**: $(uname -a)" >> "$REPORT_FILE"
echo "**Input**: $INPUT_FILE" >> "$REPORT_FILE"
echo "" >> "$REPORT_FILE"
echo "| Cores | Heap | Buffer | Min (MB/s) | **Avg (MB/s)** | Max (MB/s) | StdDev | Peak Mem (MB) | Verdict |" >> "$REPORT_FILE"
echo "|-------|------|--------|------------|----------------|------------|--------|---------------|---------|" >> "$REPORT_FILE"

calculate_stats() {
    local vals=("$@")
    if [ ${#vals[@]} -eq 0 ]; then echo "0 0 0 0"; return; fi
    echo "${vals[@]}" | tr ' ' '\n' | awk '
        BEGIN {sum=0; sumsq=0; min=999999; max=0}
        {
            sum += $1; sumsq += ($1)^2;
            if ($1 < min) min = $1;
            if ($1 > max) max = $1;
        }
        END {
            avg = sum / NR;
            variance = (sumsq - (sum^2 / NR)) / NR;
            if (variance < 0) variance = 0;
            stddev = sqrt(variance);
            printf "%.2f %.2f %.2f %.2f", min, avg, max, stddev;
        }
    '
}

run_cell() {
    local cores=$1
    local heap=$2

    # Auto-calculate record buffer based on heap string (approx 20 records per GB)
    # 512m -> 10, 1g -> 20, 2g -> 40
    local heap_val_mb
    if [[ "$heap" == *"g"* ]]; then
        heap_val_mb=$(echo "${heap%g} * 1024" | bc)
    else
        heap_val_mb=${heap%m}
    fi

    # Heuristic: 1 Record ~ 50MB (safe upper bound)
    local records=$(echo "$heap_val_mb / 50" | bc)
    if [ "$records" -lt 5 ]; then records=5; fi

    local run_id="c${cores}_h${heap}_x${MULTIPLIER}"
    echo -e "${BLUE}[$(date +%T)] Starting: Cores=$cores Heap=$heap Buf=$records (Scenario: $run_id)${NC}"

    if [ ! -f "$INPUT_FILE" ]; then
        echo -e "${RED}Error: Input file $INPUT_FILE not found!${NC}"
        exit 1
    fi

    local throughputs=()
    local peak_mem=0

    for ((i=1; i<=RUNS; i++)); do
        local log_file="$OUTPUT_DIR/${run_id}_${i}.log"
        local wet_output="$PROJECT_ROOT/target/testing/tmp/${run_id}.wet.gz"

        export JAVA_OPTS="-Xms${heap} -Xmx${heap} -XX:+UseZGC -XX:ActiveProcessorCount=${cores}"

        echo -ne "  [Run $i/$RUNS] Executing Java... "

        # Echo command for debugging if needed (uncomment to see full command)
        # echo "Command: java $JAVA_OPTS --add-modules jdk.incubator.vector -jar \"$JAR_FILE\" extract-text-bench --profile=\"light-parallel\" --engine.activeProcessorCount=\"$cores\" --engine.parallelGzip=true --engine.maxRecords=\"$records\" \"$INPUT_FILE\" \"$wet_output\" --verbose --final-report-summary"

        local start_time=$(date +%s)
        if java $JAVA_OPTS --add-modules jdk.incubator.vector -jar "$JAR_FILE" extract-text-bench \
            --profile="light-parallel" \
            --engine.activeProcessorCount="$cores" \
            --engine.parallelGzip=true \
            --engine.maxRecords="$records" \
            "$INPUT_FILE" "$wet_output" \
            --verbose --final-report-summary > "$log_file" 2>&1; then

            local end_time=$(date +%s)
            local duration=$((end_time - start_time))
            local runs_tp=$(grep "MB/s (avg)" "$log_file" | tail -n 1 | awk '{print $1}')
            local runs_mem=$(grep "memoryPeak" "$log_file" | tail -n 1 | awk '{print $(NF-2)}')

            if [[ -n "$runs_tp" ]]; then
                throughputs+=("$runs_tp")
                if (( $(echo "$runs_mem > $peak_mem" | bc -l) )); then peak_mem=$runs_mem; fi
                echo -e "${GREEN}Done (${duration}s, $runs_tp MB/s)${NC}"
            else
                echo -e "${RED}No metrics found in log!${NC}"
                tail -n 5 "$log_file"
            fi
        else
            echo -e "${RED}Process failed!${NC}"
            tail -n 10 "$log_file"
        fi
        rm -f "$wet_output"
    done

    if [ ${#throughputs[@]} -gt 0 ]; then
        read min avg max stddev <<< $(calculate_stats "${throughputs[@]}")
        echo -e "${GREEN} $avg MB/s ($peak_mem MB)${NC}"
        echo "| $cores | $heap | $records | $min | **$avg** | $max | $stddev | $peak_mem | OK |" >> "$REPORT_FILE"
    else
        echo -e "${RED} Failed${NC}"
        echo "| $cores | $heap | $records | N/A | N/A | N/A | N/A | N/A | Fail |" >> "$REPORT_FILE"
    fi
}

# --- SEARCH MATRIX ---

# 1 Core: Fine-grained search in low memory
# Range: 500M - 1.5G
for h in "512m" "768m" "1024m" "1280m" "1536m"; do
    run_cell 1 "$h"
done

# 2 Cores: Low to Medium
# Range: 768M - 2G
for h in "768m" "1024m" "1536m" "2048m" "2560m"; do
    run_cell 2 "$h"
done

# 3 Cores: The transition zone
# Range: 1G - 3G
for h in "1024m" "1536m" "2048m" "3g" "4g"; do
    run_cell 3 "$h"
done

# 4 Cores: Targeting high throughput
# Range: 1.5G - 5G
for h in "1536m" "2048m" "3g" "4g" "5g"; do
    run_cell 4 "$h"
done

# 5 Cores: High end
# Range: 2G - 5G
# Note: Skipping <2G as we know it fails/regresses
for h in "2g" "3g" "4g" "5g" "6g"; do
    run_cell 5 "$h"
done

echo ""
echo "Matrix Complete. Report: $REPORT_FILE"
cat "$REPORT_FILE"
