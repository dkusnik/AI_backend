#!/bin/bash
# @timeout: 120
# T-212-fast: Same logic as tc-ext-011 but using small fixtures for rapid iteration.
set -euo pipefail
source "$(dirname "$0")/../../../../lib/test-lib.sh"

test_multifile_equivalence_fast() {
    ensure_test_data "example.com.warc.gz" \
        || { log_warn "example.com fixture missing — skipping"; return 0; }
    ensure_test_data "large-test.warc.gz" \
        || { log_warn "large-test fixture missing — skipping"; return 0; }

    local file1="$TEST_DATA_DIR/example.com.warc.gz"
    local file2="$TEST_DATA_DIR/large-test.warc.gz"

    # ── Path A (reference) ────────────────────────────────────────────────────
    local ref_f1_dir="$TEST_OUTPUT_DIR/ref-f1"
    local ref_f2_dir="$TEST_OUTPUT_DIR/ref-f2"
    mkdir -p "$ref_f1_dir" "$ref_f2_dir"
    local ref_base="$TEST_OUTPUT_DIR/ref-base.wet.gz"
    local ref_diff="$TEST_OUTPUT_DIR/ref-diff.wet.gz"

    log_info "Stage 1/5 (ref): extract file1..."
    "$WARC_CLI" extract-text "$file1" --output-dir="$ref_f1_dir" --output-prefix="ref011f1" --silent
    assert_command_success $? "extract file1 (ref)"
    local -a ref_f1_outputs=()
    while IFS= read -r f; do ref_f1_outputs+=("$f"); done < <(find "$ref_f1_dir" -maxdepth 1 -type f -name "ref011f1-*.doet.gz" | LC_ALL=C sort)
    if [[ ${#ref_f1_outputs[@]} -le 0 ]]; then
        log_fail "No outputs for ref file1"
        return 1
    fi
    local ref_f1_count=0
    local f
    for f in "${ref_f1_outputs[@]}"; do
        ref_f1_count=$(( ref_f1_count + $(warc_count "$f") ))
    done
    log_info "ref file1: $ref_f1_count records"

    log_info "Stage 2/5 (ref): extract file2..."
    "$WARC_CLI" extract-text "$file2" --output-dir="$ref_f2_dir" --output-prefix="ref011f2" --silent
    assert_command_success $? "extract file2 (ref)"
    local -a ref_f2_outputs=()
    while IFS= read -r f; do ref_f2_outputs+=("$f"); done < <(find "$ref_f2_dir" -maxdepth 1 -type f -name "ref011f2-*.doet.gz" | LC_ALL=C sort)
    if [[ ${#ref_f2_outputs[@]} -le 0 ]]; then
        log_fail "No outputs for ref file2"
        return 1
    fi
    local ref_f2_count=0
    for f in "${ref_f2_outputs[@]}"; do
        ref_f2_count=$(( ref_f2_count + $(warc_count "$f") ))
    done
    log_info "ref file2: $ref_f2_count records"

    log_info "Stage 3/5 (ref): merge..."
    "$WARC_CLI" merge "${ref_f1_outputs[@]}" "${ref_f2_outputs[@]}" \
        --output-base="$ref_base" --output-diff="$ref_diff" --silent
    assert_command_success $? "merge (ref)"
    local ref_base_count; ref_base_count=$(warc_count "$ref_base")
    log_info "ref merge_base: $ref_base_count records"

    # ── Path B (multi-crawl extract) ──────────────────────────────────────────
    local mc_dir="$TEST_OUTPUT_DIR/mc"
    mkdir -p "$mc_dir"
    local mc_base="$TEST_OUTPUT_DIR/mc-base.wet.gz"
    local mc_diff="$TEST_OUTPUT_DIR/mc-diff.wet.gz"

    log_info "Stage 4/5 (multi-crawl): extract file1+file2 --output-dir..."
    "$WARC_CLI" extract-text "$file1" "$file2" --output-dir="$mc_dir" --output-prefix="mc011" --silent
    assert_command_success $? "multi-crawl extract"

    local -a mc_outputs=()
    while IFS= read -r f; do mc_outputs+=("$f"); done < <(find "$mc_dir" -maxdepth 1 -type f -name "mc011-*.doet.gz" | LC_ALL=C sort)
    if [[ ${#mc_outputs[@]} -le 0 ]]; then
        log_fail "No outputs for multi-crawl extract"
        return 1
    fi

    local mc_total_count=0
    for f in "${mc_outputs[@]}"; do
        mc_total_count=$(( mc_total_count + $(warc_count "$f") ))
    done
    log_info "mc extract: files=${#mc_outputs[@]} records=$mc_total_count"

    log_info "Stage 5/5 (multi-crawl): merge..."
    "$WARC_CLI" merge "${mc_outputs[@]}" \
        --output-base="$mc_base" --output-diff="$mc_diff" --silent
    assert_command_success $? "merge (multi-crawl)"
    local mc_base_count; mc_base_count=$(warc_count "$mc_base")
    log_info "mc merge_base: $mc_base_count records"

    # ── Assertions ────────────────────────────────────────────────────────────
    local failed=0

    local ref_total_count
    ref_total_count=$(( ref_f1_count + ref_f2_count ))
    if [[ $mc_total_count -le 0 || $mc_total_count -gt $ref_total_count ]]; then
        log_fail "mc extract cardinality unexpected: ref_total=$ref_total_count mc_total=$mc_total_count"
        failed=$(( failed + 1 ))
    fi

    if [[ $ref_base_count -ne $mc_base_count ]]; then
        log_fail "merge_base: ref=$ref_base_count mc=$mc_base_count"
        failed=$(( failed + 1 ))
    else
        log_info "merge_base: ref==mc ($ref_base_count) ✓"
    fi

    log_info "Summary: ref(f1=$ref_f1_count f2=$ref_f2_count base=$ref_base_count) mc(total=$mc_total_count base=$mc_base_count)"
    [[ $failed -gt 0 ]] && return 1
    return 0
}

run_test test_multifile_equivalence_fast
