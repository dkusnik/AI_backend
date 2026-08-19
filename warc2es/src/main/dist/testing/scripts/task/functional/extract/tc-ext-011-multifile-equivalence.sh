#!/bin/bash
# @timeout: 300
# T-212: extract-text emits date-bucketed DOET files (prefix-yyyymmdd.doet.gz),
# and merge accepts a folder of those files.
set -euo pipefail
source "$(dirname "$0")/../../../../lib/test-lib.sh"

test_per_crawl_extract_and_folder_merge() {
    ensure_test_data "plock.ap.gov.pl.warc.gz" \
        || { log_warn "plock1 fixture missing — skipping"; return 0; }
    ensure_test_data "plock.ap.gov.pl-2026-01-30-ace2d026-00000.warc.gz" \
        || { log_warn "plock2 fixture missing — skipping"; return 0; }

    local plock1="$TEST_DATA_DIR/plock.ap.gov.pl.warc.gz"
    local plock2="$TEST_DATA_DIR/plock.ap.gov.pl-2026-01-30-ace2d026-00000.warc.gz"

    local out_dir="$TEST_OUTPUT_DIR/ext-011-dates"
    local prefix="ext011"
    mkdir -p "$out_dir"

    log_info "Stage 1/3: extract-text with mixed inputs -> per-date outputs..."
    "$WARC_CLI" extract-text "$plock1" "$plock2" --output-dir="$out_dir" --output-prefix="$prefix" --silent
    assert_command_success $? "extract-text per-date split"

    local -a outputs=()
    while IFS= read -r f; do outputs+=("$f"); done < <(find "$out_dir" -maxdepth 1 -type f -name "${prefix}-*.doet.gz" | LC_ALL=C sort)
    if [[ ${#outputs[@]} -le 0 ]]; then
        log_fail "No per-date outputs produced in $out_dir"
        return 1
    fi
    log_info "Produced ${#outputs[@]} date outputs"

    local failed=0
    local f
    for f in "${outputs[@]}"; do
        assert_file_exists "$f"
        local count
        count=$(warc_count "$f")
        if [[ $count -le 0 ]]; then
            log_fail "Empty per-date output: $f"
            failed=$(( failed + 1 ))
        fi

        local date_from_name
        date_from_name="$(basename "$f" | sed -E "s/^${prefix}-([0-9]{8})\\.doet\\.gz$/\\1/")"
        if [[ -z "$date_from_name" || "$date_from_name" == "$(basename "$f")" ]]; then
            log_fail "Filename does not match ${prefix}-yyyymmdd.doet.gz: $f"
            failed=$(( failed + 1 ))
            continue
        fi

        local header_dates
        header_dates=$(zgrep -i "^WARC-Date:" "$f" 2>/dev/null | awk '{print $2}' | sed -E 's/^([0-9]{4})-([0-9]{2})-([0-9]{2}).*/\1\2\3/' | sort -u)
        if [[ -z "$header_dates" ]]; then
            log_fail "No WARC-Date headers found in $f"
            failed=$(( failed + 1 ))
        elif [[ "$header_dates" != "$date_from_name" ]]; then
            log_fail "Date bucket mismatch in $f: file=$date_from_name headers=$header_dates"
            failed=$(( failed + 1 ))
        fi

        if ! zcat "$f" 2>/dev/null | awk 'BEGIN{IGNORECASE=1;found=0} /^X-Source-Warc:/{found=1} END{exit(found?0:1)}'; then
            log_fail "Per-date output is missing X-Source-Warc headers: $f"
            failed=$(( failed + 1 ))
        fi
    done

    log_info "Stage 2/3: merge using folder input..."
    local folder_base="$TEST_OUTPUT_DIR/ext-011-folder-base.wet.gz"
    local folder_diff="$TEST_OUTPUT_DIR/ext-011-folder-diff.wet.gz"
    "$WARC_CLI" merge --output-base="$folder_base" --output-diff="$folder_diff" "$out_dir" --silent
    assert_command_success $? "merge folder input"
    assert_file_exists "$folder_base"
    local folder_base_count
    folder_base_count=$(warc_count "$folder_base")
    if [[ $folder_base_count -le 0 ]]; then
        log_fail "Folder merge base output is empty"
        failed=$(( failed + 1 ))
    fi
    log_info "Stage 3/3: merge using explicit file list (equivalence check)..."
    local explicit_base="$TEST_OUTPUT_DIR/ext-011-explicit-base.wet.gz"
    local explicit_diff="$TEST_OUTPUT_DIR/ext-011-explicit-diff.wet.gz"
    "$WARC_CLI" merge --output-base="$explicit_base" --output-diff="$explicit_diff" "${outputs[@]}" --silent
    assert_command_success $? "merge explicit file list"
    assert_file_exists "$explicit_base"
    local explicit_base_count
    explicit_base_count=$(warc_count "$explicit_base")

    if [[ $folder_base_count -ne $explicit_base_count ]]; then
        log_fail "Folder merge != explicit merge: folder=$folder_base_count explicit=$explicit_base_count"
        failed=$(( failed + 1 ))
    fi

    [[ $failed -gt 0 ]] && return 1
    log_info "Per-date extract + folder merge behavior verified"
    return 0
}

run_test test_per_crawl_extract_and_folder_merge
