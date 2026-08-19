#!/bin/bash
# tc-ext-002-lang-filter.sh
source "$(dirname "$0")/../../../../lib/test-lib.sh"

test_lang_filter() {
    # Using plock.ap.gov.pl sample which should have Polish
    ensure_test_data "plock.ap.gov.pl.warc.gz" || return 1
    local input="$TEST_DATA_DIR/plock.ap.gov.pl.warc.gz"
    local out_dir="$TEST_OUTPUT_DIR/ext-002-out"
    local prefix="ext002"
    mkdir -p "$out_dir"

    # Check file size - if too large, limit rows to avoid timeout
    local file_size
    file_size=$(stat -c%s "$input" 2>/dev/null || echo "0")
    local row_limit=""
    if [ "$file_size" -gt 100000000 ]; then  # >100MB
        row_limit="--processor.grep.row-limit=100"
        log_info "Large file detected ($file_size bytes), limiting to 100 records"
    fi

    log_info "Running extract-text with Polish filter..."
    "$WARC_CLI" extract-text "$input" --output-dir="$out_dir" --output-prefix="$prefix" \
        --processor.lang-detect.lang-filter=pl $row_limit

    assert_command_success $?
    assert_directory_exists "$out_dir"
    local output
    output=$(find "$out_dir" -maxdepth 1 -type f -name "${prefix}-*.doet.gz" | head -n1)
    if [ -z "$output" ]; then
        log_fail "No output DOET file produced"
        return 1
    fi
    assert_file_exists "$output"

    # Verify only pl content or empty if none found
    # We expect some PL content in plock archive

    # Extract language headers (case-insensitive grep)
    # Header format: WARC-Identified-Content-Language: pl

    local non_pl_count
    non_pl_count=$(zgrep -i "WARC-Identified-Content-Language:" "$output" | grep -iv "pl" | wc -l)

    if [ "$non_pl_count" -gt 0 ]; then
        log_fail "Found $non_pl_count records with non-Polish language"
        return 1
    fi

    local pl_count
    pl_count=$(zgrep -i "WARC-Identified-Content-Language:" "$output" | grep -i "pl" | wc -l)

    log_info "Found $pl_count Polish records"
    assert_greater_than "$pl_count" 0
}

run_test test_lang_filter
