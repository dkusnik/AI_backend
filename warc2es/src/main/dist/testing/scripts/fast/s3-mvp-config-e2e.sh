#!/bin/bash
# Run the MVP out/ config through the top-level wrapper on a tiny fixture.
source "$(dirname "$0")/../../lib/test-lib.sh"

test_mvp_config_warc2wet_smoke() {
    ensure_test_data "tiny.warc.gz" || return 1

    local wrapper="$PROJECT_ROOT/out/warc2wet.sh"
    local log="$TEST_OUTPUT_DIR/mvp-warc2wet.log"
    local undated_log="$TEST_OUTPUT_DIR/mvp-warc2wet-undated.log"
    local input_dir="$TEST_OUTPUT_DIR/two-warcs"
    local input_one="$input_dir/20260209-194120-a.warc.gz"
    local input_two="$input_dir/20260209-194121-b.warc.gz"
    local url_id="w2b-mvp-smoke-$$"
    assert_file_exists "$wrapper" || return 1

    if "$wrapper" "$TEST_DATA_DIR/tiny.warc.gz" \
        --url-id="$url_id" --crawl-id=fast > "$undated_log" 2>&1; then
        log_fail "warc2wet accepted an input with no WARC-Date or dated filename"
        return 1
    fi
    if ! grep -Fq "cannot determine crawl date from WARC-Date or filename" "$undated_log"; then
        log_fail "warc2wet did not diagnose the undated input"
        sed -n '1,120p' "$undated_log" >&2
        return 1
    fi

    mkdir -p "$input_dir"
    cp "$TEST_DATA_DIR/tiny.warc.gz" "$input_one"
    cp "$TEST_DATA_DIR/tiny.warc.gz" "$input_two"
    if ! "$wrapper" "$input_dir" --url-id="$url_id" --crawl-id=fast > "$log" 2>&1; then
        log_fail "out/warc2wet.sh failed for a two-file input directory"
        sed -n '1,120p' "$log" >&2
        return 1
    fi

    if grep -qi "worker died" "$log"; then
        log_fail "MVP config produced worker-died diagnostics"
        sed -n '1,120p' "$log" >&2
        return 1
    fi

    local produced
    produced="$PROJECT_ROOT/out/wet/$url_id/fast/20260209-194120-a-2files.wet.gz"
    if [[ ! -f "$produced" ]]; then
        log_fail "MVP config smoke did not produce expected WET output"
        sed -n '1,120p' "$log" >&2
        return 1
    fi
    local sidecar="${produced%.wet.gz}.cdxj"
    rm -f -- "$produced" "$sidecar"
}

run_test test_mvp_config_warc2wet_smoke
