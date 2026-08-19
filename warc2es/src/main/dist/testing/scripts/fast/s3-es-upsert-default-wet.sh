#!/bin/bash
# Ensure pair-only es-upsert derives exactly one managed staging directory.
source "$(dirname "$0")/../../lib/test-lib.sh"

test_es_upsert_derives_pair_staging() {
    local runtime="$TEST_OUTPUT_DIR/runtime"
    local script_src="$PROJECT_ROOT/src/main/dist/es-upsert.sh"
    local runtime_lib_src="$PROJECT_ROOT/src/main/dist/lib/scripts/runtime-lib.sh"
    local script="$runtime/es-upsert.sh"
    local output rc pair

    mkdir -p "$runtime/app/lib/scripts" "$runtime/app/bin" "$runtime/wet" "$runtime/doet"
    cp "$script_src" "$script"
    cp "$runtime_lib_src" "$runtime/app/lib/scripts/runtime-lib.sh"
    chmod +x "$script"
    printf '#!/bin/bash\necho "fake es-cli $*"\n' > "$runtime/app/bin/es-cli"
    chmod +x "$runtime/app/bin/es-cli"

    set +e
    output=$(cd "$runtime" && ./es-upsert.sh --url-id=alpha --crawl-id=crawl1 \
        --dry-run --result-format=human 2>&1)
    rc=$?
    set -e

    assert_command_failure "$rc" "empty derived staging should fail" || return 1
    if [[ "$output" != "Error: no staged .wet.gz files found for alpha/crawl1" ]]; then
        log_fail "Unexpected empty-staging error: $output"
        return 1
    fi

    pair="$runtime/wet/alpha/crawl1"
    mkdir -p "$pair"
    for name in b a; do
        printf 'WARC/1.0\r\nWARC-Type: conversion\r\nX-NAC-URL-ID: alpha\r\nX-NAC-Crawl-ID: crawl1\r\nContent-Length: 0\r\n\r\n\r\n' \
            | gzip > "$pair/$name.wet.gz"
    done

    set +e
    output=$(cd "$runtime" && ./es-upsert.sh --url-id=alpha --crawl-id=crawl1 \
        --dry-run 2>"$runtime/stderr")
    rc=$?
    set -e
    assert_command_success "$rc" "derived staging dry-run failed" || return 1
    jq -e '.status == "dry_run" and .mode == "staging" and
           [.inputs[].path] == ["wet/alpha/crawl1/a.wet.gz","wet/alpha/crawl1/b.wet.gz"]' \
        <<< "$output" >/dev/null || return 1
}

run_test test_es_upsert_derives_pair_staging
