#!/bin/bash
# Pin external-source retention and SHA-addressed publication.
source "$(dirname "$0")/../../lib/test-lib.sh"

test_es_upsert_external_source_retention() {
    local runtime="$TEST_OUTPUT_DIR/runtime"
    local scripts="$runtime/app/lib/scripts"
    local script="$runtime/es-upsert.sh"
    local inputs="$runtime/inputs"
    local all="$runtime/all"
    local es_cli="$runtime/app/bin/es-cli"
    local call_log="$runtime/es-cli.calls"
    local output

    mkdir -p "$scripts" "$inputs" "$all"
    cp "$DIST_ROOT/es-upsert.sh" "$script"
    cp "$DIST_ROOT/lib/scripts/runtime-lib.sh" "$scripts/runtime-lib.sh"
    make_fake_es_cli "$es_cli" "$call_log" || return 1

    local sha=e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
    : > "$inputs/external.wet.gz"
    output="$(bash "$script" "$inputs/external.wet.gz" --url-id=u --crawl-id=c 2>&1)" || return 1
    assert_file_exists "$inputs/external.wet.gz" || return 1
    assert_file_exists "$all/wet/u/c/$sha.wet.gz" || return 1
    grep -Fq 'published' <<< "$output" || {
        log_fail "External input publication was not reported"
        return 1
    }
}

run_test test_es_upsert_external_source_retention
