#!/bin/bash
# Directory ingestion remains an accepted direct es-upsert input form.
source "$(dirname "$0")/../../lib/test-lib.sh"

test_es_upsert_directory_characterization() {
    local runtime="$TEST_OUTPUT_DIR/runtime"
    local input="$runtime/input"
    local calls="$runtime/calls"
    mkdir -p "$runtime/app/bin" "$runtime/app/lib/scripts" "$input" "$runtime/all"
    cp "$PROJECT_ROOT/src/main/dist/es-upsert.sh" "$runtime/es-upsert.sh"
    cp "$PROJECT_ROOT/src/main/dist/lib/scripts/runtime-lib.sh" "$runtime/app/lib/scripts/runtime-lib.sh"
    make_fake_es_cli "$runtime/app/bin/es-cli" "$calls" || return 1
    : > "$input/20260101-000000-ingest-u-c.wet.gz"

    local output rc
    set +e
    output=$(bash "$runtime/es-upsert.sh" "$input" --url-id=u --crawl-id=c 2>&1)
    rc=$?
    set -e
    assert_command_success "$rc" "directory ingestion must remain accepted: $output"
}

run_test test_es_upsert_directory_characterization
