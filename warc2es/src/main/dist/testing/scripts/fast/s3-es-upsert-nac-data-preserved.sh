#!/bin/bash
# Ensure --stream=nac-data is preserved and not double-prefixed.
source "$(dirname "$0")/../../lib/test-lib.sh"

test_es_upsert_stream_nac_data_preserved() {
    local script="${DIST_ROOT:-$PROJECT_ROOT/target/dist}/es-upsert.sh"
    assert_file_exists "$script" || return 1

    local input="$TEST_OUTPUT_DIR/20260101-010101-ingest.wet.gz"
    : > "$input"

    local output rc
    set +e
    output=$(bash "$script" "$input" --stream=nac-data --url-id=u --crawl-id=c --dry-run 2>&1)
    rc=$?
    set -e

    assert_command_success "$rc" "es-upsert dry-run with --stream=nac-data should succeed" || return 1
    local selected
    selected=$(sed -n 's/.*selected → \([^ ]*\).*/\1/p' <<< "$output")
    [[ "$selected" == nac-data ]] || {
        log_fail "Expected stream nac-data in dry-run output"
        return 1
    }
}

test_es_upsert_all_forwards_one_resolved_stream() {
    local runtime="$TEST_OUTPUT_DIR/replay-runtime"
    local source_root="$PROJECT_ROOT/src/main/dist"
    local calls="$runtime/delegate.log"
    local pair="$runtime/all/wet/u/c"
    local pending="$pair/pending.wet.gz" digest
    mkdir -p "$runtime/app/lib/scripts" "$pair"
    cp "$source_root/es-upsert-all.sh" "$runtime/es-upsert-all.sh"
    cp "$source_root/lib/scripts/runtime-lib.sh" "$runtime/app/lib/scripts/runtime-lib.sh"
    printf data | gzip > "$pending"
    digest=$(sha256sum -- "$pending")
    digest=${digest%% *}
    mv -- "$pending" "$pair/$digest.wet.gz"
    cat > "$runtime/es-upsert.sh" <<'FAKE_UPSERT'
#!/bin/bash
printf '%q ' "$@" >> "$DELEGATE_LOG"
printf '\n' >> "$DELEGATE_LOG"
printf '%s\n' '{"schema":"warc2es.operator/v1","kind":"invocation","command":"es-upsert","status":"dry_run","exit_code":0,"mode":"archive-replay","inputs":[],"outputs":[],"publication":{"status":"skipped","paths":[]},"processing":null,"error":null}'
FAKE_UPSERT
    chmod +x "$runtime/es-upsert-all.sh" "$runtime/es-upsert.sh"

    local input expected output forwarded
    while IFS='|' read -r input expected; do
        : > "$calls"
        output=$(DELEGATE_LOG="$calls" "$runtime/es-upsert-all.sh" \
            --stream="$input" --dry-run)
        jq -e -s 'length == 2 and .[0].status == "dry_run" and
                  .[1].kind == "summary" and .[1].status == "ok"' \
            <<< "$output" >/dev/null || return 1
        forwarded=$(grep -o -- '--stream=[^ ]*' "$calls" || true)
        [[ "$forwarded" == "--stream=$expected" &&
           "$(grep -o -- '--stream=[^ ]*' "$calls" | wc -l)" -eq 1 ]] || {
            log_fail "es-upsert-all forwarded --stream=$input as '$forwarded'"
            return 1
        }
    done <<'CASES'
nac-data|nac-data
nac-data-release1|nac-data-release1
release1|nac-data-release1
CASES
}

run_test test_es_upsert_stream_nac_data_preserved
run_test test_es_upsert_all_forwards_one_resolved_stream
