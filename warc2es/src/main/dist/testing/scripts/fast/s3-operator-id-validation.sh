#!/bin/bash
# Validate operator provenance identifiers without rewriting them.
source "$(dirname "$0")/../../lib/test-lib.sh"

test_operator_id_validation() {
    local runtime="$TEST_OUTPUT_DIR/runtime"
    local scripts="$runtime/app/lib/scripts"
    local upsert="$runtime/es-upsert.sh"
    local delete="$runtime/es-delete.sh"
    local warc2wet="$runtime/warc2wet.sh"
    local wet_input="$runtime/artifact.wet.gz"
    local warc_input="$runtime/artifact.warc"
    local warc_args="$runtime/warc-args"
    local es_args="$runtime/es-args"
    local overlong invalid entry option output rc
    local -a args invalid_values

    mkdir -p "$runtime/app/bin" "$scripts" "$runtime/all"
    cp "$PROJECT_ROOT/src/main/dist/es-upsert.sh" "$upsert"
    cp "$PROJECT_ROOT/src/main/dist/es-delete.sh" "$delete"
    cp "$PROJECT_ROOT/src/main/dist/warc2wet.sh" "$warc2wet"
    cp "$PROJECT_ROOT/src/main/dist/lib/scripts/runtime-lib.sh" "$scripts/runtime-lib.sh"

cat > "$runtime/app/bin/es-cli" <<'FAKE_ES'
#!/bin/bash
printf '%s\n' "$@" > "$(dirname "$0")/../../es-args"
if [[ "${1:-}" == batch-delete ]]; then
    printf '%s\n' '{"total":0,"deleted":0,"version_conflicts":0,"timed_out":false,"failures":[]}'
fi
FAKE_ES
    chmod +x "$runtime/app/bin/es-cli" "$delete"

    cat > "$scripts/pipeline-lib" <<'FAKE_PIPELINE'
run_pipeline() {
    printf '%s\n' "$@" > "$WARC_ARGS_CAPTURE"
}
FAKE_PIPELINE

    local warcinfo_payload
    warcinfo_payload=$'software: NAC WARC Pipeline 1.0\r\nX-NAC-URL-ID: My-Site\r\nX-NAC-Crawl-ID: Crawl.1\r\n'
    {
        printf 'WARC/1.1\r\nWARC-Type: warcinfo\r\nContent-Type: application/warc-fields\r\nContent-Length: %s\r\n\r\n%s\r\n\r\n' \
            "${#warcinfo_payload}" "$warcinfo_payload"
        printf 'WARC/1.0\r\nWARC-Type: conversion\r\nX-NAC-URL-ID: My-Site\r\nX-NAC-Crawl-ID: Crawl.1\r\nContent-Length: 7\r\n\r\nfixture\r\n\r\n'
    } | gzip > "$wet_input"
    printf 'WARC/1.0\r\nWARC-Date: 2026-01-02T03:04:05Z\r\n\r\n' > "$warc_input"

    output=$(bash "$upsert" "$wet_input" --url-id=My-Site --crawl-id=Crawl.1 \
        --result-format=human 2>&1)
    rc=$?
    assert_command_success "$rc" "es-upsert rejected valid identifiers: $output" || return 1
    grep -Fxq -- '--url-id=My-Site' "$es_args" || {
        log_fail "es-upsert rewrote the expected URL identifier"
        return 1
    }
    grep -Fxq -- '--crawl-id=Crawl.1' "$es_args" || {
        log_fail "es-upsert rewrote the expected crawl identifier"
        return 1
    }

    mkdir -p "$runtime/input-dir"
    : > "$runtime/input-dir/20260101-000000-ingest-other-pair.wet.gz"
    output=$(bash "$upsert" "$runtime/input-dir" \
        --url-id=My-Site --crawl-id=Crawl.1 --dry-run 2>&1)
    rc=$?
    assert_command_success "$rc" "es-upsert rejected a directory expected-value check: $output" || return 1
    echo "$output" | grep -Fq '20260101-000000-ingest-other-pair.wet.gz' || {
        log_fail "es-upsert hid a potential provenance mismatch with filename filtering"
        return 1
    }

    output=$(WARC_ARGS_CAPTURE="$warc_args" bash "$warc2wet" \
        --url-id=My-Site --crawl-id=Crawl.1 --result-format=human "$warc_input" 2>&1)
    rc=$?
    assert_command_success "$rc" "warc2wet rejected valid identifiers: $output" || return 1
    grep -Fxq -- '--url-id=My-Site' "$warc_args" || {
        log_fail "warc2wet did not pass the exact URL identifier to Java"
        return 1
    }
    grep -Fxq -- '--crawl-id=Crawl.1' "$warc_args" || {
        log_fail "warc2wet did not pass the exact crawl identifier to Java"
        return 1
    }

    output=$(bash "$delete" --stream=test --url-id=My-Site --crawl-id=Crawl.1 \
        --dry-run --result-format=json 2> "$runtime/delete.stderr")
    rc=$?
    assert_command_success "$rc" "es-delete rejected valid identifiers: $output" || return 1
    jq -e '.target.url_id == "My-Site" and .target.crawl_id == "Crawl.1"' \
        <<< "$output" >/dev/null || {
        log_fail "es-delete rewrote valid identifiers"
        return 1
    }

    printf -v overlong '%0129d' 0
    invalid_values=('' '.' '..' 'contains space' 'bad/value' 'żółć' "$overlong")
    for invalid in "${invalid_values[@]}"; do
        for entry in es-upsert es-delete warc2wet; do
            for option in --url-id --crawl-id; do
                if [[ "$option" == --url-id ]]; then
                    args=("--url-id=$invalid" --crawl-id=Valid)
                else
                    args=(--url-id=Valid "--crawl-id=$invalid")
                fi

                set +e
                if [[ "$entry" == es-upsert ]]; then
                    output=$(bash "$upsert" "$wet_input" "${args[@]}" --dry-run 2>&1)
                elif [[ "$entry" == es-delete ]]; then
                    output=$(bash "$delete" --stream=test "${args[@]}" --dry-run 2>&1)
                else
                    output=$(WARC_ARGS_CAPTURE="$warc_args" bash "$warc2wet" \
                        "${args[@]}" "$warc_input" 2>&1)
                fi
                rc=$?
                set -e

                assert_command_failure "$rc" "$entry accepted invalid $option" || return 1
                echo "$output" | grep -Fq -- "$option must match [A-Za-z0-9._-]{1,128}" || {
                    log_fail "$entry emitted the wrong invalid-$option diagnostic: $output"
                    return 1
                }
            done
        done
    done
}

run_test test_operator_id_validation
