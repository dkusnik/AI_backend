#!/bin/bash
# Guard exact stream targeting without contacting Elasticsearch.
set -euo pipefail
source "$(dirname "$0")/../../lib/test-lib.sh"

make_isolated_runtime() {
    local runtime=$1
    local calls=$2
    local source_root="$PROJECT_ROOT/src/main/dist"

    mkdir -p "$runtime/app/bin" "$runtime/app/lib/scripts" "$runtime/all"
    cp "$source_root/es-reinit.sh" "$source_root/es-delete.sh" "$runtime/"
    cp "$source_root/lib/scripts/runtime-lib.sh" "$runtime/app/lib/scripts/"
    chmod +x "$runtime/es-reinit.sh" "$runtime/es-delete.sh"
    make_fake_es_cli "$runtime/app/bin/es-cli" "$calls"
}

parse_target() {
    sed -n 's/.* target=\([^ ]*\).*/\1/p' <<<"$1"
}

captured_data_stream_url() {
    local calls="$1"
    awk '{for (i = 1; i <= NF; i++) if ($i ~ /^http:\/\/localhost:9200\/_data_stream\//) print $i}' \
        "$calls"
}

test_reinit_maps_once_and_forwards_exact_stream() {
    local runtime="$TEST_OUTPUT_DIR/reinit-roundtrip"
    local calls="$runtime/calls.log"
    make_isolated_runtime "$runtime" "$calls"

    local input expected
    while IFS='|' read -r input expected; do
        : > "$calls"
        "$runtime/es-reinit.sh" --stream="$input" --yes >/dev/null
        [[ "$(sed -n '1p' "$calls")" == check-health* &&
           "$(sed -n '2p' "$calls")" == "purge $expected" &&
           "$(sed -n '3p' "$calls")" == "init $expected" &&
           "$(sed -n '4p' "$calls")" == "get-stream $expected" &&
           "$(wc -l < "$calls")" -eq 4 ]] || {
            log_fail "--stream=$input was not forwarded exactly once as $expected"
            return 1
        }
    done <<'CASES'
nac-data|nac-data
nac-data-release1|nac-data-release1
release1|nac-data-release1
CASES
}

test_low_level_cli_requires_exact_stream_names() {
    local fake_bin="$TEST_OUTPUT_DIR/fake-bin"
    local calls="$TEST_OUTPUT_DIR/curl.log"
    local cli="$PROJECT_ROOT/src/main/dist/bin/es-cli"
    local output rc
    mkdir -p "$fake_bin"
    cat > "$fake_bin/curl" <<'FAKE_CURL'
#!/bin/bash
printf '%s\n' "$*" >> "$CURL_LOG"
printf '{"acknowledged":true}\n200\n'
FAKE_CURL
    chmod +x "$fake_bin/curl"

    : > "$calls"
    PATH="$fake_bin:$PATH" CURL_LOG="$calls" ES_CLI_CONF=/dev/null ES_PASS='' \
        "$cli" init nac-data >/dev/null
    [[ "$(captured_data_stream_url "$calls")" == \
       'http://localhost:9200/_data_stream/nac-data' ]] || {
        log_fail "low-level init did not target bare nac-data exactly"
        return 1
    }

    : > "$calls"
    PATH="$fake_bin:$PATH" CURL_LOG="$calls" ES_CLI_CONF=/dev/null ES_PASS='' \
        "$cli" init >/dev/null
    [[ "$(captured_data_stream_url "$calls")" == \
       'http://localhost:9200/_data_stream/nac-data-default' ]] || {
        log_fail "no-argument low-level init lost the nac-data-default target"
        return 1
    }

    : > "$calls"
    PATH="$fake_bin:$PATH" CURL_LOG="$calls" ES_CLI_CONF=/dev/null ES_PASS='' \
        "$cli" purge nac-data-release1 >/dev/null
    [[ "$(captured_data_stream_url "$calls")" == \
       'http://localhost:9200/_data_stream/nac-data-release1' ]] || {
        log_fail "low-level purge did not target nac-data-release1 exactly"
        return 1
    }

    : > "$calls"
    PATH="$fake_bin:$PATH" CURL_LOG="$calls" ES_CLI_CONF=/dev/null ES_PASS='' \
        "$cli" purge >/dev/null
    [[ "$(captured_data_stream_url "$calls")" == \
       'http://localhost:9200/_data_stream/nac-data-default' ]] || {
        log_fail "no-argument low-level purge lost the nac-data-default target"
        return 1
    }

    local -a invalid=(release1 'nac-data-*' '')
    local value
    for value in "${invalid[@]}"; do
        : > "$calls"
        set +e
        output=$(PATH="$fake_bin:$PATH" CURL_LOG="$calls" ES_CLI_CONF=/dev/null ES_PASS='' \
            "$cli" purge "$value" 2>&1)
        rc=$?
        set -e
        [[ "$rc" -ne 0 && -n "$output" && ! -s "$calls" ]] || {
            log_fail "low-level purge accepted non-exact stream '$value'"
            return 1
        }
    done

    : > "$calls"
    set +e
    output=$(PATH="$fake_bin:$PATH" CURL_LOG="$calls" ES_CLI_CONF=/dev/null ES_PASS='' \
        "$cli" init nac-data extra 2>&1)
    rc=$?
    set -e
    [[ "$rc" -ne 0 && -n "$output" && ! -s "$calls" ]] || {
        log_fail "low-level init accepted an extra argument"
        return 1
    }
}

test_template_covers_bare_and_prefixed_streams() {
    local template="$PROJECT_ROOT/src/main/dist/conf/elasticsearch/templates/nac-data-template.json"
    jq -e '.index_patterns == ["nac-data", "nac-data-*"] and has("data_stream")' \
        "$template" >/dev/null || {
        log_fail "nac-data template does not cover exactly bare and prefixed streams"
        return 1
    }
}

test_delete_stream_shorthand_targets_exactly() {
    local runtime="$TEST_OUTPUT_DIR/delete-target"
    local calls="$runtime/calls.log"
    make_isolated_runtime "$runtime" "$calls"

    local probe original_defect
    original_defect=$(parse_target "[es-delete] target=nac-data-nac-data es=http://x")
    [[ "$original_defect" != "nac-data" ]] || {
        log_fail "Exact-target assertion accepts the original double-prefix defect"
        return 1
    }

    while IFS='|' read -r stream expected; do
        : > "$calls"
        probe=$("$runtime/es-delete.sh" \
            --stream="$stream" --url-id=a --crawl-id=b --dry-run 2>&1)
        [[ "$(parse_target "$probe")" == "$expected" ]] || {
            log_fail "--stream=$stream did not resolve exactly to $expected"
            return 1
        }
        [[ ! -s "$calls" ]] || {
            log_fail "es-delete dry-run invoked es-cli"
            return 1
        }
    done <<'CASES'
nac-data|nac-data
nac-data-release1|nac-data-release1
release1|nac-data-release1
CASES
}

run_test test_reinit_maps_once_and_forwards_exact_stream
run_test test_low_level_cli_requires_exact_stream_names
run_test test_template_covers_bare_and_prefixed_streams
run_test test_delete_stream_shorthand_targets_exactly
