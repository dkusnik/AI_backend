#!/bin/bash
# OWNER: B2-002
# Provenance transaction, publication, replay, locking, and JVM sequencing contract.
source "$(dirname "$0")/../../lib/test-lib.sh"

EMPTY_SHA=e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
declare -A BACKGROUND_GROUPS=()

register_background() {
    BACKGROUND_GROUPS["$1"]="$2"
}

unregister_background() {
    unset "BACKGROUND_GROUPS[$1]"
}

cleanup_background_processes() {
    local pid
    for pid in "${!BACKGROUND_GROUPS[@]}"; do
        if [[ "${BACKGROUND_GROUPS[$pid]}" == true ]]; then
            kill -- "-$pid" 2>/dev/null || true
        fi
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
        unset "BACKGROUND_GROUPS[$pid]"
    done
}
trap cleanup_background_processes EXIT

install_runtime() {
    local runtime="$1"
    mkdir -p "$runtime/app/bin" "$runtime/app/lib/scripts" "$runtime/wet" "$runtime/all"
    cp "$PROJECT_ROOT/src/main/dist/es-upsert.sh" "$runtime/es-upsert.sh"
    cp "$PROJECT_ROOT/src/main/dist/es-upsert-all.sh" "$runtime/es-upsert-all.sh"
    cp "$PROJECT_ROOT/src/main/dist/lib/scripts/runtime-lib.sh" \
        "$runtime/app/lib/scripts/runtime-lib.sh"
    cat > "$runtime/app/bin/es-cli" <<'FAKE_ES'
#!/bin/bash
command_name="${1:-}"
{
    printf '%q' "${1:-}"
    if [[ $# -gt 0 ]]; then
        shift
        printf ' %q' "$@"
    fi
    printf '\n'
} >> "$CALL_LOG"

if [[ "$command_name" == refresh ]]; then
    [[ "${FAKE_REFRESH_FAIL:-false}" != true ]] || exit 5
    exit 0
fi

if [[ "$command_name" == batch-delete ]]; then
    if [[ "${FAKE_DELETE_FAIL:-false}" == true ]]; then
        exit 6
    fi
    if [[ "${FAKE_DELETE_INCOMPLETE:-false}" == true ]]; then
        jq -cn '{timed_out:true,version_conflicts:1,failures:[]}'
        exit 0
    fi
    if [[ "${FAKE_DELETE_PARTIAL:-false}" == true ]]; then
        jq -cn '{total:3,deleted:2,version_conflicts:0,timed_out:false,failures:[]}'
        exit 0
    fi
    if [[ "${FAKE_DELETE_EMPTY:-false}" == true ]]; then
        exit 0
    fi
    jq -cn '{total:0,deleted:0,version_conflicts:0,timed_out:false,failures:[]}'
    exit 0
fi

if [[ "$command_name" == load-stream && -n "${LOAD_SHA_LOG:-}" ]]; then
    sha256sum -- "${1:-}" > "$LOAD_SHA_LOG"
fi

if [[ "$command_name" == load-stream && -n "${BLOCK_READY:-}" ]]; then
    : > "$BLOCK_READY"
    while [[ ! -e "${BLOCK_RELEASE:-}" ]]; do
        sleep 0.02
    done
fi

json_mode=false
for argument in "$@"; do
    [[ "$argument" == --result-format=json ]] && json_mode=true
done
if [[ "${FAKE_LOAD_FAIL:-false}" == true ]]; then
    if [[ "$json_mode" == true ]]; then
        jq -cn '{schema:"warc2es.processing/v1",status:"error",exit_code:7,
          records_in:1,records_out:0,records_indexed:0,records_skipped:1,
          errors:1,elapsed_ms:1,error:{code:"processing_failed",message:"fixture"},
          metrics:{schema:"warc2es.metrics/v1",counters:{}}}'
    fi
    exit 7
fi
if [[ "$json_mode" == true ]]; then
    jq -cn '{schema:"warc2es.processing/v1",status:"ok",exit_code:0,
      records_in:1,records_out:1,records_indexed:1,records_skipped:0,
      errors:0,elapsed_ms:1,error:null,
      metrics:{schema:"warc2es.metrics/v1",counters:{}}}'
fi
FAKE_ES
    chmod +x "$runtime/es-upsert.sh" "$runtime/es-upsert-all.sh" \
        "$runtime/app/bin/es-cli"
    : > "$runtime/calls"
}

make_wet() {
    local output="$1" url_id="$2" crawl_id="$3" payload="${4:-payload}"
    local payload_length=${#payload}
    mkdir -p "$(dirname "$output")"
    printf 'WARC/1.0\r\nWARC-Type: conversion\r\nX-NAC-URL-ID: %s\r\nX-NAC-Crawl-ID: %s\r\nContent-Length: %s\r\n\r\n%s\r\n\r\n' \
        "$url_id" "$crawl_id" "$payload_length" "$payload" | gzip > "$output"
}

wet_sha() {
    sha256sum -- "$1" | awk '{print $1}'
}

pair_manifest() {
    local pair="$1" output="$2" file
    : > "$output"
    [[ -d "$pair" ]] || return 0
    while IFS= read -r -d '' file; do
        stat -c '%n %F %a %i %s %Y' -- "$file" >> "$output" || return 1
        [[ -f "$file" ]] && sha256sum -- "$file" >> "$output"
    done < <(find "$pair" -mindepth 1 -print0 | LC_ALL=C sort -z)
}

run_upsert() {
    local runtime="$1" output="$2"
    shift 2
    set +e
    CALL_LOG="$runtime/calls" "$runtime/es-upsert.sh" "$@" >"$output" 2>"$output.stderr"
    COMMAND_RC=$?
    set -e
}

case_mandatory_pair_and_preflight() {
    local runtime="$TEST_OUTPUT_DIR/preflight"
    local input="$runtime/input"
    local output="$runtime/output"
    local before="$runtime/before" after="$runtime/after"
    install_runtime "$runtime"
    mkdir -p "$input"

    run_upsert "$runtime" "$output" "$input" --url-id=u --crawl-id=c --dry-run
    assert_command_failure "$COMMAND_RC" "empty normal directory succeeded" || return 1
    [[ ! -s "$runtime/calls" ]] || { log_fail "empty directory reached es-cli"; return 1; }

    : > "$input/empty.wet.gz"
    run_upsert "$runtime" "$output" "$input/empty.wet.gz" --dry-run
    assert_command_failure "$COMMAND_RC" "normal input without a pair succeeded" || return 1
    run_upsert "$runtime" "$output" "$input/empty.wet.gz" --url-id=u --crawl-id=c \
        --result-format=json
    assert_command_success "$COMMAND_RC" "explicit zero-record WET transaction failed" || return 1
    assert_file_exists "$runtime/all/wet/u/c/$EMPTY_SHA.wet.gz" || return 1
    [[ "$(grep -c '^load-stream ' "$runtime/calls" || true)" -eq 0 ]] || {
        log_fail "zero-record WET unnecessarily reached the Java loader"
        return 1
    }
    jq -e '.status == "ok" and .processing == null' \
        "$output" >/dev/null || {
        log_fail "zero-record WET invented a Java processing result"
        return 1
    }
    run_upsert "$runtime" "$output" --all --url-id=u --crawl-id=c
    assert_command_failure "$COMMAND_RC" "retired --all mode succeeded" || return 1

    : > "$runtime/calls"
    make_wet "$input/wrong.wet.gz" other c
    run_upsert "$runtime" "$output" "$input/wrong.wet.gz" --url-id=u --crawl-id=c
    assert_command_failure "$COMMAND_RC" "mismatched provenance succeeded" || return 1
    [[ ! -s "$runtime/calls" ]] || { log_fail "provenance mismatch reached es-cli"; return 1; }

    mkdir -p "$input/mixed"
    make_wet "$input/mixed/a-valid.wet.gz" u c valid
    make_wet "$input/mixed/z-wrong.wet.gz" other c wrong
    pair_manifest "$runtime/all/wet/u/c" "$before" || return 1
    : > "$runtime/calls"
    run_upsert "$runtime" "$output" "$input/mixed" --url-id=u --crawl-id=c \
        --result-format=json
    assert_command_failure "$COMMAND_RC" "mixed-provenance set succeeded" || return 1
    [[ ! -s "$runtime/calls" ]] || {
        log_fail "complete-set provenance failure reached es-cli"
        return 1
    }
    pair_manifest "$runtime/all/wet/u/c" "$after" || return 1
    cmp -s "$before" "$after" || {
        log_fail "complete-set provenance failure changed the published set"
        return 1
    }
    [[ "$(wc -l < "$output")" -eq 1 ]] &&
        jq -e '.status == "error" and .error.code == "provenance_invalid" and
               [.inputs[].path] == ["a-valid.wet.gz","z-wrong.wet.gz"]' \
          "$output" >/dev/null || {
        log_fail "JSON preflight failure did not emit one operator error object"
        return 1
    }

    for unsafe_stream in 'nac-data-*' '*' 'one,two' 'bad/name'; do
        : > "$runtime/calls"
        run_upsert "$runtime" "$output" "$input/empty.wet.gz" --url-id=u --crawl-id=c \
            --stream="$unsafe_stream"
        assert_command_failure "$COMMAND_RC" "unsafe stream target succeeded: $unsafe_stream" || return 1
        [[ ! -s "$runtime/calls" ]] || {
            log_fail "unsafe stream target reached es-cli: $unsafe_stream"
            return 1
        }
    done

    : > "$runtime/calls"
    run_upsert "$runtime" "$output" "$input/empty.wet.gz" --url-id=u --crawl-id=c \
        --start-date=2026-02-30
    assert_command_failure "$COMMAND_RC" "invalid calendar date reached replacement" || return 1
    [[ ! -s "$runtime/calls" ]] || { log_fail "invalid date reached es-cli"; return 1; }

    # A header-looking mismatch inside the byte-counted payload is not a record header.
    make_wet "$input/payload.wet.gz" u c $'X-NAC-URL-ID: wrong\n'
    run_upsert "$runtime" "$output" "$input/payload.wet.gz" --url-id=u --crawl-id=c --dry-run
    assert_command_success "$COMMAND_RC" "payload text was mistaken for a WARC header" || return 1
}

case_external_publication_and_one_jvm() {
    local runtime="$TEST_OUTPUT_DIR/external"
    local input="$runtime/input.wet.gz"
    local output="$runtime/result.json"
    local sha destination source_before
    local before="$runtime/before" after="$runtime/after"
    local -a call_lines=() refresh_args=() delete_args=() load_args=()
    install_runtime "$runtime"
    make_wet "$input" site crawl data
    sha="$(wet_sha "$input")"
    destination="$runtime/all/wet/site/crawl/$sha.wet.gz"

    run_upsert "$runtime" "$output" "$input" --url-id=site --crawl-id=crawl \
        --result-format=json
    assert_command_success "$COMMAND_RC" "external upsert failed" || return 1
    assert_file_exists "$input" || return 1
    assert_file_exists "$destination" || return 1
    [[ "$(wet_sha "$destination")" == "$sha" ]] || {
        log_fail "published filename does not match stored bytes"
        return 1
    }
    [[ "$(grep -c '^refresh ' "$runtime/calls")" -eq 1 &&
       "$(grep -c '^batch-delete ' "$runtime/calls")" -eq 1 &&
       "$(grep -c '^load-stream ' "$runtime/calls")" -eq 1 ]] || {
        log_fail "pair transaction did not perform one refresh, delete, and JVM load"
        return 1
    }
    mapfile -t call_lines < "$runtime/calls"
    [[ ${#call_lines[@]} -eq 3 ]] || {
        log_fail "pair replacement emitted unexpected es-cli calls"
        return 1
    }
    eval "refresh_args=(${call_lines[0]})"
    eval "delete_args=(${call_lines[1]})"
    eval "load_args=(${call_lines[2]})"
    [[ "${refresh_args[0]}" == refresh &&
       "${refresh_args[1]}" == nac-data-default ]] || {
        log_fail "pre-delete refresh was not the first exact-target call"
        return 1
    }
    [[ "${delete_args[0]}" == batch-delete && "${delete_args[1]}" == nac-data-default ]] || {
        log_fail "pair deletion did not follow the exact-target refresh"
        return 1
    }
    jq -e --arg url site --arg crawl crawl '
      . == {query:{bool:{filter:[{"term":{"nac-url-id":$url}},
                                {"term":{"nac-crawl-id":$crawl}}]}}}
    ' <<<"${delete_args[2]}" >/dev/null || {
        log_fail "pair deletion query was not the exact provenance conjunction"
        return 1
    }
    [[ "${load_args[0]}" == load-stream && "${load_args[2]}" == nac-data-default ]] || {
        log_fail "one complete load did not follow pair deletion"
        return 1
    }
    jq -e --arg path "all/wet/site/crawl/$sha.wet.gz" '
      .status == "ok" and .mode == "explicit" and
      .publication == {status:"published",paths:[$path]} and
      .processing.schema == "warc2es.processing/v1"
    ' "$output" >/dev/null || return 1

    source_before="$(wet_sha "$input")"
    pair_manifest "$runtime/all/wet/site/crawl" "$before" || return 1
    : > "$runtime/calls"
    set +e
    CALL_LOG="$runtime/calls" FAKE_REFRESH_FAIL=true "$runtime/es-upsert.sh" "$input" \
        --url-id=site --crawl-id=crawl --result-format=json \
        >"$output" 2>"$output.stderr"
    COMMAND_RC=$?
    set -e
    assert_command_failure "$COMMAND_RC" "failed pre-delete refresh was accepted" || return 1
    mapfile -t call_lines < "$runtime/calls"
    [[ ${#call_lines[@]} -eq 1 && "${call_lines[0]}" == 'refresh nac-data-default' ]] || {
        log_fail "failed pre-delete refresh reached delete or load"
        return 1
    }
    jq -e '.status == "error" and
           .error.code == "elasticsearch_refresh_failed"' "$output" >/dev/null || {
        log_fail "refresh failure did not emit the stable operator error"
        return 1
    }
    [[ "$(wet_sha "$input")" == "$source_before" ]] || {
        log_fail "refresh failure changed the external source"
        return 1
    }
    pair_manifest "$runtime/all/wet/site/crawl" "$after" || return 1
    cmp -s "$before" "$after" || {
        log_fail "refresh failure changed the published set"
        return 1
    }

    for delete_mode in fail incomplete partial empty; do
        : > "$runtime/calls"
        set +e
        case "$delete_mode" in
            fail)
                CALL_LOG="$runtime/calls" FAKE_DELETE_FAIL=true "$runtime/es-upsert.sh" "$input" \
                    --url-id=site --crawl-id=crawl >"$output" 2>"$output.stderr"
                ;;
            incomplete)
                CALL_LOG="$runtime/calls" FAKE_DELETE_INCOMPLETE=true "$runtime/es-upsert.sh" "$input" \
                    --url-id=site --crawl-id=crawl >"$output" 2>"$output.stderr"
                ;;
            partial)
                CALL_LOG="$runtime/calls" FAKE_DELETE_PARTIAL=true "$runtime/es-upsert.sh" "$input" \
                    --url-id=site --crawl-id=crawl >"$output" 2>"$output.stderr"
                ;;
            empty)
                CALL_LOG="$runtime/calls" FAKE_DELETE_EMPTY=true "$runtime/es-upsert.sh" "$input" \
                    --url-id=site --crawl-id=crawl >"$output" 2>"$output.stderr"
                ;;
        esac
        COMMAND_RC=$?
        set -e
        assert_command_failure "$COMMAND_RC" "$delete_mode pair deletion was accepted" || return 1
        [[ "$(grep -c '^load-stream ' "$runtime/calls" || true)" -eq 0 ]] || {
            log_fail "$delete_mode pair deletion reached the Java loader"
            return 1
        }
        [[ "$(wet_sha "$input")" == "$source_before" ]] || {
            log_fail "$delete_mode pair deletion changed the external source"
            return 1
        }
        pair_manifest "$runtime/all/wet/site/crawl" "$after" || return 1
        cmp -s "$before" "$after" || {
            log_fail "$delete_mode pair deletion changed the published set"
            return 1
        }
    done
}

case_pair_staging_extends_published_set() {
    local runtime="$TEST_OUTPUT_DIR/staging-union"
    local seed="$runtime/seed.wet.gz"
    local staged_a="$runtime/wet/site/crawl/a.wet.gz"
    local staged_b="$runtime/wet/site/crawl/b.wet.gz"
    local staged_failed="$runtime/wet/site/crawl/c.wet.gz"
    local output="$runtime/output"
    local seed_sha a_sha b_sha archive_count
    install_runtime "$runtime"

    make_wet "$seed" site crawl seed
    seed_sha="$(wet_sha "$seed")"
    run_upsert "$runtime" "$output" "$seed" --url-id=site --crawl-id=crawl
    assert_command_success "$COMMAND_RC" "seed publication failed" || return 1

    make_wet "$staged_b" site crawl staged-b
    make_wet "$staged_a" site crawl staged-a
    a_sha="$(wet_sha "$staged_a")"
    b_sha="$(wet_sha "$staged_b")"
    : > "$runtime/calls"
    run_upsert "$runtime" "$output" --url-id=site --crawl-id=crawl
    assert_command_success "$COMMAND_RC" "pair-only staged union failed" || return 1
    [[ ! -e "$staged_a" && ! -e "$staged_b" ]] || {
        log_fail "successful pair-only ingestion left consumed staging files"
        return 1
    }
    for digest in "$seed_sha" "$a_sha" "$b_sha"; do
        assert_file_exists "$runtime/all/wet/site/crawl/$digest.wet.gz" || return 1
    done
    [[ "$(grep -c '^load-stream ' "$runtime/calls")" -eq 1 ]] || {
        log_fail "pair-only union did not use exactly one Java load"
        return 1
    }
    jq -e --arg seed "all/wet/site/crawl/$seed_sha.wet.gz" \
        --arg staged_a "wet/site/crawl/a.wet.gz" \
        --arg staged_b "wet/site/crawl/b.wet.gz" '
      .status == "ok" and .mode == "staging" and
      [.inputs[].path] == [$seed,$staged_a,$staged_b] and
      (.publication.paths | length) == 3
    ' "$output" >/dev/null || return 1

    make_wet "$staged_failed" site crawl staged-failure
    : > "$runtime/calls"
    set +e
    CALL_LOG="$runtime/calls" FAKE_LOAD_FAIL=true "$runtime/es-upsert.sh" \
        --url-id=site --crawl-id=crawl >"$output" 2>"$output.stderr"
    COMMAND_RC=$?
    set -e
    assert_command_failure "$COMMAND_RC" "failed pair-only Java load succeeded" || return 1
    assert_file_exists "$staged_failed" || return 1
    archive_count="$(find "$runtime/all/wet/site/crawl" -maxdepth 1 -name '*.wet.gz' | wc -l)"
    [[ "$archive_count" -eq 3 ]] || {
        log_fail "failed staged union changed the published set"
        return 1
    }
}

case_managed_idempotence_replacement_and_failure() {
    local runtime="$TEST_OUTPUT_DIR/managed"
    local output="$runtime/output"
    local first="$runtime/wet/first.wet.gz"
    local duplicate="$runtime/wet/duplicate.wet.gz"
    local second="$runtime/wet/second.wet.gz"
    local failed="$runtime/wet/failed.wet.gz"
    local first_sha second_sha failed_sha
    install_runtime "$runtime"

    make_wet "$first" u c first
    first_sha="$(wet_sha "$first")"
    run_upsert "$runtime" "$output" "$first" --url-id=u --crawl-id=c
    assert_command_success "$COMMAND_RC" "managed publication failed" || return 1
    [[ ! -e "$first" ]] || { log_fail "managed source was not moved"; return 1; }
    assert_file_exists "$runtime/all/wet/u/c/$first_sha.wet.gz" || return 1

    : > "$runtime/all/wet/u/c/.$first_sha.tmp.STALE1"
    cp "$runtime/all/wet/u/c/$first_sha.wet.gz" "$duplicate"
    run_upsert "$runtime" "$output" "$duplicate" --url-id=u --crawl-id=c
    assert_command_success "$COMMAND_RC" "same-SHA retry failed" || return 1
    [[ ! -e "$duplicate" ]] || { log_fail "same-SHA managed source was not removed"; return 1; }
    [[ ! -e "$runtime/all/wet/u/c/.$first_sha.tmp.STALE1" ]] || {
        log_fail "retry did not remove a stale sibling publication temporary"
        return 1
    }

    make_wet "$second" u c second
    second_sha="$(wet_sha "$second")"
    run_upsert "$runtime" "$output" "$second" --url-id=u --crawl-id=c
    assert_command_success "$COMMAND_RC" "replacement publication failed" || return 1
    [[ ! -e "$runtime/all/wet/u/c/$first_sha.wet.gz" ]] || {
        log_fail "superseded published WET was retained"
        return 1
    }
    assert_file_exists "$runtime/all/wet/u/c/$second_sha.wet.gz" || return 1

    make_wet "$failed" u c failed
    failed_sha="$(wet_sha "$failed")"
    set +e
    CALL_LOG="$runtime/calls" FAKE_LOAD_FAIL=true "$runtime/es-upsert.sh" "$failed" \
        --url-id=u --crawl-id=c >"$output" 2>"$output.stderr"
    COMMAND_RC=$?
    set -e
    assert_command_failure "$COMMAND_RC" "failed Java load succeeded" || return 1
    assert_file_exists "$failed" || return 1
    assert_file_exists "$runtime/all/wet/u/c/$second_sha.wet.gz" || return 1
    [[ ! -e "$runtime/all/wet/u/c/$failed_sha.wet.gz" ]] || {
        log_fail "failed load was published"
        return 1
    }
}

case_external_input_is_snapshotted() {
    local runtime="$TEST_OUTPUT_DIR/snapshot"
    local input="$runtime/input.wet.gz"
    local output="$runtime/output"
    local ready="$runtime/ready" release="$runtime/release"
    local original_sha pid rc
    install_runtime "$runtime"
    make_wet "$input" site crawl original
    original_sha="$(wet_sha "$input")"

    setsid env CALL_LOG="$runtime/calls" LOAD_SHA_LOG="$runtime/load-sha" \
        BLOCK_READY="$ready" BLOCK_RELEASE="$release" \
        "$runtime/es-upsert.sh" "$input" --url-id=site --crawl-id=crawl \
        >"$output" 2>"$output.stderr" &
    pid=$!
    register_background "$pid" true
    wait_for_file "$ready" || { log_fail "blocked Java load did not start"; return 1; }
    [[ "$(awk '{print $1}' "$runtime/load-sha")" == "$original_sha" ]] || {
        log_fail "Java loader did not consume the verified transaction snapshot"
        return 1
    }
    make_wet "$input" site crawl changed-after-snapshot
    : > "$release"
    set +e
    wait "$pid"
    rc=$?
    set -e
    unregister_background "$pid"
    assert_command_success "$rc" "snapshotted external transaction failed" || return 1
    assert_file_exists "$runtime/all/wet/site/crawl/$original_sha.wet.gz" || return 1
    [[ "$(wet_sha "$runtime/all/wet/site/crawl/$original_sha.wet.gz")" == "$original_sha" ]] || {
        log_fail "published bytes differ from the bytes sent to Java"
        return 1
    }
    [[ "$(wet_sha "$input")" != "$original_sha" ]] || {
        log_fail "mutation fixture did not change the caller's live input"
        return 1
    }
}

case_archive_replay_and_corruption() {
    local runtime="$TEST_OUTPUT_DIR/replay"
    local source="$runtime/source.wet.gz"
    local source_two="$runtime/source-two.wet.gz"
    local output="$runtime/result.json"
    local sha sha_two pair before after
    install_runtime "$runtime"
    make_wet "$source" u c replay
    sha="$(wet_sha "$source")"
    make_wet "$source_two" u c replay-two
    sha_two="$(wet_sha "$source_two")"
    pair="$runtime/all/wet/u/c"
    mkdir -p "$pair"
    cp "$source" "$pair/$sha.wet.gz"
    cp "$source_two" "$pair/$sha_two.wet.gz"
    pair_manifest "$pair" "$runtime/before" || return 1

    run_upsert "$runtime" "$output" --from-archive "$pair" --result-format=json
    assert_command_success "$COMMAND_RC" "archive replay failed" || return 1
    pair_manifest "$pair" "$runtime/after" || return 1
    cmp -s "$runtime/before" "$runtime/after" || {
        log_fail "archive replay changed the published pair manifest"
        return 1
    }
    jq -e --arg path "all/wet/u/c/$sha.wet.gz" --arg path_two "all/wet/u/c/$sha_two.wet.gz" '
      .mode == "archive-replay" and
      .publication.status == "unchanged" and
      (.publication.paths | sort) == ([$path,$path_two] | sort)' "$output" >/dev/null || return 1

    install_runtime "$TEST_OUTPUT_DIR/corrupt"
    runtime="$TEST_OUTPUT_DIR/corrupt"
    make_wet "$runtime/source.wet.gz" u c replay
    mkdir -p "$runtime/all/wet/u/c"
    cp "$runtime/source.wet.gz" "$runtime/all/wet/u/c/$EMPTY_SHA.wet.gz"
    run_upsert "$runtime" "$output" --from-archive "$runtime/all/wet/u/c" \
        --result-format=json
    assert_command_failure "$COMMAND_RC" "corrupt content-addressed WET replay succeeded" || return 1
    [[ ! -s "$runtime/calls" ]] || { log_fail "corrupt replay reached es-cli"; return 1; }
    jq -e '.status == "error" and .mode == "archive-replay" and
           (.inputs | length) == 1 and .error.code == "archive_corrupt"' \
        "$output" >/dev/null || {
        log_fail "archive preflight error did not retain truthful mode and discovered input"
        return 1
    }
}

wait_for_file() {
    local file="$1"
    local attempt
    for ((attempt = 0; attempt < 250; attempt++)); do
        [[ -e "$file" ]] && return 0
        sleep 0.02
    done
    return 1
}

case_lock_hierarchy_and_process_death() {
    local runtime="$TEST_OUTPUT_DIR/locks"
    local input_same="$runtime/same.wet.gz"
    local input_other="$runtime/other.wet.gz"
    local ready="$runtime/ready"
    local release="$runtime/release"
    local output="$runtime/output"
    local holder holder_rc calls_before calls_after
    local store_before="$runtime/store-before" store_after="$runtime/store-after"
    local source_before
    install_runtime "$runtime"
    make_wet "$input_same" u c same
    make_wet "$input_other" u other other

    RUNTIME_DIR="$runtime" READY="$ready" RELEASE="$release" \
      bash -c '
        source "$1"
        runtime_lock_pair u c
        : > "$READY"
        while [[ ! -e "$RELEASE" ]]; do sleep 0.02; done
      ' _ "$runtime/app/lib/scripts/runtime-lib.sh" &
    holder=$!
    register_background "$holder" false
    wait_for_file "$ready" || { log_fail "lock holder did not start"; return 1; }

    source_before="$(wet_sha "$input_same")"
    pair_manifest "$runtime/all/wet" "$store_before" || return 1
    calls_before="$(wc -l < "$runtime/calls")"
    run_upsert "$runtime" "$output" "$input_same" --url-id=u --crawl-id=c
    holder_rc="$COMMAND_RC"
    calls_after="$(wc -l < "$runtime/calls")"
    [[ "$holder_rc" -eq 75 ]] || { log_fail "same-pair contention exit was $holder_rc"; return 1; }
    [[ "$calls_before" -eq "$calls_after" ]] || {
        log_fail "same-pair contention reached Elasticsearch"
        return 1
    }
    [[ "$(wet_sha "$input_same")" == "$source_before" ]] || {
        log_fail "same-pair contention changed its source"
        return 1
    }
    pair_manifest "$runtime/all/wet" "$store_after" || return 1
    cmp -s "$store_before" "$store_after" || {
        log_fail "same-pair contention changed the published store"
        return 1
    }

    run_upsert "$runtime" "$output" "$input_other" --url-id=u --crawl-id=other
    assert_command_success "$COMMAND_RC" "distinct pair was blocked by a shared global lock" || return 1

    kill "$holder"
    wait "$holder" 2>/dev/null || true
    unregister_background "$holder"
    rm -f -- "$ready" "$release"
    run_upsert "$runtime" "$output" "$input_same" --url-id=u --crawl-id=c
    assert_command_success "$COMMAND_RC" "process death did not release pair ownership" || return 1
    assert_file_exists "$runtime/var/locks/warc2es/pairs/u/c.lock" || return 1
}

case_simultaneous_first_publication() {
    local runtime="$TEST_OUTPUT_DIR/first-publication"
    local input crawl sha pid rc
    local -a pids=() shas=()
    install_runtime "$runtime"

    for crawl in c1 c2 c3 c4 c5 c6; do
        input="$runtime/$crawl.wet.gz"
        make_wet "$input" u "$crawl" "$crawl"
        shas+=("$(wet_sha "$input")")
        CALL_LOG="$runtime/calls" "$runtime/es-upsert.sh" "$input" \
            --url-id=u --crawl-id="$crawl" >"$runtime/$crawl.out" \
            2>"$runtime/$crawl.err" &
        pid=$!
        pids+=("$pid")
        register_background "$pid" false
    done

    set +e
    rc=0
    for pid in "${pids[@]}"; do
        wait "$pid" || rc=1
        unregister_background "$pid"
    done
    set -e
    [[ "$rc" -eq 0 ]] || {
        log_fail "concurrent first publication failed"
        return 1
    }
    for index in "${!shas[@]}"; do
        crawl="c$((index + 1))"
        assert_file_exists "$runtime/all/wet/u/$crawl/${shas[index]}.wet.gz" || return 1
    done
}

case_replay_orchestration_order() {
    local runtime="$TEST_OUTPUT_DIR/all"
    local output="$runtime/results.jsonl"
    local source sha pair pid ready="$runtime/ready" release="$runtime/release"
    install_runtime "$runtime"
    for pair in z/c a/c; do
        source="$runtime/$pair.wet.gz"
        make_wet "$source" "${pair%/*}" "${pair#*/}" "$pair"
        sha="$(wet_sha "$source")"
        mkdir -p "$runtime/all/wet/$pair"
        cp "$source" "$runtime/all/wet/$pair/$sha.wet.gz"
    done

    setsid env CALL_LOG="$runtime/calls" BLOCK_READY="$ready" BLOCK_RELEASE="$release" \
        "$runtime/es-upsert-all.sh" >"$output" 2>"$output.stderr" &
    pid=$!
    register_background "$pid" true
    wait_for_file "$ready" || { log_fail "first archive delegate did not start"; return 1; }
    [[ "$(grep -c '^load-stream ' "$runtime/calls")" -eq 1 ]] || {
        log_fail "second archive delegate overlapped the blocked first delegate"
        return 1
    }
    : > "$release"
    set +e
    wait "$pid"
    COMMAND_RC=$?
    set -e
    unregister_background "$pid"
    assert_command_success "$COMMAND_RC" "ordered archive replay failed" || return 1
    jq -se '
      length == 3 and .[0].kind == "invocation" and .[0].inputs[0].path != null and
      .[1].kind == "invocation" and .[2].kind == "summary" and
      .[2].total == 2 and .[2].succeeded == 2 and .[2].failed == 0
    ' "$output" >/dev/null || return 1
    mapfile -t load_lines < <(grep '^load-stream ' "$runtime/calls")
    [[ ${#load_lines[@]} -eq 2 && "${load_lines[0]}" == *'/a/c/'* &&
       "${load_lines[1]}" == *'/z/c/'* ]] || {
        log_fail "archive pair replay order was not bytewise and sequential"
        return 1
    }
}

case_replay_orchestration_guards() {
    local runtime="$TEST_OUTPUT_DIR/all-guards"
    local output="$runtime/results.jsonl"
    local source="$runtime/source.wet.gz" sha
    install_runtime "$runtime"

    set +e
    CALL_LOG="$runtime/calls" "$runtime/es-upsert-all.sh" --stream='*' \
        >"$output" 2>"$output.stderr"
    COMMAND_RC=$?
    set -e
    assert_command_failure "$COMMAND_RC" "empty replay accepted an unsafe stream" || return 1
    [[ ! -s "$runtime/calls" ]] || { log_fail "unsafe empty replay reached a delegate"; return 1; }

    mv "$runtime/all" "$runtime/all-real"
    mkdir -p "$runtime/empty-target"
    ln -s "$runtime/empty-target" "$runtime/all"
    set +e
    CALL_LOG="$runtime/calls" "$runtime/es-upsert-all.sh" >"$output" 2>"$output.stderr"
    COMMAND_RC=$?
    set -e
    assert_command_failure "$COMMAND_RC" "empty archive symlink was accepted" || return 1
    [[ ! -s "$runtime/calls" ]] || { log_fail "empty archive symlink reached a delegate"; return 1; }

    mv "$runtime/all" "$runtime/all-link"
    mv "$runtime/all-real" "$runtime/all"
    make_wet "$source" u c replay
    sha="$(wet_sha "$source")"
    mkdir -p "$runtime/all/wet/u/c"
    cp "$source" "$runtime/all/wet/u/c/$sha.wet.gz"
    cat > "$runtime/es-upsert.sh" <<'FAKE_UPSERT'
#!/bin/bash
processing='{"schema":"warc2es.processing/v1","status":"ok","exit_code":0,"records_in":1,"records_out":1,"records_indexed":1,"records_skipped":0,"errors":0,"elapsed_ms":1,"error":null,"metrics":{"schema":"warc2es.metrics/v1","counters":{}}}'
case "${FAKE_DELEGATE_MODE:-mismatch}" in
    mismatch)
        printf '{"schema":"warc2es.operator/v1","kind":"invocation","command":"es-upsert","status":"ok","exit_code":0,"mode":"archive-replay","inputs":[],"outputs":[],"publication":{"status":"unchanged","paths":[]},"processing":%s,"error":null}\n' "$processing"
        exit 9
        ;;
    null_success)
        printf '%s\n' '{"schema":"warc2es.operator/v1","kind":"invocation","command":"es-upsert","status":"ok","exit_code":0,"mode":"archive-replay","inputs":[],"outputs":[],"publication":{"status":"unchanged","paths":[]},"processing":null,"error":null}'
        ;;
    partial)
        printf '{"schema":"warc2es.operator/v1","kind":"invocation","command":"es-upsert","status":"partial","exit_code":5,"mode":"archive-replay","inputs":[],"outputs":[],"publication":{"status":"unchanged","paths":[]},"processing":%s,"error":{"code":"cleanup_failed","message":"fixture"}}\n' "$processing"
        exit 5
        ;;
esac
FAKE_UPSERT
    chmod +x "$runtime/es-upsert.sh"
    set +e
    CALL_LOG="$runtime/calls" "$runtime/es-upsert-all.sh" >"$output" 2>"$output.stderr"
    COMMAND_RC=$?
    set -e
    assert_command_failure "$COMMAND_RC" "delegate exit/object mismatch succeeded" || return 1
    jq -se '
      length == 2 and .[0].kind == "invocation" and .[0].status == "error" and
      .[0].exit_code == 9 and .[0].error.code == "processing_result_invalid" and
      .[1].kind == "summary" and .[1].failed == 1
    ' "$output" >/dev/null || {
        log_fail "delegate exit/object mismatch was not replaced by a truthful fallback"
        return 1
    }

    set +e
    FAKE_DELEGATE_MODE=null_success "$runtime/es-upsert-all.sh" >"$output" 2>"$output.stderr"
    COMMAND_RC=$?
    set -e
    assert_command_failure "$COMMAND_RC" "null-processing nonzero-record success was accepted" || return 1
    jq -se '.[0].status == "error" and .[0].exit_code == 1 and
            .[0].error.code == "processing_result_invalid" and .[1].failed == 1' \
        "$output" >/dev/null || return 1

    set +e
    FAKE_DELEGATE_MODE=partial "$runtime/es-upsert-all.sh" >"$output" 2>"$output.stderr"
    COMMAND_RC=$?
    set -e
    assert_command_failure "$COMMAND_RC" "valid delegated partial did not fail the summary" || return 1
    jq -se 'length == 2 and .[0].status == "partial" and .[0].exit_code == 5 and
            .[0].error.code == "cleanup_failed" and .[1].failed == 1' \
        "$output" >/dev/null || {
        log_fail "valid delegated partial was not preserved unchanged"
        return 1
    }
}

setup_test_env
run_stage "mandatory pair and complete byte-counted preflight" case_mandatory_pair_and_preflight || true
run_stage "external source retention, delete-create, and one JVM" case_external_publication_and_one_jvm || true
run_stage "pair-only staging extends the published set in one JVM" case_pair_staging_extends_published_set || true
run_stage "managed same-SHA cleanup, replacement, and failure preservation" case_managed_idempotence_replacement_and_failure || true
run_stage "external input bytes are snapshotted before pair deletion" case_external_input_is_snapshotted || true
run_stage "archive replay is read-only and corruption fails closed" case_archive_replay_and_corruption || true
run_stage "global/pair locks contend safely and release on process death" case_lock_hierarchy_and_process_death || true
run_stage "distinct pairs can create lock and publication roots concurrently" case_simultaneous_first_publication || true
run_stage "archive orchestrator delegates in bytewise sequential order" case_replay_orchestration_order || true
run_stage "archive orchestrator rejects unsafe roots and delegate protocol mismatch" case_replay_orchestration_guards || true
finish_stages
rc=$?
cleanup_background_processes
cleanup_test_env
exit "$rc"
