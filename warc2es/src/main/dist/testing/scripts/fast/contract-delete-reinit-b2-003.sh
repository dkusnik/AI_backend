#!/bin/bash
# OWNER: B2-003
# Delete/reinit targeting, JSON, cleanup, dry-run, and coordination contract.
source "$(dirname "$0")/../../lib/test-lib.sh"

declare -A HOLDER_PIDS=()

cleanup_holders() {
    local pid
    for pid in "${!HOLDER_PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
        unset "HOLDER_PIDS[$pid]"
    done
}
trap cleanup_holders EXIT

install_runtime() {
    local runtime="$1"
    rm -rf -- "$runtime"
    mkdir -p "$runtime/app/bin" "$runtime/app/lib/scripts" "$runtime/all"
    cp "$PROJECT_ROOT/src/main/dist/es-delete.sh" "$runtime/es-delete.sh"
    cp "$PROJECT_ROOT/src/main/dist/es-reinit.sh" "$runtime/es-reinit.sh"
    cp "$PROJECT_ROOT/src/main/dist/es-upsert.sh" "$runtime/es-upsert.sh"
    cp "$PROJECT_ROOT/src/main/dist/es-upsert-all.sh" "$runtime/es-upsert-all.sh"
    cp "$PROJECT_ROOT/src/main/dist/lib/scripts/runtime-lib.sh" \
        "$runtime/app/lib/scripts/runtime-lib.sh"
    cat > "$runtime/app/bin/es-cli" <<'FAKE_ES'
#!/bin/bash
printf '%s\n' "$*" >> "$CALL_LOG"
if [[ "${FAKE_MODE:-}" == health-fail && "${1:-}" == check-health ]]; then
    exit 5
fi
if [[ "${FAKE_MODE:-}" == reinit-fail && "${1:-}" == purge ]]; then
    exit 6
fi
if [[ "${1:-}" == batch-delete ]]; then
    if [[ -n "${ARCHIVE_PROBE:-}" && -e "$ARCHIVE_PROBE" ]]; then
        printf '%s\n' archive-present >> "$CALL_LOG"
    fi
    case "${FAKE_MODE:-ok}" in
        fail) exit 6 ;;
        incomplete)
            jq -cn '{total:2,deleted:1,version_conflicts:0,timed_out:false,failures:[]}'
            ;;
        empty) ;;
        malformed) printf '%s\n' not-json ;;
        timed-out)
            jq -cn '{total:2,deleted:2,version_conflicts:0,timed_out:true,failures:[]}'
            ;;
        conflicts)
            jq -cn '{total:2,deleted:2,version_conflicts:1,timed_out:false,failures:[]}'
            ;;
        failures)
            jq -cn '{total:2,deleted:2,version_conflicts:0,timed_out:false,failures:[{}]}'
            ;;
        *)
            jq -cn '{total:2,deleted:2,version_conflicts:0,timed_out:false,failures:[]}'
            ;;
    esac
fi
FAKE_ES
    chmod +x "$runtime/es-delete.sh" "$runtime/es-reinit.sh" \
        "$runtime/es-upsert.sh" "$runtime/es-upsert-all.sh" \
        "$runtime/app/bin/es-cli"
    : > "$runtime/calls"
}

case_archive_symlink_fails_before_es() {
    local runtime="$TEST_OUTPUT_DIR/delete-symlink"
    local result="$runtime/result.json"
    local pair="$runtime/all/wet/u/c"
    local unsafe="$pair/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.wet.gz"
    install_runtime "$runtime"
    mkdir -p "$pair"
    ln -s "$runtime/outside" "$unsafe"

    run_json "$runtime" es-delete "$result" --stream=test --url-id=u \
        --crawl-id=c --dry-run
    assert_command_failure "$COMMAND_RC" "symlink candidate delete succeeded" || return 1
    [[ ! -s "$runtime/calls" && -L "$unsafe" && ! -e "$runtime/var" ]] || {
        log_fail "unsafe archive candidate reached ES or filesystem mutation"
        return 1
    }
    jq -e '.status == "error" and .error.code == "archive_unsafe"' \
        "$result" >/dev/null || return 1
}

case_corrupt_content_address_fails_before_es() {
    local runtime="$TEST_OUTPUT_DIR/delete-corrupt"
    local result="$runtime/result.json"
    local artifact
    install_runtime "$runtime"
    artifact=$(publish_wet "$runtime" u c one)
    printf corrupt >> "$artifact"

    run_json "$runtime" es-delete "$result" --stream=test --url-id=u --crawl-id=c
    assert_command_failure "$COMMAND_RC" "corrupt published WET delete succeeded" || return 1
    [[ ! -s "$runtime/calls" && -f "$artifact" && ! -e "$runtime/var" ]] || {
        log_fail "corrupt content-addressed WET reached ES or cleanup"
        return 1
    }
    jq -e '.status == "error" and .error.code == "archive_unsafe"' \
        "$result" >/dev/null || return 1
}

case_unsafe_lock_dry_run_fails_without_mutation() {
    local runtime="$TEST_OUTPUT_DIR/unsafe-lock"
    local result="$runtime/result.json"
    local lock_root="$runtime/var/locks/warc2es"
    local outside="$runtime/outside"
    local command rc
    install_runtime "$runtime"
    mkdir -p "$lock_root"
    : > "$outside"
    ln -s "$outside" "$lock_root/global.lock"

    for command in es-delete es-reinit; do
        : > "$runtime/calls"
        set +e
        if [[ "$command" == es-delete ]]; then
            CALL_LOG="$runtime/calls" "$runtime/$command.sh" --stream=test \
                --all-documents --dry-run --result-format=json \
                > "$result" 2> "$result.stderr"
        else
            CALL_LOG="$runtime/calls" "$runtime/$command.sh" --stream=test \
                --dry-run --result-format=json > "$result" 2> "$result.stderr"
        fi
        rc=$?
        set -e
        assert_command_failure "$rc" "$command accepted an unsafe lock symlink" || return 1
        [[ ! -s "$runtime/calls" && -L "$lock_root/global.lock" && -f "$outside" ]] || {
            log_fail "$command unsafe-lock dry-run changed external state"
            return 1
        }
        jq -e '.status == "error" and .error.code == "lock_unsafe"' \
            "$result" >/dev/null || return 1
    done
}

case_hard_link_locks_fail_without_truncation() {
    local runtime="$TEST_OUTPUT_DIR/hard-link-global"
    local result="$runtime/result.json"
    local sentinel="$runtime/sentinel" lock_root artifact
    install_runtime "$runtime"
    lock_root="$runtime/var/locks/warc2es"
    mkdir -p "$lock_root"
    printf 'do-not-truncate' > "$sentinel"
    ln "$sentinel" "$lock_root/global.lock"

    run_json "$runtime" es-reinit "$result" --stream=test --yes
    assert_command_failure "$COMMAND_RC" "hard-linked global lock was accepted" || return 1
    [[ "$(cat "$sentinel")" == do-not-truncate && ! -s "$runtime/calls" ]] || {
        log_fail "global lock validation truncated the sentinel or reached Elasticsearch"
        return 1
    }
    jq -e '.status == "error" and .error.code == "lock_unsafe"' \
        "$result" >/dev/null || return 1

    runtime="$TEST_OUTPUT_DIR/hard-link-pair"
    result="$runtime/result.json"
    sentinel="$runtime/sentinel"
    install_runtime "$runtime"
    artifact=$(publish_wet "$runtime" u c one)
    lock_root="$runtime/var/locks/warc2es"
    mkdir -p "$lock_root/pairs/u"
    printf 'do-not-truncate' > "$sentinel"
    ln "$sentinel" "$lock_root/pairs/u/c.lock"

    run_json "$runtime" es-delete "$result" --stream=test --url-id=u --crawl-id=c
    assert_command_failure "$COMMAND_RC" "hard-linked pair lock was accepted" || return 1
    [[ "$(cat "$sentinel")" == do-not-truncate && ! -s "$runtime/calls" && -f "$artifact" ]] || {
        log_fail "pair lock validation truncated the sentinel or reached external effects"
        return 1
    }
    jq -e '.status == "error" and .error.code == "lock_unsafe"' \
        "$result" >/dev/null || return 1
}

publish_wet() {
    local runtime="$1" url_id="$2" crawl_id="$3" payload="$4"
    local pair="$runtime/all/wet/$url_id/$crawl_id"
    local pending="$pair/pending.wet.gz"
    local digest
    mkdir -p "$pair"
    printf '%s' "$payload" | gzip > "$pending"
    digest=$(sha256sum -- "$pending")
    digest=${digest%% *}
    mv -- "$pending" "$pair/$digest.wet.gz"
    printf '%s\n' "$pair/$digest.wet.gz"
}

run_json() {
    local runtime="$1" command="$2" output="$3"
    shift 3
    set +e
    CALL_LOG="$runtime/calls" "$runtime/$command.sh" "$@" \
        --result-format=json > "$output" 2> "$output.stderr"
    COMMAND_RC=$?
    set -e
}

case_delete_dry_run_and_scope_validation() {
    local runtime="$TEST_OUTPUT_DIR/delete-dry"
    local result="$runtime/result.json"
    local artifact before_calls output rc
    install_runtime "$runtime"
    artifact=$(publish_wet "$runtime" u c one)

    run_json "$runtime" es-delete "$result" --stream=test --url-id=u \
        --crawl-id=c --dry-run
    assert_command_success "$COMMAND_RC" "pair JSON dry-run failed" || return 1
    [[ "$(wc -l < "$result")" -eq 1 && ! -s "$runtime/calls" && \
       -e "$artifact" && ! -e "$runtime/var" ]] || {
        log_fail "pair dry-run made a call or filesystem mutation"
        return 1
    }
    jq -e '
      .schema == "warc2es.operator/v1" and .command == "es-delete" and
      .status == "dry_run" and .exit_code == 0 and .mode == "pair" and
      .target.stream == "nac-data-test" and .target.scope == "pair" and
      .target.url_id == "u" and .target.crawl_id == "c" and
      .target.query == {query:{bool:{filter:[{"term":{"nac-url-id":"u"}},{"term":{"nac-crawl-id":"c"}}]}}} and
      .target.locks == [
        {path:"var/locks/warc2es/global.lock",mode:"shared"},
        {path:"var/locks/warc2es/pairs/u/c.lock",mode:"exclusive"}
      ] and
      .publication.status == "planned" and
      .publication.paths == [$path] and
      .publication.cleanup == {matched:1,removed:0,failed:0,failed_paths:[]} and
      .processing == null and .error == null
    ' --arg path "all/wet/u/c/$(basename "$artifact")" "$result" >/dev/null || return 1

    run_json "$runtime" es-delete "$result" --stream=test --all-documents --dry-run
    assert_command_success "$COMMAND_RC" "all-documents JSON dry-run failed" || return 1
    jq -e '.mode == "all-documents" and .target.scope == "all-documents" and
           .target.url_id == null and .target.crawl_id == null and
           .publication.status == "planned"' "$result" >/dev/null || return 1

    before_calls=$(wc -l < "$runtime/calls")
    local -a invalid_cases=(
        '--url-id=u --crawl-id=c --dry-run'
        '--stream=* --url-id=u --crawl-id=c --dry-run'
        '--stream=test --url-id=u --dry-run'
        '--stream=test --url-id= --crawl-id= --dry-run'
        '--stream=test --url-id=u --crawl-id=c --all-documents --dry-run'
        '--stream=test --all-documents --yes --dry-run'
        '--stream=test --all-documents --force --dry-run'
    )
    local arguments
    for arguments in "${invalid_cases[@]}"; do
        set +e
        # The fixture intentionally expands one argument vector.
        # shellcheck disable=SC2086
        output=$(CALL_LOG="$runtime/calls" "$runtime/es-delete.sh" $arguments 2>&1)
        rc=$?
        set -e
        assert_command_failure "$rc" "invalid delete scope succeeded: $arguments" || return 1
        [[ -n "$output" ]] || { log_fail "invalid delete scope had no diagnostic"; return 1; }
    done
    run_json "$runtime" es-delete "$result" --unknown-before-format
    assert_command_failure "$COMMAND_RC" "unknown delete option succeeded" || return 1
    [[ "$(wc -l < "$result")" -eq 1 ]] || {
        log_fail "late JSON selection did not produce exactly one result"
        return 1
    }
    jq -e '.status == "error" and .error.code == "unknown_option"' \
        "$result" >/dev/null || return 1
    [[ "$(wc -l < "$runtime/calls")" -eq "$before_calls" && -e "$artifact" && \
       ! -e "$runtime/var" ]] || {
        log_fail "invalid or dry-run delete changed external state"
        return 1
    }
}

case_pair_delete_is_es_first_and_exact() {
    local runtime="$TEST_OUTPUT_DIR/delete-pair"
    local result="$runtime/result.json"
    local first second retained
    install_runtime "$runtime"
    first=$(publish_wet "$runtime" u c one)
    second=$(publish_wet "$runtime" u c two)
    retained=$(publish_wet "$runtime" u other retained)

    set +e
    CALL_LOG="$runtime/calls" ARCHIVE_PROBE="$first" \
        "$runtime/es-delete.sh" --stream=test --url-id=u --crawl-id=c \
        --result-format=json > "$result" 2> "$result.stderr"
    COMMAND_RC=$?
    set -e
    assert_command_success "$COMMAND_RC" "pair deletion failed" || return 1
    [[ ! -e "$first" && ! -e "$second" && -e "$retained" ]] || {
        log_fail "pair cleanup was not exact"
        return 1
    }
    [[ "$(sed -n '1p' "$runtime/calls")" == batch-delete* && \
       "$(sed -n '2p' "$runtime/calls")" == archive-present && \
       "$(grep -c '^batch-delete ' "$runtime/calls")" -eq 1 ]] || {
        log_fail "Elasticsearch deletion did not precede exact archive cleanup"
        return 1
    }
    grep -Fq '"nac-url-id":"u"' "$runtime/calls" || return 1
    grep -Fq '"nac-crawl-id":"c"' "$runtime/calls" || return 1
    jq -e '.status == "ok" and .publication.status == "removed" and
           .publication.cleanup == {matched:2,removed:2,failed:0,failed_paths:[]}' \
        "$result" >/dev/null || return 1
}

case_elasticsearch_failure_preserves_archive() {
    local runtime="$TEST_OUTPUT_DIR/delete-es-failure"
    local result="$runtime/result.json"
    local artifact mode
    install_runtime "$runtime"
    artifact=$(publish_wet "$runtime" u c one)

    for mode in fail incomplete empty malformed timed-out conflicts failures; do
        : > "$runtime/calls"
        set +e
        CALL_LOG="$runtime/calls" FAKE_MODE="$mode" \
            "$runtime/es-delete.sh" --stream=test --url-id=u --crawl-id=c \
            --result-format=json > "$result" 2> "$result.stderr"
        COMMAND_RC=$?
        set -e
        assert_command_failure "$COMMAND_RC" "$mode Elasticsearch result succeeded" || return 1
        [[ -e "$artifact" ]] || {
            log_fail "$mode Elasticsearch result removed a published WET"
            return 1
        }
        jq -e '.status == "error" and .publication.status == "skipped" and
               .publication.cleanup.removed == 0 and .publication.cleanup.failed == 0' \
            "$result" >/dev/null || return 1
    done
}

case_cleanup_partial_continues_in_order() {
    local runtime="$TEST_OUTPUT_DIR/delete-partial"
    local result="$runtime/result.json"
    local first second first_relative
    install_runtime "$runtime"
    first=$(publish_wet "$runtime" u c one)
    second=$(publish_wet "$runtime" u c two)
    if [[ "$second" < "$first" ]]; then
        local swap="$first"
        first="$second"
        second="$swap"
    fi
    first_relative="${first#"$runtime"/}"
    mkdir -p "$runtime/fake-bin"
    cat > "$runtime/fake-bin/rm" <<'FAKE_RM'
#!/bin/bash
for argument in "$@"; do
    if [[ "$argument" == "${FAIL_RM_PATH:-}" ]]; then
        exit 1
    fi
done
exec /usr/bin/rm "$@"
FAKE_RM
    chmod +x "$runtime/fake-bin/rm"

    set +e
    PATH="$runtime/fake-bin:$PATH" FAIL_RM_PATH="$first" CALL_LOG="$runtime/calls" \
        "$runtime/es-delete.sh" --stream=test --url-id=u --crawl-id=c \
        --result-format=json > "$result" 2> "$result.stderr"
    COMMAND_RC=$?
    set -e
    assert_command_failure "$COMMAND_RC" "partial cleanup returned success" || return 1
    [[ -e "$first" && ! -e "$second" ]] || {
        log_fail "cleanup did not continue after the injected first failure"
        return 1
    }
    jq -e '.status == "partial" and .exit_code != 0 and
           .error.code == "archive_cleanup_failed" and
           .publication.status == "partial" and
           .publication.cleanup.matched == 2 and
           .publication.cleanup.removed == 1 and
           .publication.cleanup.failed == 1 and
           .publication.cleanup.failed_paths == [$failed]' \
        --arg failed "$first_relative" "$result" >/dev/null || return 1
}

case_all_documents_removes_every_published_wet() {
    local runtime="$TEST_OUTPUT_DIR/delete-all"
    local result="$runtime/result.json"
    local replay="$runtime/replay.jsonl"
    local first second unrelated
    install_runtime "$runtime"
    first=$(publish_wet "$runtime" a c one)
    second=$(publish_wet "$runtime" z d two)
    unrelated="$runtime/all/doet/future.doet.gz"
    mkdir -p "$(dirname "$unrelated")"
    : > "$unrelated"

    run_json "$runtime" es-delete "$result" --stream=test --all-documents
    assert_command_success "$COMMAND_RC" "whole-stream deletion failed" || return 1
    [[ ! -e "$first" && ! -e "$second" && -e "$unrelated" ]] || {
        log_fail "whole-stream cleanup did not remove exactly every published WET"
        return 1
    }
    grep -Fq '{"query":{"match_all":{}}}' "$runtime/calls" || return 1
    jq -e '.status == "ok" and .mode == "all-documents" and
           .publication.cleanup == {matched:2,removed:2,failed:0,failed_paths:[]}' \
        "$result" >/dev/null || return 1

    : > "$runtime/calls"
    set +e
    CALL_LOG="$runtime/calls" "$runtime/es-upsert-all.sh" --stream=test \
        > "$replay" 2> "$replay.stderr"
    COMMAND_RC=$?
    set -e
    assert_command_success "$COMMAND_RC" "replay-all failed on empty unpublished pairs" || return 1
    [[ ! -s "$runtime/calls" ]] || {
        log_fail "replay-all delegated an empty provenance directory"
        return 1
    }
    jq -e '.kind == "summary" and .status == "ok" and
           .total == 0 and .succeeded == 0 and .failed == 0' \
        "$replay" >/dev/null || return 1
}

start_lock_holder() {
    local lock_path="$1" mode="$2" ready="$3" release="$4"
    mkdir -p "$(dirname "$lock_path")"
    (
        exec 9> "$lock_path"
        if [[ "$mode" == shared ]]; then
            flock -s 9
        else
            flock -x 9
        fi
        : > "$ready"
        while [[ ! -e "$release" ]]; do sleep 0.02; done
    ) &
    HOLDER_PID=$!
    HOLDER_PIDS["$HOLDER_PID"]=1
    for _ in {1..100}; do
        [[ -e "$ready" ]] && return 0
        sleep 0.02
    done
    return 1
}

stop_lock_holder() {
    local release="$1" pid="$2"
    : > "$release"
    wait "$pid"
    unset "HOLDER_PIDS[$pid]"
}

case_whole_store_coordination_is_fail_fast() {
    local runtime="$TEST_OUTPUT_DIR/locks"
    local result="$runtime/result.json"
    local global_lock="$runtime/var/locks/warc2es/global.lock"
    local ready="$runtime/ready" release="$runtime/release" artifact temporary pid rc
    install_runtime "$runtime"
    artifact=$(publish_wet "$runtime" u c one)
    temporary="$(dirname "$artifact")/.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.tmp.fixture"
    : > "$temporary"
    start_lock_holder "$global_lock" shared "$ready" "$release" || return 1
    pid="$HOLDER_PID"

    set +e
    CALL_LOG="$runtime/calls" "$runtime/es-delete.sh" --stream=test \
        --all-documents --result-format=json > "$result" 2> "$result.stderr"
    rc=$?
    set -e
    [[ "$rc" -eq 75 ]] || { log_fail "whole delete contention exit was $rc"; return 1; }
    jq -e '.status == "error" and .exit_code == 75 and .error.code == "busy"' \
        "$result" >/dev/null || return 1

    set +e
    CALL_LOG="$runtime/calls" "$runtime/es-reinit.sh" --stream=test --yes \
        --result-format=json > "$result" 2> "$result.stderr"
    rc=$?
    set -e
    [[ "$rc" -eq 75 ]] || { log_fail "reinit contention exit was $rc"; return 1; }
    [[ ! -s "$runtime/calls" && -e "$artifact" && -f "$temporary" ]] || {
        log_fail "busy whole-store operation reached ES or archive cleanup"
        return 1
    }
    stop_lock_holder "$release" "$pid"

    rm -f -- "$ready" "$release"
    start_lock_holder "$global_lock" exclusive "$ready" "$release" || return 1
    pid="$HOLDER_PID"
    set +e
    CALL_LOG="$runtime/calls" "$runtime/es-delete.sh" --stream=test \
        --url-id=u --crawl-id=c --result-format=json > "$result" 2> "$result.stderr"
    rc=$?
    set -e
    [[ "$rc" -eq 75 && ! -s "$runtime/calls" && -e "$artifact" && -f "$temporary" ]] || {
        log_fail "pair delete did not respect the exclusive global lock"
        return 1
    }
    stop_lock_holder "$release" "$pid"
}

case_reinit_preserves_archive_and_reports_json() {
    local runtime="$TEST_OUTPUT_DIR/reinit"
    local result="$runtime/result.json"
    local artifact rc
    install_runtime "$runtime"
    artifact=$(publish_wet "$runtime" u c one)

    run_json "$runtime" es-reinit "$result" --unknown-before-format
    assert_command_failure "$COMMAND_RC" "unknown reinit option succeeded" || return 1
    [[ "$(wc -l < "$result")" -eq 1 ]] || return 1
    jq -e '.status == "error" and .error.code == "unknown_option"' \
        "$result" >/dev/null || return 1

    run_json "$runtime" es-reinit "$result" --stream=test --dry-run
    assert_command_success "$COMMAND_RC" "reinit dry-run failed" || return 1
    [[ ! -s "$runtime/calls" && ! -e "$runtime/var" && -e "$artifact" ]] || {
        log_fail "reinit dry-run made a call or filesystem mutation"
        return 1
    }
    jq -e '.status == "dry_run" and .mode == "reinit" and
           .target.stream == "nac-data-test" and .target.scope == "stream" and
           .target.url_id == null and .target.crawl_id == null and
           .target.operation == "purge-and-init" and
           .target.locks == [{path:"var/locks/warc2es/global.lock",mode:"exclusive"}] and
           .publication == null and .processing == null' "$result" >/dev/null || return 1

    set +e
    CALL_LOG="$runtime/calls" "$runtime/es-reinit.sh" --stream=test \
        --result-format=json > "$result" 2> "$result.stderr"
    rc=$?
    set -e
    assert_command_failure "$rc" "non-TTY reinit without --yes succeeded" || return 1
    [[ ! -s "$runtime/calls" && -e "$artifact" ]] || {
        log_fail "confirmation refusal reached Elasticsearch or archive cleanup"
        return 1
    }

    : > "$runtime/calls"
    set +e
    CALL_LOG="$runtime/calls" FAKE_MODE=health-fail \
        "$runtime/es-reinit.sh" --stream=test --yes --result-format=json \
        > "$result" 2> "$result.stderr"
    rc=$?
    set -e
    [[ "$rc" -eq 5 && -e "$artifact" ]] || {
        log_fail "failed reinit changed the archive or returned the wrong exit"
        return 1
    }
    jq -e '.status == "error" and .exit_code == 5 and
           .error.code == "elasticsearch_unavailable" and .publication == null' \
        "$result" >/dev/null || return 1

    : > "$runtime/calls"
    run_json "$runtime" es-reinit "$result" --stream=test --yes
    assert_command_success "$COMMAND_RC" "confirmed reinit failed" || return 1
    [[ -e "$artifact" ]] || { log_fail "reinit removed a published WET"; return 1; }
    [[ "$(sed -n '1p' "$runtime/calls")" == check-health &&
       "$(sed -n '2p' "$runtime/calls")" == "purge nac-data-test" &&
       "$(sed -n '3p' "$runtime/calls")" == "init nac-data-test" &&
       "$(sed -n '4p' "$runtime/calls")" == "get-stream nac-data-test" &&
       "$(wc -l < "$runtime/calls")" -eq 4 ]] || {
        log_fail "reinit did not use the expected accepted-write sequence"
        return 1
    }
    jq -e '.status == "ok" and .exit_code == 0 and .publication == null and .error == null' \
        "$result" >/dev/null || return 1
}

setup_test_env
run_stage "delete dry-run and destructive scope validation" case_delete_dry_run_and_scope_validation || true
run_stage "archive symlinks fail before external effects" case_archive_symlink_fails_before_es || true
run_stage "corrupt content address fails before external effects" case_corrupt_content_address_fails_before_es || true
run_stage "unsafe lock dry-run fails without mutation" case_unsafe_lock_dry_run_fails_without_mutation || true
run_stage "hard-linked locks fail without truncation" case_hard_link_locks_fail_without_truncation || true
run_stage "pair deletion is Elasticsearch-first and exact" case_pair_delete_is_es_first_and_exact || true
run_stage "Elasticsearch failure preserves published WETs" case_elasticsearch_failure_preserves_archive || true
run_stage "cleanup partial continues in byte order" case_cleanup_partial_continues_in_order || true
run_stage "all-documents removes every published WET" case_all_documents_removes_every_published_wet || true
run_stage "whole-store coordination is fail-fast" case_whole_store_coordination_is_fail_fast || true
run_stage "reinit preserves archive and reports JSON" case_reinit_preserves_archive_and_reports_json || true
finish_stages
result=$?
cleanup_holders
cleanup_test_env
exit "$result"
