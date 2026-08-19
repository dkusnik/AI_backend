#!/bin/bash
# D3-004: final release-archive acceptance gate.
# @timeout: 600
set -euo pipefail
export LC_ALL=C
# shellcheck source=/dev/null
source "$(dirname "$0")/../../lib/test-lib.sh"

ES_URL="${ES_URL:-http://localhost:9200}"
ES_USER="${ES_USER:-elastic}"
ES_PASS="${ES_PASS:-${ELASTIC_PASSWORD:-}}"
ES_CLI_CONF=/dev/null
export ES_URL ES_USER ES_PASS ES_CLI_CONF

TESTING_ROOT="$PROJECT_ROOT/target/dist/testing"
REGISTRY="$TESTING_ROOT/registry.yaml"
PLAN="$PROJECT_ROOT/../warc2es-plans/refactor-TODO.md"
EXECUTION_LOG="$PROJECT_ROOT/../warc2es-plans/execution-log.txt"
RELEASE_ARCHIVE="$PROJECT_ROOT/target/warc2es-linux.tar.gz"
INSTALL_ROOT=""
FINAL_STREAM=""
CLEANUP_STREAM=false

declare -a ACCEPTANCE_TARGETS=()
declare -A TARGET_SEEN=()

cleanup_release_gate() {
    local cli="$ES_CLI"
    if [[ -n "$INSTALL_ROOT" && -x "$INSTALL_ROOT/app/bin/es-cli" ]]; then
        cli="$INSTALL_ROOT/app/bin/es-cli"
    fi
    if [[ "$CLEANUP_STREAM" == true && -n "$FINAL_STREAM" ]]; then
        "$cli" delete-stream "$FINAL_STREAM" >/dev/null 2>&1 || true
    fi
}
trap cleanup_release_gate EXIT

add_acceptance_target() {
    local target="$1"
    if [[ -z "${TARGET_SEEN[$target]:-}" ]]; then
        TARGET_SEEN["$target"]=1
        ACCEPTANCE_TARGETS+=("$target")
    fi
}

normalize_registry() {
    local output="$1"
    awk '
      function flush() {
        if (id != "" && scope != "" && script != "") {
          gsub(/[[:space:]]/, "", tags)
          printf "%s\t%s\t%s\t%s\n", id, scope, script, tags
        }
      }
      /^[[:space:]]*-[[:space:]]+id:[[:space:]]*/ {
        flush()
        id=$0
        sub(/^[[:space:]]*-[[:space:]]+id:[[:space:]]*/, "", id)
        gsub(/^['\''"]|['\''"]$/, "", id)
        scope=""; script=""; tags=""
      }
      /^[[:space:]]*scope:[[:space:]]*/ {
        scope=$0
        sub(/^[[:space:]]*scope:[[:space:]]*/, "", scope)
        gsub(/^['\''"]|['\''"]$/, "", scope)
      }
      /^[[:space:]]*script:[[:space:]]*/ {
        script=$0
        sub(/^[[:space:]]*script:[[:space:]]*/, "", script)
        gsub(/^['\''"]|['\''"]$/, "", script)
      }
      /^[[:space:]]*tags:[[:space:]]*\[/ {
        tags=$0
        sub(/^[[:space:]]*tags:[[:space:]]*\[/, "", tags)
        sub(/\][[:space:]]*$/, "", tags)
      }
      END { flush() }
    ' "$REGISTRY" > "$output"
}

assert_completed_owner() {
    local owner="$1"
    awk -v owner="$owner" '
      $1 == "###" { active=($2 == owner); next }
      active && /^- Status:/ { found=($0 ~ /`\[x\]`/); exit }
      END { exit(found ? 0 : 1) }
    ' "$PLAN" || {
        log_fail "A+ contract owner is not complete in the TODO: $owner"
        return 1
    }
    awk -v owner="$owner" '
      $2 == owner && $3 == "done" { found=1 }
      END { exit(found ? 0 : 1) }
    ' "$EXECUTION_LOG" || {
        log_fail "A+ contract owner has no completed execution-log entry: $owner"
        return 1
    }
}

assert_registry_target() {
    local table="$1" target="$2" required_scope="$3"
    local -a matches=()
    local id scope script tags
    mapfile -t matches < <(awk -F '\t' -v target="$target" '$1 == target' "$table")
    [[ ${#matches[@]} -eq 1 ]] || {
        log_fail "Expected one registry row for $target, found ${#matches[@]}"
        return 1
    }
    IFS=$'\t' read -r id scope script tags <<< "${matches[0]}"
    [[ "$scope" == "$required_scope" ]] || {
        log_fail "$target is in scope $scope, expected $required_scope"
        return 1
    }
    [[ -f "$TESTING_ROOT/$script" ]] || {
        log_fail "$target references missing test script: $script"
        return 1
    }
}

stage_red_lifecycle() {
    local table="$TEST_OUTPUT_DIR/registry.tsv"
    local file relative line2 owner owner_count=0
    local -a matches=() sentinel_rows=()
    local id scope script tags sentinel_output="$TEST_OUTPUT_DIR/red-sentinel.log" sentinel_rc

    normalize_registry "$table"
    while IFS= read -r -d '' file; do
        grep -q '^# OWNER:' "$file" || continue
        line2="$(sed -n '2p' "$file")"
        [[ "$line2" =~ ^#[[:space:]]OWNER:[[:space:]]([A-Z0-9+-]+)$ ]] || {
            log_fail "OWNER marker is not on line 2 or is malformed: $file"
            return 1
        }
        owner="${BASH_REMATCH[1]}"
        relative="${file#"$TESTING_ROOT/"}"
        mapfile -t matches < <(awk -F '\t' -v script="$relative" '$3 == script' "$table")
        [[ ${#matches[@]} -eq 1 ]] || {
            log_fail "OWNER test must have exactly one registry row: $relative"
            return 1
        }
        IFS=$'\t' read -r id scope script tags <<< "${matches[0]}"
        [[ "$scope" == fast && "$relative" != scripts/red/* ]] || {
            log_fail "OWNER test was not promoted to fast: $relative"
            return 1
        }
        assert_completed_owner "$owner" || return 1
        add_acceptance_target "$id"
        owner_count=$((owner_count + 1))
    done < <(find "$TESTING_ROOT/scripts" -type f -name '*.sh' -print0 | sort -z)
    [[ "$owner_count" -gt 0 ]] || {
        log_fail "No OWNER-marked contract tests were enumerated"
        return 1
    }

    mapfile -t matches < <(awk -F '\t' '$2 == "red"' "$table")
    for line2 in "${matches[@]}"; do
        IFS=$'\t' read -r id scope script tags <<< "$line2"
        [[ "$id" == red-known-failing-regression ]] || {
            log_fail "Unexpected ownerless test remains in red: $id"
            return 1
        }
        if sed -n '2p' "$TESTING_ROOT/$script" | grep -q '^# OWNER:'; then
            log_fail "OWNER-bearing test remains in red: $id"
            return 1
        fi
    done

    mapfile -t sentinel_rows < <(awk -F '\t' '$1 == "red-known-failing-regression"' "$table")
    [[ ${#sentinel_rows[@]} -eq 1 ]] || {
        log_fail "The intentional red sentinel is missing or duplicated"
        return 1
    }
    IFS=$'\t' read -r id scope script tags <<< "${sentinel_rows[0]}"
    [[ "$scope" == red && "$script" == scripts/red/red-known-failing-regression.sh &&
       ",$tags," == *,red,* && ",$tags," == *,diagnostics,* ]] || {
        log_fail "The intentional red sentinel registry contract changed"
        return 1
    }
    set +e
    "$TESTING_ROOT/$script" >"$sentinel_output" 2>&1
    sentinel_rc=$?
    set -e
    [[ "$sentinel_rc" -ne 0 ]] || {
        log_fail "The intentional red sentinel unexpectedly passed"
        return 1
    }
    grep -Fq 'Intentional failure: known failing regression sentinel' "$sentinel_output" || {
        log_fail "The intentional red sentinel failed for the wrong reason"
        return 1
    }
}

stage_contract_matrix() {
    local table="$TEST_OUTPUT_DIR/registry.tsv"
    local -a matrix=(
        'directory input accepted|A+-009|characterization-es-upsert-directory|fast'
        'no archive migration path|A+-009|characterization-no-archive-migration|fast'
        'recursive canonical traversal|B1-009|contract-traversal-b1-009|fast'
        'bytewise fixture order|B1-009|contract-traversal-b1-009|fast'
        'replay delegates per provenance directory|B2-002|contract-upsert-transaction-b2-002|fast'
        'replay preserves all|B2-002|contract-upsert-transaction-b2-002|fast'
        'pair required and one JVM|B2-002|contract-upsert-transaction-b2-002|fast'
        'aggregate provenance transaction|B1-010|contract-operator-json-b1-010|fast'
        'delete-create idempotent replacement|B2-002|contract-upsert-transaction-b2-002|fast'
        'content-addressed corruption failure|B2-002|contract-upsert-transaction-b2-002|fast'
        'same-pair contention before mutation|B2-002|contract-upsert-transaction-b2-002|fast'
        'distinct-pair lock hierarchy|B2-002|contract-upsert-transaction-b2-002|fast'
        'dead process releases ownership|B2-002|contract-upsert-transaction-b2-002|fast'
        'whole-store exclusion|B2-003|contract-delete-reinit-b2-003|fast'
        'WET document identity|C2-003|contract-provenance-c2-003|fast'
        'identity collision validation|C2-003|contract-provenance-c2-003|fast'
        'one Java JSON object|C2-004|contract-processing-json-c2-004|fast'
        'Java result success and failure|C2-004|contract-processing-json-c2-004|fast'
        'processing metric nullability|C2-004|contract-processing-json-c2-004|fast'
        'shell status-exit reconciliation|B1-010|contract-operator-json-b1-010|fast'
        'missing Java result fallback|B1-010|contract-operator-json-b1-010|fast'
        'final operator status|B1-010|contract-operator-json-b1-010|fast'
        'dry-run has no side effects|B1-010|contract-operator-json-b1-010|fast'
        'cross-layer result envelope|D3-005|contract-cross-layer-d3-005|integration'
        'direct JSON and replay summary|B1-010|contract-operator-json-b1-010|fast'
        'path JSON and UTF-8 policy|B1-010|contract-operator-json-b1-010|fast'
        'accepted artifact extensions|B1-009|contract-traversal-b1-009|fast'
        'direct and replay JVM limits|B2-002|contract-upsert-transaction-b2-002|fast'
        'SHA and capture-date replay equivalence|B2-002|int-replay-order-independence|integration'
        'warc2wet shell contract|B1-011|contract-warc2wet-shell-b1-011|fast'
        'record-level per-day split|C2-001|contract-per-day-c2-001|fast'
        'success-only output publication|C2-002|contract-output-publication-c2-002|fast'
        'delete and reinit output cleanup|B2-003|contract-delete-reinit-b2-003|fast'
    )
    local row label owner target required_scope
    local -A checked_owners=()
    local matrix_rc run_index=0 run_root="$TEST_OUTPUT_DIR/contract-matrix"
    local staged_dist="$PROJECT_ROOT/target/dist"
    local script_path run_log
    local -a target_rows=()

    [[ ${#matrix[@]} -eq 33 ]] || {
        log_fail "Frozen A+ matrix must contain exactly 33 rows"
        return 1
    }
    for row in "${matrix[@]}"; do
        IFS='|' read -r label owner target required_scope <<< "$row"
        [[ -n "$label" && -n "$owner" && -n "$target" ]] || return 1
        assert_registry_target "$table" "$target" "$required_scope" || return 1
        if [[ -z "${checked_owners[$owner]:-}" ]]; then
            assert_completed_owner "$owner" || return 1
            checked_owners["$owner"]=1
        fi
        add_acceptance_target "$target"
    done

    mkdir -p "$run_root"
    for target in "${ACCEPTANCE_TARGETS[@]}"; do
        mapfile -t target_rows < <(awk -F '\t' -v target="$target" '$1 == target' "$table")
        [[ ${#target_rows[@]} -eq 1 ]] || return 1
        IFS=$'\t' read -r id required_scope script tags <<< "${target_rows[0]}"
        script_path="$TESTING_ROOT/$script"
        run_index=$((run_index + 1))
        run_log="$run_root/$run_index-$target.log"
        mkdir -p "$run_root/$run_index"
        set +e
        PROJECT_ROOT="$PROJECT_ROOT" DIST_ROOT="$staged_dist" \
            TESTING_TMP_DIR="$run_root/$run_index" \
            timeout 300 bash "$script_path" >"$run_log" 2>&1
        matrix_rc=$?
        set -e
        if [[ "$matrix_rc" -ne 0 ]]; then
            tail -n 120 "$run_log" >&2
            log_fail "Promoted contract failed: $target"
            return 1
        fi
        if grep -Eq '^TESTCASE\|.*\|SKIP(\||$)' "$run_log"; then
            tail -n 40 "$run_log" >&2
            log_fail "Promoted contract skipped: $target"
            return 1
        fi
    done
}

assert_help_from_foreign_cwd() {
    local command="$1" label="$2"
    local stdout="$TEST_OUTPUT_DIR/help-$label.stdout"
    local stderr="$TEST_OUTPUT_DIR/help-$label.stderr"
    (cd "$TEST_OUTPUT_DIR/foreign-cwd" && "$command" --help >"$stdout" 2>"$stderr") || {
        log_fail "$label --help failed from a foreign working directory"
        return 1
    }
    grep -qi '^usage:' "$stdout" || {
        log_fail "$label --help did not print usage"
        return 1
    }
    [[ ! -s "$stderr" ]] || {
        log_fail "$label --help wrote diagnostics"
        return 1
    }
}

stage_install_release() {
    local build_log="$TEST_OUTPUT_DIR/release-check.log"
    local command output

    if ! make -C "$PROJECT_ROOT" release-check >"$build_log" 2>&1; then
        tail -n 120 "$build_log" >&2
        log_fail "make release-check failed"
        return 1
    fi
    assert_file_exists "$RELEASE_ARCHIVE" || return 1
    INSTALL_ROOT="$TEST_OUTPUT_DIR/release-install"
    [[ ! -e "$INSTALL_ROOT" ]] || {
        log_fail "Release install target was not fresh: $INSTALL_ROOT"
        return 1
    }
    mkdir -p "$INSTALL_ROOT" "$TEST_OUTPUT_DIR/foreign-cwd"
    tar -xzf "$RELEASE_ARCHIVE" -C "$INSTALL_ROOT" || return 1

    for command in warc2wet.sh wet-merge.sh es-upsert.sh es-upsert-all.sh es-delete.sh es-reinit.sh; do
        assert_help_from_foreign_cwd "$INSTALL_ROOT/$command" "${command%.sh}" || return 1
    done
    assert_help_from_foreign_cwd "$INSTALL_ROOT/app/bin/warc-cli" warc-cli || return 1
    output="$TEST_OUTPUT_DIR/es-cli-url.stdout"
    (cd "$TEST_OUTPUT_DIR/foreign-cwd" &&
        "$INSTALL_ROOT/app/bin/es-cli" where-es-url >"$output" 2>"$output.stderr") || {
        log_fail "es-cli where-es-url failed from a foreign working directory"
        return 1
    }
    [[ "$(cat "$output")" == "$ES_URL" && ! -s "$output.stderr" ]] || {
        log_fail "Packaged es-cli did not use the explicit Elasticsearch URL"
        return 1
    }
}

stage_release_allowlist() {
    local forbidden data_dir
    local template="$INSTALL_ROOT/app/conf/elasticsearch/templates/nac-data-template.json"

    for forbidden in .profile testing app/tmp app/var var log app/lib/scripts/pipeline-direct; do
        [[ ! -e "$INSTALL_ROOT/$forbidden" ]] || {
            log_fail "Forbidden release member was installed: $forbidden"
            return 1
        }
    done
    [[ -z "$(find "$INSTALL_ROOT" -mindepth 1 -name .profile -print -quit)" ]] || {
        log_fail "Release archive contains a deployment profile"
        return 1
    }
    for data_dir in in wet doet all; do
        [[ -d "$INSTALL_ROOT/$data_dir" ]] || return 1
        [[ -z "$(find "$INSTALL_ROOT/$data_dir" -mindepth 1 -print -quit)" ]] || {
            log_fail "Release archive contains runtime data under $data_dir/"
            return 1
        }
    done
    if [[ -n "$ES_PASS" ]] && grep -R -F -q -- "$ES_PASS" "$INSTALL_ROOT"; then
        log_fail "Release archive contains the supplied Elasticsearch secret"
        return 1
    fi
    jq -e '.index_patterns == ["nac-data", "nac-data-*"]' "$template" >/dev/null || {
        log_fail "Packaged template does not cover exact nac-data stream names"
        return 1
    }
}

installed_es_cli() {
    "$INSTALL_ROOT/app/bin/es-cli" "$@"
}

refresh_stream() {
    installed_es_cli refresh "$FINAL_STREAM" >/dev/null
}

query_pair() {
    local output="$1" url_id="$2" crawl_id="$3"
    local auth=() query
    [[ -z "$ES_PASS" ]] || auth=(-u "$ES_USER:$ES_PASS")
    query="$(jq -cn --arg url_id "$url_id" --arg crawl_id "$crawl_id" '
      {size:10,query:{bool:{filter:[
        {term:{"nac-url-id":$url_id}},
        {term:{"nac-crawl-id":$crawl_id}}
      ]}}}
    ')"
    curl -fsS --connect-timeout 5 --max-time 30 "${auth[@]}" \
        -H 'Content-Type: application/json' -X GET \
        "${ES_URL%/}/$FINAL_STREAM/_search" --data-binary "$query" > "$output"
}

assert_fixture_documents() {
    local snapshot="$1" url_id="$2" crawl_id="$3"
    jq -e --arg url_id "$url_id" --arg crawl_id "$crawl_id" '
      .hits.total.value == 2 and
      ([.hits.hits[]._source."nac-url-id"] | unique) == [$url_id] and
      ([.hits.hits[]._source."nac-crawl-id"] | unique) == [$crawl_id] and
      ([.hits.hits[]._source."warc-uri"] | sort) ==
        ["https://example.test/day-one", "https://example.test/day-two"] and
      ([.hits.hits[]._source."warc-date"] | sort) ==
        ["2026-01-01T23:59:59Z", "2026-01-02T00:00:01Z"]
    ' "$snapshot" >/dev/null || {
        log_fail "Packaged workflow did not produce the expected two provenance documents"
        return 1
    }
}

snapshot_archive() {
    local output="$1" file relative digest size
    : > "$output"
    while IFS= read -r -d '' file; do
        relative="${file#"$INSTALL_ROOT/all/"}"
        digest="$(sha256sum -- "$file" | awk '{print $1}')"
        size="$(stat -c %s -- "$file")"
        printf '%s\t%s\t%s\n' "$relative" "$size" "$digest" >> "$output"
    done < <(find "$INSTALL_ROOT/all" -type f -print0 | sort -z)
}

run_extract() {
    local fixture="$1" url_id="$2" crawl_id="$3" result="$4"
    (cd "$TEST_OUTPUT_DIR/foreign-cwd" &&
        "$INSTALL_ROOT/warc2wet.sh" --url-id="$url_id" --crawl-id="$crawl_id" \
        --result-format=json "$fixture" >"$result" 2>"$result.stderr") || return 1
    jq -e '.schema == "warc2es.operator/v1" and .command == "warc2wet" and
           .status == "ok" and .exit_code == 0 and (.outputs | length) == 1' \
        "$result" >/dev/null
}

run_ingest() {
    local url_id="$1" crawl_id="$2" result="$3"
    (cd "$TEST_OUTPUT_DIR/foreign-cwd" &&
        "$INSTALL_ROOT/es-upsert.sh" --url-id="$url_id" \
        --crawl-id="$crawl_id" --stream="$FINAL_STREAM" --es-url="$ES_URL" \
        --result-format=json >"$result" 2>"$result.stderr") || return 1
    jq -e '.schema == "warc2es.operator/v1" and .command == "es-upsert" and
           .status == "ok" and .exit_code == 0 and .publication.status == "published"' \
        "$result" >/dev/null
}

stage_packaged_roundtrip() {
    local suffix url_id crawl_id fixture staged_dir staged_file staged_sha pair_dir
    local result="$TEST_OUTPUT_DIR/operator.json"
    local snapshot="$TEST_OUTPUT_DIR/documents.json"
    local archive_before="$TEST_OUTPUT_DIR/archive-before.txt"
    local archive_after="$TEST_OUTPUT_DIR/archive-after.txt"
    local replay="$TEST_OUTPUT_DIR/replay.ndjson"
    local -a published=()

    IFS= read -r suffix < /proc/sys/kernel/random/uuid
    suffix="${suffix//-/}"
    FINAL_STREAM="nac-data-d3-release-$suffix"
    url_id="d3releaseurl$suffix"
    crawl_id="d3releasecrawl$suffix"
    fixture="$INSTALL_ROOT/in/multi-day.warc.gz"
    cp "$PROJECT_ROOT/src/test/resources/multi-day.warc.gz" "$fixture"

    if installed_es_cli get-stream "$FINAL_STREAM" >/dev/null 2>&1; then
        log_fail "High-entropy release stream already exists: $FINAL_STREAM"
        return 1
    fi
    CLEANUP_STREAM=true
    (cd "$TEST_OUTPUT_DIR/foreign-cwd" &&
        "$INSTALL_ROOT/es-reinit.sh" --stream="$FINAL_STREAM" --es-url="$ES_URL" \
        --yes --result-format=json >"$result" 2>"$result.stderr") || return 1
    jq -e --arg stream "$FINAL_STREAM" '
      .status == "ok" and .exit_code == 0 and .target.stream == $stream
    ' "$result" >/dev/null || return 1
    installed_es_cli get-stream "$FINAL_STREAM" >/dev/null || return 1
    if installed_es_cli get-stream "nac-data-$FINAL_STREAM" >/dev/null 2>&1; then
        log_fail "Exact stream name was double-prefixed"
        return 1
    fi

    run_extract "$fixture" "$url_id" "$crawl_id" "$result" || return 1
    staged_dir="$INSTALL_ROOT/wet/$url_id/$crawl_id"
    staged_file="$staged_dir/multi-day.wet.gz"
    assert_file_exists "$staged_file" || return 1
    staged_sha="$(sha256sum -- "$staged_file" | awk '{print $1}')"
    run_ingest "$url_id" "$crawl_id" "$result" || return 1
    pair_dir="$INSTALL_ROOT/all/wet/$url_id/$crawl_id"
    [[ ! -e "$staged_file" && -f "$pair_dir/$staged_sha.wet.gz" && -f "$fixture" ]] || {
        log_fail "Managed staging was not published to its exact SHA path"
        return 1
    }
    refresh_stream || return 1
    query_pair "$snapshot" "$url_id" "$crawl_id" || return 1
    assert_fixture_documents "$snapshot" "$url_id" "$crawl_id" || return 1

    (cd "$TEST_OUTPUT_DIR/foreign-cwd" &&
        "$INSTALL_ROOT/es-delete.sh" --stream="$FINAL_STREAM" --url-id="$url_id" \
        --crawl-id="$crawl_id" --es-url="$ES_URL" --result-format=json \
        >"$result" 2>"$result.stderr") || return 1
    jq -e '.status == "ok" and .exit_code == 0 and
           .publication.cleanup.matched == 1 and .publication.cleanup.removed == 1' \
        "$result" >/dev/null || return 1
    refresh_stream || return 1
    query_pair "$snapshot" "$url_id" "$crawl_id" || return 1
    jq -e '.hits.total.value == 0' "$snapshot" >/dev/null || return 1
    [[ -z "$(find "$pair_dir" -maxdepth 1 -type f -name '*.wet.gz' -print -quit 2>/dev/null)" ]] || {
        log_fail "Pair delete left a published WET"
        return 1
    }

    run_extract "$fixture" "$url_id" "$crawl_id" "$result" || return 1
    run_ingest "$url_id" "$crawl_id" "$result" || return 1
    mapfile -d '' -t published < <(find "$pair_dir" -maxdepth 1 -type f -name '*.wet.gz' -print0)
    [[ ${#published[@]} -eq 1 ]] || {
        log_fail "Republish did not create exactly one published WET"
        return 1
    }
    refresh_stream || return 1
    query_pair "$snapshot" "$url_id" "$crawl_id" || return 1
    assert_fixture_documents "$snapshot" "$url_id" "$crawl_id" || return 1
    snapshot_archive "$archive_before"

    (cd "$TEST_OUTPUT_DIR/foreign-cwd" &&
        "$INSTALL_ROOT/es-reinit.sh" --stream="$FINAL_STREAM" --es-url="$ES_URL" \
        --yes --result-format=json >"$result" 2>"$result.stderr") || return 1
    jq -e --arg stream "$FINAL_STREAM" '
      .status == "ok" and .exit_code == 0 and .target.stream == $stream
    ' "$result" >/dev/null || return 1
    snapshot_archive "$archive_after"
    cmp -s "$archive_before" "$archive_after" || {
        log_fail "Reinit changed the published archive"
        return 1
    }
    refresh_stream || return 1
    query_pair "$snapshot" "$url_id" "$crawl_id" || return 1
    jq -e '.hits.total.value == 0' "$snapshot" >/dev/null || return 1

    (cd "$TEST_OUTPUT_DIR/foreign-cwd" &&
        "$INSTALL_ROOT/es-upsert-all.sh" --stream="$FINAL_STREAM" --es-url="$ES_URL" \
        --result-format=json >"$replay" 2>"$replay.stderr") || return 1
    [[ "$(wc -l < "$replay")" -eq 2 ]] || {
        log_fail "Packaged replay did not emit one invocation and one summary"
        return 1
    }
    tail -n 1 "$replay" | jq -e '
      .schema == "warc2es.operator/v1" and .kind == "summary" and
      .command == "es-upsert-all" and .status == "ok" and .exit_code == 0 and
      .total == 1 and .succeeded == 1 and .failed == 0
    ' >/dev/null || return 1
    refresh_stream || return 1
    query_pair "$snapshot" "$url_id" "$crawl_id" || return 1
    assert_fixture_documents "$snapshot" "$url_id" "$crawl_id" || return 1
    snapshot_archive "$archive_after"
    cmp -s "$archive_before" "$archive_after" || {
        log_fail "Archive replay changed the published archive"
        return 1
    }
    [[ -f "$fixture" ]] || {
        log_fail "Packaged workflow removed the source WARC"
        return 1
    }
    head -n 1 "$replay" | jq -e '
      .schema == "warc2es.operator/v1" and .kind == "invocation" and
      .command == "es-upsert" and .mode == "archive-replay" and
      .status == "ok" and .exit_code == 0
    ' >/dev/null || return 1
    installed_es_cli delete-stream "$FINAL_STREAM" >/dev/null || {
        log_fail "Final release stream cleanup failed"
        return 1
    }
    if installed_es_cli get-stream "$FINAL_STREAM" >/dev/null 2>&1; then
        log_fail "Final release stream still exists after cleanup"
        return 1
    fi
    CLEANUP_STREAM=false
}

test_release_product_gate() {
    [[ "${RUN_DESTRUCTIVE_ES_TESTS:-false}" == true ]] || {
        log_fail "D3-004 requires RUN_DESTRUCTIVE_ES_TESTS=true"
        return 1
    }
    [[ "${REQUIRE_ES:-false}" == true ]] || {
        log_fail "D3-004 requires REQUIRE_ES=true so unavailable Elasticsearch cannot be skipped"
        return 1
    }
    [[ "${D3_RELEASE_DISPOSABLE_CLUSTER:-false}" == true ]] || {
        log_fail "D3-004 requires D3_RELEASE_DISPOSABLE_CLUSTER=true because es-reinit reapplies shared bootstrap resources"
        return 1
    }
    [[ -f "$PLAN" && -f "$EXECUTION_LOG" ]] || {
        log_fail "D3-004 requires the tracked TODO and execution log"
        return 1
    }
    assert_completed_owner D3-003 || return 1
    assert_completed_owner D3-005 || return 1

    run_stage release-install stage_install_release || return 1
    installed_es_cli check-health >/dev/null || {
        log_fail "Elasticsearch is required for D3-004 but is unavailable at $ES_URL"
        return 1
    }
    run_stage red-lifecycle stage_red_lifecycle || return 1
    run_stage frozen-contract-matrix stage_contract_matrix || return 1
    run_stage release-allowlist stage_release_allowlist || return 1
    run_stage packaged-roundtrip stage_packaged_roundtrip || return 1
    finish_stages
}

run_test test_release_product_gate
