#!/bin/bash
# Concurrent warc2wet invocations must not reset each other's RocksDB scratch space.
source "$(dirname "$0")/../../lib/test-lib.sh"

test_warc2wet_rocksdb_isolation() {
    local runtime="$TEST_OUTPUT_DIR/runtime"
    local scripts="$runtime/app/lib/scripts"
    local wrapper="$runtime/warc2wet.sh"
    local sync="$runtime/sync"
    local input_one="$runtime/one.warc"
    local input_two="$runtime/two.warc"
    local pid_one pid_two rc_one rc_two path_one path_two leftovers

    mkdir -p "$scripts" "$runtime/app/var/db" "$sync"
    cp "$PROJECT_ROOT/src/main/dist/warc2wet.sh" "$wrapper"
    cp "$PROJECT_ROOT/src/main/dist/lib/scripts/runtime-lib.sh" "$scripts/runtime-lib.sh"

    cat > "$scripts/pipeline-lib" <<'FAKE_PIPELINE'
run_pipeline() {
    local arg db="" ready="0"
    for arg in "$@"; do
        case "$arg" in
            --processor.doet-accumulator.rocksdb-path=*) db="${arg#*=}" ;;
        esac
    done
    [[ -n "$db" ]] || { echo "missing RocksDB path" >&2; return 70; }

    printf '%s\n' "$db" > "$SYNC_DIR/$RUN_TOKEN.path"
    if [[ "$RUN_TOKEN" == two ]]; then
        for _ in {1..400}; do
            [[ -f "$SYNC_DIR/one.ready" ]] && break
            sleep 0.01
        done
        [[ -f "$SYNC_DIR/one.ready" ]] || {
            echo "first invocation did not open its scratch directory" >&2
            return 71
        }
    fi
    rm -rf -- "$db"
    mkdir -p "$db"
    : > "$db/$RUN_TOKEN.marker"
    : > "$SYNC_DIR/$RUN_TOKEN.ready"

    for _ in {1..400}; do
        ready=$(find "$SYNC_DIR" -maxdepth 1 -type f -name '*.ready' | wc -l)
        if [[ "$ready" -ge 2 ]]; then
            break
        fi
        sleep 0.01
    done
    [[ "$ready" -ge 2 ]] || { echo "concurrency rendezvous timed out" >&2; return 71; }
    [[ -f "$db/$RUN_TOKEN.marker" ]] || {
        echo "RocksDB scratch was reset by the other invocation" >&2
        return 72
    }
}
FAKE_PIPELINE

    printf 'WARC/1.0\r\nWARC-Date: 2026-01-02T03:04:05Z\r\n\r\n' > "$input_one"
    cp "$input_one" "$input_two"

    set +e
    RUN_TOKEN=one SYNC_DIR="$sync" bash "$wrapper" \
        --url-id=Site --crawl-id=one --result-format=human \
        "$input_one" > "$runtime/one.log" 2>&1 &
    pid_one=$!
    RUN_TOKEN=two SYNC_DIR="$sync" bash "$wrapper" \
        --url-id=Site --crawl-id=two --result-format=human \
        "$input_two" > "$runtime/two.log" 2>&1 &
    pid_two=$!
    wait "$pid_one"; rc_one=$?
    wait "$pid_two"; rc_two=$?
    set -e

    if [[ "$rc_one" -ne 0 || "$rc_two" -ne 0 ]]; then
        log_fail "Concurrent warc2wet invocations interfered: rc=$rc_one/$rc_two"
        sed -n '1,120p' "$runtime/one.log" >&2
        sed -n '1,120p' "$runtime/two.log" >&2
        return 1
    fi

    path_one=$(<"$sync/one.path")
    path_two=$(<"$sync/two.path")
    [[ "$path_one" != "$path_two" ]] || {
        log_fail "Concurrent invocations used the same RocksDB path: $path_one"
        return 1
    }
    [[ "$path_one" == "$runtime/app/var/db/doet."* && \
       "$path_two" == "$runtime/app/var/db/doet."* ]] || {
        log_fail "RocksDB scratch escaped app/var/db"
        return 1
    }

    leftovers=$(find "$runtime/app/var/db" -maxdepth 1 -type d -name 'doet.*' -print)
    [[ -z "$leftovers" ]] || {
        log_fail "warc2wet left RocksDB scratch directories: $leftovers"
        return 1
    }
}

run_test test_warc2wet_rocksdb_isolation
