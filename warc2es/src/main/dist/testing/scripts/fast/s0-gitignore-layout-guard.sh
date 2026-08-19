#!/bin/bash
# Guard the repository ignore contract after the target/dist + out split.
source "$(dirname "$0")/../../lib/test-lib.sh"

test_gitignore_layout_guard() {
    local repo_root
    local line
    local rc=0

    repo_root="$(cd "$PROJECT_ROOT/.." && pwd)"

    line=$(git -C "$repo_root" check-ignore --no-index -v warc2es/target/dist 2>/dev/null | tail -n 1 || true)
    if [[ "$line" != *"target/"* ]]; then
        log_fail "warc2es/target/dist is not ignored by the target/ rule"
        rc=1
    fi

    line=$(git -C "$repo_root" check-ignore --no-index -v warc2es/out/app 2>/dev/null | tail -n 1 || true)
    if [[ "$line" != *"out/"* ]]; then
        log_fail "warc2es/out/app is not ignored by the out/ rule"
        rc=1
    fi

    line=$(git -C "$repo_root" check-ignore --no-index -v warc2es/src/main/dist/native/libreadability_jni.so 2>/dev/null | tail -n 1 || true)
    if [[ "$line" != *"**/src/**/*.so"* ]]; then
        log_fail "source-tree native libraries are not blocked by the src/**/*.so rule"
        rc=1
    fi

    line=$(git -C "$repo_root" check-ignore --no-index -v warc2es/src/main/dist/warc2wet.sh 2>/dev/null | tail -n 1 || true)
    if [[ "$line" != *"!**/src/main/dist/**"* ]]; then
        log_fail "src/main/dist scripts are not allowed by the source dist allow rule"
        rc=1
    fi

    line=$(git -C "$repo_root" check-ignore --no-index -v warc2es/src/main/dist/bin/warc-cli 2>/dev/null | tail -n 1 || true)
    if [[ "$line" != *"!**/dist/bin/*"* ]]; then
        log_fail "dist/bin scripts are not allowed by the dist/bin allow rule"
        rc=1
    fi

    return "$rc"
}

run_test test_gitignore_layout_guard
