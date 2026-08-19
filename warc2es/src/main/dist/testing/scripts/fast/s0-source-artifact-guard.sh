#!/bin/bash
# Guard against tracked generated artifacts under source trees.
source "$(dirname "$0")/../../lib/test-lib.sh"

test_source_artifact_guard() {
    local repo_root
    local matches

    repo_root="$(cd "$PROJECT_ROOT/.." && pwd)"
    matches=$(git -C "$repo_root" ls-files | grep -E '(^|/)src/.*\.(so|jar|tar\.gz|jfr)$' || true)

    if [[ -n "$matches" ]]; then
        log_fail "Tracked generated artifacts found under source trees:"
        printf '%s\n' "$matches" >&2
        return 1
    fi
}

run_test test_source_artifact_guard
