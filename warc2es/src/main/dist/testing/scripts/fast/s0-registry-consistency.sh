#!/bin/bash
# Enforce registry coverage for executable test scripts and explicit helper allowlist.
# Task-ID: T-257
set -euo pipefail
source "$(dirname "$0")/../../lib/test-lib.sh"

test_registry_consistency() {
    local root="$PROJECT_ROOT/src/main/dist/testing"
    local scripts_dir="$root/scripts"
    local registry="$root/registry.yaml"
    local allowlist="$root/helper-allowlist.txt"
    local work_dir="$TEST_OUTPUT_DIR/registry-check"
    mkdir -p "$work_dir"

    local all="$work_dir/all.txt"
    local registered="$work_dir/registered.txt"
    local allowed_helpers="$work_dir/allowed-helpers.txt"
    local unregistered="$work_dir/unregistered.txt"
    local missing_registered="$work_dir/missing-registered.txt"

    find "$scripts_dir" -type f -name '*.sh' \
        | sed "s#^$scripts_dir/##" \
        | sort > "$all"

    awk -F': ' '/script: /{print $2}' "$registry" \
        | sed 's#^scripts/##' \
        | sort > "$registered"

    grep -Ev '^\s*#|^\s*$' "$allowlist" | sort > "$allowed_helpers"

    comm -23 "$all" "$registered" > "$unregistered"
    comm -13 "$all" "$registered" > "$missing_registered"

    if [[ -s "$missing_registered" ]]; then
        log_fail "registry.yaml references missing scripts:"
        sed 's/^/  - /' "$missing_registered" >&2
        return 1
    fi

    local unexpected="$work_dir/unexpected-unregistered.txt"
    comm -23 "$unregistered" "$allowed_helpers" > "$unexpected"
    if [[ -s "$unexpected" ]]; then
        log_fail "Unregistered scripts not in helper allowlist:"
        sed 's/^/  - /' "$unexpected" >&2
        return 1
    fi

    local stale_allow="$work_dir/stale-allowlist.txt"
    comm -13 "$unregistered" "$allowed_helpers" > "$stale_allow"
    if [[ -s "$stale_allow" ]]; then
        log_fail "Helper allowlist contains entries no longer unregistered:"
        sed 's/^/  - /' "$stale_allow" >&2
        return 1
    fi

    log_info "Registry consistency check passed"
    return 0
}

run_test test_registry_consistency
