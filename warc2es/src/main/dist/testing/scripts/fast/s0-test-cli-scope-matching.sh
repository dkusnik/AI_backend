#!/bin/bash
# s0-test-cli-scope-matching.sh — T-190
# Verify test-cli target resolution uses path-segment matching, not arbitrary
# substring matching. Specifically: target "red" must resolve only
# scripts/red/** and must not include unrelated scripts whose filenames
# happen to contain "red" as a substring (e.g. "required").
source "$(dirname "$0")/../../lib/test-lib.sh"

test_red_target_does_not_match_required() {
    log_info "Verifying 'red' target excludes scripts containing 'required' in filename..."

    local scripts_base
    scripts_base="$(dirname "$0")/../.."
    [[ -d "$scripts_base/scripts" ]] || scripts_base="$PROJECT_ROOT/src/main/dist/testing"

    local matches
    matches=$(find "$scripts_base/scripts" -path "*/red*" -name "*.sh" | sort)

    # Must include the intentional red test
    echo "$matches" | grep -q "red-known-failing-regression.sh" \
        || { log_fail "red target did not find red-known-failing-regression.sh"; return 1; }

    # Must NOT include required-args script (contains 'red' as substring inside 'required')
    if echo "$matches" | grep -q "required"; then
        log_fail "red target matched a path containing 'required' (substring collision)"
        echo "Matched paths:"
        echo "$matches"
        return 1
    fi

    log_info "Matches: $(echo "$matches" | wc -l | tr -d ' ') script(s) — no false positives"
}

test_s1_target_does_not_match_synthetic_integration() {
    log_info "Verifying 's1' target excludes synthetic integration scripts..."

    local scripts_base
    scripts_base="$(dirname "$0")/../.."
    [[ -d "$scripts_base/scripts" ]] || scripts_base="$PROJECT_ROOT/src/main/dist/testing"

    local matches
    matches=$(find "$scripts_base/scripts" -path "*/s1*" -name "*.sh" | sort)

    if echo "$matches" | grep -q "synthetic-s1"; then
        log_fail "s1 target matched test-merge-synthetic-s1-incremental.sh (segment collision)"
        echo "Matched paths:"
        echo "$matches"
        return 1
    fi

    log_info "s1 matches: $(echo "$matches" | wc -l | tr -d ' ') script(s) — no integration bleed"
}

run_test test_red_target_does_not_match_required
run_test test_s1_target_does_not_match_synthetic_integration
