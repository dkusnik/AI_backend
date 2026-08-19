#!/bin/bash
# Validate review-signoff-gate behavior matrix for missing, placeholder, malformed, and valid sign-offs.
# Task-ID: T-192
source "$(dirname "$0")/../../../../lib/test-lib.sh"

make_signoff() {
    local file="$1"
    local date="$2"
    local commit="$3"
    local scope="$4"
    local findings="$5"
    local signoff="$6"

    cat > "$file" <<EOT
# Sign-Off
Date: $date
Commit: $commit
Scope: $scope
Findings: $findings
Sign-off: $signoff
EOT
}

test_review_signoff_gate_matrix() {
    local gate="$PROJECT_ROOT/src/main/dist/testing/scripts/pre-commit/review-signoff-gate.sh"
    local claude="$TEST_OUTPUT_DIR/claude.md"
    local codex="$TEST_OUTPUT_DIR/codex.md"

    local out code

    make_signoff "$claude" "2026-02-14" "abcdef1" "src/main/dist/testing" "none" "APPROVED"

    set +e
    out=$(CLAUDE_REVIEW_FILE="$claude" CODEX_REVIEW_FILE="$TEST_OUTPUT_DIR/missing.md" "$gate" 2>&1)
    code=$?
    set -e
    assert_command_failure "$code" "missing sign-off file must fail" || return 1
    echo "$out" | grep -qi "missing" || {
        log_fail "missing file scenario did not emit missing message"
        return 1
    }

    make_signoff "$codex" "YYYY-MM-DD" "<hash>" "<files/modules>" "<none|summary>" "APPROVED"
    set +e
    out=$(CLAUDE_REVIEW_FILE="$claude" CODEX_REVIEW_FILE="$codex" "$gate" 2>&1)
    code=$?
    set -e
    assert_command_failure "$code" "placeholder metadata must fail" || return 1
    echo "$out" | grep -qi "placeholder\|must match" || {
        log_fail "placeholder scenario did not emit validation error"
        return 1
    }

    make_signoff "$codex" "2026-02-14" "abcdef1" "src/main/dist/testing" "none" "REJECTED"
    set +e
    out=$(CLAUDE_REVIEW_FILE="$claude" CODEX_REVIEW_FILE="$codex" "$gate" 2>&1)
    code=$?
    set -e
    assert_command_failure "$code" "malformed sign-off must fail" || return 1
    echo "$out" | grep -qi "APPROVED" || {
        log_fail "malformed sign-off scenario did not mention APPROVED requirement"
        return 1
    }

    make_signoff "$codex" "2026-02-14" "abcdef123456" "src/main/dist/testing,tests" "none" "APPROVED"
    out=$(CLAUDE_REVIEW_FILE="$claude" CODEX_REVIEW_FILE="$codex" "$gate" 2>&1)
    code=$?
    assert_command_success "$code" "valid sign-off metadata should pass" || return 1
    echo "$out" | grep -q "\[PASS\]" || {
        log_fail "valid scenario did not emit PASS"
        return 1
    }
}

run_test test_review_signoff_gate_matrix
