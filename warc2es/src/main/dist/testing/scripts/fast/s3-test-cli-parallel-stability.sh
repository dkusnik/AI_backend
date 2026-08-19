#!/bin/bash
# s3-test-cli-parallel-stability.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

test_test_cli_parallel_stability_subset() {
    local cli="$BIN_DIR/test-cli"
    assert_file_exists "$cli" || return 1

    set +e
    "$cli" --parallel=2 scripts/fast/s0-help-text.sh scripts/fast/s0-version-check.sh scripts/fast/s1-command-usage-nonempty.sh >/dev/null 2>&1
    local code=$?
    set -e

    assert_command_success "$code" "parallel test-cli subset should pass" || return 1
}

run_test test_test_cli_parallel_stability_subset
