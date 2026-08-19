#!/bin/bash
# Intentional red test case: validates harness FAIL signaling/reporting path.
source "$(dirname "$0")/../../lib/test-lib.sh"

test_known_failing_regression_signal() {
    log_fail "Intentional failure: known failing regression sentinel"
    return 1
}

run_test test_known_failing_regression_signal
