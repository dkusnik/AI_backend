#!/bin/bash
# tc-es-012-connection-failure.sh — T-199
# Verify es-cli produces a clear error and non-zero exit when ES is unreachable.
# The tool must not hang indefinitely.
# @timeout: 30
source "$(dirname "$0")/../../../../lib/test-lib.sh"

DEAD_ES_URL="http://127.0.0.1:19200"

test_check_health_unreachable() {
    log_info "Testing es-cli check-health against unreachable ES host ($DEAD_ES_URL)..."

    local exit_code
    set +e
    timeout 20s env ES_URL="$DEAD_ES_URL" "$ES_CLI" check-health > /dev/null 2>&1
    exit_code=$?
    set -e

    if [[ $exit_code -eq 124 ]]; then
        log_fail "es-cli check-health hung (timeout after 20s) on unreachable host"
        return 1
    fi

    if [[ $exit_code -eq 0 ]]; then
        log_fail "es-cli check-health exited 0 on unreachable host (expected non-zero)"
        return 1
    fi

    log_info "check-health exit=$exit_code — non-zero on unreachable host OK"
}

test_list_indices_unreachable() {
    log_info "Testing es-cli list-indices against unreachable ES host..."

    local exit_code
    set +e
    timeout 20s env ES_URL="$DEAD_ES_URL" "$ES_CLI" list-indices > /dev/null 2>&1
    exit_code=$?
    set -e

    if [[ $exit_code -eq 124 ]]; then
        log_fail "es-cli list-indices hung (timeout) on unreachable host"
        return 1
    fi

    if [[ $exit_code -eq 0 ]]; then
        log_fail "es-cli list-indices exited 0 on unreachable host"
        return 1
    fi

    log_info "list-indices exit=$exit_code — non-zero OK"
}

test_search_unreachable() {
    log_info "Testing es-cli search against unreachable ES host..."

    local exit_code
    set +e
    timeout 20s env ES_URL="$DEAD_ES_URL" "$ES_CLI" search "test query" > /dev/null 2>&1
    exit_code=$?
    set -e

    if [[ $exit_code -eq 124 ]]; then
        log_fail "es-cli search hung (timeout) on unreachable host"
        return 1
    fi

    if [[ $exit_code -eq 0 ]]; then
        log_fail "es-cli search exited 0 on unreachable host"
        return 1
    fi

    log_info "search exit=$exit_code — non-zero OK"
}

run_test test_check_health_unreachable
run_test test_list_indices_unreachable
run_test test_search_unreachable
