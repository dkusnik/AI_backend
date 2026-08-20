#!/bin/bash
# @timeout: 1800
# tc-build-002-unit-tests.sh
source "$(dirname "$0")/../../../../lib/test-lib.sh"

test_maven_unit_tests() {
    local sandbox="$TEST_OUTPUT_DIR/maven-unit-tests-repo"
    mkdir -p "$sandbox"

    log_info "Preparing isolated source snapshot in $sandbox..."
    rsync -a --delete \
        --exclude=".git/" \
        --exclude="/dist/" \
        --exclude="/target/" \
        --exclude="/src/native/target/" \
        --exclude="/tmp/" \
        "$PROJECT_ROOT/" "$sandbox/" || {
        log_fail "Failed to create isolated source snapshot"
        return 1
    }

    log_info "Running mvn test in isolated repo..."
    local output
    set +e
    output=$(cd "$sandbox" && mvn test 2>&1)
    local code=$?
    set -e

    echo "$output"

    if ! assert_command_success "$code" "mvn test failed"; then
        echo "TESTCASE|maven-unit-tests|FAIL|exit=$code"
        return 1
    fi

    echo "TESTCASE|maven-unit-tests|PASS"
    return 0
}

run_test test_maven_unit_tests
