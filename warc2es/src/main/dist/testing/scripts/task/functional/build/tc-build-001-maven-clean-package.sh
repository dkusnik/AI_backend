#!/bin/bash
# @timeout: 1800
# tc-build-001-maven-clean-package.sh
source "$(dirname "$0")/../../../../lib/test-lib.sh"

test_maven_clean_package() {
    local sandbox="$TEST_OUTPUT_DIR/maven-clean-package-repo"
    mkdir -p "$sandbox"

    log_info "Preparing isolated source snapshot in $sandbox..."
    rsync -a --delete \
        --exclude=".git/" \
        --exclude="/dist/" \
        --exclude="/target/" \
        --exclude="/warc/" \
        --exclude="/src/native/" \
        --exclude="/tmp/" \
        "$PROJECT_ROOT/" "$sandbox/" || {
        log_fail "Failed to create isolated source snapshot"
        return 1
    }

    log_info "Running mvn clean package in isolated repo..."
    local output
    set +e
    output=$(cd "$sandbox" && mvn clean package -DskipTests -Dnative.optional=true 2>&1)
    local code=$?
    set -e

    echo "$output"

    if ! assert_command_success "$code" "mvn clean package failed"; then
        echo "TESTCASE|maven-clean-package|FAIL|exit=$code"
        return 1
    fi

    local built_jar="$sandbox/target/dist/lib/pipeline.jar"
    if ! assert_file_exists "$built_jar"; then
        echo "TESTCASE|maven-clean-package-artifact|FAIL|missing=$built_jar"
        return 1
    fi

    if [[ -d "$sandbox/dist" ]]; then
        log_fail "mvn clean package produced legacy root dist/: $sandbox/dist"
        echo "TESTCASE|maven-clean-package-no-root-dist|FAIL|path=$sandbox/dist"
        return 1
    fi

    echo "TESTCASE|maven-clean-package-no-root-dist|PASS|target=target/dist"
    echo "TESTCASE|maven-clean-package|PASS|artifact=$built_jar"
    return 0
}

run_test test_maven_clean_package
