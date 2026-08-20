#!/bin/bash
# src/main/dist/testing/test-lib.sh
# Shared variables and functions for test-cli test scripts

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Preserve script invocation context for run_test help support.
TEST_SCRIPT_ARGS=("$@")
TEST_SCRIPT_PATH="${BASH_SOURCE[1]:-}"

# Determine project root
# Assuming this script is sourced by a script in src/main/dist/testing/scripts/*/*
# or directly by test-cli which sets PROJECT_ROOT
if [ -z "${PROJECT_ROOT:-}" ]; then
    # Fallback: locate repository root by walking up to pom.xml
    FIND_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    while [[ "$FIND_ROOT" != "/" && ! -f "$FIND_ROOT/pom.xml" ]]; do
        FIND_ROOT="$(dirname "$FIND_ROOT")"
    done
    PROJECT_ROOT="$FIND_ROOT"
fi

DIST_ROOT="${DIST_ROOT:-$PROJECT_ROOT/target/dist}"
BIN_DIR="$DIST_ROOT/bin"
TESTING_TMP_DIR="${TESTING_TMP_DIR:-$PROJECT_ROOT/target/testing/tmp}"
TEST_DATA_DIR="${TEST_DATA_DIR:-$PROJECT_ROOT/../shared}"
if [[ -z "$TEST_DATA_DIR" || ! -f "$TEST_DATA_DIR/tiny.warc.gz" ]]; then
    TEST_DATA_DIR="$PROJECT_ROOT/../shared"
fi
TEST_OUTPUT_DIR="$TESTING_TMP_DIR/test-output-$$"

# Ensure bin dir is in path
export PATH="$BIN_DIR:$PATH"
export WARC_CLI="$BIN_DIR/warc-cli"
export ES_CLI="$BIN_DIR/es-cli"
export DIST_ROOT WARC_DIST_DIR="${WARC_DIST_DIR:-$DIST_ROOT}"

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[PASS]${NC} $1"
}

log_fail() {
    echo -e "${RED}[FAIL]${NC} $1" >&2
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# WARC record counting — case-insensitive, matches WARC 1.0 and 1.1.
# Handles both single files and MULTI_WARC directories (sums all *.gz inside).
warc_count() {
    local target="$1"
    if [[ -d "$target" ]]; then
        local total=0
        for f in "$target"/*.gz "$target"/*.warc "$target"/*.wet; do
            [[ -f "$f" ]] || continue
            local n
            n=$(zgrep -ic '^warc/1\.[01]' "$f" 2>/dev/null) || true
            total=$(( total + ${n:-0} ))
        done
        echo "$total"
    else
        local n
        n=$(zgrep -ic '^warc/1\.[01]' "$target" 2>/dev/null) || true
        echo "${n:-0}"
    fi
}

# Assertions

# usage: assert_command_success <exit_code> [message]
assert_command_success() {
    local code="$1"
    local msg="${2:-Command failed}"
    if [ "$code" -eq 0 ]; then
        return 0
    else
        log_fail "$msg (Exit code: $code)"
        return 1
    fi
}

# usage: assert_command_failure <exit_code> [message]
assert_command_failure() {
    local code="$1"
    local msg="${2:-Command unexpectedly succeeded}"
    if [ "$code" -ne 0 ]; then
        return 0
    else
        log_fail "$msg (Exit code: $code)"
        return 1
    fi
}

# usage: assert_file_exists <path>
assert_file_exists() {
    local file="$1"
    if [ -f "$file" ]; then
        return 0
    else
        log_fail "File not found: $file"
        return 1
    fi
}

# usage: assert_directory_exists <path>
assert_directory_exists() {
    local dir="$1"
    if [ -d "$dir" ]; then
        return 0
    else
        log_fail "Directory not found: $dir"
        return 1
    fi
}

# usage: assert_contains <needle> <haystack_file>
assert_contains() {
    local needle="$1"
    local file="$2"
    if grep -q "$needle" "$file"; then
        return 0
    else
        log_fail "File $file does not contain '$needle'"
        return 1
    fi
}

# usage: assert_not_contains <needle> <haystack_file>
assert_not_contains() {
    local needle="$1"
    local file="$2"
    if grep -q "$needle" "$file"; then
        log_fail "File $file unexpectedly contains '$needle'"
        return 1
    else
        return 0
    fi
}

# usage: assert_greater_than <actual> <expected>
assert_greater_than() {
    local actual="$1"
    local expected="$2"
    if [ "$actual" -gt "$expected" ]; then
        return 0
    else
        log_fail "Expected > $expected, got $actual"
        return 1
    fi
}

# Fault-injection fixture helpers

# usage: make_unwritable_directory <path>
# Creates a directory whose mode has no write bits. Callers that run as root
# should assert the mode rather than relying on -w, which root may bypass.
make_unwritable_directory() {
    local dir="$1"
    mkdir -p "$dir" || return 1
    chmod 0555 "$dir"
}

# usage: make_truncated_archive_fixture <source> <destination> [size-bytes]
# The default keeps half the source bytes, with a minimum of one byte.
make_truncated_archive_fixture() {
    local source="$1"
    local destination="$2"
    local requested_size="${3:-}"

    [[ -f "$source" ]] || {
        log_fail "Archive fixture source not found: $source"
        return 1
    }

    local source_size
    source_size=$(wc -c < "$source") || return 1
    [[ "$source_size" -gt 1 ]] || {
        log_fail "Archive fixture source is too small to truncate: $source"
        return 1
    }

    local truncated_size="$requested_size"
    if [[ -z "$truncated_size" ]]; then
        truncated_size=$((source_size / 2))
        [[ "$truncated_size" -gt 0 ]] || truncated_size=1
    fi
    if [[ ! "$truncated_size" =~ ^[0-9]+$ ]] || [[ "$truncated_size" -ge "$source_size" ]]; then
        log_fail "Truncated size must be a non-negative integer smaller than $source_size"
        return 1
    fi

    mkdir -p "$(dirname "$destination")" || return 1
    head -c "$truncated_size" "$source" > "$destination"
}

# usage: make_stub_executable <path> <sleep-forever|exit-nonzero> [exit-code]
make_stub_executable() {
    local target="$1"
    local behavior="$2"
    local exit_code="${3:-1}"

    mkdir -p "$(dirname "$target")" || return 1
    case "$behavior" in
        sleep-forever)
            {
                printf '%s\n' '#!/bin/bash'
                printf '%s\n' 'while :; do sleep 3600; done'
            } > "$target"
            ;;
        exit-nonzero)
            if [[ ! "$exit_code" =~ ^[1-9][0-9]*$ ]] || [[ "$exit_code" -gt 255 ]]; then
                log_fail "Stub exit code must be between 1 and 255"
                return 1
            fi
            {
                printf '%s\n' '#!/bin/bash'
                printf 'exit %s\n' "$exit_code"
            } > "$target"
            ;;
        *)
            log_fail "Unknown stub behavior: $behavior"
            return 1
            ;;
    esac
    chmod +x "$target"
}

# usage: make_fake_es_cli <path> <call-log> [exit-code]
# Each invocation is one shell-escaped line, so arguments containing whitespace
# remain unambiguous and can be reconstructed with Bash's printf %q format.
make_fake_es_cli() {
    local target="$1"
    local call_log="$2"
    local exit_code="${3:-0}"

    if [[ ! "$exit_code" =~ ^[0-9]+$ ]] || [[ "$exit_code" -gt 255 ]]; then
        log_fail "Fake es-cli exit code must be between 0 and 255"
        return 1
    fi

    mkdir -p "$(dirname "$target")" "$(dirname "$call_log")" || return 1
    : > "$call_log"
    # The single-quoted strings are source for the generated executable.
    # shellcheck disable=SC2016
    {
        printf '%s\n' '#!/bin/bash'
        printf 'call_log=%q\n' "$call_log"
        printf '%s\n' 'command_name="${1:-}"'
        printf '%s\n' 'printf "%q" "$1" >> "$call_log"'
        printf '%s\n' 'shift'
        printf '%s\n' 'printf " %q" "$@" >> "$call_log"'
        printf '%s\n' 'printf "\n" >> "$call_log"'
        printf '%s\n' 'if [[ "$command_name" == batch-delete ]]; then'
        printf '%s\n' '  printf '\''{"total":0,"deleted":0,"version_conflicts":0,"timed_out":false,"failures":[]}\n'\'''
        printf '%s\n' 'fi'
        printf 'exit %s\n' "$exit_code"
    } > "$target"
    chmod +x "$target"
}

# usage: make_call_capture_wrapper <wrapper-path> <target-executable> <capture-directory>
#
# One invocation directory is created per call. argv.nul contains the complete
# argument vector as NUL-delimited bytes; env/*.state distinguishes set from
# unset values; stdout, stderr and exit-status preserve the target result. The
# wrapper replays both output streams and exits with the target status, so the
# capture remains correct under `if ! wrapper ...` and caller redirections.
make_call_capture_wrapper() {
    local wrapper="$1"
    local target="$2"
    local capture_root="$3"

    mkdir -p "$(dirname "$wrapper")" "$capture_root" || return 1
    # The single-quoted strings are source for the generated executable.
    # shellcheck disable=SC2016
    {
        printf '%s\n' '#!/bin/bash'
        printf 'capture_target=%q\n' "$target"
        printf 'capture_root=%q\n' "$capture_root"
        printf '%s\n' 'mkdir -p "$capture_root" || exit 125'
        printf '%s\n' 'sequence=1'
        printf '%s\n' 'while :; do'
        printf '%s\n' '    printf -v invocation_dir "%s/invocation-%06d" "$capture_root" "$sequence"'
        printf '%s\n' '    if mkdir "$invocation_dir" 2>/dev/null; then break; fi'
        printf '%s\n' '    sequence=$((sequence + 1))'
        printf '%s\n' 'done'
        printf '%s\n' ': > "$invocation_dir/argv.nul"'
        printf '%s\n' 'if [[ "$#" -gt 0 ]]; then printf "%s\0" "$@" > "$invocation_dir/argv.nul"; fi'
        printf '%s\n' 'mkdir "$invocation_dir/env"'
        printf '%s\n' 'for variable in ES_URL ES_USER ES_PASS; do'
        printf '%s\n' '    if [[ -v "$variable" ]]; then'
        printf '%s\n' '        printf "%s\n" set > "$invocation_dir/env/$variable.state"'
        printf '%s\n' '        printf "%s" "${!variable}" > "$invocation_dir/env/$variable.value"'
        printf '%s\n' '    else'
        printf '%s\n' '        printf "%s\n" unset > "$invocation_dir/env/$variable.state"'
        printf '%s\n' '    fi'
        printf '%s\n' 'done'
        printf '%s\n' '"$capture_target" "$@" > "$invocation_dir/stdout" 2> "$invocation_dir/stderr"'
        printf '%s\n' 'status=$?'
        printf '%s\n' 'printf "%s\n" "$status" > "$invocation_dir/exit-status"'
        printf '%s\n' 'cat "$invocation_dir/stdout"'
        printf '%s\n' 'cat "$invocation_dir/stderr" >&2'
        printf '%s\n' 'exit "$status"'
    } > "$wrapper"
    chmod +x "$wrapper"
}

# Setup and Teardown helpers
setup_test_env() {
    mkdir -p "$TEST_OUTPUT_DIR"
}

cleanup_test_env() {
    if [ -d "$TEST_OUTPUT_DIR" ] && [ -z "${KEEP_TEST_OUTPUT:-}" ]; then
        rm -rf "$TEST_OUTPUT_DIR"
    fi
}

ensure_test_data() {
    local file="$1"
    if [ ! -f "$TEST_DATA_DIR/$file" ]; then
        log_warn "Test data $file not found in $TEST_DATA_DIR"
        return 1
    fi
    return 0
}

# Helper to run a test function and report result
# usage: run_test <function_name>
run_test() {
    local func_name="$1"

    if [[ "${TEST_SCRIPT_ARGS[0]:-}" == "--help" || "${TEST_SCRIPT_ARGS[0]:-}" == "-h" ]]; then
        print_test_help "$func_name"
        return 0
    fi

    log_info "Running $func_name..."

    setup_test_env

    if $func_name; then
        log_success "$func_name passed"
        cleanup_test_env
        return 0
    else
        log_fail "$func_name failed"
        cleanup_test_env
        return 1
    fi
}

extract_test_description() {
    local script_path="$1"
    if [[ -z "$script_path" || ! -f "$script_path" ]]; then
        return 0
    fi

    # Pick the first meaningful comment line after shebang/filename header.
    awk '
      NR == 1 && /^#!/ { next }
      /^# / {
        line = substr($0, 3)
        if (line ~ /\.sh$/) next
        if (line ~ /^[[:space:]]*$/) next
        print line
        exit
      }
    ' "$script_path"
}

# ---------------------------------------------------------------------------
# Stage-based test runner — for multi-step golden / e2e scenarios.
# Each stage emits a TESTCASE|name|PASS/FAIL line consumed by the test-cli
# harness (collect_case_json).  Scripts using run_stage should call
# finish_stages at the end; they should also set -euo pipefail so that a
# stage failure propagates and stops the script.
# ---------------------------------------------------------------------------

STAGE_PASS=0
STAGE_FAIL=0

# usage: run_stage <display-name> <function>
run_stage() {
    local name="$1"
    local func="$2"
    log_info "=== Stage: $name ==="
    local exit_code=0
    "$func" || exit_code=$?
    if [[ $exit_code -eq 0 ]]; then
        STAGE_PASS=$((STAGE_PASS + 1))
        log_success "$name"
        echo "TESTCASE|$name|PASS"
    else
        STAGE_FAIL=$((STAGE_FAIL + 1))
        log_fail "$name"
        echo "TESTCASE|$name|FAIL"
        return $exit_code
    fi
}

# Call at the end of a golden script to print totals and return exit code.
finish_stages() {
    echo ""
    local total=$((STAGE_PASS + STAGE_FAIL))
    log_info "Golden stages: $total total | ${GREEN}$STAGE_PASS passed${NC} | ${RED}$STAGE_FAIL failed${NC}"
    [[ "$STAGE_FAIL" -gt 0 ]] && return 1 || return 0
}

print_test_help() {
    local func_name="$1"
    local script_path="$TEST_SCRIPT_PATH"
    local script_name
    script_name="$(basename "${script_path:-test-script}")"
    local desc
    desc="$(extract_test_description "$script_path")"

    echo "Usage: $script_name [--help]"
    echo
    if [[ -n "$desc" ]]; then
        echo "Description: $desc"
    else
        local pretty="${func_name#test_}"
        pretty="${pretty//_/ }"
        echo "Description: ${pretty}."
    fi
    echo "Test Function: $func_name"
}
