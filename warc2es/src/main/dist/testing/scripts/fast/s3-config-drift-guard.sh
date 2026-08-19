#!/bin/bash
# Guard shared base settings and warc2wet semantics in admin and MVP configs.
source "$(dirname "$0")/../../lib/test-lib.sh"

extract_shared_base_config() {
    local file="$1"
    awk '
      /^global:/ { in_global=1; next }
      in_global && /^[^[:space:]]/ { exit }
      in_global && /^  verbosity:/ {
        line=$0
        sub(/[[:space:]]+#.*/, "", line)
        sub(/^  verbosity:[[:space:]]*/, "", line)
        gsub(/["'\''[:space:]]/, "", line)
        print "verbosity=" line
        next
      }
      in_global && /^  engine:/ { in_engine=1; next }
      in_engine && /^  [^[:space:]]/ { in_engine=0 }
      in_engine && /^    [A-Za-z][A-Za-z0-9_-]*:/ {
        line=$0
        sub(/[[:space:]]+#.*/, "", line)
        sub(/^    /, "", line)
        key=line
        sub(/:.*/, "", key)
        sub(/^[^:]*:[[:space:]]*/, "", line)
        gsub(/["'\''[:space:]]/, "", line)
        print "engine." key "=" line
      }
    ' "$file" | LC_ALL=C sort
}

write_expected_shared_base_config() {
    local output="$1"
    printf '%s\n' \
        'verbosity=BRIEF' \
        'engine.type=virtual' \
        'engine.concurrency=50' \
        'engine.shutdownTimeout=60' \
        'engine.recordSizeThresholdMB=10' \
        'engine.maxRecords=5' \
        'engine.isalEnabled=true' \
        | LC_ALL=C sort > "$output"
}

compare_shared_base_config() {
    local expected="$1"
    local actual="$2"
    local label="$3"

    if cmp -s "$expected" "$actual"; then
        return 0
    fi
    echo "shared base config mismatch: $label" >&2
    diff -u "$expected" "$actual" >&2 || true
    return 1
}

extract_warc2wet_semantic_block() {
    local file="$1"
    awk '
      /^  warc2wet:/ { in_pipeline=1; next }
      in_pipeline && /^  [^[:space:]].*:/ { exit }
      in_pipeline {
        line=$0
        sub(/[[:space:]]+#.*/, "", line)
        sub(/[[:space:]]+$/, "", line)
        trimmed=line
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", trimmed)
        if (trimmed == "") next
        # These exact values are effective-equivalent: pdftotext is the module
        # default, while fastText settings are inert when use-fasttext=false.
        if (trimmed == "pdftotext-path: pdftotext") next
        if (trimmed == "fasttext-model-path: \"../conf/lid.176.ftz\"") next
        if (trimmed == "fasttext-process-count: 3") next
        print line
      }
    ' "$file"
}

test_warc2wet_semantic_config_drift_guard() {
    local admin="$PROJECT_ROOT/src/main/resources/config.yaml"
    local mvp="$PROJECT_ROOT/src/main/resources/config-out.yaml"
    local admin_block="$TEST_OUTPUT_DIR/admin-warc2wet.txt"
    local mvp_block="$TEST_OUTPUT_DIR/mvp-warc2wet.txt"

    extract_warc2wet_semantic_block "$admin" > "$admin_block"
    extract_warc2wet_semantic_block "$mvp" > "$mvp_block"

    if [[ ! -s "$admin_block" || ! -s "$mvp_block" ]]; then
        log_fail "warc2wet semantics could not be extracted"
        return 1
    fi

    if ! cmp -s "$admin_block" "$mvp_block"; then
        log_fail "warc2wet semantics drifted between config.yaml and config-out.yaml"
        diff -u "$admin_block" "$mvp_block" >&2 || true
        return 1
    fi
}

test_shared_base_config_drift_guard() {
    local admin="$PROJECT_ROOT/src/main/resources/config.yaml"
    local mvp="$PROJECT_ROOT/src/main/resources/config-out.yaml"
    local expected="$TEST_OUTPUT_DIR/expected-base.txt"
    local admin_actual="$TEST_OUTPUT_DIR/admin-base.txt"
    local mvp_actual="$TEST_OUTPUT_DIR/mvp-base.txt"
    local mutated="$TEST_OUTPUT_DIR/config-out-mutated.yaml"
    local mutated_actual="$TEST_OUTPUT_DIR/mutated-base.txt"
    local mutation_error="$TEST_OUTPUT_DIR/mutation.err"

    write_expected_shared_base_config "$expected"
    extract_shared_base_config "$admin" > "$admin_actual"
    extract_shared_base_config "$mvp" > "$mvp_actual"

    compare_shared_base_config "$expected" "$admin_actual" "config.yaml" || return 1
    compare_shared_base_config "$expected" "$mvp_actual" "config-out.yaml" || return 1

    if ! sed '0,/concurrency: 50/s//concurrency: 51/' "$mvp" > "$mutated"; then
        log_fail "could not create one-sided config mutation"
        return 1
    fi
    extract_shared_base_config "$mutated" > "$mutated_actual"
    if ! grep -Fxq 'engine.concurrency=51' "$mutated_actual" \
        || grep -Fxq 'engine.concurrency=50' "$mutated_actual"; then
        log_fail "one-sided engine mutation was not materialized exactly"
        return 1
    fi
    if compare_shared_base_config "$expected" "$mutated_actual" "injected config-out.yaml" \
        2> "$mutation_error"; then
        log_fail "one-sided engine mutation was not detected"
        return 1
    fi
    if ! grep -Fq "shared base config mismatch: injected config-out.yaml" "$mutation_error"; then
        log_fail "one-sided engine mutation did not emit the stable mismatch diagnostic"
        return 1
    fi
}

run_test test_shared_base_config_drift_guard || exit 1
run_test test_warc2wet_semantic_config_drift_guard
