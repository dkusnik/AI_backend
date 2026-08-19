#!/bin/bash
# Relative dedupe and grep paths must resolve from the invoking directory.
source "$(dirname "$0")/../../lib/test-lib.sh"

test_relative_dedupe_and_grep_paths() {
    local foreign="$TEST_OUTPUT_DIR/foreign-cwd"
    local fake_bin="$TEST_OUTPUT_DIR/fake-bin"
    local input="input.wet"
    local deduped="deduped-$$.wet"
    local filtered="filtered-$$.wet"

    mkdir -p "$foreign"
    cp "$PROJECT_ROOT/src/test/resources/doet-ordering/baseline-2026-01-15.wet" "$foreign/$input"
    rm -f "$BIN_DIR/$deduped" "$BIN_DIR/$filtered"

    (cd "$foreign" && "$WARC_CLI" dedupe "$input" "$deduped" --silent) || return 1
    [[ -f "$foreign/$deduped" ]] || {
        log_fail "dedupe did not create its output in the invoking directory"
        return 1
    }
    [[ ! -e "$BIN_DIR/$deduped" ]] || {
        log_fail "dedupe wrote into the distribution tree"
        return 1
    }

    (cd "$foreign" && "$WARC_CLI" grep "$input" "$filtered" --silent) || return 1
    [[ -f "$foreign/$filtered" ]] || {
        log_fail "grep did not create its output in the invoking directory"
        return 1
    }
    [[ ! -e "$BIN_DIR/$filtered" ]] || {
        log_fail "grep wrote into the distribution tree"
        return 1
    }

    mkdir -p "$fake_bin"
    cat > "$fake_bin/java" << 'EOF'
#!/bin/bash
printf '%s\n' "$@" > "$WARC_TEST_JAVA_ARGS"
EOF
    chmod +x "$fake_bin/java"

    assert_pipeline_input_from_foreign_cwd() {
        local name="$1"
        shift
        local captured="$TEST_OUTPUT_DIR/$name-java-args"
        (cd "$foreign" && PATH="$fake_bin:$PATH" WARC_TEST_JAVA_ARGS="$captured" "$@") || return 1
        grep -Fx "$foreign/$input" "$captured" > /dev/null || {
            log_fail "$name did not pass an input resolved from the invoking directory"
            return 1
        }
    }

    assert_pipeline_input_from_foreign_cwd regen-digests \
        "$WARC_CLI" regen-digests "$input" regen-digests.wet --silent || return 1
    assert_pipeline_input_from_foreign_cwd regen-zip \
        "$WARC_CLI" regen-zip "$input" regen-zip.wet --silent || return 1
    assert_pipeline_input_from_foreign_cwd convert \
        "$WARC_CLI" convert "$input" convert.wet --silent || return 1

    assert_pipeline_input_from_foreign_cwd es-load \
        "$ES_CLI" load "$input" --silent || return 1
    assert_pipeline_input_from_foreign_cwd es-load-index \
        "$ES_CLI" load-index "$input" foreign-index --silent || return 1
    assert_pipeline_input_from_foreign_cwd es-load-stream \
        "$ES_CLI" load-stream "$input" foreign-stream --silent || return 1
    assert_pipeline_input_from_foreign_cwd es-unload \
        "$ES_CLI" unload "$input" --silent || return 1
    assert_pipeline_input_from_foreign_cwd es-merge \
        "$ES_CLI" merge "$input" --silent || return 1
}

run_test test_relative_dedupe_and_grep_paths
