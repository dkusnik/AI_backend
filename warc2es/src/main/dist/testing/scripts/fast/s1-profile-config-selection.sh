#!/bin/bash
# s1-profile-config-selection.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

test_profile_config_is_selected() {
    ensure_test_data "tiny.warc.gz" || return 1
    local input="$TEST_DATA_DIR/tiny.warc.gz"
    local profile="selection-$$"
    local config="$DIST_ROOT/conf/config-${profile}.yaml"
    local out rc

    cp "$DIST_ROOT/conf/config.yaml" "$config" || return 1
    out=$("$WARC_CLI" --profile="$profile" info "$input" --dry-run --verbose 2>&1)
    rc=$?
    rm -f -- "$config"

    assert_command_success "$rc" "profile-specific config failed" || return 1
    echo "$out" | grep -Fq "Config: $config" || {
        log_fail "Profile-specific config was not selected"
        return 1
    }
}

run_test test_profile_config_is_selected
