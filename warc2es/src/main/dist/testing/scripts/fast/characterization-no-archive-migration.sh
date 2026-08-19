#!/bin/bash
# No legacy archive migration, alias, or dual-write path exists.
source "$(dirname "$0")/../../lib/test-lib.sh"

test_no_archive_migration_characterization() {
    local scripts=(
        "$PROJECT_ROOT/src/main/dist/es-upsert.sh"
        "$PROJECT_ROOT/src/main/dist/es-delete.sh"
        "$PROJECT_ROOT/src/main/dist/es-reinit.sh"
        "$PROJECT_ROOT/src/main/dist/lib/scripts/runtime-lib.sh"
    )
    if rg -i 'legacy.*(archive|wet)|archive.*(migration|legacy)|dual[-_ ]write|migrat.*archive' "${scripts[@]}"; then
        log_fail "Unexpected archive migration or compatibility path"
        return 1
    fi
}

run_test test_no_archive_migration_characterization
