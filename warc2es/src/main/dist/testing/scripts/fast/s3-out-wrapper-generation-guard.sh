#!/bin/bash
# Guard that top-level out scripts are the sole command implementations.
source "$(dirname "$0")/../../lib/test-lib.sh"

test_out_wrapper_generation_guard() {
    local pom="$PROJECT_ROOT/pom.xml"
    local out_root="$PROJECT_ROOT/out"
    local entry
    local prune_block required
    local rc=0
    assert_file_exists "$pom" || return 1

    grep -Fq 'file="${project.basedir}/src/main/dist/warc2wet.sh"' "$pom" || {
        log_fail "Missing out/warc2wet.sh copy source in pom.xml"
        return 1
    }
    grep -Fq 'tofile="${project.basedir}/out/warc2wet.sh"' "$pom" || {
        log_fail "Missing out/warc2wet.sh copy target in pom.xml"
        return 1
    }
    grep -Fq 'file="${project.basedir}/src/main/dist/es-upsert.sh"' "$pom" || {
        log_fail "Missing out/es-upsert.sh copy source in pom.xml"
        return 1
    }
    grep -Fq 'tofile="${project.basedir}/out/es-upsert.sh"' "$pom" || {
        log_fail "Missing out/es-upsert.sh copy target in pom.xml"
        return 1
    }
    grep -Fq 'file="${project.basedir}/src/main/dist/es-upsert-all.sh"' "$pom" || {
        log_fail "Missing out/es-upsert-all.sh copy source in pom.xml"
        return 1
    }
    grep -Fq 'tofile="${project.basedir}/out/es-upsert-all.sh"' "$pom" || {
        log_fail "Missing out/es-upsert-all.sh copy target in pom.xml"
        return 1
    }
    grep -Fq 'file="${project.basedir}/src/main/dist/wet-merge.sh"' "$pom" || {
        log_fail "Missing out/wet-merge.sh copy source in pom.xml"
        return 1
    }
    grep -Fq 'tofile="${project.basedir}/out/wet-merge.sh"' "$pom" || {
        log_fail "Missing out/wet-merge.sh copy target in pom.xml"
        return 1
    }
    grep -Fq 'file="${project.basedir}/src/main/dist/es-reinit.sh"' "$pom" || {
        log_fail "Missing out/es-reinit.sh wrapper copy source in pom.xml"
        return 1
    }
    grep -Fq 'tofile="${project.basedir}/out/es-reinit.sh"' "$pom" || {
        log_fail "Missing out/es-reinit.sh wrapper copy target in pom.xml"
        return 1
    }
    grep -Fq 'file="${project.basedir}/src/main/dist/es-delete.sh"' "$pom" || {
        log_fail "Missing out/es-delete.sh wrapper copy source in pom.xml"
        return 1
    }
    grep -Fq 'tofile="${project.basedir}/out/es-delete.sh"' "$pom" || {
        log_fail "Missing out/es-delete.sh wrapper copy target in pom.xml"
        return 1
    }
    grep -Fq '<exclude name="scripts/pipeline-direct" />' "$pom" || {
        log_fail "Raw pipeline entrypoint is not excluded from out/"
        return 1
    }

    if [[ -d "$PROJECT_ROOT/dist" ]]; then
        log_fail "Legacy root dist/ directory exists after package assembly"
        rc=1
    fi

    assert_file_exists "$out_root/warc2wet.sh" || rc=1
    assert_file_exists "$out_root/wet-merge.sh" || rc=1
    assert_file_exists "$out_root/es-upsert.sh" || rc=1
    assert_file_exists "$out_root/es-upsert-all.sh" || rc=1
    assert_file_exists "$out_root/es-reinit.sh" || rc=1
    assert_file_exists "$out_root/es-delete.sh" || rc=1

    for entry in warc2wet.sh wet-merge.sh es-upsert.sh es-upsert-all.sh es-reinit.sh es-delete.sh; do
        if [[ ! -x "$out_root/$entry" ]]; then
            log_fail "out/$entry is not executable"
            rc=1
        fi
    done

    if [[ -d "$out_root/testing" || -d "$out_root/docker" ]]; then
        log_fail "out/ contains forbidden testing/ or docker/ directory"
        rc=1
    fi

    prune_block=$(awk '
        /<delete includeemptydirs="true" quiet="true">/ { capture = 1 }
        capture { print }
        capture && /<\/delete>/ { exit }
    ' "$pom")
    if [[ -z "$prune_block" || "$prune_block" != *'</delete>'* ]]; then
        log_fail "Missing generated out/ prune block in package assembly"
        rc=1
    else
        for required in \
            '<fileset dir="${project.basedir}/out"' \
            'erroronmissingdir="false"' \
            'followsymlinks="false"' \
            '<exclude name=".profile" />' \
            '<exclude name="in" />' \
            '<exclude name="in/**" />' \
            '<exclude name="wet" />' \
            '<exclude name="wet/**" />' \
            '<exclude name="doet" />' \
            '<exclude name="doet/**" />' \
            '<exclude name="all" />' \
            '<exclude name="all/**" />' \
            '<exclude name="var" />' \
            '<exclude name="var/locks" />' \
            '<exclude name="var/locks/warc2es" />' \
            '<exclude name="var/locks/warc2es/**" />'; do
            if ! grep -Fq "$required" <<< "$prune_block"; then
                log_fail "Generated out/ prune block is missing: $required"
                rc=1
            fi
        done
    fi
    grep -Fq 'native.so.missing.required' "$pom" || {
        log_fail "Missing native-library fail-by-default gate in package assembly"
        rc=1
    }
    grep -Fq -- '-Dnative.optional=true' "$pom" || {
        log_fail "Missing documented native.optional Maven escape"
        rc=1
    }
    grep -Fq 'erroronmissingdir="false"' "$pom" || {
        log_fail "Native copy must tolerate missing source dir only after fail/escape gate"
        rc=1
    }
    if [[ ! -f "$out_root/app/native/libreadability_jni.so" ]]; then
        log_fail "out/app/native/libreadability_jni.so missing after normal package assembly"
        rc=1
    fi

    if find "$out_root" -type d \( -name testing -o -name docker \) -print -quit | grep -q .; then
        log_fail "out/ contains nested forbidden testing/ or docker/ directory"
        rc=1
    fi

    while IFS= read -r entry; do
        case "$entry" in
            .profile|.profile.example|README.md|app|in|out|all|wet|doet|var|warc2wet.sh|wet-merge.sh|es-upsert.sh|es-upsert-all.sh|es-reinit.sh|es-delete.sh)
                ;;
            *)
                log_fail "Unexpected top-level out/ entry: $entry"
                rc=1
                ;;
        esac
    done < <(find "$out_root" -mindepth 1 -maxdepth 1 -printf '%f\n')

    if [[ -e "$out_root/var" ]]; then
        if [[ -L "$out_root/var" || ! -d "$out_root/var" ]]; then
            log_fail "out/var must be a real runtime-state directory"
            rc=1
        elif find "$out_root/var" -mindepth 1 -maxdepth 1 \
             ! -name locks -print -quit | grep -q .; then
            log_fail "out/var contains state outside the approved locks subtree"
            rc=1
        elif [[ -e "$out_root/var/locks" &&
                ( -L "$out_root/var/locks" || ! -d "$out_root/var/locks" ) ]]; then
            log_fail "out/var/locks must be a real directory"
            rc=1
        elif [[ -d "$out_root/var/locks" ]] &&
             find "$out_root/var/locks" -mindepth 1 -maxdepth 1 \
               ! -name warc2es -print -quit | grep -q .; then
            log_fail "out/var/locks contains state outside warc2es"
            rc=1
        elif [[ -e "$out_root/var/locks/warc2es" &&
                ( -L "$out_root/var/locks/warc2es" ||
                  ! -d "$out_root/var/locks/warc2es" ) ]]; then
            log_fail "out/var/locks/warc2es must be a real directory"
            rc=1
        fi
    fi

    if [[ -d "$out_root/app/lib/scripts" ]]; then
        while IFS= read -r entry; do
            case "$entry" in
                pipeline-lib|runtime-lib.sh)
                    ;;
                *)
                    log_fail "Unexpected out/app/lib/scripts entry: $entry"
                    rc=1
                    ;;
            esac
        done < <(find "$out_root/app/lib/scripts" -maxdepth 1 -type f -printf '%f\n')
    fi

    if [[ ! -x "$PROJECT_ROOT/target/dist/lib/scripts/pipeline-direct" ]]; then
        log_fail "Raw pipeline entrypoint missing from target/dist test layout"
        rc=1
    fi
    if [[ -e "$out_root/app/lib/scripts/pipeline" || -e "$out_root/app/lib/scripts/pipeline-direct" ]]; then
        log_fail "Raw pipeline entrypoint leaked into out/"
        rc=1
    fi

    for entry in warc2wet.sh es-upsert.sh es-upsert-all.sh wet-merge.sh; do
        if [[ -e "$PROJECT_ROOT/src/main/dist/lib/scripts/$entry" ]]; then
            log_fail "Duplicate source implementation remains: lib/scripts/$entry"
            rc=1
        fi
        if [[ -e "$out_root/app/lib/scripts/$entry" ]]; then
            log_fail "Duplicate packaged implementation remains: app/lib/scripts/$entry"
            rc=1
        fi
        grep -Fq 'runtime_resolve_layout "$SCRIPT_DIR"' "$PROJECT_ROOT/src/main/dist/$entry" || {
            log_fail "$entry is not a complete root-level implementation"
            rc=1
        }
        if grep -Fq 'exec "$SCRIPT_DIR/app/lib/scripts/' "$PROJECT_ROOT/src/main/dist/$entry"; then
            log_fail "$entry is still a thin wrapper"
            rc=1
        fi
    done

    return "$rc"
}

run_test test_out_wrapper_generation_guard
