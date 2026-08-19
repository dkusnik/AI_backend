#!/usr/bin/env bash
# Packaging freshness gate backed by target/assembly-manifest.
#
# Root overrides for isolated verification:
#   WARC_GUARD_SRC_ROOT  repository/source root (default: nearest pom.xml)
#   WARC_GUARD_OUT_ROOT  assembled runtime root (default: <source>/out)
#   WARC_GUARD_MANIFEST  manifest path (default: <source>/target/assembly-manifest)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

find_source_root() {
    local candidate="$SCRIPT_DIR"
    while [[ "$candidate" != "/" && ! -f "$candidate/pom.xml" ]]; do
        candidate="$(dirname "$candidate")"
    done
    printf '%s\n' "$candidate"
}

SOURCE_ROOT="${WARC_GUARD_SRC_ROOT:-$(find_source_root)}"
OUT_ROOT="${WARC_GUARD_OUT_ROOT:-$SOURCE_ROOT/out}"
MANIFEST="${WARC_GUARD_MANIFEST:-$SOURCE_ROOT/target/assembly-manifest}"

export PROJECT_ROOT="$SOURCE_ROOT"
# Resolved from this script in both the source and copied dist trees.
# shellcheck disable=SC1091
source "$SCRIPT_DIR/../../lib/test-lib.sh"

sha256_file() {
    local checksum
    checksum="$(sha256sum -- "$1")" || return 1
    checksum="${checksum%% *}"
    [[ "$checksum" =~ ^[0-9a-f]{64}$ ]] || return 1
    printf '%s\n' "$checksum"
}

sha256_stream() {
    local checksum
    checksum="$(sha256sum)" || return 1
    checksum="${checksum%% *}"
    [[ "$checksum" =~ ^[0-9a-f]{64}$ ]] || return 1
    printf '%s\n' "$checksum"
}

# Hash an inventory as path/hash NUL pairs. Missing inputs are included too, so
# adding or removing a build input invalidates an existing manifest.
sha256_inputs() (
    set -o pipefail
    local input absolute relative file_sum
    {
        for input in "$@"; do
            absolute="$SOURCE_ROOT/$input"
            if [[ -f "$absolute" ]]; then
                file_sum="$(sha256_file "$absolute")" || return 1
                printf 'file\0%s\0%s\0' "$input" "$file_sum"
            elif [[ -d "$absolute" ]]; then
                while IFS= read -r -d '' absolute; do
                    relative="${absolute#"$SOURCE_ROOT"/}"
                    file_sum="$(sha256_file "$absolute")" || return 1
                    printf 'file\0%s\0%s\0' "$relative" "$file_sum"
                done < <(find "$absolute" -type f -print0 | LC_ALL=C sort -z)
            else
                printf 'missing\0%s\0' "$input"
            fi
        done
    } | sha256_stream
)

source_hash() {
    local source_spec="$1"
    case "$source_spec" in
        @java-build)
            sha256_inputs pom.xml src/main/java src/main/resources
            ;;
        @maven-runtime)
            sha256_inputs pom.xml
            ;;
        @native-build)
            sha256_inputs src/native/Cargo.toml src/native/Cargo.lock src/native/Makefile src/native/build.rs src/native/src
            ;;
        @*)
            log_fail "Unknown build-input descriptor in assembly manifest: $source_spec"
            return 1
            ;;
        /*|../*|*/../*|*/..)
            log_fail "Unsafe source path in assembly manifest: $source_spec"
            return 1
            ;;
        *)
            if [[ ! -f "$SOURCE_ROOT/$source_spec" ]]; then
                log_fail "Assembly source is missing: $source_spec"
                return 1
            fi
            sha256_file "$SOURCE_ROOT/$source_spec"
            ;;
    esac
}

source_for_output() {
    local output_path="$1"
    local staged_path

    case "$output_path" in
        .profile.example)
            SOURCE_SPEC='src/main/dist/.profile'
            return
            ;;
        app/native/libreadability_jni.so)
            SOURCE_SPEC='@native-build'
            return
            ;;
        app/lib/pipeline.jar)
            SOURCE_SPEC='@java-build'
            return
            ;;
        app/lib/*.jar)
            SOURCE_SPEC='@maven-runtime'
            return
            ;;
        app/conf/config.yaml)
            SOURCE_SPEC='src/main/resources/config-out.yaml'
            return
            ;;
        app/*)
            staged_path="src/main/dist/${output_path#app/}"
            ;;
        *)
            staged_path="src/main/dist/$output_path"
            ;;
    esac

    if [[ -f "$SOURCE_ROOT/$staged_path" ]]; then
        SOURCE_SPEC="$staged_path"
        return
    fi

    log_fail "No assembly source mapping for out/$output_path"
    return 1
}

target_for_output() {
    case "$1" in
        app/conf/config.yaml)
            # out uses config-out.yaml; target/dist/conf/config.yaml is the
            # non-runtime source configuration and is intentionally different.
            TARGET_PATH=
            ;;
        app/*)
            TARGET_PATH="$SOURCE_ROOT/target/dist/${1#app/}"
            ;;
        *)
            TARGET_PATH="$SOURCE_ROOT/target/dist/$1"
            ;;
    esac
}

check_staging_parity() {
    local output_path="$1"
    local expected_sum="$2"
    local actual_sum

    target_for_output "$output_path"
    if [[ -n "$TARGET_PATH" ]]; then
        if [[ ! -f "$TARGET_PATH" ]]; then
            log_fail "Staged target output is missing: ${TARGET_PATH#"$SOURCE_ROOT"/}"
            return 1
        fi
        actual_sum="$(sha256_file "$TARGET_PATH")" || return 1
        if [[ "$actual_sum" != "$expected_sum" ]]; then
            log_fail "${TARGET_PATH#"$SOURCE_ROOT"/} differs from out/$output_path"
            return 1
        fi
    fi

    if [[ "$output_path" == app/native/libreadability_jni.so ]]; then
        actual_sum="$(sha256_file "$SOURCE_ROOT/src/native/target/release/libreadability_jni.so")" ||
            return 1
        if [[ "$actual_sum" != "$expected_sum" ]]; then
            log_fail "Native release output differs from out/$output_path"
            return 1
        fi
    fi
}

generated_output_files() {
    # .profile is preserved across assembly; app/log, app/tmp, app/var and the
    # root operator lock subtree hold runtime state. The remaining top-level
    # directories hold workflow input/output. Only app/var/rocksdb/.gitkeep is
    # distribution-owned runtime scaffolding.
    {
        find "$OUT_ROOT" -type f \
            ! -path "$OUT_ROOT/.profile" \
            ! -path "$OUT_ROOT/app/log/*" \
            ! -path "$OUT_ROOT/app/tmp/*" \
            ! -path "$OUT_ROOT/app/var/*" \
            ! -path "$OUT_ROOT/var/locks/warc2es/*" \
            ! -path "$OUT_ROOT/in/*" \
            ! -path "$OUT_ROOT/out/*" \
            ! -path "$OUT_ROOT/wet/*" \
            ! -path "$OUT_ROOT/doet/*" \
            ! -path "$OUT_ROOT/all/*" \
            -print0
        if [[ -f "$OUT_ROOT/app/var/rocksdb/.gitkeep" ]]; then
            printf '%s\0' "$OUT_ROOT/app/var/rocksdb/.gitkeep"
        fi
    } | LC_ALL=C sort -z
}

write_manifest() {
    local manifest_dir manifest_tmp output_file output_path
    local source_spec source_sum output_sum count=0

    if [[ ! -d "$OUT_ROOT" ]]; then
        log_fail "Assembled runtime not found: $OUT_ROOT"
        return 1
    fi

    manifest_dir="$(dirname "$MANIFEST")"
    mkdir -p "$manifest_dir"
    manifest_tmp="$(mktemp "$manifest_dir/.assembly-manifest.XXXXXX")"
    trap 'rm -f -- "$manifest_tmp"' RETURN

    while IFS= read -r -d '' output_file; do
        output_path="${output_file#"$OUT_ROOT"/}"
        source_for_output "$output_path" || return 1
        source_spec="$SOURCE_SPEC"
        source_sum="$(source_hash "$source_spec")" || return 1
        output_sum="$(sha256_file "$output_file")" || return 1

        if [[ "$source_spec" != @* && "$source_sum" != "$output_sum" ]]; then
            log_fail "Copied assembly output differs from its source: out/$output_path"
            return 1
        fi
        check_staging_parity "$output_path" "$output_sum" || return 1

        printf '%s\0%s\0%s\0%s\0\0' \
            "$source_spec" "$source_sum" "$output_path" "$output_sum" >> "$manifest_tmp"
        count=$((count + 1))
    done < <(generated_output_files)

    if (( count == 0 )); then
        log_fail "Refusing to write an empty assembly manifest"
        return 1
    fi

    mv -- "$manifest_tmp" "$MANIFEST"
    trap - RETURN
    log_info "Wrote $MANIFEST ($count generated files)"
}

test_packaging_freshness() {
    local source_spec expected_source_sum output_path expected_output_sum separator
    local actual_source_sum actual_output_sum output_file count=0
    declare -A listed_outputs=()

    local removed_implementation
    for removed_implementation in \
        app/lib/scripts/es-upsert.sh \
        app/lib/scripts/warc2wet.sh \
        app/lib/scripts/wet-merge.sh; do
        if [[ -e "$OUT_ROOT/$removed_implementation" ]]; then
            log_fail "Removed duplicate implementation survived assembly: out/$removed_implementation"
            return 1
        fi
    done

    if [[ ! -f "$MANIFEST" ]]; then
        log_fail "Assembly manifest not found: $MANIFEST — rebuild package artifacts"
        return 1
    fi
    if [[ ! -d "$OUT_ROOT" ]]; then
        log_fail "Assembled runtime not found: $OUT_ROOT — rebuild package artifacts"
        return 1
    fi

    local runtime_path executable
    for runtime_path in \
        .profile .profile.example in wet doet all var/locks/warc2es; do
        if [[ ! -e "$OUT_ROOT/$runtime_path" ]]; then
            log_fail "Required preserved/runtime path is missing: out/$runtime_path"
            return 1
        fi
    done
    for runtime_path in app/tmp out; do
        if [[ -e "$OUT_ROOT/$runtime_path" ]]; then
            log_fail "Unused runtime directory survived assembly: out/$runtime_path"
            return 1
        fi
    done
    for executable in \
        warc2wet.sh wet-merge.sh es-upsert.sh es-upsert-all.sh \
        es-delete.sh es-reinit.sh; do
        if [[ ! -x "$OUT_ROOT/$executable" ]]; then
            log_fail "Operator command is not executable: out/$executable"
            return 1
        fi
        if [[ ! -x "$SOURCE_ROOT/target/dist/$executable" ]]; then
            log_fail "Staged operator command is not executable: target/dist/$executable"
            return 1
        fi
    done

    exec 3< "$MANIFEST"
    while :; do
        source_spec=
        if ! IFS= read -r -d '' source_spec <&3; then
            if [[ -n "$source_spec" ]]; then
                log_fail "Trailing unterminated bytes in assembly manifest"
                exec 3<&-
                return 1
            fi
            break
        fi
        if ! IFS= read -r -d '' expected_source_sum <&3 ||
           ! IFS= read -r -d '' output_path <&3 ||
           ! IFS= read -r -d '' expected_output_sum <&3 ||
           ! IFS= read -r -d '' separator <&3; then
            log_fail "Truncated assembly manifest record"
            exec 3<&-
            return 1
        fi
        if [[ -n "$separator" || -z "$source_spec" || -z "$output_path" ]]; then
            log_fail "Invalid assembly manifest record"
            exec 3<&-
            return 1
        fi
        if [[ "$output_path" == /* || "$output_path" == ../* ||
              "$output_path" == */../* || "$output_path" == */.. ]]; then
            log_fail "Unsafe output path in assembly manifest: $output_path"
            exec 3<&-
            return 1
        fi
        if [[ -n "${listed_outputs["$output_path"]+present}" ]]; then
            log_fail "Duplicate output in assembly manifest: $output_path"
            exec 3<&-
            return 1
        fi
        listed_outputs["$output_path"]=1
        count=$((count + 1))

        actual_source_sum="$(source_hash "$source_spec")" || return 1
        if [[ "$actual_source_sum" != "$expected_source_sum" ]]; then
            log_fail "Assembly source is stale: $source_spec"
            return 1
        fi

        output_file="$OUT_ROOT/$output_path"
        if [[ ! -f "$output_file" ]]; then
            log_fail "Assembly output is missing: out/$output_path"
            return 1
        fi
        actual_output_sum="$(sha256_file "$output_file")" || {
            log_fail "Cannot hash assembly output: out/$output_path"
            return 1
        }
        if [[ "$actual_output_sum" != "$expected_output_sum" ]]; then
            log_fail "Assembly output was modified: out/$output_path"
            return 1
        fi
        if [[ "$source_spec" != @* && "$actual_source_sum" != "$actual_output_sum" ]]; then
            log_fail "Copied assembly output is stale: out/$output_path"
            return 1
        fi
        check_staging_parity "$output_path" "$expected_output_sum" || return 1
    done
    exec 3<&-

    if (( count == 0 )); then
        log_fail "Assembly manifest contains no records"
        return 1
    fi

    while IFS= read -r -d '' output_file; do
        output_path="${output_file#"$OUT_ROOT"/}"
        if [[ -z "${listed_outputs["$output_path"]+present}" ]]; then
            log_fail "Unlisted generated output: out/$output_path"
            return 1
        fi
    done < <(generated_output_files)

    log_success "Assembly manifest matches current sources and generated outputs"
}

PRESERVATION_SENTINELS=()

cleanup_preservation_sentinels() {
    local sentinel
    for sentinel in "${PRESERVATION_SENTINELS[@]}"; do
        [[ -z "$sentinel" ]] || rm -f -- "$sentinel"
    done
}

test_runtime_preservation_rebuild() {
    local directory sentinel stale_path profile_before file file_hash index identity
    local -a preserved_files=()
    local -a preserved_hashes=()
    local -a lock_files=()
    local -a lock_identities=()
    local -a stale_files=()

    PRESERVATION_SENTINELS=()
    trap cleanup_preservation_sentinels EXIT HUP INT TERM

    if [[ ! -f "$OUT_ROOT/.profile" ]]; then
        log_fail "Live runtime profile is missing: $OUT_ROOT/.profile"
        return 1
    fi
    profile_before="$(sha256_file "$OUT_ROOT/.profile")" || return 1

    for directory in in wet doet all var/locks/warc2es; do
        if [[ ! -d "$OUT_ROOT/$directory" ]]; then
            log_fail "Preserved runtime directory is missing: $OUT_ROOT/$directory"
            return 1
        fi
        sentinel="$(mktemp "$OUT_ROOT/$directory/.d1-preserve.XXXXXX")" ||
            return 1
        PRESERVATION_SENTINELS+=("$sentinel")
        printf 'D1-001 preserve %s\n' "$directory" > "$sentinel"
    done

    preserved_files+=("$OUT_ROOT/.profile")
    while IFS= read -r -d '' file; do
        preserved_files+=("$file")
    done < <(
        find "$OUT_ROOT/in" "$OUT_ROOT/wet" "$OUT_ROOT/doet" "$OUT_ROOT/all" \
            "$OUT_ROOT/var/locks/warc2es" \
            -type f -print0 | LC_ALL=C sort -z
    )
    for file in "${preserved_files[@]}"; do
        file_hash="$(sha256_file "$file")" || return 1
        preserved_hashes+=("$file_hash")
    done

    while IFS= read -r -d '' file; do
        lock_files+=("$file")
        identity="$(stat -c '%d:%i' -- "$file")" || return 1
        lock_identities+=("$identity")
    done < <(
        find "$OUT_ROOT/var/locks/warc2es" -type f -print0 | LC_ALL=C sort -z
    )

    mkdir -p "$OUT_ROOT/app/tmp" "$OUT_ROOT/out"
    for directory in "$OUT_ROOT" "$OUT_ROOT/app/tmp" "$OUT_ROOT/out"; do
        stale_path="$(mktemp "$directory/.d1-stale.XXXXXX")" || return 1
        PRESERVATION_SENTINELS+=("$stale_path")
        stale_files+=("$stale_path")
        printf 'D1-001 stale generated output\n' > "$stale_path"
    done

    if ! make -C "$SOURCE_ROOT" clean || ! make -C "$SOURCE_ROOT"; then
        log_fail "Clean rebuild failed during the runtime-preservation probe"
        return 1
    fi

    if [[ "$(sha256_file "$OUT_ROOT/.profile")" != "$profile_before" ]]; then
        log_fail "Clean rebuild changed the live runtime profile"
        return 1
    fi
    for index in "${!preserved_files[@]}"; do
        file="${preserved_files[$index]}"
        if [[ ! -f "$file" ]]; then
            log_fail "Clean rebuild removed preserved runtime data: ${file#"$OUT_ROOT"/}"
            return 1
        fi
        if [[ "$(sha256_file "$file")" != "${preserved_hashes[$index]}" ]]; then
            log_fail "Clean rebuild changed preserved runtime data: ${file#"$OUT_ROOT"/}"
            return 1
        fi
    done
    for index in "${!lock_files[@]}"; do
        file="${lock_files[$index]}"
        if [[ ! -f "$file" ]]; then
            log_fail "Clean rebuild unlinked a stable lock file: ${file#"$OUT_ROOT"/}"
            return 1
        fi
        identity="$(stat -c '%d:%i' -- "$file")" || return 1
        if [[ "$identity" != "${lock_identities[$index]}" ]]; then
            log_fail "Clean rebuild replaced a stable lock inode: ${file#"$OUT_ROOT"/}"
            return 1
        fi
    done
    for stale_path in "${stale_files[@]}"; do
        if [[ -e "$stale_path" ]]; then
            log_fail "Clean rebuild retained stale generated output: ${stale_path#"$OUT_ROOT"/}"
            return 1
        fi
    done
    if [[ -e "$OUT_ROOT/app/tmp" || -e "$OUT_ROOT/out" ]]; then
        log_fail "Clean rebuild retained an unused runtime directory"
        return 1
    fi

    cleanup_preservation_sentinels
    PRESERVATION_SENTINELS=()
    trap - EXIT HUP INT TERM
    log_success "Clean rebuild preserved profile/data/lock hashes and lock inodes, and purged stale generated output"
}

case "${1:-}" in
    --write-manifest)
        write_manifest
        ;;
    --preservation-rebuild)
        test_runtime_preservation_rebuild
        ;;
    --help|-h)
        cat <<'EOF'
Usage: s0-packaging-freshness.sh [--write-manifest|--preservation-rebuild]

Without arguments, verify target/assembly-manifest against current sources and
out/. Use WARC_GUARD_SRC_ROOT, WARC_GUARD_OUT_ROOT, and WARC_GUARD_MANIFEST to
check an isolated candidate tree.

--preservation-rebuild injects uniquely named sentinels, runs make clean and
make, verifies every pre-existing/injected profile, runtime-data and lock hash
plus stable lock inodes, then removes only the injected test sentinels.
EOF
        ;;
    *)
        # test-cli supplies runner flags such as --debug to every script.
        # Preserve the ordinary run_test contract for those arguments.
        run_test test_packaging_freshness
        ;;
esac
