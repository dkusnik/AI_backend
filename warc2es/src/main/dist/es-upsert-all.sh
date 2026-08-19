#!/usr/bin/env bash
# es-upsert-all.sh — replay every published provenance directory sequentially.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "$SCRIPT_DIR/app/lib/scripts/runtime-lib.sh" ]]; then
  RUNTIME_LIB="$SCRIPT_DIR/app/lib/scripts/runtime-lib.sh"
elif [[ -f "$SCRIPT_DIR/lib/scripts/runtime-lib.sh" ]]; then
  RUNTIME_LIB="$SCRIPT_DIR/lib/scripts/runtime-lib.sh"
else
  echo "Error: runtime-lib.sh not found from $SCRIPT_DIR" >&2
  exit 1
fi
# shellcheck source=/dev/null
source "$RUNTIME_LIB"

runtime_resolve_layout "$SCRIPT_DIR"
UPSERT="$SCRIPT_DIR/es-upsert.sh"
ARCHIVE_ROOT="$RUNTIME_DIR/all/wet"
DIR_FILE=""
ROOT_FILE=""
DELEGATE_FILE=""
cleanup() {
  [[ -z "$DIR_FILE" ]] || rm -f -- "$DIR_FILE"
  [[ -z "$ROOT_FILE" ]] || rm -f -- "$ROOT_FILE"
  [[ -z "$DELEGATE_FILE" ]] || rm -f -- "$DELEGATE_FILE"
}
trap cleanup EXIT

usage() {
  cat <<'EOF'
Usage: es-upsert-all.sh [options]

Replay every published WET provenance directory sequentially. Output is NDJSON:
one delegated es-upsert invocation per directory, followed by one summary.

Options:
  --stream=<id|exact-name>  shorthand or exact nac-data stream (default: nac-data-default)
  --es-url=<url>            override ES_URL
  --dry-run                 resolve and report without loading
  --result-format=json      explicit NDJSON result mode (the default)
  -h, --help                show this help
EOF
}

forward_args=()
STREAM_ID=""
STREAM_SET=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --stream=*)
      [[ "$STREAM_SET" == false ]] || { echo "Error: --stream may be specified only once" >&2; exit 1; }
      STREAM_ID="${1#*=}"
      STREAM_SET=true
      shift
      ;;
    --stream)
      [[ $# -ge 2 ]] || { echo "Error: $1 requires a value" >&2; exit 1; }
      [[ "$STREAM_SET" == false ]] || { echo "Error: --stream may be specified only once" >&2; exit 1; }
      STREAM_ID="${2:-}"
      STREAM_SET=true
      shift 2
      ;;
    --es-url=*)
      forward_args+=("$1")
      shift
      ;;
    --es-url)
      [[ $# -ge 2 ]] || { echo "Error: $1 requires a value" >&2; exit 1; }
      forward_args+=("$1" "${2:-}")
      shift 2
      ;;
    --dry-run)
      forward_args+=("$1")
      shift
      ;;
    --result-format=json)
      shift
      ;;
    --result-format=human|--result-format)
      echo "Error: es-upsert-all.sh emits NDJSON; use --result-format=json" >&2
      exit 1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --*)
      echo "Error: unknown option: $1" >&2
      exit 1
      ;;
    *)
      echo "Error: unexpected positional argument: $1" >&2
      exit 1
      ;;
  esac
done

[[ -x "$UPSERT" ]] || { echo "Error: $UPSERT missing or not executable" >&2; exit 1; }
[[ "$STREAM_SET" == false || -n "$STREAM_ID" ]] || {
  echo "Error: --stream requires a non-empty value" >&2
  exit 1
}
resolved_stream="$(stream_name "$STREAM_ID")"
runtime_stream_name_is_safe "$resolved_stream" || {
  echo "Error: resolved stream is not one exact safe Elasticsearch name" >&2
  exit 1
}
forward_args+=("--stream=$resolved_stream")

relative_dirs=()
if [[ -L "$RUNTIME_DIR/all" || ( -e "$RUNTIME_DIR/all" && ! -d "$RUNTIME_DIR/all" ) ]]; then
  echo "Error: published root is unsafe: $RUNTIME_DIR/all" >&2
  exit 1
fi
if [[ -e "$ARCHIVE_ROOT" || -L "$ARCHIVE_ROOT" ]]; then
  if [[ -L "$RUNTIME_DIR/all" || -L "$ARCHIVE_ROOT" || ! -d "$ARCHIVE_ROOT" ||
        "$(realpath -e -- "$ARCHIVE_ROOT")" != "$ARCHIVE_ROOT" ]]; then
    echo "Error: published WET root is missing or unsafe: $ARCHIVE_ROOT" >&2
    exit 1
  fi

  ROOT_FILE="$(mktemp "${TMPDIR:-/tmp}/warc2es-replay-root.XXXXXX")"
  DIR_FILE="$(mktemp "${TMPDIR:-/tmp}/warc2es-replay-dirs.XXXXXX")"
  if ! find "$ARCHIVE_ROOT" -mindepth 1 -maxdepth 1 -print0 >"$ROOT_FILE"; then
    echo "Error: failed to discover published URL directories" >&2
    exit 1
  fi
  mapfile -d '' -t url_directories <"$ROOT_FILE"
  : >"$DIR_FILE"
  for url_directory in "${url_directories[@]}"; do
    url_id="$(basename "$url_directory")"
    if [[ -L "$url_directory" || ! -d "$url_directory" ]] ||
       ! runtime_identifier_is_valid "$url_id"; then
      echo "Error: invalid published URL directory: $url_directory" >&2
      exit 1
    fi
    if ! find "$url_directory" -mindepth 1 -maxdepth 1 -print0 >"$ROOT_FILE"; then
      echo "Error: failed to discover published crawl directories: $url_directory" >&2
      exit 1
    fi
    mapfile -d '' -t crawl_directories <"$ROOT_FILE"
    for crawl_directory in "${crawl_directories[@]}"; do
      crawl_id="$(basename "$crawl_directory")"
      if [[ -L "$crawl_directory" || ! -d "$crawl_directory" ]] ||
         ! runtime_identifier_is_valid "$crawl_id"; then
        echo "Error: invalid published crawl directory: $crawl_directory" >&2
        exit 1
      fi
      shopt -s nullglob dotglob
      crawl_entries=("$crawl_directory"/*)
      shopt -u nullglob dotglob
      has_published_wet=false
      for entry in "${crawl_entries[@]}"; do
        base="$(basename "$entry")"
        if [[ "$base" =~ ^[0-9a-f]{64}\.wet\.gz$ && -f "$entry" && ! -L "$entry" ]]; then
          has_published_wet=true
        elif [[ "$base" =~ ^\.[0-9a-f]{64}\.tmp\.[A-Za-z0-9]+$ &&
                -f "$entry" && ! -L "$entry" ]]; then
          continue
        else
          echo "Error: invalid member in published WET pair directory: $entry" >&2
          exit 1
        fi
      done
      [[ "$has_published_wet" == true ]] || continue
      printf '%s\0' "$url_id/$crawl_id" >>"$DIR_FILE"
    done
  done
  if ! LC_ALL=C sort -zu -o "$DIR_FILE" "$DIR_FILE"; then
    echo "Error: failed to order published provenance directories" >&2
    exit 1
  fi
  mapfile -d '' -t relative_dirs <"$DIR_FILE"
  rm -f -- "$ROOT_FILE" "$DIR_FILE"
  ROOT_FILE=""
  DIR_FILE=""
fi

total=0
succeeded=0
failed=0
for relative in "${relative_dirs[@]}"; do
  directory="$ARCHIVE_ROOT/$relative"
  DELEGATE_FILE="$(mktemp "${TMPDIR:-/tmp}/warc2es-replay-result.XXXXXX")"
  set +e
  "$UPSERT" --from-archive "$directory" --result-format=json "${forward_args[@]}" >"$DELEGATE_FILE"
  delegate_exit=$?
  set -e

  total=$((total + 1))
  first_byte="$(head -c 1 "$DELEGATE_FILE")"
  final_bytes="$(tail -c 2 "$DELEGATE_FILE" | od -An -tx1 -v | tr -d ' \n')"
  if [[ "$first_byte" == "{" && "$final_bytes" == "7d0a" &&
        "$(wc -l < "$DELEGATE_FILE")" -eq 1 ]] &&
     jq -e -s --argjson delegate_exit "$delegate_exit" \
       --arg empty_sha 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' '
       length == 1 and
       (.[0] | type == "object" and
        .schema == "warc2es.operator/v1" and .kind == "invocation" and
        .command == "es-upsert" and
        .mode == "archive-replay" and
        (.status == "ok" or .status == "dry_run" or .status == "error" or .status == "partial") and
        (.exit_code | type == "number") and .exit_code >= 0 and
        ((.exit_code | floor) == .exit_code) and
        .exit_code == $delegate_exit and
        (.inputs | type == "array") and .outputs == [] and
        (.publication | type == "object") and
        (.publication.paths | type == "array") and
        (.processing == null or
         ((.processing | type == "object") and
          .processing.schema == "warc2es.processing/v1")) and
        (.error == null or (.error | type == "object")) and
        (if .status == "ok"
         then .exit_code == 0 and .error == null and
              ((.processing | type == "object") or
               (.processing == null and (.publication.paths | length) == 1 and
                (.publication.paths[0] | endswith("/" + $empty_sha + ".wet.gz")))) and
              .publication.status == "unchanged"
         elif .status == "dry_run"
         then .exit_code == 0 and .error == null and .processing == null and
              .publication.status == "skipped"
         elif .status == "error"
         then .exit_code > 0 and (.error | type == "object") and
              .publication.status == "skipped"
         else .exit_code > 0 and (.error | type == "object") and
              (.publication.status == "skipped" or .publication.status == "unchanged")
         end))
     ' "$DELEGATE_FILE" >/dev/null 2>&1; then
    cat "$DELEGATE_FILE"
    if [[ "$delegate_exit" -eq 0 ]] &&
       jq -e '(.status == "ok" or .status == "dry_run") and .exit_code == 0' \
          "$DELEGATE_FILE" >/dev/null; then
      succeeded=$((succeeded + 1))
    else
      failed=$((failed + 1))
    fi
  else
    failed=$((failed + 1))
    fallback_exit="$delegate_exit"
    [[ "$fallback_exit" -ne 0 ]] || fallback_exit=1
    fallback_inputs="$(runtime_operator_paths_json)"
    fallback_publication="$(runtime_operator_publication_json skipped '[]')"
    runtime_operator_emit_invocation es-upsert error "$fallback_exit" archive-replay \
      "$fallback_inputs" '[]' "$fallback_publication" null \
      processing_result_invalid "Delegated es-upsert result is invalid"
  fi
  rm -f -- "$DELEGATE_FILE"
  DELEGATE_FILE=""
done

runtime_operator_emit_summary es-upsert-all "$total" "$succeeded" "$failed"
exit "$RUNTIME_OPERATOR_SUMMARY_EXIT_CODE"
