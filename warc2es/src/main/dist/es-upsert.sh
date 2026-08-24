#!/usr/bin/env bash
# es-upsert.sh — replace one provenance pair in Elasticsearch and publish its WET set.
set -euo pipefail

ORIG_PWD="$(pwd)"
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
runtime_enter_script_dir "$APP_DIR/lib/scripts"
RESULT_FILE=""
DELETE_FILE=""
declare -a PUBLICATION_TEMPS=()
declare -a SNAPSHOT_FILES=()
cleanup() {
  local temporary
  [[ -z "$RESULT_FILE" ]] || rm -f -- "$RESULT_FILE"
  [[ -z "$DELETE_FILE" ]] || rm -f -- "$DELETE_FILE"
  for temporary in "${PUBLICATION_TEMPS[@]}"; do
    [[ -z "$temporary" ]] || rm -f -- "$temporary"
  done
  runtime_unlock_pair
  runtime_leave_script_dir
}
trap cleanup EXIT

runtime_source_profile

ES_CLI="$APP_DIR/bin/es-cli"
INPUT_PATH=""
FROM_ARCHIVE=false
STREAM_ID="nac-data-default"
URL_ID=""
CRAWL_ID=""
URL_ID_SET=false
CRAWL_ID_SET=false
START_DATE=""
ES_URL="${ES_URL:-http://localhost:9200}"
DRY_RUN=false
RESULT_FORMAT="json"
mode="explicit"
IMPLICIT_STAGING=false
inputs_json='[]'
DATA_DIR=$RUNTIME_DIR


usage() {
  cat <<'EOF'
Usage: es-upsert.sh --url-id=<id> --crawl-id=<id> [options]
       es-upsert.sh <file|dir> --url-id=<id> --crawl-id=<id> [options]
       es-upsert.sh --from-archive <pair-dir> [options]

Without a positional path, extend one provenance pair from every staged WET in
wet/<url-id>/<crawl-id> plus its published set. An explicit file or directory
is the exact replacement set. Archive replay derives the identifiers from
all/wet/<url-id>/<crawl-id>/ and is read-only.

Options:
  --from-archive=<dir>      replay one published provenance directory read-only
  --stream=<id|exact-name>  shorthand or exact nac-data stream (default: nac-data-default)
  --url-id=<id>             expected provenance URL identifier (normal mode)
  --crawl-id=<id>           expected provenance crawl identifier (normal mode)
  --start-date=<YYYY-MM-DD> override the pipeline start date
  --es-url=<url>            override ES_URL (default: http://localhost:9200)
  --dry-run                 validate and report without locks or mutation
  --result-format=<format>  json (default) or human
  -h, --help                show this help

Use es-upsert-all.sh to reload all.
EOF
}

operator_log() {
  if [[ "$RESULT_FORMAT" == "json" ]]; then
    echo "$*" >&2
  else
    echo "$*"
  fi
}

operator_failure() {
  local code="$1"
  local message="$2"
  local exit_code="${3:-1}"

  if [[ "$RESULT_FORMAT" == "json" ]]; then
    local publication_json
    publication_json="$(runtime_operator_publication_json skipped '[]')"
    runtime_operator_emit_invocation es-upsert error "$exit_code" "$mode" \
      "$inputs_json" '[]' "$publication_json" null "$code" "$message"
  else
    echo "Error: $message" >&2
  fi
  exit "$exit_code"
}

start_date_is_valid() {
  local value="$1"
  local normalized

  [[ "$value" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || return 1
  normalized="$(date -u -d "$value" +%F 2>/dev/null)" || return 1
  [[ "$normalized" == "$value" ]]
}

arrays_equal() {
  local left_name="$1"
  local right_name="$2"
  local -n left_ref="$left_name"
  local -n right_ref="$right_name"
  local index

  [[ ${#left_ref[@]} -eq ${#right_ref[@]} ]] || return 1
  for ((index = 0; index < ${#left_ref[@]}; index++)); do
    [[ "${left_ref[index]}" == "${right_ref[index]}" ]] || return 1
  done
}

validate_selected_wets() {
  local files_name="$1"
  local digests_name="$2"
  local -n files_ref="$files_name"
  local -n digests_ref="$digests_name"
  local file digest

  digests_ref=()
  for file in "${files_ref[@]}"; do
    runtime_validate_wet_provenance "$file" "$URL_ID" "$CRAWL_ID" || return 1
    digest="$(runtime_sha256_file "$file")" || return 1
    digests_ref+=("$digest")
  done
}

validate_published_ancestors() {
  local create="$1"
  local directory canonical parent
  local -a directories=(
    "$DATA_DIR/all"
    "$DATA_DIR/all/wet"
    "$DATA_DIR/all/wet/$URL_ID"
    "$DATA_DIR/all/wet/$URL_ID/$CRAWL_ID"
  )

  for directory in "${directories[@]}"; do
    if [[ -L "$directory" ]]; then
      echo "Error: published WET path contains a symlink: $directory" >&2
      return 1
    fi
    if [[ ! -e "$directory" ]]; then
      if [[ "$create" == true ]]; then
        if mkdir -- "$directory" 2>/dev/null; then
          parent="$(dirname "$directory")"
          sync -f "$parent" || return 1
        elif [[ ! -d "$directory" || -L "$directory" ]]; then
          echo "Error: cannot create safe published WET directory: $directory" >&2
          return 1
        fi
      else
        return 0
      fi
    fi
    if [[ ! -d "$directory" || -L "$directory" ]]; then
      echo "Error: published WET path is not a directory: $directory" >&2
      return 1
    fi
    canonical="$(realpath -e -- "$directory")" || return 1
    if [[ "$canonical" != "$directory" ]]; then
      echo "Error: published WET path is not canonical: $directory" >&2
      return 1
    fi
  done
}

collect_published_wets() {
  local files_name="$1"
  local digests_name="$2"
  local -n files_ref="$files_name"
  local -n digests_ref="$digests_name"
  local pair_dir="$DATA_DIR/all/wet/$URL_ID/$CRAWL_ID"
  local file base digest expected_digest entry_file
  local -a entries=()

  files_ref=()
  digests_ref=()
  PUBLISHED_TEMP_FOUND=false
  validate_published_ancestors false || return 1
  [[ -e "$pair_dir" ]] || return 0

  entry_file="$(mktemp "${TMPDIR:-/tmp}/warc2es-published.XXXXXX")" || return 1
  if ! find "$pair_dir" -mindepth 1 -maxdepth 1 -print0 >"$entry_file"; then
    rm -f -- "$entry_file"
    return 1
  fi
  mapfile -d '' -t entries <"$entry_file"
  rm -f -- "$entry_file"
  for file in "${entries[@]}"; do
    base="$(basename "$file")"
    if [[ "$base" =~ ^\.[0-9a-f]{64}\.tmp\.[A-Za-z0-9]+$ ]]; then
      if [[ -L "$file" || ! -f "$file" ]]; then
        echo "Error: unsafe publication temporary: $file" >&2
        return 1
      fi
      PUBLISHED_TEMP_FOUND=true
      continue
    fi
    if [[ -L "$file" || ! -f "$file" || ! "$base" =~ ^[0-9a-f]{64}\.wet\.gz$ ]]; then
      echo "Error: invalid member in published WET pair directory: $file" >&2
      return 1
    fi
  done

  runtime_find_data_files wet "$pair_dir" "$files_name" || return 1
  for file in "${files_ref[@]}"; do
    if [[ "$(dirname "$file")" != "$pair_dir" ]]; then
      echo "Error: published WET member is outside its pair directory: $file" >&2
      return 1
    fi
    base="$(basename "$file")"
    if [[ ! "$base" =~ ^([0-9a-f]{64})\.wet\.gz$ ]]; then
      echo "Error: published WET name is not content-addressed: $file" >&2
      return 1
    fi
    expected_digest="${BASH_REMATCH[1]}"
    digest="$(runtime_sha256_file "$file")" || return 1
    if [[ "$digest" != "$expected_digest" ]]; then
      echo "Error: corrupt published WET digest: $file" >&2
      return 1
    fi
    runtime_validate_wet_provenance "$file" "$URL_ID" "$CRAWL_ID" || return 1
    digests_ref+=("$digest")
  done
}

collect_staged_wets() {
  local files_name="$1"
  local -n files_ref="$files_name"
  local pair_dir="$DATA_DIR/wet/$URL_ID/$CRAWL_ID"
  local directory canonical entry_file file base sort_file
  local -a directories=(
    "$DATA_DIR/wet"
    "$DATA_DIR/wet/$URL_ID"
    "$pair_dir"
  )
  local -a entries=()

  files_ref=()
  for directory in "${directories[@]}"; do
    [[ -e "$directory" || -L "$directory" ]] || return 0
    if [[ -L "$directory" || ! -d "$directory" ]]; then
      echo "Error: managed staging path is unsafe: $directory" >&2
      return 1
    fi
    canonical="$(realpath -e -- "$directory")" || return 1
    if [[ "$canonical" != "$directory" ]]; then
      echo "Error: managed staging path is not canonical: $directory" >&2
      return 1
    fi
  done

  entry_file="$(mktemp "${TMPDIR:-/tmp}/warc2es-staged.XXXXXX")" || return 1
  if ! find "$pair_dir" -mindepth 1 -maxdepth 1 -print0 >"$entry_file"; then
    rm -f -- "$entry_file"
    return 1
  fi
  mapfile -d '' -t entries <"$entry_file"
  rm -f -- "$entry_file"

  for file in "${entries[@]}"; do
    base="$(basename "$file")"
    if [[ "$base" =~ ^\..+\.wet\.gz\.[A-Za-z0-9]+\.tmp$ && -f "$file" && ! -L "$file" ]]; then
      continue
    fi
    if [[ -L "$file" || ! -f "$file" || "$base" != *.wet.gz ]]; then
      echo "Error: invalid member in managed WET pair directory: $file" >&2
      return 1
    fi
    if ! _runtime_path_is_utf8 "$base"; then
      printf 'Error: managed WET name is not valid UTF-8: %q\n' "$file" >&2
      return 1
    fi
    files_ref+=("$file")
  done

  if [[ ${#files_ref[@]} -gt 0 ]]; then
    sort_file="$(mktemp "${TMPDIR:-/tmp}/warc2es-staged-sort.XXXXXX")" || return 1
    if ! printf '%s\0' "${files_ref[@]}" >"$sort_file" ||
       ! LC_ALL=C sort -zu -o "$sort_file" "$sort_file"; then
      rm -f -- "$sort_file"
      return 1
    fi
    mapfile -d '' -t files_ref <"$sort_file"
    rm -f -- "$sort_file"
  fi
}

cleanup_stale_publication_temps() {
  local pair_dir="$DATA_DIR/all/wet/$URL_ID/$CRAWL_ID"
  local entry_file file base
  local -a entries=()

  [[ -d "$pair_dir" ]] || return 0
  entry_file="$(mktemp "${TMPDIR:-/tmp}/warc2es-stale-published.XXXXXX")" || return 1
  if ! find "$pair_dir" -mindepth 1 -maxdepth 1 -print0 >"$entry_file"; then
    rm -f -- "$entry_file"
    return 1
  fi
  mapfile -d '' -t entries <"$entry_file"
  rm -f -- "$entry_file"
  for file in "${entries[@]}"; do
    base="$(basename "$file")"
    [[ "$base" =~ ^\.[0-9a-f]{64}\.tmp\.[A-Za-z0-9]+$ ]] || continue
    if [[ -L "$file" || ! -f "$file" ]]; then
      echo "Error: unsafe publication temporary: $file" >&2
      return 1
    fi
    rm -f -- "$file" || return 1
  done
  sync -f "$pair_dir"
}

classify_managed_staging() {
  local file="$1"
  [[ -n "$STAGING_ROOT" && "$file" == "$STAGING_ROOT"/* ]]
}

prepare_selected_snapshots() {
  local source digest temporary actual index

  SNAPSHOT_FILES=()
  validate_published_ancestors true || return 1
  for ((index = 0; index < ${#ingest_files[@]}; index++)); do
    source="${ingest_files[index]}"
    digest="${selected_digests[index]}"
    temporary="$(mktemp "$DATA_DIR/all/wet/$URL_ID/$CRAWL_ID/.$digest.tmp.XXXXXX")" || return 1
    PUBLICATION_TEMPS+=("$temporary")
    cp -- "$source" "$temporary" || return 1
    actual="$(runtime_sha256_file "$temporary")" || return 1
    if [[ "$actual" != "$digest" ]]; then
      echo "Error: WET input changed while preparing its transaction snapshot: $source" >&2
      return 1
    fi
    sync -f "$temporary" || return 1
    SNAPSHOT_FILES+=("$temporary")
  done
  sync -f "$DATA_DIR/all/wet/$URL_ID/$CRAWL_ID"
}

publish_selected_set() {
  local pair_dir="$DATA_DIR/all/wet/$URL_ID/$CRAWL_ID"
  local source digest destination index old_file old_digest
  local cleanup_failed=false
  local -A selected_set=()
  local -A representative=()

  publication_paths=()
  if [[ "$IMPLICIT_STAGING" == true ]]; then
    for ((index = 0; index < ${#published_files[@]}; index++)); do
      old_file="${published_files[index]}"
      old_digest="${published_digests[index]}"
      selected_set["$old_digest"]=1
      if [[ -z "${publication_seen["$old_digest"]+x}" ]]; then
        publication_seen["$old_digest"]=1
        publication_paths+=("${old_file#"$DATA_DIR"/}")
      fi
    done
  fi
  for ((index = 0; index < ${#SNAPSHOT_FILES[@]}; index++)); do
    source="${SNAPSHOT_FILES[index]}"
    digest="${selected_digests[index]}"
    selected_set["$digest"]=1
    if [[ -z "${representative["$digest"]+x}" ]]; then
      representative["$digest"]="$source"
    fi
  done

  for digest in "${selected_digests[@]}"; do
    destination="$pair_dir/$digest.wet.gz"
    if [[ -z "${publication_seen["$digest"]+x}" ]]; then
      publication_seen["$digest"]=1
      publication_paths+=("${destination#"$DATA_DIR"/}")
    fi
    [[ -e "$destination" ]] && continue
    source="${representative["$digest"]}"
    mv -- "$source" "$destination" || return 1
    PUBLICATION_CHANGED=true
    operator_log "[es-upsert] published → $destination"
  done

  sync -f "$pair_dir" || return 1

  if [[ "$IMPLICIT_STAGING" == false ]]; then
    for ((index = 0; index < ${#published_files[@]}; index++)); do
      old_file="${published_files[index]}"
      old_digest="${published_digests[index]}"
      if [[ -z "${selected_set["$old_digest"]+x}" ]]; then
        rm -f -- "$old_file" || return 1
        PUBLICATION_CHANGED=true
        operator_log "[es-upsert] unpublished superseded → $old_file"
      fi
    done
  fi
  sync -f "$pair_dir" || return 1

  # The published pair is now the exact selected set. Managed staging cleanup
  # is deliberately last and best-effort; external sources are never removed.
  for ((index = 0; index < ${#ingest_files[@]}; index++)); do
    source="${ingest_files[index]}"
    digest="${selected_digests[index]}"
    destination="$pair_dir/$digest.wet.gz"
    if classify_managed_staging "$source" && [[ -e "$source" && "$source" != "$destination" ]]; then
      if [[ "$(runtime_sha256_file "$source")" != "$digest" ]]; then
        echo "Error: managed WET changed before staging cleanup: $source" >&2
        cleanup_failed=true
        continue
      fi
      if ! rm -f -- "$source" || ! sync -f "$(dirname "$source")"; then
        echo "Error: cannot remove managed staging WET: $source" >&2
        cleanup_failed=true
      fi
    fi
  done

  if [[ "$PUBLICATION_CHANGED" == true ]]; then
    publication_status="published"
  else
    publication_status="unchanged"
  fi
  [[ "$cleanup_failed" == false ]]
}

report_present_selected_paths() {
  local pair_dir="$DATA_DIR/all/wet/$URL_ID/$CRAWL_ID"
  local digest destination actual
  local -A seen=()

  publication_paths=()
  for digest in "${selected_digests[@]}"; do
    [[ -z "${seen["$digest"]+x}" ]] || continue
    seen["$digest"]=1
    destination="$pair_dir/$digest.wet.gz"
    [[ -f "$destination" && ! -L "$destination" ]] || continue
    actual="$(runtime_sha256_file "$destination")" || continue
    [[ "$actual" == "$digest" ]] || continue
    publication_paths+=("${destination#"$DATA_DIR"/}")
  done
}

# ── argument parsing ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --all)
      echo "Error: --all is retired; use es-upsert-all.sh" >&2
      exit 1
      ;;
    --from-archive=*)
      [[ "$FROM_ARCHIVE" == false && -z "$INPUT_PATH" ]] || {
        echo "Error: --from-archive may be specified only once" >&2
        exit 1
      }
      INPUT_PATH="${1#*=}"
      FROM_ARCHIVE=true
      shift
      ;;
    --from-archive)
      [[ $# -ge 2 ]] || { echo "Error: --from-archive requires a value" >&2; exit 1; }
      [[ "$FROM_ARCHIVE" == false && -z "$INPUT_PATH" ]] || {
        echo "Error: --from-archive may be specified only once" >&2
        exit 1
      }
      INPUT_PATH="${2:-}"
      FROM_ARCHIVE=true
      shift 2
      ;;
    --stream=*) STREAM_ID="${1#*=}"; shift ;;
    --stream)
      [[ $# -ge 2 ]] || { echo "Error: --stream requires a value" >&2; exit 1; }
      STREAM_ID="${2:-}"
      shift 2
      ;;
    --url-id=*) URL_ID="${1#*=}"; URL_ID_SET=true; shift ;;
    --url-id)
      [[ $# -ge 2 ]] || { echo "Error: --url-id requires a value" >&2; exit 1; }
      URL_ID="${2:-}"
      URL_ID_SET=true
      shift 2
      ;;
    --crawl-id=*) CRAWL_ID="${1#*=}"; CRAWL_ID_SET=true; shift ;;
    --crawl-id)
      [[ $# -ge 2 ]] || { echo "Error: --crawl-id requires a value" >&2; exit 1; }
      CRAWL_ID="${2:-}"
      CRAWL_ID_SET=true
      shift 2
      ;;
    --data-dir=*) DATA_DIR="${1#*=}"; shift ;;
    --data-dir)
      [[ $# -ge 2 ]] || { echo "Error: --data-dir requires a value" >&2; exit 1; }
      DATA_DIR="${2:-}"
      shift 2
      ;;
    --start-date=*) START_DATE="${1#*=}"; shift ;;
    --start-date)
      [[ $# -ge 2 ]] || { echo "Error: --start-date requires a value" >&2; exit 1; }
      START_DATE="${2:-}"
      shift 2
      ;;
    --es-url=*) ES_URL="${1#*=}"; shift ;;
    --es-url)
      [[ $# -ge 2 ]] || { echo "Error: --es-url requires a value" >&2; exit 1; }
      ES_URL="${2:-}"
      shift 2
      ;;
    --dry-run) DRY_RUN=true; shift ;;
    --result-format=*) RESULT_FORMAT="${1#*=}"; shift ;;
    --result-format)
      [[ $# -ge 2 ]] || { echo "Error: --result-format requires a value" >&2; exit 1; }
      RESULT_FORMAT="${2:-}"
      shift 2
      ;;
    -h|--help) usage; exit 0 ;;
    --*) echo "Error: unknown option: $1" >&2; exit 1 ;;
    *)
      [[ "$FROM_ARCHIVE" == false ]] || {
        echo "Error: unexpected positional argument with --from-archive: $1" >&2
        exit 1
      }
      [[ -z "$INPUT_PATH" ]] || { echo "Error: unexpected positional argument: $1" >&2; exit 1; }
      INPUT_PATH="$1"
      shift
      ;;
  esac
done

case "$RESULT_FORMAT" in
  human|json) ;;
  *) echo "Error: --result-format must be human or json" >&2; exit 1 ;;
esac
if [[ "$FROM_ARCHIVE" == true ]]; then
  mode="archive-replay"
  [[ -n "$INPUT_PATH" ]] || operator_failure input_required "--from-archive requires one pair directory" 1
elif [[ -z "$INPUT_PATH" ]]; then
  mode="staging"
  IMPLICIT_STAGING=true
else
  mode="explicit"
fi
[[ -x "$ES_CLI" ]] || operator_failure dependency_missing "$ES_CLI is missing or not executable" 1

if [[ -n "$START_DATE" ]] && ! start_date_is_valid "$START_DATE"; then
  operator_failure invalid_start_date "--start-date must be a real calendar date in YYYY-MM-DD form" 1
fi

if [[ "$FROM_ARCHIVE" == false ]]; then
  if [[ "$URL_ID_SET" != true || "$CRAWL_ID_SET" != true ]]; then
    operator_failure provenance_required \
      "normal input requires both --url-id and --crawl-id" 1
  fi
  runtime_identifier_is_valid "$URL_ID" || \
    operator_failure invalid_url_id \
      "--url-id must match [A-Za-z0-9._-]{1,128} and must not be . or .." 1
  runtime_identifier_is_valid "$CRAWL_ID" || \
    operator_failure invalid_crawl_id \
      "--crawl-id must match [A-Za-z0-9._-]{1,128} and must not be . or .." 1
  if [[ "$IMPLICIT_STAGING" == true ]]; then
    INPUT_PATH="$DATA_DIR/wet/$URL_ID/$CRAWL_ID"
  fi
fi

if [[ "$INPUT_PATH" != /* ]]; then
  INPUT_PATH="$ORIG_PWD/$INPUT_PATH"
fi
if [[ ! -e "$INPUT_PATH" && ! -L "$INPUT_PATH" ]]; then
  if [[ "$IMPLICIT_STAGING" == true ]]; then
    operator_failure input_empty "no staged .wet.gz files found for $URL_ID/$CRAWL_ID" 1
  fi
  operator_failure input_not_found "input path not found: $INPUT_PATH" 1
fi
if ! INPUT_PATH="$(runtime_resolve_path "$INPUT_PATH")"; then
  operator_failure input_invalid "Cannot resolve input path: $INPUT_PATH" 1
fi

if [[ "$FROM_ARCHIVE" == true ]]; then
  ARCHIVE_ROOT="$DATA_DIR/all/wet"
  if [[ -L "$DATA_DIR/all" || -L "$ARCHIVE_ROOT" || ! -d "$ARCHIVE_ROOT" ]]; then
    operator_failure archive_unsafe "Archive replay root is missing or unsafe: $ARCHIVE_ROOT" 1
  fi
  if ! ARCHIVE_ROOT="$(realpath -e -- "$ARCHIVE_ROOT")"; then
    operator_failure archive_unsafe "Cannot resolve archive replay root: $ARCHIVE_ROOT" 1
  fi
  if [[ "$INPUT_PATH" != "$ARCHIVE_ROOT"/* ]]; then
    operator_failure archive_scope_invalid \
      "--from-archive must name a provenance directory below $ARCHIVE_ROOT" 1
  fi
  archive_relative="${INPUT_PATH#"$ARCHIVE_ROOT"/}"
  if [[ "$archive_relative" != */* || "${archive_relative#*/}" == */* ]]; then
    operator_failure archive_scope_invalid \
      "--from-archive must name exactly <url-id>/<crawl-id>" 1
  fi
  derived_url_id="${archive_relative%%/*}"
  derived_crawl_id="${archive_relative#*/}"
  runtime_identifier_is_valid "$derived_url_id" || \
    operator_failure invalid_url_id "Archive url-id is invalid" 1
  runtime_identifier_is_valid "$derived_crawl_id" || \
    operator_failure invalid_crawl_id "Archive crawl-id is invalid" 1
  if [[ "$URL_ID_SET" == true && "$URL_ID" != "$derived_url_id" ]]; then
    operator_failure provenance_mismatch "--url-id does not match the archive directory" 1
  fi
  if [[ "$CRAWL_ID_SET" == true && "$CRAWL_ID" != "$derived_crawl_id" ]]; then
    operator_failure provenance_mismatch "--crawl-id does not match the archive directory" 1
  fi
  URL_ID="$derived_url_id"
  CRAWL_ID="$derived_crawl_id"
else
  if [[ "$IMPLICIT_STAGING" == false && -e "$DATA_DIR/all" ]]; then
    archive_guard="$(realpath -e -- "$DATA_DIR/all")" || \
      operator_failure archive_unsafe "Cannot resolve published root" 1
    if [[ "$INPUT_PATH" == "$archive_guard" || "$INPUT_PATH" == "$archive_guard"/* ]]; then
      operator_failure archive_scope_invalid \
        "normal input beneath all/ is forbidden; use --from-archive" 1
    fi
  fi
fi

stream="$(stream_name "$STREAM_ID")"
runtime_stream_name_is_safe "$stream" || \
  operator_failure invalid_stream "Resolved stream is not one exact safe Elasticsearch name" 1

declare -a ingest_files=()
declare -a selected_digests=()
declare -a published_files=()
declare -a published_digests=()
declare -a publication_paths=()
declare -A publication_seen=()
if [[ "$IMPLICIT_STAGING" == true ]]; then
  collect_staged_wets ingest_files || \
    operator_failure input_discovery_failed "Managed WET staging discovery failed" 1
else
  runtime_find_data_files wet "$INPUT_PATH" ingest_files || \
    operator_failure input_discovery_failed "WET input discovery failed" 1
fi
if [[ ${#ingest_files[@]} -eq 0 ]]; then
  if [[ "$IMPLICIT_STAGING" == true ]]; then
    operator_failure input_empty "no staged .wet.gz files found for $URL_ID/$CRAWL_ID" 1
  fi
  operator_failure input_empty "no .wet.gz files found in input directory: $INPUT_PATH" 1
fi
if [[ "$RESULT_FORMAT" == "json" && "$IMPLICIT_STAGING" == false ]]; then
  inputs_json="$(runtime_operator_inputs_json "$INPUT_PATH" 0 "${ingest_files[@]}")" || \
    operator_failure input_json_failed "Cannot serialize selected WET paths" 1
fi
validate_selected_wets ingest_files selected_digests || \
  operator_failure provenance_invalid "Complete WET provenance validation failed" 1
collect_published_wets published_files published_digests || \
  operator_failure archive_corrupt "Published WET set validation failed" 1
if [[ "$RESULT_FORMAT" == "json" ]]; then
  if [[ "$IMPLICIT_STAGING" == true ]]; then
    inputs_json="$(runtime_operator_inputs_json "$DATA_DIR" 0 \
      "${published_files[@]}" "${ingest_files[@]}")" || \
      operator_failure input_json_failed "Cannot serialize the staged pair transaction" 1
  fi
fi

if [[ "$FROM_ARCHIVE" == true ]]; then
  if [[ "$PUBLISHED_TEMP_FOUND" == true ]]; then
    operator_failure archive_corrupt \
      "Archive replay refuses a pair directory with publication temporaries" 1
  fi
  if ! arrays_equal ingest_files published_files || ! arrays_equal selected_digests published_digests; then
    operator_failure archive_incomplete \
      "Archive replay input is not the complete published pair set" 1
  fi
fi

STAGING_ROOT=""
if [[ -d "$DATA_DIR/wet" && ! -L "$DATA_DIR/wet" ]]; then
  STAGING_ROOT="$(realpath -e -- "$DATA_DIR/wet")"
fi

operator_log "[es-upsert] ${#ingest_files[@]} file(s), pair=$URL_ID/$CRAWL_ID → ES $ES_URL"
operator_log "[es-upsert] selected → $stream"
for file in "${ingest_files[@]}"; do
  operator_log "[es-upsert] selected $(basename "$file")"
done

if [[ "$DRY_RUN" == true ]]; then
  if [[ "$RESULT_FORMAT" == "json" ]]; then
    publication_json="$(runtime_operator_publication_json skipped '[]')"
    runtime_operator_emit_invocation es-upsert dry_run 0 "$mode" "$inputs_json" '[]' \
      "$publication_json" null "" ""
  else
    echo "  (dry-run, skipping locks, delete, load, and publication)"
  fi
  exit 0
fi

set +e
runtime_lock_pair "$URL_ID" "$CRAWL_ID"
lock_exit=$?
set -e
if [[ "$lock_exit" -ne 0 ]]; then
  if [[ "$lock_exit" -eq 75 ]]; then
    operator_failure busy "Provenance pair is busy" 75
  fi
  operator_failure lock_unsafe "Cannot acquire safe provenance locks" 1
fi

declare -a rechecked_files=()
declare -a rechecked_digests=()
declare -a rechecked_published_files=()
declare -a rechecked_published_digests=()
if [[ "$FROM_ARCHIVE" == false ]]; then
  cleanup_stale_publication_temps || \
    operator_failure cleanup_failed "Cannot remove stale publication temporaries" 1
fi
if [[ "$IMPLICIT_STAGING" == true ]]; then
  collect_staged_wets rechecked_files || \
    operator_failure input_changed "Managed staging discovery failed after lock acquisition" 1
else
  runtime_find_data_files wet "$INPUT_PATH" rechecked_files || \
    operator_failure input_changed "Input discovery failed after lock acquisition" 1
fi
validate_selected_wets rechecked_files rechecked_digests || \
  operator_failure input_changed "Input validation failed after lock acquisition" 1
if ! arrays_equal ingest_files rechecked_files || ! arrays_equal selected_digests rechecked_digests; then
  operator_failure input_changed "Input set changed while acquiring the provenance lock" 1
fi
collect_published_wets rechecked_published_files rechecked_published_digests || \
  operator_failure archive_corrupt "Published WET set failed revalidation" 1
if [[ "$FROM_ARCHIVE" == true && "$PUBLISHED_TEMP_FOUND" == true ]]; then
  operator_failure archive_corrupt "Archive replay found a publication temporary" 1
fi
if ! arrays_equal published_files rechecked_published_files || \
   ! arrays_equal published_digests rechecked_published_digests; then
  operator_failure archive_changed "Published WET set changed while acquiring the provenance lock" 1
fi

declare -a load_files=()
declare -a process_files=()
if [[ "$FROM_ARCHIVE" == true ]]; then
  load_files=("${ingest_files[@]}")
else
  prepare_selected_snapshots || \
    operator_failure snapshot_failed \
      "Cannot prepare verified WET snapshots before Elasticsearch replacement" 1
  if [[ "$IMPLICIT_STAGING" == true ]]; then
    load_files=("${published_files[@]}" "${SNAPSHOT_FILES[@]}")
  else
    load_files=("${SNAPSHOT_FILES[@]}")
  fi
fi
for file in "${load_files[@]}"; do
  [[ -s "$file" ]] && process_files+=("$file")
done

set +e
runtime_es_cli refresh "$stream" >/dev/null
refresh_exit=$?
set -e
if [[ "$refresh_exit" -ne 0 ]]; then
  operator_failure elasticsearch_refresh_failed \
    "Elasticsearch pre-delete refresh failed" "$refresh_exit"
fi

delete_query="$(jq -cn --arg url_id "$URL_ID" --arg crawl_id "$CRAWL_ID" \
  '{query:{bool:{filter:[{"term":{"nac-url-id":$url_id}},{"term":{"nac-crawl-id":$crawl_id}}]}}}')"
DELETE_FILE="$(mktemp "${TMPDIR:-/tmp}/warc2es-delete.XXXXXX")"
set +e
runtime_es_cli batch-delete "$stream" "$delete_query" >"$DELETE_FILE"
delete_exit=$?
set -e
if [[ "$delete_exit" -ne 0 ]]; then
  operator_failure elasticsearch_delete_failed "Elasticsearch pair deletion failed" "$delete_exit"
fi
if [[ ! -s "$DELETE_FILE" ]] || ! jq -e \
    'type == "object" and
     .timed_out == false and
     (.version_conflicts | type == "number") and .version_conflicts == 0 and
     (.failures | type == "array") and (.failures | length == 0) and
     (.total | type == "number") and .total >= 0 and (.total | floor) == .total and
     (.deleted | type == "number") and .deleted >= 0 and (.deleted | floor) == .deleted and
     .deleted == .total' \
    "$DELETE_FILE" >/dev/null 2>&1; then
  operator_failure elasticsearch_delete_incomplete "Elasticsearch pair deletion reported failures" 1
fi
rm -f -- "$DELETE_FILE"
DELETE_FILE=""

extra_args=("--url-id=$URL_ID" "--crawl-id=$CRAWL_ID")
[[ -n "$START_DATE" ]] && extra_args+=("--start-date=$START_DATE")

processing_json="null"
operator_status="ok"
operator_exit=0
error_code=""
error_message=""
declare -a load_args=()

if [[ ${#process_files[@]} -eq 0 ]]; then
  operator_log "[es-upsert] zero-record transaction; no JVM required"
else
  load_args=(load-stream "${process_files[0]}" "$stream" "${extra_args[@]}")
  if [[ ${#process_files[@]} -gt 1 ]]; then
    load_args+=("${process_files[@]:1}")
  fi
fi

if [[ ${#process_files[@]} -eq 0 ]]; then
  :
elif [[ "$RESULT_FORMAT" == "json" ]]; then
  RESULT_FILE="$(mktemp "${TMPDIR:-/tmp}/warc2es-processing.XXXXXX")"
  set +e
  runtime_es_cli "${load_args[@]}" --result-format=json >"$RESULT_FILE"
  java_exit=$?
  set -e
  runtime_operator_validate_processing "$RESULT_FILE" "$java_exit"
  processing_json="$RUNTIME_OPERATOR_PROCESSING_JSON"
  operator_status="$RUNTIME_OPERATOR_STATUS"
  operator_exit="$RUNTIME_OPERATOR_EXIT_CODE"
  error_code="$RUNTIME_OPERATOR_ERROR_CODE"
  error_message="$RUNTIME_OPERATOR_ERROR_MESSAGE"
  rm -f -- "$RESULT_FILE"
  RESULT_FILE=""
else
  if ! runtime_es_cli "${load_args[@]}"; then
    echo "Error: Elasticsearch load failed after pair deletion; published WETs are unchanged" >&2
    exit 1
  fi
fi

publication_status="unchanged"
PUBLICATION_CHANGED=false
if [[ "$RESULT_FORMAT" == "json" && "$operator_exit" -ne 0 ]]; then
  publication_status="skipped"
elif [[ "$FROM_ARCHIVE" == true ]]; then
  for file in "${ingest_files[@]}"; do
    publication_paths+=("${file#"$DATA_DIR"/}")
  done
else
  if ! publish_selected_set; then
    echo "Error: Elasticsearch succeeded but WET publication did not complete" >&2
    operator_status="partial"
    operator_exit=1
    error_code="publication_failed"
    error_message="Elasticsearch succeeded but WET publication did not complete"
    report_present_selected_paths
    if [[ "$PUBLICATION_CHANGED" == true ]]; then
      publication_status="published"
    else
      publication_status="unchanged"
    fi
  fi
fi

if [[ "$RESULT_FORMAT" == "json" ]]; then
  paths_json="$(runtime_operator_paths_json "${publication_paths[@]}")"
  publication_json="$(runtime_operator_publication_json "$publication_status" "$paths_json")"
  runtime_operator_emit_invocation es-upsert "$operator_status" "$operator_exit" "$mode" \
    "$inputs_json" '[]' "$publication_json" "$processing_json" \
    "$error_code" "$error_message"
fi

exit "$operator_exit"
