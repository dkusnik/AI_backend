#!/usr/bin/env bash
# es-delete.sh — delete one provenance pair or every document in one stream.
set -euo pipefail
export LC_ALL=C

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "$SCRIPT_DIR/app/lib/scripts/runtime-lib.sh" ]]; then
  RUNTIME_LIB="$SCRIPT_DIR/app/lib/scripts/runtime-lib.sh"
elif [[ -f "$SCRIPT_DIR/lib/scripts/runtime-lib.sh" ]]; then
  RUNTIME_LIB="$SCRIPT_DIR/lib/scripts/runtime-lib.sh"
elif [[ -f "$SCRIPT_DIR/runtime-lib.sh" ]]; then
  RUNTIME_LIB="$SCRIPT_DIR/runtime-lib.sh"
else
  echo "Error: runtime-lib.sh not found from $SCRIPT_DIR" >&2
  exit 1
fi
# shellcheck source=/dev/null
source "$RUNTIME_LIB"

runtime_enter_script_dir "$SCRIPT_DIR"
DELETE_FILE=""
cleanup() {
  [[ -z "$DELETE_FILE" ]] || rm -f -- "$DELETE_FILE"
  runtime_unlock_pair
  runtime_leave_script_dir
}
trap cleanup EXIT

runtime_resolve_layout "$SCRIPT_DIR"
runtime_source_profile

ES_CLI="$APP_DIR/bin/es-cli"
STREAM_VALUE=""
STREAM_SET=false
STREAM_NAME=""
URL_ID=""
CRAWL_ID=""
URL_ID_SET=false
CRAWL_ID_SET=false
ALL_DOCUMENTS=false
ES_URL="${ES_URL:-http://localhost:9200}"
DRY_RUN=false
RESULT_FORMAT="human"
mode="pair"
target_json='{"stream":null,"scope":null,"url_id":null,"crawl_id":null}'
publication_json='{"status":"skipped","paths":[],"cleanup":{"matched":0,"removed":0,"failed":0,"failed_paths":[]}}'
query='null'
lock_plan_json='[]'
lock_desc=""

declare -a archive_files=()
declare -a archive_paths=()
declare -a failed_paths=()

usage() {
  cat <<'EOF'
Usage: es-delete.sh --stream=<id|nac-data-...> --url-id=<id> --crawl-id=<id> [options]
       es-delete.sh --stream=<id|nac-data-...> --all-documents [options]

Delete one exact provenance pair or every document from one explicit stream.
Published WET cleanup occurs only after Elasticsearch accepts the deletion.

Options:
  --stream=<value>           required stream id or full stream name
  --url-id=<id>              exact provenance URL identifier (pair scope)
  --crawl-id=<id>            exact provenance crawl identifier (pair scope)
  --all-documents            select the whole stream; identifiers are forbidden
  --es-url=<url>             override ES_URL (default: http://localhost:9200)
  --dry-run                  validate and report; make no ES or filesystem changes
  --result-format=<format>   human (default) or json
  -h, --help                 show this help
EOF
}

operator_log() {
  if [[ "$RESULT_FORMAT" == json ]]; then
    echo "$*" >&2
  else
    echo "$*"
  fi
}

operator_failure() {
  local code="$1"
  local message="$2"
  local exit_code="${3:-1}"

  echo "Error: $message" >&2
  if [[ "$RESULT_FORMAT" == json ]]; then
    runtime_operator_emit_control_invocation es-delete error "$exit_code" "$mode" \
      "$target_json" "$publication_json" "$code" "$message"
  fi
  exit "$exit_code"
}

set_target_json() {
  target_json="$(jq -cn \
    --arg stream "$STREAM_NAME" \
    --arg scope "$mode" \
    --arg url_id "$URL_ID" \
    --arg crawl_id "$CRAWL_ID" \
    --argjson query "$query" \
    --argjson locks "$lock_plan_json" \
    '{stream:$stream,scope:$scope,
      url_id:(if $scope == "pair" then $url_id else null end),
      crawl_id:(if $scope == "pair" then $crawl_id else null end),
      query:$query,locks:$locks}')"
}

set_publication_json() {
  local status="$1"
  local paths_name="$2"
  local matched="$3"
  local removed="$4"
  local failed="$5"
  local failed_name="$6"
  local -n paths_ref="$paths_name"
  local -n failed_ref="$failed_name"
  local paths_json failed_json

  paths_json="$(runtime_operator_paths_json "${paths_ref[@]}")"
  failed_json="$(runtime_operator_paths_json "${failed_ref[@]}")"
  publication_json="$(jq -cn \
    --arg status "$status" \
    --argjson paths "$paths_json" \
    --argjson matched "$matched" \
    --argjson removed "$removed" \
    --argjson failed "$failed" \
    --argjson failed_paths "$failed_json" \
    '{status:$status,paths:$paths,
      cleanup:{matched:$matched,removed:$removed,failed:$failed,failed_paths:$failed_paths}}')"
}

validate_archive_directory() {
  local directory="$1"
  local canonical

  if [[ -L "$directory" ]]; then
    echo "Error: published WET path contains a symlink: $directory" >&2
    return 1
  fi
  [[ -e "$directory" ]] || return 2
  if [[ ! -d "$directory" ]]; then
    echo "Error: published WET path is not a directory: $directory" >&2
    return 1
  fi
  canonical="$(realpath -e -- "$directory")" || return 1
  if [[ "$canonical" != "$directory" ]]; then
    echo "Error: published WET path is not canonical: $directory" >&2
    return 1
  fi
}

append_pair_files() {
  local pair_dir="$1"
  local entry base expected_digest actual_digest
  local -a entries=()

  shopt -s nullglob dotglob
  entries=("$pair_dir"/*)
  shopt -u nullglob dotglob
  for entry in "${entries[@]}"; do
    base="$(basename "$entry")"
    if [[ "$base" =~ ^\.[0-9a-f]{64}\.tmp\.[A-Za-z0-9]+$ ]]; then
      if [[ -L "$entry" || ! -f "$entry" ]]; then
        echo "Error: unsafe publication temporary: $entry" >&2
        return 1
      fi
      continue
    fi
    if [[ -L "$entry" || ! -f "$entry" || ! "$base" =~ ^[0-9a-f]{64}\.wet\.gz$ ]]; then
      echo "Error: invalid member in published WET pair directory: $entry" >&2
      return 1
    fi
    expected_digest="${base%.wet.gz}"
    actual_digest="$(runtime_sha256_file "$entry")" || return 1
    if [[ "$actual_digest" != "$expected_digest" ]]; then
      echo "Error: corrupt published WET digest: $entry" >&2
      return 1
    fi
    archive_files+=("$entry")
  done
}

collect_pair_files() {
  local directory
  local -a directories=(
    "$RUNTIME_DIR/all"
    "$RUNTIME_DIR/all/wet"
    "$RUNTIME_DIR/all/wet/$URL_ID"
    "$RUNTIME_DIR/all/wet/$URL_ID/$CRAWL_ID"
  )

  archive_files=()
  for directory in "${directories[@]}"; do
    if validate_archive_directory "$directory"; then
      continue
    else
      case $? in
        2) return 0 ;;
        *) return 1 ;;
      esac
    fi
  done
  append_pair_files "${directories[3]}"
}

collect_all_files() {
  local all_dir="$RUNTIME_DIR/all"
  local wet_root="$RUNTIME_DIR/all/wet"
  local url_dir crawl_dir entry url_id crawl_id
  local -a url_dirs=() crawl_dirs=()

  archive_files=()
  if validate_archive_directory "$all_dir"; then
    :
  else
    [[ $? -eq 2 ]] && return 0
    return 1
  fi
  if validate_archive_directory "$wet_root"; then
    :
  else
    [[ $? -eq 2 ]] && return 0
    return 1
  fi

  shopt -s nullglob dotglob
  url_dirs=("$wet_root"/*)
  shopt -u nullglob dotglob
  for url_dir in "${url_dirs[@]}"; do
    url_id="$(basename "$url_dir")"
    runtime_identifier_is_valid "$url_id" || {
      echo "Error: invalid url-id directory in published WET store: $url_dir" >&2
      return 1
    }
    validate_archive_directory "$url_dir" || return 1
    shopt -s nullglob dotglob
    crawl_dirs=("$url_dir"/*)
    shopt -u nullglob dotglob
    for crawl_dir in "${crawl_dirs[@]}"; do
      crawl_id="$(basename "$crawl_dir")"
      runtime_identifier_is_valid "$crawl_id" || {
        echo "Error: invalid crawl-id directory in published WET store: $crawl_dir" >&2
        return 1
      }
      validate_archive_directory "$crawl_dir" || return 1
      append_pair_files "$crawl_dir" || return 1
    done
  done
}

collect_archive_files() {
  local file
  local -a sorted=()

  if [[ "$mode" == pair ]]; then
    collect_pair_files || return 1
  else
    collect_all_files || return 1
  fi
  if [[ ${#archive_files[@]} -gt 0 ]]; then
    mapfile -d '' -t sorted < <(printf '%s\0' "${archive_files[@]}" | sort -z)
    archive_files=("${sorted[@]}")
  fi
  archive_paths=()
  for file in "${archive_files[@]}"; do
    archive_paths+=("${file#"$RUNTIME_DIR"/}")
  done
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

detect_result_format() {
  local -a arguments=("$@")
  local index
  for ((index = 0; index < ${#arguments[@]}; index++)); do
    case "${arguments[index]}" in
      --result-format=*) RESULT_FORMAT="${arguments[index]#*=}" ;;
      --result-format)
        if ((index + 1 < ${#arguments[@]})); then
          RESULT_FORMAT="${arguments[index + 1]}"
        fi
        ;;
    esac
  done
}

detect_result_format "$@"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --stream=*)
      [[ "$STREAM_SET" == false ]] || operator_failure invalid_arguments "--stream may be specified only once"
      STREAM_VALUE="${1#*=}"
      STREAM_SET=true
      shift
      ;;
    --stream)
      [[ $# -ge 2 ]] || operator_failure invalid_arguments "--stream requires a value"
      [[ "$STREAM_SET" == false ]] || operator_failure invalid_arguments "--stream may be specified only once"
      STREAM_VALUE="${2:-}"
      STREAM_SET=true
      shift 2
      ;;
    --url-id=*) URL_ID="${1#*=}"; URL_ID_SET=true; shift ;;
    --url-id)
      [[ $# -ge 2 ]] || operator_failure invalid_arguments "--url-id requires a value"
      URL_ID="${2:-}"
      URL_ID_SET=true
      shift 2
      ;;
    --crawl-id=*) CRAWL_ID="${1#*=}"; CRAWL_ID_SET=true; shift ;;
    --crawl-id)
      [[ $# -ge 2 ]] || operator_failure invalid_arguments "--crawl-id requires a value"
      CRAWL_ID="${2:-}"
      CRAWL_ID_SET=true
      shift 2
      ;;
    --all-documents) ALL_DOCUMENTS=true; shift ;;
    --es-url=*) ES_URL="${1#*=}"; shift ;;
    --es-url)
      [[ $# -ge 2 ]] || operator_failure invalid_arguments "--es-url requires a value"
      ES_URL="${2:-}"
      shift 2
      ;;
    --dry-run) DRY_RUN=true; shift ;;
    --result-format=*) RESULT_FORMAT="${1#*=}"; shift ;;
    --result-format)
      [[ $# -ge 2 ]] || operator_failure invalid_arguments "--result-format requires a value"
      RESULT_FORMAT="${2:-}"
      shift 2
      ;;
    --yes|--force)
      operator_failure unsupported_option "$1 is not supported; destructive scope is explicit"
      ;;
    -h|--help) usage; exit 0 ;;
    --*) operator_failure unknown_option "unknown option: $1" ;;
    *) operator_failure invalid_arguments "unexpected positional argument: $1" ;;
  esac
done

case "$RESULT_FORMAT" in
  human|json) ;;
  *) echo "Error: --result-format must be human or json" >&2; exit 1 ;;
esac
[[ "$STREAM_SET" == true && -n "$STREAM_VALUE" ]] || \
  operator_failure stream_required "one non-empty --stream is required"

if [[ "$ALL_DOCUMENTS" == true ]]; then
  mode="all-documents"
  if [[ "$URL_ID_SET" == true || "$CRAWL_ID_SET" == true ]]; then
    operator_failure invalid_scope "--all-documents does not accept --url-id or --crawl-id"
  fi
else
  mode="pair"
  if [[ "$URL_ID_SET" != true || "$CRAWL_ID_SET" != true ]]; then
    operator_failure provenance_required \
      "pair deletion requires non-empty --url-id and --crawl-id"
  fi
  runtime_identifier_is_valid "$URL_ID" || operator_failure invalid_url_id \
    "--url-id must match [A-Za-z0-9._-]{1,128} and must not be . or .."
  runtime_identifier_is_valid "$CRAWL_ID" || operator_failure invalid_crawl_id \
    "--crawl-id must match [A-Za-z0-9._-]{1,128} and must not be . or .."
fi

STREAM_NAME="$(stream_name "$STREAM_VALUE")"
runtime_stream_name_is_safe "$STREAM_NAME" || operator_failure invalid_stream \
  "resolved stream is not one exact safe Elasticsearch name"
[[ -x "$ES_CLI" ]] || operator_failure dependency_missing \
  "$ES_CLI is missing or not executable"

if [[ "$mode" == pair ]]; then
  query="$(jq -cn --arg url_id "$URL_ID" --arg crawl_id "$CRAWL_ID" \
    '{query:{bool:{filter:[{"term":{"nac-url-id":$url_id}},{"term":{"nac-crawl-id":$crawl_id}}]}}}')"
  filter_desc="nac-url-id=$URL_ID nac-crawl-id=$CRAWL_ID"
  lock_plan_json="$(jq -cn \
    --arg global 'var/locks/warc2es/global.lock' \
    --arg pair "var/locks/warc2es/pairs/$URL_ID/$CRAWL_ID.lock" \
    '[{path:$global,mode:"shared"},{path:$pair,mode:"exclusive"}]')"
  lock_desc="var/locks/warc2es/global.lock(shared), var/locks/warc2es/pairs/$URL_ID/$CRAWL_ID.lock(exclusive)"
else
  query='{"query":{"match_all":{}}}'
  filter_desc="match_all (--all-documents)"
  lock_plan_json='[{"path":"var/locks/warc2es/global.lock","mode":"exclusive"}]'
  lock_desc="var/locks/warc2es/global.lock(exclusive)"
fi
set_target_json
runtime_validate_lock_targets "$mode" "$URL_ID" "$CRAWL_ID" || \
  operator_failure lock_unsafe "lock targets are unsafe"

collect_archive_files || operator_failure archive_unsafe \
  "published WET candidate discovery failed"
failed_paths=()
set_publication_json skipped archive_paths "${#archive_paths[@]}" 0 0 failed_paths

operator_log "[es-delete] target=$STREAM_NAME es=$ES_URL filter=$filter_desc"
operator_log "[es-delete] locks=$lock_desc"
if [[ "$DRY_RUN" == true ]]; then
  set_publication_json planned archive_paths "${#archive_paths[@]}" 0 0 failed_paths
  if [[ "$RESULT_FORMAT" == json ]]; then
    runtime_operator_emit_control_invocation es-delete dry_run 0 "$mode" \
      "$target_json" "$publication_json" "" ""
  else
    echo "[es-delete] dry-run query:"
    echo "$query"
    echo "[es-delete] dry-run published WET candidates: ${#archive_paths[@]}"
  fi
  exit 0
fi

set +e
if [[ "$mode" == pair ]]; then
  runtime_lock_pair "$URL_ID" "$CRAWL_ID"
else
  runtime_lock_global
fi
lock_exit=$?
set -e
if [[ "$lock_exit" -ne 0 ]]; then
  if [[ "${RUNTIME_LOCK_ERROR_CODE:-}" == busy ]]; then
    operator_failure busy "another conflicting provenance operation is active" 75
  fi
  operator_failure lock_failed "cannot acquire the mutation lock" "$lock_exit"
fi

# Read indirectly through arrays_equal's nameref parameters.
# shellcheck disable=SC2034
previous_files=("${archive_files[@]}")
# shellcheck disable=SC2034
previous_paths=("${archive_paths[@]}")
collect_archive_files || operator_failure archive_unsafe \
  "published WET candidates failed revalidation after lock acquisition"
if ! arrays_equal previous_files archive_files || ! arrays_equal previous_paths archive_paths; then
  operator_failure archive_changed \
    "published WET candidates changed while acquiring the mutation lock"
fi
set_publication_json skipped archive_paths "${#archive_paths[@]}" 0 0 failed_paths

DELETE_FILE="$(mktemp "${TMPDIR:-/tmp}/warc2es-delete.XXXXXX")" || \
  operator_failure temporary_failed "cannot create Elasticsearch response file"
set +e
runtime_es_cli batch-delete "$STREAM_NAME" "$query" >"$DELETE_FILE"
delete_exit=$?
set -e
if [[ "$delete_exit" -ne 0 ]]; then
  operator_failure elasticsearch_delete_failed "Elasticsearch deletion failed" "$delete_exit"
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
  operator_failure elasticsearch_delete_incomplete \
    "Elasticsearch deletion reported an incomplete result"
fi
rm -f -- "$DELETE_FILE"
DELETE_FILE=""

removed=0
failed=0
failed_paths=()
for ((index = 0; index < ${#archive_files[@]}; index++)); do
  file="${archive_files[index]}"
  relative="${archive_paths[index]}"
  if rm -- "$file" && sync -f "$(dirname "$file")"; then
    removed=$((removed + 1))
  else
    echo "Error: cannot remove published WET: $relative" >&2
    failed=$((failed + 1))
    failed_paths+=("$relative")
  fi
done

if [[ "$failed" -gt 0 ]]; then
  set_publication_json partial archive_paths "${#archive_paths[@]}" "$removed" "$failed" failed_paths
  if [[ "$RESULT_FORMAT" == json ]]; then
    runtime_operator_emit_control_invocation es-delete partial 1 "$mode" \
      "$target_json" "$publication_json" archive_cleanup_failed \
      "Elasticsearch deletion succeeded but published WET cleanup was partial"
  else
    echo "Error: Elasticsearch deletion succeeded but $failed published WET file(s) could not be removed" >&2
  fi
  exit 1
fi

set_publication_json removed archive_paths "${#archive_paths[@]}" "$removed" 0 failed_paths
if [[ "$RESULT_FORMAT" == json ]]; then
  runtime_operator_emit_control_invocation es-delete ok 0 "$mode" \
    "$target_json" "$publication_json" "" ""
else
  echo "[es-delete] removed $removed published WET file(s)"
fi
