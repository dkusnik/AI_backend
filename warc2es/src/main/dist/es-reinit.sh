#!/usr/bin/env bash
# es-reinit.sh — purge and recreate one explicit Elasticsearch stream.
set -euo pipefail

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
cleanup() {
  runtime_unlock_global
  runtime_leave_script_dir
}
trap cleanup EXIT

runtime_resolve_layout "$SCRIPT_DIR"
runtime_source_profile

ES_CLI="$APP_DIR/bin/es-cli"
STREAM_VALUE=""
STREAM_SET=false
STREAM_NAME=""
ES_URL="${ES_URL:-http://localhost:9200}"
ASSUME_YES=false
DRY_RUN=false
RESULT_FORMAT="human"
target_json='{"stream":null,"scope":"stream","url_id":null,"crawl_id":null}'

usage() {
  cat <<'EOF'
Usage: es-reinit.sh --stream=<id|nac-data-...> [options]

Purge and recreate exactly one empty Elasticsearch stream. Published WETs are
preserved; replay remains the separate es-upsert-all.sh operation.

Options:
  --stream=<value>           required stream id or full stream name
  --es-url=<url>             override ES_URL (default: http://localhost:9200)
  --yes, --force             skip interactive confirmation
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
    runtime_operator_emit_control_invocation es-reinit error "$exit_code" reinit \
      "$target_json" null "$code" "$message"
  fi
  exit "$exit_code"
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
    --es-url=*) ES_URL="${1#*=}"; shift ;;
    --es-url)
      [[ $# -ge 2 ]] || operator_failure invalid_arguments "--es-url requires a value"
      ES_URL="${2:-}"
      shift 2
      ;;
    --yes|--force) ASSUME_YES=true; shift ;;
    --dry-run) DRY_RUN=true; shift ;;
    --result-format=*) RESULT_FORMAT="${1#*=}"; shift ;;
    --result-format)
      [[ $# -ge 2 ]] || operator_failure invalid_arguments "--result-format requires a value"
      RESULT_FORMAT="${2:-}"
      shift 2
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

STREAM_NAME="$(stream_name "$STREAM_VALUE")"
runtime_stream_name_is_safe "$STREAM_NAME" || operator_failure invalid_stream \
  "resolved stream is not one exact safe Elasticsearch name"
target_json="$(jq -cn --arg stream "$STREAM_NAME" \
  '{stream:$stream,scope:"stream",url_id:null,crawl_id:null,
    operation:"purge-and-init",
    locks:[{path:"var/locks/warc2es/global.lock",mode:"exclusive"}]}')"
[[ -x "$ES_CLI" ]] || operator_failure dependency_missing \
  "$ES_CLI is missing or not executable"

runtime_validate_lock_targets global || operator_failure lock_unsafe \
  "lock targets are unsafe"

operator_log "[es-reinit] target=$STREAM_NAME es=$ES_URL"
operator_log "[es-reinit] operation=purge-and-init locks=var/locks/warc2es/global.lock(exclusive)"
if [[ "$DRY_RUN" == true ]]; then
  if [[ "$RESULT_FORMAT" == json ]]; then
    runtime_operator_emit_control_invocation es-reinit dry_run 0 reinit \
      "$target_json" null "" ""
  else
    echo "[es-reinit] dry-run; published WETs remain unchanged"
  fi
  exit 0
fi

if [[ "$ASSUME_YES" != true ]]; then
  if [[ ! -t 0 ]]; then
    operator_failure confirmation_required \
      "refusing destructive action without a TTY; re-run with --yes"
  fi
  echo "This will purge and recreate $STREAM_NAME."
  read -r -p "Type '$STREAM_NAME' to continue: " confirm
  if [[ "$confirm" != "$STREAM_NAME" ]]; then
    operator_failure confirmation_failed "confirmation did not match the target stream"
  fi
fi

set +e
runtime_lock_global
lock_exit=$?
set -e
if [[ "$lock_exit" -ne 0 ]]; then
  if [[ "${RUNTIME_LOCK_ERROR_CODE:-}" == busy ]]; then
    operator_failure busy "another conflicting provenance operation is active" 75
  fi
  operator_failure lock_failed "cannot acquire the whole-store mutation lock" "$lock_exit"
fi

set +e
runtime_es_cli check-health >/dev/null
es_exit=$?
set -e
[[ "$es_exit" -eq 0 ]] || operator_failure elasticsearch_unavailable \
  "Elasticsearch health check failed" "$es_exit"

set +e
runtime_es_cli purge "$STREAM_NAME" >/dev/null && \
  runtime_es_cli init "$STREAM_NAME" >/dev/null
es_exit=$?
set -e
[[ "$es_exit" -eq 0 ]] || operator_failure elasticsearch_reinit_failed \
  "Elasticsearch purge or initialization failed" "$es_exit"

set +e
runtime_es_cli get-stream "$STREAM_NAME" >/dev/null
es_exit=$?
set -e
[[ "$es_exit" -eq 0 ]] || operator_failure elasticsearch_reinit_failed \
  "Elasticsearch did not accept the recreated stream" "$es_exit"

if [[ "$RESULT_FORMAT" == json ]]; then
  runtime_operator_emit_control_invocation es-reinit ok 0 reinit \
    "$target_json" null "" ""
else
  echo "[es-reinit] completed for $STREAM_NAME; published WETs were preserved"
fi
