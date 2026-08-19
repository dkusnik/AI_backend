#!/usr/bin/env bash
# warc2wet.sh — extract text from WARCs to runtime WET outputs.
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
ROCKSDB_DIR=""
RESULT_FILE=""
PUBLICATION_REPORT=""
cleanup() {
  if [[ -n "$ROCKSDB_DIR" ]]; then
    rm -rf -- "$ROCKSDB_DIR"
  fi
  [[ -z "$RESULT_FILE" ]] || rm -f -- "$RESULT_FILE"
  [[ -z "$PUBLICATION_REPORT" ]] || rm -f -- "$PUBLICATION_REPORT"
  runtime_unlock_pair
  runtime_leave_script_dir
}
trap cleanup EXIT

PIPELINE_LIB="$APP_DIR/lib/scripts/pipeline-lib"
if [[ ! -f "$PIPELINE_LIB" ]]; then
  echo "Error: pipeline-lib not found" >&2
  exit 1
fi
# shellcheck source=/dev/null
source "$PIPELINE_LIB"

WET_DIR="$RUNTIME_DIR/wet"

usage() {
  cat <<'EOF'
Usage: warc2wet.sh [options] <input> [input ...]

Extract one or more WARC files into WET output under <runtime>/wet.
Files and directories may be mixed; directories are scanned recursively.

Options:
  --per-day                write one WET file per record-level crawl date
  --url-id=<id>            required provenance URL identifier
  --crawl-id=<id>          required provenance crawl identifier
  --result-format=<format> json (default) or human
  -h, --help               show this help

Single input: <runtime>/wet/<url-id>/<crawl-id>/<source>.wet.gz
Multiple inputs: <runtime>/wet/<url-id>/<crawl-id>/<first-source>-<N>files.wet.gz
Per-day inserts -<YYYYMMDD> before .wet.gz.
EOF
}

# ── argument parsing ──────────────────────────────────────────────────────────
MODE="single"
URL_ID=""
CRAWL_ID=""
URL_ID_SET=false
CRAWL_ID_SET=false
RESULT_FORMAT="json"
POSITIONAL=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --per-day)
      MODE="per-day"
      shift
      ;;
    --url-id=*)
      URL_ID="${1#*=}"
      URL_ID_SET=true
      shift
      ;;
    --url-id)
      [[ $# -ge 2 ]] || { echo "Error: --url-id requires a value" >&2; exit 1; }
      URL_ID="${2:-}"
      URL_ID_SET=true
      shift 2
      ;;
    --crawl-id=*)
      CRAWL_ID="${1#*=}"
      CRAWL_ID_SET=true
      shift
      ;;
    --crawl-id)
      [[ $# -ge 2 ]] || { echo "Error: --crawl-id requires a value" >&2; exit 1; }
      CRAWL_ID="${2:-}"
      CRAWL_ID_SET=true
      shift 2
      ;;
    --result-format=*)
      RESULT_FORMAT="${1#*=}"
      shift
      ;;
    --result-format)
      [[ $# -ge 2 ]] || { echo "Error: --result-format requires a value" >&2; exit 1; }
      RESULT_FORMAT="${2:-}"
      shift 2
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
      POSITIONAL+=("$1")
      shift
      ;;
  esac
done

case "$RESULT_FORMAT" in
  human|json) ;;
  *) echo "Error: --result-format must be human or json" >&2; exit 1 ;;
esac

[[ "$URL_ID_SET" == true ]] || { echo "Error: --url-id is required" >&2; exit 1; }
[[ "$CRAWL_ID_SET" == true ]] || { echo "Error: --crawl-id is required" >&2; exit 1; }
runtime_validate_identifier "--url-id" "$URL_ID"
runtime_validate_identifier "--crawl-id" "$CRAWL_ID"
[[ ${#POSITIONAL[@]} -gt 0 ]] || { echo "Error: at least one WARC input is required" >&2; exit 1; }

# ── resolve inputs: accept files and directories ─────────────────────────────
all_inputs=()
declare -A seen_inputs=()
inputs_json='[]'
for input_index in "${!POSITIONAL[@]}"; do
  arg="${POSITIONAL[$input_index]}"
  [[ "$arg" != /* ]] && arg="$ORIG_PWD/$arg"
  if [[ ! -e "$arg" && ! -L "$arg" ]]; then
    echo "Error: input not found: $arg" >&2
    exit 1
  fi
  if ! IFS= read -r -d '' canonical_root < <(realpath -ze -- "$arg"); then
    echo "Error: cannot resolve input root: $arg" >&2
    exit 1
  fi
  dir_files=()
  kept_files=()
  runtime_find_data_files warc "$arg" dir_files || exit 1
  for f in "${dir_files[@]}"; do
    if [[ -z "${seen_inputs["$f"]+x}" ]]; then
      seen_inputs["$f"]=1
      all_inputs+=("$f")
      kept_files+=("$f")
    fi
  done
  if [[ "$RESULT_FORMAT" == "json" && ${#kept_files[@]} -gt 0 ]]; then
    root_inputs_json="$(runtime_operator_inputs_json "$canonical_root" "$input_index" \
      "${kept_files[@]}")" || exit 1
    inputs_json="$(jq -cn --argjson left "$inputs_json" --argjson right "$root_inputs_json" \
      '$left + $right')" || exit 1
  fi
done

if [[ ${#all_inputs[@]} -eq 0 ]]; then
  echo "Error: no WARC input files found" >&2
  exit 1
fi

OUTPUT_DIR="$WET_DIR/$URL_ID/$CRAWL_ID"
first_input_name="$(basename "${all_inputs[0]}")"
case "$first_input_name" in
  *.warc.gz) output_stem="${first_input_name%.warc.gz}" ;;
  *.warc) output_stem="${first_input_name%.warc}" ;;
  *)
    echo "Error: cannot derive WET name from input: ${all_inputs[0]}" >&2
    exit 1
    ;;
esac
if [[ ${#all_inputs[@]} -gt 1 ]]; then
  output_stem="${output_stem}-${#all_inputs[@]}files"
fi
[[ -n "$output_stem" ]] || {
  echo "Error: cannot derive a non-empty WET output name from ${all_inputs[0]}" >&2
  exit 1
}
OUTPUT_NAME="$output_stem.wet.gz"

# ── helpers ───────────────────────────────────────────────────────────────────
warc_first_record_stamp() {
  local f="$1"
  local date_str=""
  case "$f" in
    *.warc.gz)  date_str=$(zcat   "$f" 2>/dev/null | head -c 4096 | grep -m1 "^WARC-Date:" | tr -d '\r') || true ;;
    *.warc)     date_str=$(head -c 4096 "$f"         | grep -m1 "^WARC-Date:" | tr -d '\r') || true ;;
  esac

  if [[ "$date_str" =~ ([0-9]{4})-([0-9]{2})-([0-9]{2})[Tt[:space:]]([0-9]{2}):([0-9]{2}):([0-9]{2}) ]]; then
    echo "${BASH_REMATCH[1]}${BASH_REMATCH[2]}${BASH_REMATCH[3]}-${BASH_REMATCH[4]}${BASH_REMATCH[5]}${BASH_REMATCH[6]}"
    return
  fi
  if [[ "$date_str" =~ ([0-9]{4})-([0-9]{2})-([0-9]{2}) ]]; then
    echo "${BASH_REMATCH[1]}${BASH_REMATCH[2]}${BASH_REMATCH[3]}-000000"
    return
  fi

  # Fallback to an explicit timestamp/date in the filename.
  local base; base="$(basename "$f")"
  if [[ "$base" =~ ([0-9]{8})[-_]?([0-9]{6}) ]]; then
    echo "${BASH_REMATCH[1]}-${BASH_REMATCH[2]}"
    return
  fi
  if [[ "$base" =~ ([0-9]{8}) ]]; then
    echo "${BASH_REMATCH[1]}-$(date -r "$f" +%H%M%S)"
    return
  fi
  echo "Error: cannot determine crawl date from WARC-Date or filename: $f" >&2
  return 1
}

run_extraction() {
  operator_log "  Input:  ${#all_inputs[@]} file(s)"
  operator_log "  Output: $OUTPUT_DIR"
  (
    run_pipeline warc2wet \
    --brief \
    --processor.doet-accumulator.rocksdb-path="$ROCKSDB_DIR" \
    "${output_args[@]}" \
    --url-id="$URL_ID" \
    --crawl-id="$CRAWL_ID" \
    "${all_inputs[@]}" \
    "${result_args[@]}"
  )
}

# Keep B1-006's pre-Java rejection until C2-001 moves complete record-level
# date validation into Java. These shell probes do not define per-day grouping.
for f in "${all_inputs[@]}"; do
  warc_first_record_stamp "$f" >/dev/null || exit 1
done

set +e
runtime_lock_pair "$URL_ID" "$CRAWL_ID"
lock_exit=$?
set -e
if [[ "$lock_exit" -ne 0 ]]; then
  [[ "$lock_exit" -eq 75 ]] && exit 75
  echo "Error: cannot acquire safe provenance locks" >&2
  exit 1
fi

mkdir -p "$OUTPUT_DIR"
mkdir -p "$APP_DIR/var/db"
ROCKSDB_DIR="$(mktemp -d "$APP_DIR/var/db/doet.XXXXXX")"
PUBLICATION_REPORT="$(mktemp "${TMPDIR:-/tmp}/warc2es-publication.XXXXXX")"

operator_log() {
  if [[ "$RESULT_FORMAT" == "json" ]]; then
    echo "$*" >&2
  else
    echo "$*"
  fi
}

output_args=()
if [[ "$MODE" == "single" ]]; then
  output_args+=(
    --output.file="$OUTPUT_DIR/$OUTPUT_NAME"
    --consumer.codec.output-format=wet
    --consumer.codec.cdx-sidecar=false
  )
else
  # C2-001 owns record-level date bucketing. This shell already supplies the
  # one-process, multi-output call shape and a stable pair-scoped output root.
  output_args+=(
    --output.file="$OUTPUT_DIR"
    --consumer.codec.output-format=multi-warc
    --consumer.codec.cdx-sidecar=false
    "--consumer.codec.output-name-template=$output_stem-{source}.wet.gz"
    --processor.doet-accumulator.bucket-prefix=
    --processor.doet-accumulator.per-day=true
  )
fi
output_args+=(--consumer.codec.publication-report="$PUBLICATION_REPORT")
# A derived source target is the identity of this staging batch. Java still
# publishes through sibling temporaries, so replacing it cannot expose a
# partially written WET.
output_args+=(--force)

result_args=()
[[ "$RESULT_FORMAT" == "json" ]] && result_args+=(--result-format=json)

operator_log "[warc2wet] $MODE — ${#all_inputs[@]} file(s) → $OUTPUT_DIR"
processing_json="null"
operator_status="ok"
operator_exit=0
error_code=""
error_message=""

if [[ "$RESULT_FORMAT" == "json" ]]; then
  RESULT_FILE="$(mktemp "${TMPDIR:-/tmp}/warc2es-processing.XXXXXX")"
  set +e
  run_extraction >"$RESULT_FILE"
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
  set +e
  run_extraction
  operator_exit=$?
  set -e
  exit "$operator_exit"
fi

output_paths=()
published_output_paths=()
output_stats_json="null"
publication_report_json=""
publication_report_invalid=false
if [[ -s "$PUBLICATION_REPORT" ]]; then
  if ! publication_report_json="$(jq -cse '
      if length == 1 and
         (.[0] | type == "object" and
          .schema == "warc2es.output-publication/v1" and
          (.status == "published" or .status == "partial" or
           .status == "error" or .status == "discarded") and
          (.planned | type == "number") and .planned >= 0 and (.planned | floor) == .planned and
          (.published | type == "array") and all(.published[]; type == "string") and
          (.published | length) <= .planned and
          (if .status == "published" then (.published | length) == .planned
           elif .status == "partial" then (.published | length) > 0 and (.published | length) < .planned
           else (.published | length) == 0 end))
      then .[0]
      else error("invalid publication report")
      end
    ' "$PUBLICATION_REPORT" 2>/dev/null)"; then
    publication_report_invalid=true
  fi
fi

if [[ "$publication_report_invalid" == true ]]; then
  operator_status="error"
  operator_exit=1
  error_code="publication_report_invalid"
  error_message="Java output publication report is invalid"
elif [[ -n "$publication_report_json" ]]; then
  publication_status="$(jq -r '.status' <<<"$publication_report_json")"
  if { [[ "$publication_status" == "published" ]] && [[ "$java_exit" -ne 0 ]]; } ||
     { [[ "$publication_status" != "published" ]] && [[ "$java_exit" -eq 0 ]]; }; then
    publication_report_invalid=true
  fi
  while IFS= read -r published_path; do
    [[ "$publication_report_invalid" == false ]] || break
    [[ -n "$published_path" ]] || continue
    if [[ "$published_path" != /* ]] || ! canonical_output="$(realpath -e -- "$published_path")"; then
      publication_report_invalid=true
      break
    fi
    canonical_output_dir="$(realpath -e -- "$OUTPUT_DIR")"
    output_name="$(basename "$canonical_output")"
    if [[ "$(dirname "$canonical_output")" != "$canonical_output_dir" ]] ||
       { [[ "$MODE" == "single" ]] && [[ "$output_name" != "$OUTPUT_NAME" ]]; } ||
       { [[ "$MODE" == "per-day" ]] &&
         { output_suffix="${output_name#"$output_stem-"}";
           [[ "$output_suffix" == "$output_name" || ! "$output_suffix" =~ ^[0-9]{8}\.wet\.gz$ ]]; }; }; then
      publication_report_invalid=true
      break
    fi
    output_paths+=("wet/$URL_ID/$CRAWL_ID/$output_name")
    published_output_paths+=("$canonical_output")
  done < <(jq -r '.published[]' <<<"$publication_report_json")

  if [[ "$publication_report_invalid" == false ]] &&
     jq -e 'has("output_stats")' <<<"$publication_report_json" >/dev/null; then
    if ! jq -e '
        def uint: type == "number" and . >= 0 and floor == .;
        def histogram:
          type == "object" and
          all(to_entries[]; (.key | type == "string") and (.value | uint));
        def stats:
          type == "object" and
          (.count | uint) and (.content_bytes | uint) and
          (.mime_types | histogram) and (.languages | histogram) and
          (.missing_language | uint) and (.missing_mimetype | uint) and
          ((.date_min == null) or (.date_min | type == "string")) and
          ((.date_max == null) or (.date_max | type == "string"));
        .output_stats as $stats |
        ($stats | stats) and
        ($stats.artifacts | type == "array") and
        all($stats.artifacts[]; (.path | type == "string") and stats) and
        ([$stats.artifacts[].path] == .published) and
        ($stats.count == ([$stats.artifacts[].count] | add // 0)) and
        ($stats.content_bytes == ([$stats.artifacts[].content_bytes] | add // 0)) and
        ($stats.missing_language == ([$stats.artifacts[].missing_language] | add // 0)) and
        ($stats.missing_mimetype == ([$stats.artifacts[].missing_mimetype] | add // 0))
      ' <<<"$publication_report_json" >/dev/null; then
      publication_report_invalid=true
    else
      compressed_sizes=()
      for published_output in "${published_output_paths[@]}"; do
        if ! compressed_size="$(stat -c '%s' -- "$published_output")"; then
          publication_report_invalid=true
          break
        fi
        compressed_sizes+=("$compressed_size")
      done

      if [[ "$publication_report_invalid" == false ]]; then
        public_paths_json="$(runtime_operator_paths_json "${output_paths[@]}")"
        compressed_sizes_json="$(jq -cn '$ARGS.positional | map(tonumber)' \
          --args "${compressed_sizes[@]}")"
        java_output_stats="$(jq -c '.output_stats' <<<"$publication_report_json")"
        output_stats_json="$(jq -cn \
          --argjson source "$java_output_stats" \
          --argjson paths "$public_paths_json" \
          --argjson sizes "$compressed_sizes_json" '
          {
            count:$source.count,
            compressed_bytes:($sizes | add // 0),
            content_bytes:$source.content_bytes,
            mime_types:$source.mime_types,
            languages:$source.languages,
            missing_language:$source.missing_language,
            missing_mimetype:$source.missing_mimetype,
            date_min:$source.date_min,
            date_max:$source.date_max,
            artifacts:[
              range(0; ($source.artifacts | length)) as $index |
              $source.artifacts[$index] | {
                path:$paths[$index],
                count,
                compressed_bytes:$sizes[$index],
                content_bytes,
                mime_types,
                languages,
                missing_language,
                missing_mimetype,
                date_min,
                date_max
              }
            ]
          }')"
      fi
    fi
  fi

  if [[ "$publication_report_invalid" == true ]]; then
    output_paths=()
    output_stats_json="null"
    operator_status="error"
    operator_exit=1
    error_code="publication_report_invalid"
    error_message="Java output publication report contains invalid output metadata"
  elif [[ "$publication_status" == "partial" ]]; then
    operator_status="partial"
  fi
elif [[ "$operator_exit" -eq 0 ]]; then
  # Compatibility for test doubles that predate the private publication report.
  if [[ "$MODE" == "single" ]]; then
    if [[ -f "$OUTPUT_DIR/$OUTPUT_NAME" ]]; then
      output_paths+=("wet/$URL_ID/$CRAWL_ID/$OUTPUT_NAME")
    else
      operator_status="error"
      operator_exit=1
      error_code="internal_error"
      error_message="Java reported success without producing the WET output"
    fi
  else
    produced_files=()
    if ! runtime_find_data_files wet "$OUTPUT_DIR" produced_files; then
      operator_status="error"
      operator_exit=1
      error_code="internal_error"
      error_message="Failed to enumerate produced WET files"
    else
      for output in "${produced_files[@]}"; do
        output_name="$(basename "$output")"
        output_suffix="${output_name#"$output_stem-"}"
        [[ "$output_suffix" != "$output_name" && "$output_suffix" =~ ^[0-9]{8}\.wet\.gz$ ]] || continue
        output_paths+=("${output#"$RUNTIME_DIR"/}")
      done
    fi
  fi
fi

outputs_json="$(runtime_operator_paths_json "${output_paths[@]}")"
runtime_operator_emit_invocation warc2wet "$operator_status" "$operator_exit" extract \
  "$inputs_json" "$outputs_json" null "$processing_json" "$error_code" "$error_message" \
  "$output_stats_json"
exit "$operator_exit"
