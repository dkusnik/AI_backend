#!/usr/bin/env bash
# wet-merge.sh — merge WET files from wet/ into DOET files in doet/
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
TEMP_DIRS=()
cleanup() {
  runtime_leave_script_dir
  for d in "${TEMP_DIRS[@]}"; do
    [[ -d "$d" ]] && rm -rf "$d"
  done
}
trap cleanup EXIT

CLI="$APP_DIR/bin/warc-cli"

usage() {
  cat <<'EOF'
Usage: wet-merge.sh [wet-dir] [doet-dir]

Merge dated WET files from wet-dir (default: <runtime>/wet) into consecutive
date-group DOET files in doet-dir (default: <runtime>/doet).

Options:
  -h, --help  show this help

Output: <YYYYMMDD>.doet.gz or <first-date>--<last-date>.doet.gz
EOF
}

POSITIONAL=()
while [[ $# -gt 0 ]]; do
  case "$1" in
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

WET_DIR="${POSITIONAL[0]:-$RUNTIME_DIR/wet}"
DOET_DIR="${POSITIONAL[1]:-$RUNTIME_DIR/doet}"

if [[ "$WET_DIR" != /* ]]; then
  WET_DIR="$ORIG_PWD/$WET_DIR"
fi
if [[ "$DOET_DIR" != /* ]]; then
  DOET_DIR="$ORIG_PWD/$DOET_DIR"
fi

if [[ ! -d "$WET_DIR" ]]; then
  echo "Error: WET directory not found: $WET_DIR" >&2
  exit 1
fi
WET_DIR="$(runtime_resolve_path "$WET_DIR")"
mkdir -p "$DOET_DIR"
DOET_DIR="$(runtime_resolve_path "$DOET_DIR")"

if [[ ! -x "$CLI" ]]; then
  echo "Error: $CLI missing or not executable" >&2; exit 1
fi

# ── collect dated WET files ───────────────────────────────────────────────────
# Only consider files whose names start with YYYYMMDD (produced by warc2wet --per-day)
mapfile -t wet_files < <(find "$WET_DIR" -maxdepth 1 -type f -name "????????.wet.gz" | LC_ALL=C sort)

if [[ ${#wet_files[@]} -eq 0 ]]; then
  echo "No dated WET files (YYYYMMDD.wet.gz) found in $WET_DIR" >&2; exit 0
fi

# Extract sorted unique dates
mapfile -t dates < <(
  for f in "${wet_files[@]}"; do
    basename "$f" | grep -oE '^[0-9]{8}'
  done | sort -u
)

# ── group consecutive calendar dates ─────────────────────────────────────────
next_day() { date -d "$1 +1 day" +%Y%m%d; }

groups=()       # each element: "date1 date2 ..." (space-separated)
group=("${dates[0]}")

for (( i=1; i<${#dates[@]}; i++ )); do
  if [[ "$(next_day "${group[-1]}")" == "${dates[$i]}" ]]; then
    group+=("${dates[$i]}")
  else
    groups+=("${group[*]}")
    group=("${dates[$i]}")
  fi
done
groups+=("${group[*]}")

# ── merge each group ──────────────────────────────────────────────────────────
echo "[wet-merge] ${#wet_files[@]} WET file(s) → ${#groups[@]} DOET group(s)"

for g in "${groups[@]}"; do
  read -r -a gd <<< "$g"
  first="${gd[0]}"
  last="${gd[-1]}"

  # Collect WET files for this date group
  inputs=()
  for d in "${gd[@]}"; do
    f="$WET_DIR/${d}.wet.gz"
    [[ -f "$f" ]] && inputs+=("$f")
  done

  if [[ ${#inputs[@]} -eq 0 ]]; then
    echo "  Warning: no files found for group $g, skipping" >&2; continue
  fi

  if [[ "$first" == "$last" ]]; then
    output="$DOET_DIR/${first}.doet.gz"
  else
    output="$DOET_DIR/${first}--${last}.doet.gz"
  fi

  echo "[wet-merge] $first..$last — ${#inputs[@]} file(s) → $output"
  db_dir="$(mktemp -d -t wet-merge-rocksdb-XXXXXX)"
  TEMP_DIRS+=("$db_dir")
  "$CLI" dedupe "${inputs[@]}" "$output" \
    --brief \
    --deduplicate-scope=global \
    --processor.doet-accumulator.rocksdb-path="$db_dir"
  rm -rf "$db_dir"
done
