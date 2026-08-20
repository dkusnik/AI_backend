#!/usr/bin/env bash
# Operator-workflow helpers for the five top-level runtime scripts.
#
# This library owns runtime layout discovery, operator-relative path resolution,
# deployment profile loading, stream naming, identifier helpers, and es-cli
# invocation. JVM classpath/config/options and the final Java exec belong in
# pipeline-lib; keep that boundary even where the two layers need similar small
# shell helpers.

: "${ORIG_PWD:=$PWD}"

runtime_die() {
  # es-cli's die() is command-local and intentionally keeps its existing text.
  echo "Error: $1" >&2
  exit 1
}

runtime_enter_script_dir() {
  pushd "$1" > /dev/null
}

runtime_leave_script_dir() {
  popd > /dev/null || true
}

runtime_resolve_layout() {
  local script_dir="$1"

  if [[ -d "$script_dir/app/bin" || -d "$script_dir/app/lib/scripts" ]]; then
    APP_DIR="$script_dir/app"
    RUNTIME_DIR="$script_dir"
  elif [[ -f "$script_dir/pipeline-lib" || -x "$script_dir/../../bin/warc-cli" || -x "$script_dir/../../bin/es-cli" ]]; then
    APP_DIR="$(cd "$script_dir/../.." && pwd)"
    if [[ "$(basename "$APP_DIR")" == "app" ]]; then
      RUNTIME_DIR="$(dirname "$APP_DIR")"
    else
      RUNTIME_DIR="$APP_DIR"
    fi
  elif [[ -d "$script_dir/lib/scripts" ]]; then
    APP_DIR="$script_dir"
    RUNTIME_DIR="$script_dir"
  else
    runtime_die "cannot resolve runtime layout from $script_dir"
  fi
}

runtime_source_profile() {
  # shellcheck source=/dev/null
  if [[ -f "$RUNTIME_DIR/.profile" ]]; then
    source "$RUNTIME_DIR/.profile"
  fi
}

runtime_es_cli() {
  ES_URL="$ES_URL" ES_USER="${ES_USER:-}" ES_PASS="${ES_PASS:-}" "$ES_CLI" "$@"
}

runtime_resolve_path() {
  # bin/{warc-cli,es-cli} resolve_path() preserve non-existent relative paths;
  # this operator helper canonicalizes through an existing parent directory.
  local path="$1"
  if [[ "$path" != /* ]]; then
    path="$ORIG_PWD/$path"
  fi
  if [[ -d "$path" ]]; then
    (cd "$path" && pwd)
  else
    local dir
    dir="$(cd "$(dirname "$path")" && pwd)"
    printf '%s/%s\n' "$dir" "$(basename "$path")"
  fi
}

_runtime_data_file_supported() {
  local kind="$1"
  local path="$2"
  case "$kind:$path" in
    wet:*.wet.gz|warc:*.warc|warc:*.warc.gz) return 0 ;;
    *) return 1 ;;
  esac
}

_runtime_path_is_utf8() {
  printf '%s' "$1" | iconv -f UTF-8 -t UTF-8 >/dev/null 2>&1
}

runtime_find_data_files() {
  local kind="$1"
  local root="$2"
  local output_name="${3:-}"
  local canonical_root canonical candidate relative candidate_file sort_file
  local -a candidates=()
  local -a canonical_files=()
  local -a relative_files=()
  local -a sorted_relative=()
  local -a sorted_files=()
  local -A seen=()

  case "$kind" in
    wet|warc) ;;
    *)
      echo "Error: unsupported data-file kind: $kind" >&2
      return 1
      ;;
  esac

  if [[ ! -e "$root" && ! -L "$root" ]]; then
    echo "Error: input path not found: $root" >&2
    return 1
  fi

  if [[ -f "$root" ]]; then
    if ! _runtime_data_file_supported "$kind" "$root"; then
      echo "Error: unsupported $kind input file: $root" >&2
      return 1
    fi
    if ! IFS= read -r -d '' canonical < <(realpath -ze -- "$root"); then
      echo "Error: cannot resolve input file: $root" >&2
      return 1
    fi
    if ! _runtime_data_file_supported "$kind" "$canonical"; then
      echo "Error: resolved $kind input has an unsupported extension: $root" >&2
      return 1
    fi
    relative="${canonical##*/}"
    if ! _runtime_path_is_utf8 "$relative"; then
      printf 'Error: supported artifact path is not valid UTF-8: %q\n' "$root" >&2
      return 1
    fi
    canonical_files=("$canonical")
  elif [[ -d "$root" ]]; then
    if ! IFS= read -r -d '' canonical_root < <(realpath -ze -- "$root"); then
      echo "Error: cannot resolve input directory: $root" >&2
      return 1
    fi

    candidate_file="$(mktemp "${TMPDIR:-/tmp}/warc2es-find.XXXXXX")" || return 1
    case "$kind" in
      wet)
        if ! find -L "$canonical_root" -mindepth 1 -name '*.wet.gz' -print0 >"$candidate_file"; then
          rm -f -- "$candidate_file"
          echo "Error: failed to traverse input directory: $root" >&2
          return 1
        fi
        ;;
      warc)
        if ! find -L "$canonical_root" -mindepth 1 \( -name '*.warc' -o -name '*.warc.gz' \) \
            -print0 >"$candidate_file"; then
          rm -f -- "$candidate_file"
          echo "Error: failed to traverse input directory: $root" >&2
          return 1
        fi
        ;;
    esac
    mapfile -d '' -t candidates <"$candidate_file"
    rm -f -- "$candidate_file"

    for candidate in "${candidates[@]}"; do
      if ! IFS= read -r -d '' canonical < <(realpath -ze -- "$candidate"); then
        printf 'Error: supported artifact cannot be resolved: %q\n' "$candidate" >&2
        return 1
      fi
      if [[ ! -f "$canonical" ]]; then
        printf 'Error: supported artifact is not a regular file: %q\n' "$candidate" >&2
        return 1
      fi
      if [[ "$canonical_root" != / && "$canonical" != "$canonical_root"/* ]]; then
        printf 'Error: supported artifact escapes its positional root: %q\n' "$candidate" >&2
        return 1
      fi
      relative="${canonical#"$canonical_root"/}"
      [[ "$canonical_root" == / ]] && relative="${canonical#/}"
      if ! _runtime_data_file_supported "$kind" "$relative"; then
        printf 'Error: resolved artifact has an unsupported extension: %q\n' "$candidate" >&2
        return 1
      fi
      if ! _runtime_path_is_utf8 "$relative"; then
        printf 'Error: supported artifact path is not valid UTF-8: %q\n' "$candidate" >&2
        return 1
      fi
      if [[ -z "${seen["$canonical"]+x}" ]]; then
        seen["$canonical"]=1
        relative_files+=("$relative")
      fi
    done
  else
    echo "Error: input path is not a regular file or directory: $root" >&2
    return 1
  fi

  if [[ ${#relative_files[@]} -gt 0 ]]; then
    sort_file="$(mktemp "${TMPDIR:-/tmp}/warc2es-sort.XXXXXX")" || return 1
    if ! printf '%s\0' "${relative_files[@]}" >"$sort_file" ||
       ! LC_ALL=C sort -zu -o "$sort_file" "$sort_file"; then
      rm -f -- "$sort_file"
      echo "Error: failed to order discovered artifacts: $root" >&2
      return 1
    fi
    mapfile -d '' -t sorted_relative <"$sort_file"
    rm -f -- "$sort_file"
    for relative in "${sorted_relative[@]}"; do
      if [[ "$canonical_root" == / ]]; then
        sorted_files+=("/$relative")
      else
        sorted_files+=("$canonical_root/$relative")
      fi
    done
  elif [[ ${#canonical_files[@]} -gt 0 ]]; then
    sorted_files=("${canonical_files[@]}")
  fi

  if [[ -n "$output_name" ]]; then
    local -n output_ref="$output_name"
    output_ref=("${sorted_files[@]}")
  elif [[ ${#sorted_files[@]} -gt 0 ]]; then
    printf '%s\0' "${sorted_files[@]}"
  fi
}

stream_name() {
  local id="$1"
  if [[ -z "$id" ]]; then
    printf 'nac-data-default\n'
  elif [[ "$id" == "nac-data" || "$id" == nac-data-* ]]; then
    printf '%s\n' "$id"
  else
    printf 'nac-data-%s\n' "$id"
  fi
}

strip_data_extension() {
  local base
  base="$(basename "$1")"
  for suffix in .wet.gz .wet.zst .wet.lz4 .wet.xz .wet .doet.gz .doet.zst .doet.lz4 .doet.xz .doet; do
    if [[ "$base" == *"$suffix" ]]; then
      printf '%s\n' "${base%"$suffix"}"
      return
    fi
  done
  printf '%s\n' "$base"
}

normalize_segment() {
  local raw="$1"
  printf '%s' "$raw" \
    | tr '[:upper:]' '[:lower:]' \
    | sed -E 's/[^a-z0-9]+/_/g; s/^_+//; s/_+$//; s/_+/_/g'
}

runtime_validate_identifier() {
  local option="$1"
  local value="$2"
  runtime_identifier_is_valid "$value" || \
    runtime_die "$option must match [A-Za-z0-9._-]{1,128} and must not be . or .."
}

runtime_identifier_is_valid() {
  local value="$1"
  [[ "$value" =~ ^[A-Za-z0-9._-]{1,128}$ && "$value" != "." && "$value" != ".." ]]
}

runtime_stream_name_is_safe() {
  local value="$1"
  [[ "$value" =~ ^nac-data(-[a-z0-9][a-z0-9._-]{0,245})?$ ]]
}

runtime_sha256_file() {
  local file="$1"
  local digest checksum_record

  # --zero disables GNU sha256sum's filename escaping. Parsing its ordinary
  # newline format breaks as soon as a valid path contains a backslash/newline.
  if ! IFS= read -r -d '' checksum_record < <(sha256sum --zero -- "$file"); then
    echo "Error: cannot hash WET input: $file" >&2
    return 1
  fi
  digest="${checksum_record:0:64}"
  [[ "${checksum_record:64:2}" == "  " && "$digest" =~ ^[0-9a-f]{64}$ ]] || {
    echo "Error: sha256sum returned an invalid digest for: $file" >&2
    return 1
  }
  printf '%s\n' "$digest"
}

runtime_validate_wet_provenance() {
  local file="$1"
  local expected_url_id="$2"
  local expected_crawl_id="$3"

  # An explicit zero-byte WET is the frozen zero-record transaction. Non-empty
  # files are parsed by compressed-byte record boundaries; line searches can
  # mistake payload text for WARC headers and are deliberately not used here.
  [[ -s "$file" ]] || return 0
  command -v gzip >/dev/null 2>&1 || {
    echo "Error: gzip is required to validate WET input" >&2
    return 1
  }
  command -v perl >/dev/null 2>&1 || {
    echo "Error: perl is required to validate WET record boundaries" >&2
    return 1
  }

  gzip -cd -- "$file" | perl -e '
    use strict;
    use warnings;

    my ($expected_url, $expected_crawl, $path) = @ARGV;

    sub invalid {
      my ($message) = @_;
      die "Error: invalid WET provenance in $path: $message\n";
    }

    sub header_line {
      my $line = <STDIN>;
      return undef unless defined $line;
      invalid("header line exceeds 1 MiB") if length($line) > 1024 * 1024;
      invalid("unterminated header line") unless $line =~ s/\n\z//;
      $line =~ s/\r\z//;
      return $line;
    }

    while (1) {
      my $line = header_line();
      while (defined($line) && $line eq "") {
        $line = header_line();
      }
      last unless defined $line;
      invalid("expected WARC/1.0 or WARC/1.1 record start")
        unless $line eq "WARC/1.0" || $line eq "WARC/1.1";

      my %headers;
      while (1) {
        $line = header_line();
        invalid("unexpected EOF in record headers") unless defined $line;
        last if $line eq "";
        invalid("malformed header") unless $line =~ /^([^:]+):(.*)$/s;
        my ($name, $value) = (lc($1), $2);
        $name =~ s/^\s+|\s+$//g;
        $value =~ s/^\s+|\s+$//g;
        if ($name eq "content-length" ||
            $name eq "warc-type" ||
            $name eq "x-nac-url-id" ||
            $name eq "x-nac-crawl-id") {
          invalid("duplicate $name header") if exists $headers{$name};
          $headers{$name} = $value;
        }
      }

      invalid("missing Content-Length") unless exists $headers{"content-length"};
      invalid("invalid Content-Length")
        unless $headers{"content-length"} =~ /^\d+$/;

      my $is_warcinfo = lc($headers{"warc-type"} // "") eq "warcinfo";
      unless ($is_warcinfo) {
        invalid("missing X-NAC-URL-ID") unless exists $headers{"x-nac-url-id"};
        invalid("missing X-NAC-Crawl-ID") unless exists $headers{"x-nac-crawl-id"};
        invalid("X-NAC-URL-ID does not match --url-id")
          unless $headers{"x-nac-url-id"} eq $expected_url;
        invalid("X-NAC-Crawl-ID does not match --crawl-id")
          unless $headers{"x-nac-crawl-id"} eq $expected_crawl;
      }

      my $remaining = 0 + $headers{"content-length"};
      invalid("WARCINFO payload exceeds 1 MiB")
        if $is_warcinfo && $remaining > 1024 * 1024;
      my $warcinfo_payload = "";
      while ($remaining > 0) {
        my $chunk = "";
        my $wanted = $remaining > 1024 * 1024 ? 1024 * 1024 : $remaining;
        my $read = read(STDIN, $chunk, $wanted);
        invalid("cannot read record payload") unless defined $read;
        invalid("unexpected EOF in record payload") if $read == 0;
        $warcinfo_payload .= $chunk if $is_warcinfo;
        $remaining -= $read;
      }

      if ($is_warcinfo) {
        my %fields;
        for my $field (split /\r?\n/, $warcinfo_payload) {
          next if $field eq "";
          invalid("malformed WARCINFO field") unless $field =~ /^([^:]+):(.*)$/s;
          my ($name, $value) = (lc($1), $2);
          $name =~ s/^\s+|\s+$//g;
          $value =~ s/^\s+|\s+$//g;
          next unless $name eq "x-nac-url-id" || $name eq "x-nac-crawl-id";
          invalid("duplicate WARCINFO $name field") if exists $fields{$name};
          $fields{$name} = $value;
        }
        invalid("missing X-NAC-URL-ID in WARCINFO")
          unless exists $fields{"x-nac-url-id"};
        invalid("missing X-NAC-Crawl-ID in WARCINFO")
          unless exists $fields{"x-nac-crawl-id"};
        invalid("WARCINFO X-NAC-URL-ID does not match --url-id")
          unless $fields{"x-nac-url-id"} eq $expected_url;
        invalid("WARCINFO X-NAC-Crawl-ID does not match --crawl-id")
          unless $fields{"x-nac-crawl-id"} eq $expected_crawl;
      }
    }
  ' -- "$expected_url_id" "$expected_crawl_id" "$file"
}

_runtime_prepare_lock_directory() {
  local directory="$1"
  local canonical parent

  if [[ -L "$directory" ]]; then
    echo "Error: unsafe lock directory symlink: $directory" >&2
    return 1
  fi
  if [[ ! -e "$directory" ]]; then
    if mkdir -- "$directory" 2>/dev/null; then
      parent="$(dirname "$directory")"
      sync -f "$parent" || return 1
    elif [[ ! -d "$directory" || -L "$directory" ]]; then
      echo "Error: cannot create safe lock directory: $directory" >&2
      return 1
    fi
  fi
  if [[ ! -d "$directory" || -L "$directory" ]]; then
    echo "Error: unsafe lock directory: $directory" >&2
    return 1
  fi
  canonical="$(realpath -e -- "$directory")" || return 1
  if [[ "$canonical" != "$directory" ]]; then
    echo "Error: lock directory does not resolve canonically: $directory" >&2
    return 1
  fi
}

_runtime_verify_open_lock() {
  local path="$1"
  local descriptor="$2"
  local path_identity descriptor_identity descriptor_metadata

  _runtime_validate_existing_lock_file "$path" || return 1
  if [[ ! -f "/proc/$$/fd/$descriptor" ]]; then
    echo "Error: open lock is not a regular file: $path" >&2
    return 1
  fi
  descriptor_metadata="$(stat -Lc '%h:%u' -- "/proc/$$/fd/$descriptor")" || return 1
  if [[ "$descriptor_metadata" != "1:$EUID" ]]; then
    echo "Error: open lock has unsafe ownership or link count: $path" >&2
    return 1
  fi

  path_identity="$(stat -Lc '%d:%i' -- "$path")" || return 1
  descriptor_identity="$(stat -Lc '%d:%i' -- "/proc/$$/fd/$descriptor")" || return 1
  if [[ "$path_identity" != "$descriptor_identity" ]]; then
    echo "Error: lock file changed while opening: $path" >&2
    return 1
  fi
}

_runtime_validate_existing_lock_file() {
  local path="$1"
  local canonical metadata

  if [[ -L "$path" || ! -f "$path" ]]; then
    echo "Error: unsafe lock file: $path" >&2
    return 1
  fi
  canonical="$(realpath -e -- "$path")" || return 1
  if [[ "$canonical" != "$path" ]]; then
    echo "Error: lock file does not resolve canonically: $path" >&2
    return 1
  fi
  metadata="$(stat -Lc '%h:%u' -- "$path")" || return 1
  if [[ "$metadata" != "1:$EUID" ]]; then
    echo "Error: lock file has unsafe ownership or link count: $path" >&2
    return 1
  fi
}

runtime_validate_lock_targets() {
  local scope="$1"
  local url_id="${2:-}"
  local crawl_id="${3:-}"
  local lock_root="$RUNTIME_DIR/var/locks/warc2es"
  local global_path="$lock_root/global.lock"
  local directory path canonical
  local -a directories=(
    "$RUNTIME_DIR/var"
    "$RUNTIME_DIR/var/locks"
    "$lock_root"
  )
  local -a lock_files=("$global_path")

  RUNTIME_LOCK_ERROR_CODE="lock_unsafe"
  if [[ "$scope" == pair ]]; then
    directories+=("$lock_root/pairs" "$lock_root/pairs/$url_id")
    lock_files+=("$lock_root/pairs/$url_id/$crawl_id.lock")
  fi

  for directory in "${directories[@]}"; do
    if [[ -L "$directory" ]]; then
      echo "Error: unsafe lock directory symlink: $directory" >&2
      return 1
    fi
    [[ -e "$directory" ]] || continue
    if [[ ! -d "$directory" ]]; then
      echo "Error: unsafe lock directory: $directory" >&2
      return 1
    fi
    canonical="$(realpath -e -- "$directory")" || return 1
    if [[ "$canonical" != "$directory" ]]; then
      echo "Error: lock directory does not resolve canonically: $directory" >&2
      return 1
    fi
  done

  for path in "${lock_files[@]}"; do
    [[ -e "$path" || -L "$path" ]] || continue
    _runtime_validate_existing_lock_file "$path" || return 1
  done
}

runtime_unlock_pair() {
  if [[ -n "${RUNTIME_PAIR_LOCK_FD:-}" ]]; then
    exec {RUNTIME_PAIR_LOCK_FD}>&-
    RUNTIME_PAIR_LOCK_FD=""
  fi
  runtime_unlock_global
}

runtime_unlock_global() {
  if [[ -n "${RUNTIME_GLOBAL_LOCK_FD:-}" ]]; then
    exec {RUNTIME_GLOBAL_LOCK_FD}>&-
    RUNTIME_GLOBAL_LOCK_FD=""
  fi
}

runtime_lock_global() {
  local lock_root="$RUNTIME_DIR/var/locks/warc2es"
  local global_path="$lock_root/global.lock"
  local directory

  RUNTIME_LOCK_ERROR_CODE="lock_unsafe"
  RUNTIME_GLOBAL_LOCK_FD=""
  command -v flock >/dev/null 2>&1 || {
    echo "Error: flock is required for provenance coordination" >&2
    return 1
  }

  for directory in \
    "$RUNTIME_DIR/var" \
    "$RUNTIME_DIR/var/locks" \
    "$lock_root"; do
    _runtime_prepare_lock_directory "$directory" || {
      runtime_unlock_global
      return 1
    }
  done

  if [[ -L "$global_path" ]]; then
    echo "Error: unsafe lock file symlink: $global_path" >&2
    return 1
  fi
  exec {RUNTIME_GLOBAL_LOCK_FD}>>"$global_path" || return 1
  _runtime_verify_open_lock "$global_path" "$RUNTIME_GLOBAL_LOCK_FD" || {
    runtime_unlock_global
    return 1
  }
  if ! flock -n -x "$RUNTIME_GLOBAL_LOCK_FD"; then
    RUNTIME_LOCK_ERROR_CODE="busy"
    echo "Error: busy: another whole-store or provenance operation is active" >&2
    runtime_unlock_global
    return 75
  fi
}

runtime_lock_pair() {
  local url_id="$1"
  local crawl_id="$2"
  local lock_root="$RUNTIME_DIR/var/locks/warc2es"
  local global_path="$lock_root/global.lock"
  local pair_path="$lock_root/pairs/$url_id/$crawl_id.lock"
  local directory

  RUNTIME_LOCK_ERROR_CODE="lock_unsafe"
  RUNTIME_GLOBAL_LOCK_FD=""
  RUNTIME_PAIR_LOCK_FD=""
  command -v flock >/dev/null 2>&1 || {
    echo "Error: flock is required for provenance coordination" >&2
    return 1
  }

  for directory in \
    "$RUNTIME_DIR/var" \
    "$RUNTIME_DIR/var/locks" \
    "$lock_root" \
    "$lock_root/pairs" \
    "$lock_root/pairs/$url_id"; do
    _runtime_prepare_lock_directory "$directory" || {
      runtime_unlock_pair
      return 1
    }
  done

  if [[ -L "$global_path" ]]; then
    echo "Error: unsafe lock file symlink: $global_path" >&2
    return 1
  fi
  exec {RUNTIME_GLOBAL_LOCK_FD}>>"$global_path" || return 1
  _runtime_verify_open_lock "$global_path" "$RUNTIME_GLOBAL_LOCK_FD" || {
    runtime_unlock_pair
    return 1
  }
  if ! flock -n -s "$RUNTIME_GLOBAL_LOCK_FD"; then
    RUNTIME_LOCK_ERROR_CODE="busy"
    echo "Error: busy: whole-store operation holds the global lock" >&2
    runtime_unlock_pair
    return 75
  fi

  if [[ -L "$pair_path" ]]; then
    echo "Error: unsafe lock file symlink: $pair_path" >&2
    runtime_unlock_pair
    return 1
  fi
  exec {RUNTIME_PAIR_LOCK_FD}>>"$pair_path" || {
    runtime_unlock_pair
    return 1
  }
  _runtime_verify_open_lock "$pair_path" "$RUNTIME_PAIR_LOCK_FD" || {
    runtime_unlock_pair
    return 1
  }
  if ! flock -n -x "$RUNTIME_PAIR_LOCK_FD"; then
    RUNTIME_LOCK_ERROR_CODE="busy"
    echo "Error: busy: provenance pair $url_id/$crawl_id is already being mutated" >&2
    runtime_unlock_pair
    return 75
  fi
}

FILE_DATE=""
FILE_URL_ID=""
FILE_CRAWL_ID=""

parse_file_metadata() {
  local file="$1"
  local base
  base="$(strip_data_extension "$file")"

  FILE_DATE=""
  FILE_URL_ID=""
  FILE_CRAWL_ID=""

  if [[ "$base" =~ ^([0-9]{8})-([0-9]{6})-ingest(-([a-z0-9_]+)(-([a-z0-9_]+))?)?$ ]]; then
    FILE_DATE="${BASH_REMATCH[1]}"
    FILE_URL_ID="${BASH_REMATCH[4]:-}"
    FILE_CRAWL_ID="${BASH_REMATCH[6]:-}"
    return 0
  fi

  if [[ "$base" =~ ^([0-9]{8}) ]]; then
    FILE_DATE="${BASH_REMATCH[1]}"
    return 0
  fi

  return 1
}

runtime_json_escape() {
  # es-cli has a command-local equivalent; this name keeps operator workflows
  # from depending on the admin CLI implementation.
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  value="${value//$'\n'/\\n}"
  value="${value//$'\r'/\\r}"
  value="${value//$'\t'/\\t}"
  printf '%s' "$value"
}

runtime_json_path() {
  local root="$1"
  local path="$2"
  local relative

  if [[ -d "$root" ]]; then
    relative="${path#"${root%/}/"}"
  else
    relative="$(basename "$root")"
  fi
  if ! _runtime_path_is_utf8 "$relative"; then
    printf 'Error: invalid_utf8 input path: %q\n' "$path" >&2
    return 1
  fi
  printf '%s\n' "$relative"
}

runtime_operator_inputs_json() {
  local root="$1"
  local input_index="$2"
  shift 2
  local path
  local -a relative_paths=()

  command -v jq >/dev/null 2>&1 || {
    echo "Error: jq is required for JSON result output" >&2
    return 1
  }
  for path in "$@"; do
    relative_paths+=("$(runtime_json_path "$root" "$path")") || return 1
  done
  jq -cn --argjson input_index "$input_index" --args \
    '$ARGS.positional | map({input_index: $input_index, path: .})' \
    "${relative_paths[@]}"
}

runtime_operator_paths_json() {
  command -v jq >/dev/null 2>&1 || {
    echo "Error: jq is required for JSON result output" >&2
    return 1
  }
  jq -cn --args '$ARGS.positional' "$@"
}

runtime_operator_publication_json() {
  local status="$1"
  local paths_json="$2"
  jq -cn --arg status "$status" --argjson paths "$paths_json" \
    '{status: $status, paths: $paths}'
}

runtime_operator_processing_sentinel() {
  local discriminator="$1"
  local process_exit="$2"
  jq -cn --arg discriminator "$discriminator" --argjson exit_code "$process_exit" \
    '{schema:"warc2es.processing/v1",status:"error",exit_code:$exit_code}
     + {($discriminator):true}'
}

runtime_operator_validate_processing() {
  local result_file="$1"
  local process_exit="$2"
  local parsed reported_status reported_exit first_byte final_bytes

  RUNTIME_OPERATOR_STATUS="error"
  RUNTIME_OPERATOR_EXIT_CODE="$process_exit"
  [[ "$RUNTIME_OPERATOR_EXIT_CODE" -ne 0 ]] || RUNTIME_OPERATOR_EXIT_CODE=1
  RUNTIME_OPERATOR_ERROR_CODE=""
  RUNTIME_OPERATOR_ERROR_MESSAGE=""

  if [[ ! -s "$result_file" ]]; then
    RUNTIME_OPERATOR_PROCESSING_JSON="$(runtime_operator_processing_sentinel result_missing "$process_exit")"
    RUNTIME_OPERATOR_ERROR_CODE="processing_result_missing"
    RUNTIME_OPERATOR_ERROR_MESSAGE="Java processing result is missing"
    return 0
  fi

  first_byte="$(head -c 1 "$result_file")"
  final_bytes="$(tail -c 2 "$result_file" | od -An -tx1 -v | tr -d ' \n')"
  if [[ "$first_byte" != "{" || "$final_bytes" != "7d0a" ]] ||
     ! parsed="$(jq -cse '
       if length == 1 and
          (.[0] | type == "object" and
           .schema == "warc2es.processing/v1" and
           (.status == "ok" or .status == "dry_run" or .status == "error") and
           has("exit_code") and (.exit_code | type == "number") and
           .exit_code >= 0 and ((.exit_code | floor) == .exit_code) and
           has("records_in") and has("records_out") and has("records_indexed") and
           has("records_skipped") and has("errors") and has("elapsed_ms") and
           has("error") and has("metrics"))
       then .[0]
       else error("invalid processing result")
       end
     ' "$result_file" 2>/dev/null)"; then
    RUNTIME_OPERATOR_PROCESSING_JSON="$(runtime_operator_processing_sentinel result_invalid "$process_exit")"
    RUNTIME_OPERATOR_ERROR_CODE="processing_result_invalid"
    RUNTIME_OPERATOR_ERROR_MESSAGE="Java processing result is invalid"
    return 0
  fi

  RUNTIME_OPERATOR_PROCESSING_JSON="$parsed"
  reported_status="$(jq -r '.status' <<<"$parsed")"
  reported_exit="$(jq -r '.exit_code' <<<"$parsed")"

  if [[ "$reported_exit" -ne "$process_exit" ]] ||
     { [[ "$process_exit" -eq 0 ]] && [[ "$reported_status" != "ok" && "$reported_status" != "dry_run" ]]; } ||
     { [[ "$process_exit" -ne 0 ]] && [[ "$reported_status" != "error" ]]; }; then
    RUNTIME_OPERATOR_ERROR_CODE="processing_protocol_error"
    RUNTIME_OPERATOR_ERROR_MESSAGE="Java processing status disagrees with its process exit"
    return 0
  fi

  if [[ "$process_exit" -eq 0 ]]; then
    RUNTIME_OPERATOR_STATUS="$reported_status"
    RUNTIME_OPERATOR_EXIT_CODE=0
  else
    RUNTIME_OPERATOR_ERROR_CODE="processing_failed"
    RUNTIME_OPERATOR_ERROR_MESSAGE="Java processing failed"
  fi
}

runtime_operator_emit_invocation() {
  local command="$1"
  local status="$2"
  local exit_code="$3"
  local mode="$4"
  local inputs_json="$5"
  local outputs_json="$6"
  local publication_json="$7"
  local processing_json="$8"
  local error_code="$9"
  local error_message="${10}"
  local output_stats_json="${11:-null}"

  jq -cn \
    --arg command "$command" \
    --arg status "$status" \
    --argjson exit_code "$exit_code" \
    --arg mode "$mode" \
    --argjson inputs "$inputs_json" \
    --argjson outputs "$outputs_json" \
    --argjson publication "$publication_json" \
    --argjson processing "$processing_json" \
    --argjson output_stats "$output_stats_json" \
    --arg error_code "$error_code" \
    --arg error_message "$error_message" \
    '{schema:"warc2es.operator/v1",kind:"invocation",command:$command,
      status:$status,exit_code:$exit_code,mode:$mode,inputs:$inputs,outputs:$outputs,
      publication:$publication,processing:$processing,
      error:(if $error_code == "" then null else {code:$error_code,message:$error_message} end)}
     | if $output_stats == null then . else . + {output_stats:$output_stats} end'
}

runtime_operator_emit_control_invocation() {
  local command="$1"
  local status="$2"
  local exit_code="$3"
  local mode="$4"
  local target_json="$5"
  local publication_json="$6"
  local error_code="$7"
  local error_message="$8"

  jq -cn \
    --arg command "$command" \
    --arg status "$status" \
    --argjson exit_code "$exit_code" \
    --arg mode "$mode" \
    --argjson target "$target_json" \
    --argjson publication "$publication_json" \
    --arg error_code "$error_code" \
    --arg error_message "$error_message" \
    '{schema:"warc2es.operator/v1",kind:"invocation",command:$command,
      status:$status,exit_code:$exit_code,mode:$mode,inputs:[],outputs:[],
      target:$target,publication:$publication,processing:null,
      error:(if $error_code == "" then null else {code:$error_code,message:$error_message} end)}'
}

runtime_operator_emit_summary() {
  local command="$1"
  local total="$2"
  local succeeded="$3"
  local failed="$4"
  local status="ok"

  RUNTIME_OPERATOR_SUMMARY_EXIT_CODE=0
  if [[ "$failed" -gt 0 ]]; then
    RUNTIME_OPERATOR_SUMMARY_EXIT_CODE=1
    if [[ "$succeeded" -gt 0 ]]; then
      status="partial"
    else
      status="error"
    fi
  fi
  jq -cn --arg command "$command" --arg status "$status" \
    --argjson exit_code "$RUNTIME_OPERATOR_SUMMARY_EXIT_CODE" \
    --argjson total "$total" --argjson succeeded "$succeeded" --argjson failed "$failed" \
    '{schema:"warc2es.operator/v1",kind:"summary",command:$command,status:$status,
      exit_code:$exit_code,total:$total,succeeded:$succeeded,failed:$failed,error:null}'
}
