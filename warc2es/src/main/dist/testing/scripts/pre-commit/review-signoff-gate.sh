#!/usr/bin/env bash
set -euo pipefail

fail() {
  echo "[FAIL] $*" >&2
  exit 1
}

ROOT="$(cd "$(dirname "$0")" && pwd)"
while [[ "$ROOT" != "/" && ! -f "$ROOT/pom.xml" ]]; do
  ROOT="$(dirname "$ROOT")"
done
[[ -f "$ROOT/pom.xml" ]] || fail "Could not locate repository root (pom.xml)"
CLAUDE_REVIEW_FILE="${CLAUDE_REVIEW_FILE:-$ROOT/docs/reviews/claude_signoff.md}"
CODEX_REVIEW_FILE="${CODEX_REVIEW_FILE:-$ROOT/docs/reviews/codex_signoff.md}"

require_nonempty_field() {
  local who="$1"
  local file="$2"
  local field="$3"
  grep -qiP "^${field}:\\s*\\S" "$file" || fail "$who sign-off missing '${field}:' value in $file"
}

require_signoff() {
  local who="$1"
  local file="$2"

  [[ -f "$file" ]] || fail "$who sign-off file missing: $file"
  require_nonempty_field "$who" "$file" "Date"
  require_nonempty_field "$who" "$file" "Commit"
  require_nonempty_field "$who" "$file" "Scope"
  require_nonempty_field "$who" "$file" "Findings"

  grep -qiP "^Date:\\s*[0-9]{4}-[0-9]{2}-[0-9]{2}\\s*$" "$file" || fail "$who sign-off Date must match YYYY-MM-DD in $file"
  grep -qiP "^Commit:\\s*[0-9a-f]{7,40}\\s*$" "$file" || fail "$who sign-off Commit must be git hash (7-40 hex) in $file"
  grep -qiP "^Scope:\\s*(<|$|files/modules\\s*$)" "$file" && fail "$who sign-off contains placeholder Scope in $file"
  grep -qiP "^Findings:\\s*(<|none\\|summary\\s*$)" "$file" && fail "$who sign-off contains placeholder Findings in $file"
  grep -qiP "^Sign-off:\\s*APPROVED\\s*$" "$file" || fail "$who sign-off missing exact 'Sign-off: APPROVED' in $file"
}

require_signoff "Claude 4.5" "$CLAUDE_REVIEW_FILE"
require_signoff "Codex" "$CODEX_REVIEW_FILE"

echo "[PASS] Review sign-off gate passed"
