#!/usr/bin/env bash
# @name: config-load-external-001
# @group: smoke
# @level: L0
# @timeout: 10s
# @keywords: configuration, fast
# @runs: 1

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Look for PROJECT_ROOT by finding pom.xml
FIND_ROOT="$SCRIPT_DIR"
while [[ "$FIND_ROOT" != "/" && ! -f "$FIND_ROOT/pom.xml" ]]; do
    FIND_ROOT="$(dirname "$FIND_ROOT")"
done
PROJECT_ROOT="$FIND_ROOT"
CLI="${DIST_ROOT:-$PROJECT_ROOT/target/dist}/bin/warc-cli"
SHARED="$PROJECT_ROOT/shared"
[[ ! -e "$SHARED/example.com.warc.gz" && ! -e "$SHARED/bench-500m.warc.gz" ]] && SHARED="$PROJECT_ROOT/../shared"
INPUT="$SHARED/tiny.warc.gz"
[[ -f "$INPUT" ]] || INPUT="$SHARED/example.com.warc.gz"
# Test config loading via default config resolution.
if "$CLI" info "$INPUT" --dry-run --verbose 2>&1 | grep -qiE "Config: .*/config.yaml|EFFECTIVE CONFIG"; then
    echo "[config-load-external-001] OK"
    exit 0
else
    echo "[config-load-external-001] NOK: Expected config to be loaded"
    exit 1
fi
