#!/usr/bin/env bash
# @timeout: 600
# tc-e2e-002-python-search.sh - cross-project WARC to Python search test
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WARC2ES_ROOT="$(cd "$SCRIPT_DIR/../../../../../../.." && pwd)"
PYTHON_SEARCH_TEST="$WARC2ES_ROOT/python-search/tests/test-warc2es-pipeline.sh"

[[ -f "$PYTHON_SEARCH_TEST" ]] || {
    echo "Cross-project test not found: $PYTHON_SEARCH_TEST" >&2
    exit 1
}

bash "$PYTHON_SEARCH_TEST" "$@"
