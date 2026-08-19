#!/usr/bin/env bash
# out/.profile — ES connection settings for warc2es scripts
#
# Sourced by: es-cli, es-reinit.sh, es-delete.sh, es-upsert.sh
# Edit this file for your deployment. Never commit passwords.
#
# puppet deployments: set ELASTIC_PASSWORD in the host environment;
# this file maps it to ES_PASS automatically.

export ES_URL="${ES_URL:-http://localhost:9200}"
export ES_USER="${ES_USER:-elastic}"
# Map ELASTIC_PASSWORD (set by puppet) → ES_PASS (used by es-cli)
export ES_PASS="${ES_PASS:-${ELASTIC_PASSWORD:-}}"
export KIBANA_URL="${KIBANA_URL:-http://localhost:5601}"
export ES_GUI_URL="${ES_GUI_URL:-$ES_URL}"
