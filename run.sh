#!/usr/bin/env bash
set -euo pipefail
SOURCE=${1:-"s3://REPLACE_ME_BUCKET/logs"}
EXPERIMENT=${2:-"ex-python"}
if command -v uv >/dev/null 2>&1; then
  uv run ${EXPERIMENT}/main.py "$SOURCE"
else
  python -m ${EXPERIMENT}.main "$SOURCE"
fi
