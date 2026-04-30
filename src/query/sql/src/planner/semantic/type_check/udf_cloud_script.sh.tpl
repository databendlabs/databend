#!/usr/bin/env bash
set -euo pipefail
WORKDIR=/app
mkdir -p "$WORKDIR"
cd "$WORKDIR"

{{PACKAGES_BLOCK}}
{{IMPORTS_BLOCK}}

cat <<'{{CODE_MARKER}}' > /app/udf.py
{{UDF_CODE}}
{{CODE_MARKER}}
cat <<'{{SERVER_MARKER}}' > /app/server.py
{{SERVER_STUB}}
{{SERVER_MARKER}}
exec python /app/server.py
