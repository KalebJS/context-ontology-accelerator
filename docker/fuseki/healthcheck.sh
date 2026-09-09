#!/usr/bin/env bash
# Wait for the coa dataset then create it if missing (fuseki docker image
# supports config-based dataset creation; this is a fallback used by the
# compose healthcheck).
set -euo pipefail
for i in $(seq 1 60); do
  if curl -sf http://localhost:3030/$/ping >/dev/null 2>&1; then
    echo "fuseki up"
    exit 0
  fi
  sleep 2
done
echo "fuseki not ready" >&2
exit 1
