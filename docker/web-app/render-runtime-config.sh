#!/bin/bash
# Render runtime-config.json from environment at container start.
set -euo pipefail

: "${WEB_API_ENDPOINT:=http://localhost:9090}"
: "${KC_ISSUER_PUBLIC:=http://localhost:8280/realms/coa}"
: "${KC_WEB_CLIENT_ID:=coa-web}"
: "${CM_PORT:=8081}"
: "${VERSION:=0.0.0}"

# queryEndpointOverride: same-origin /invocations — nginx proxies it to the
# context-manager container (SSE streams pass through unbuffered).
cat > /usr/share/nginx/html/runtime-config.json <<EOF
{
  "region": "us-east-1",
  "authority": "${KC_ISSUER_PUBLIC}",
  "clientId": "${KC_WEB_CLIENT_ID}",
  "apiEndpoint": "${WEB_API_ENDPOINT}",
  "queryEndpointOverride": "/invocations",
  "version": "${VERSION}"
}
EOF
echo "[web-app] runtime-config.json rendered (apiEndpoint=${WEB_API_ENDPOINT})"
