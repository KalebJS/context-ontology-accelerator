#!/usr/bin/env bash
# Run the Playwright E2E suite (on the host) against the local Docker stack.
set -euo pipefail

cd "$(dirname "$0")/../.."

export E2E_BASE_URL="${E2E_BASE_URL:-http://localhost:3000}"
export E2E_USERNAME="${E2E_USERNAME:-admin@coa.local}"
export E2E_PASSWORD="${E2E_PASSWORD:-Passw0rd!}"
export E2E_IDP="${E2E_IDP:-keycloak}"

cd packages/web-app
pnpm exec playwright test "$@"
