#!/usr/bin/env bash
# COA Docker stack — smoke E2E: waits for health, then drives the core flows
# through the gateway (Keycloak token → auth → namespace → source scan →
# review/approve → induce → accept → Tier-2 query → MCP).
set -euo pipefail

cd "$(dirname "$0")/../.."

GATEWAY="${GATEWAY_URL:-http://localhost:9090}"
KC_PUBLIC="${KC_ISSUER_PUBLIC:-http://localhost:8280/realms/coa}"
E2E_USERNAME="${E2E_USERNAME:-admin@coa.local}"
E2E_PASSWORD="${E2E_PASSWORD:-Passw0rd!}"

say() { echo "[e2e] $*"; }
fail() { echo "[e2e] FAIL: $*" >&2; exit 1; }

# ── 1. Wait for core services ────────────────────────────────────────────
say "waiting for services…"
for url in \
  "http://localhost:${LOCALSTACK_PORT:-8888}/_localstack/health" \
  "${GATEWAY}/health" \
  "http://localhost:${OPENSEARCH_PORT:-9200}" \
  "http://localhost:${FUSEKI_PORT:-3030}/$/ping" \
  "http://localhost:${VKG_PORT:-8180}/health"; do
  if ! curl -sf "$url" > /dev/null; then
    fail "service not healthy: $url (is the stack up? docker compose ps)"
  fi
done
say "all core services healthy"

# ── 2. Keycloak token mint ───────────────────────────────────────────────
say "minting token from Keycloak…"
TOKEN_RESP=$(curl -sf "${KC_PUBLIC}/protocol/openid-connect/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "grant_type=password" \
  --data-urlencode "client_id=${KC_WEB_CLIENT_ID:-coa-web}" \
  --data-urlencode "username=${E2E_USERNAME}" \
  --data-urlencode "password=${E2E_PASSWORD}" \
  --data-urlencode "scope=openid")
ACCESS_TOKEN=$(echo "$TOKEN_RESP" | python3 -c "import sys,json;print(json.load(sys.stdin).get('access_token',''))")
[ -n "$ACCESS_TOKEN" ] || fail "no access_token in Keycloak response: ${TOKEN_RESP:0:200}"
say "token minted"

AUTH="Authorization: Bearer ${ACCESS_TOKEN}"

# ── 3. Authenticated gateway call (list namespaces) ──────────────────────
say "GET /namespaces via gateway…"
NS_RESP=$(curl -sf "${GATEWAY}/namespaces" -H "$AUTH") || fail "gateway /namespaces failed"
say "namespaces: ${NS_RESP:0:120}"

# ── 4. Create a namespace (reuses an existing one if the script reruns) ───
say "POST /namespaces…"
CREATE_RESP=$(curl -s -X POST "${GATEWAY}/namespaces" \
  -H "$AUTH" -H "Content-Type: application/json" \
  -d '{"name":"e2e-demo-ns","displayName":"E2E Demo","description":"Docker E2E namespace","owner":"admin@coa.local"}')
HTTP_CODE=$(echo "$CREATE_RESP" | tail -c 4)
if echo "$CREATE_RESP" | grep -q "already exists"; then
  say "namespace exists — resolving it"
  NS_ID=$(curl -sf "${GATEWAY}/namespaces" -H "$AUTH" | python3 -c "import sys,json;d=json.load(sys.stdin);print([n['namespaceId'] for n in d.get('namespaces',[]) if n.get('name')=='e2e-demo-ns'][0])")
else
  NS_ID=$(echo "$CREATE_RESP" | python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('namespace',{}).get('namespaceId') or d.get('namespaceId',''))" 2>/dev/null || true)
fi
[ -n "$NS_ID" ] || fail "namespace create failed: ${CREATE_RESP:0:300}"
say "namespace resolved: ${NS_ID}"

# ── 5. Register a DATABASE source (the demo Postgres) ────────────────────
# Rerun-safe: reuse a COMPLETED source named demo-postgres; otherwise create
# one, suffixing the name if a stale (non-completed) row already holds it —
# a stale row was created before the ssl opt-out existed and cannot be
# repaired in place (host/port/engine/secret are immutable by design).
say "resolving demo-postgres source…"
SRC_NAME="demo-postgres"
EXISTING=$(curl -sf "${GATEWAY}/namespaces/${NS_ID}/sources?maxResults=50" -H "$AUTH" || echo '{"items":[]}')
SRC_ID=$(echo "$EXISTING" | python3 -c "
import sys, json
items = json.load(sys.stdin).get('items', [])
# Database sources terminate at APPROVED (or stay PENDING_REVIEW awaiting
# review); either can be reused — approve/idempotent-accept below handle both.
usable = [i for i in items if i.get('name', '').startswith('demo-postgres') and i.get('status') in ('COMPLETED', 'APPROVED')]
stale = [i for i in items if i.get('name', '').startswith('demo-postgres') and i.get('status') not in ('COMPLETED', 'APPROVED')]
print(usable[0]['sourceId'] if usable else ('__CREATE__:' + stale[0]['sourceId'] if stale else '__CREATE__'))
")
if [[ "$SRC_ID" != __CREATE__* ]]; then
  say "reusing usable source: ${SRC_ID}"
else
  [[ "$SRC_ID" == "__CREATE__:"* ]] && SRC_NAME="demo-postgres-${RANDOM}"
  say "POST /namespaces/{id}/sources (JDBC demo Postgres as ${SRC_NAME})…"
  SRC_RESP=$(curl -sf -X POST "${GATEWAY}/namespaces/${NS_ID}/sources" \
    -H "$AUTH" -H "Content-Type: application/json" \
    -d "{
    \"name\": \"${SRC_NAME}\",
    \"sourceType\": \"DATABASE\",
    \"databaseSource\": {
      \"name\": \"${SRC_NAME}\",
      \"jdbcConfiguration\": {
        \"engine\": \"POSTGRESQL\",
        \"host\": \"postgres\",
        \"port\": 5432,
        \"databaseName\": \"coa_demo\",
        \"credentialSecretArn\": \"arn:aws:secretsmanager:us-east-1:000000000000:secret:coa/local/demo-postgres-QPqONL\",
        \"options\": { \"ssl\": false }
      },
      \"metadataEnrichmentEnabled\": true
    }
  }")
  SRC_ID=$(echo "$SRC_RESP" | python3 -c "import sys,json;print(json.load(sys.stdin).get('sourceId',''))" 2>/dev/null || true)
  [ -n "$SRC_ID" ] || fail "source create failed: ${SRC_RESP:0:300}"
  say "source created: ${SRC_ID}"
fi

# ── 6. Wait for the initial scan to reach terminal state ─────────────────
# Source creation itself enqueues the first scan (SCAN_QUEUE_URL set in the
# local stack), so no separate rescan call is needed. PENDING_REVIEW is the
# expected post-scan state (enrichment done, awaiting steward review).
say "waiting for scan to reach PENDING_REVIEW (scan worker: discovery→federation→enrichment)…"
# An already-APPROVED source (previous run) has no new scan to wait for.
STATUS=""
for i in $(seq 1 60); do
  STATUS=$(curl -sf "${GATEWAY}/namespaces/${NS_ID}/sources/${SRC_ID}" -H "$AUTH" | python3 -c "import sys,json;print(json.load(sys.stdin).get('status',''))" 2>/dev/null || echo "")
  say "  scan status: ${STATUS} ($i/60)"
  case "$STATUS" in
    PENDING_REVIEW|APPROVED|COMPLETED) break ;;
    SCAN_FAILED|FAILED) fail "scan failed — check scan-workers logs: docker compose logs scan-workers" ;;
  esac
  sleep 5
done
case "$STATUS" in PENDING_REVIEW|APPROVED|COMPLETED) ;; *) fail "scan did not complete (last status: ${STATUS})" ;; esac

# ── 7. Bulk approve the source (steward sign-off gates induction) ────────
# Model phase requires the source in APPROVED status and its tables approved;
# bulk approve is the API that flips both (worker: cascade → APPROVED).
say "POST /namespaces/{id}/sources/{id}/approve…"
APPROVE_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
  "${GATEWAY}/namespaces/${NS_ID}/sources/${SRC_ID}/approve" -H "$AUTH")
case "$APPROVE_CODE" in
  202|409) say "approve accepted (${APPROVE_CODE})" ;;
  *) fail "source approve failed (HTTP ${APPROVE_CODE})" ;;
esac

say "waiting for source to reach APPROVED…"
STATUS=""
for i in $(seq 1 30); do
  STATUS=$(curl -sf "${GATEWAY}/namespaces/${NS_ID}/sources/${SRC_ID}" -H "$AUTH" \
    | python3 -c "import sys,json;print(json.load(sys.stdin).get('status',''))" 2>/dev/null || echo "")
  case "$STATUS" in
    APPROVED) break ;;
    APPROVAL_FAILED) fail "bulk approve failed — check scan-workers logs" ;;
  esac
  sleep 2
done
[ "$STATUS" = "APPROVED" ] || fail "source never reached APPROVED (last status: ${STATUS})"
say "source APPROVED"

# ── 8. Ontology induction (Model phase) ──────────────────────────────────
# Induce from the approved source's catalog, then accept the proposal — accept
# runs the ingest pipeline that writes the class embeddings the CM's Tier-2
# retriever searches ({OSS_INDEX}-{namespace} in OpenSearch).
say "POST /namespaces/{id}/induce (workbench datasource induction)…"
ONTO_PREFIX="http://localhost/ontologies/e2e-demo"
INDUCE_RESP=$(curl -sf -X POST "${GATEWAY}/namespaces/${NS_ID}/induce" \
  -H "$AUTH" -H "Content-Type: application/json" \
  -d "{
    \"datasourceIds\": [\"${SRC_ID}\"],
    \"ontologyUriPrefix\": \"${ONTO_PREFIX}\",
    \"label\": \"E2E Demo Ontology\",
    \"groundingMode\": \"NONE\"
  }")
JOB_ID=$(echo "$INDUCE_RESP" | python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('jobId') or d.get('job_id',''))" 2>/dev/null || true)
[ -n "$JOB_ID" ] || fail "induction start failed: ${INDUCE_RESP:0:300}"

say "waiting for induction job ${JOB_ID}…"
JOB_STATUS=""
for i in $(seq 1 60); do
  JOB_STATUS=$(curl -sf "${GATEWAY}/namespaces/${NS_ID}/induce/jobs/${JOB_ID}" -H "$AUTH" \
    | python3 -c "import sys,json;print(json.load(sys.stdin).get('status',''))" 2>/dev/null || echo "")
  case "$JOB_STATUS" in
    COMPLETED|completed) break ;;
    FAILED|failed) fail "induction failed — check ontology-engine logs" ;;
  esac
  sleep 3
done
case "$JOB_STATUS" in COMPLETED|completed) ;; *) fail "induction never completed (last status: ${JOB_STATUS})" ;; esac
say "induction COMPLETED"

say "resolving proposal and accepting (writes ontology embeddings)…"
# Rerun-safe: induction dedups onto a structurally identical proposal, so the
# one already-accepted proposal may be all we find. Accepting an accepted
# proposal is idempotent (ingest steps are no-ops/overwrites) — 409 is fine.
PROPOSAL_ID=$(curl -sf "${GATEWAY}/namespaces/${NS_ID}/proposals" -H "$AUTH" \
  | python3 -c "
import sys, json
items = json.load(sys.stdin)
if isinstance(items, dict):
    items = items.get('items') or items.get('proposals') or []
for pref in ('pending', 'updated'):
    cand = [p for p in items if p.get('status') == pref and p.get('proposal_id')]
    if cand:
        print(cand[0]['proposal_id']); break
else:
    cand = [p for p in items if p.get('status') == 'accepted' and p.get('proposal_id')]
    print(cand[0]['proposal_id'] if cand else '')" 2>/dev/null || true)
[ -n "$PROPOSAL_ID" ] || fail "no pending proposal found after induction"
ACCEPT_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
  "${GATEWAY}/namespaces/${NS_ID}/proposals/${PROPOSAL_ID}/accept" -H "$AUTH" -H "Content-Type: application/json" -d '{}')
case "$ACCEPT_CODE" in
  202|200|409) say "accept kicked off (${ACCEPT_CODE})" ;;
  *) fail "proposal accept failed (HTTP ${ACCEPT_CODE})" ;;
esac

say "waiting for proposal ${PROPOSAL_ID} to reach accepted…"
P_STATUS=""
for i in $(seq 1 60); do
  P_STATUS=$(curl -sf "${GATEWAY}/namespaces/${NS_ID}/proposals/${PROPOSAL_ID}" -H "$AUTH" \
    | python3 -c "import sys,json;d=json.load(sys.stdin);p=d if isinstance(d,dict) and 'status' in d and 'proposal_id' in d else {};print(p.get('status',''))" 2>/dev/null || echo "")
  case "$P_STATUS" in
    accepted) break ;;
    accept_failed) fail "proposal accept failed — check ontology-engine logs" ;;
  esac
  sleep 3
done
[ "$P_STATUS" = "accepted" ] || fail "proposal never reached accepted (last status: ${P_STATUS})"
say "proposal accepted — ontology embeddings live"

# ── 9. Query via the gateway (Tier-2 NL→SQL over the induced ontology) ───
# The gateway is the only surface carrying a resolved authorizer context
# (JWT→Cedar, mirroring API Gateway); hitting the data-layer container
# directly would 403 with an empty caller profile.
say "POST /namespaces/{id}/query (gateway → data-layer → CM → Tier 2 JDBC)…"
Q_RESP=$(curl -s -X POST "${GATEWAY}/namespaces/${NS_ID}/query" \
  -H "$AUTH" -H "Content-Type: application/json" \
  -d '{"query":"How many customers are in the US-EAST region?"}')
say "query response: ${Q_RESP:0:400}"

# Tier-2 failure mode is an empty-context CM reply (trace tier3, no SQL) —
# a row-shaped result (resultRows/rows) proves the ontology retrieval + JDBC
# execution worked.
if echo "$Q_RESP" | grep -qE '"(resultRows|rows)"'; then
  say "query returned row data (Tier-2 JDBC execution confirmed)"
else
  fail "query response has no row data (Tier-2 likely fell through): ${Q_RESP:0:400}"
fi

# ── 10. MCP server reachable ─────────────────────────────────────────────
# FastMCP (streamable-http) exposes /mcp, not /ping — do the same JSON-RPC
# initialize handshake the container healthcheck uses.
say "checking MCP server…"
MCP_INIT=$(curl -s -X POST "http://localhost:${MCP_PORT:-8002}/mcp" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"coa-e2e","version":"0"}}}')
echo "$MCP_INIT" | grep -q '"serverInfo"' || fail "mcp-server initialize failed: ${MCP_INIT:0:300}"
say "MCP healthy"

say "FULL E2E PASSED (scan → approve → induce → accept → Tier-2 query → MCP)"
