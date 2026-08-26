# Context Manager

Serve layer orchestration service hosted on Bedrock AgentCore Runtime. Receives queries from consumer surfaces (Data Layer API Lambda, Playground WebSocket, MCP) and coordinates downstream services through tiered resolution.

## Endpoints

### POST /invocations

Standard AgentCore entrypoint for synchronous request/response.

### GET /ping

Health check (provided automatically by AgentCore SDK).

### WebSocket /ws

Bidirectional streaming endpoint for the Playground UI. Maintains a persistent connection for multiple request/response exchanges.

**Connection URLs:**

| Access Method | URL | Use Case |
|---------------|-----|----------|
| CloudFront (browser) | `wss://<CloudFrontDomain>/ws` | Playground UI in public deployment mode |
| ALB (VPC-internal) | `ws://<AlbDnsName>/ws` | Services within VPC or private deployment mode |
| Direct VPC endpoint | `wss://bedrock-agentcore.<region>.amazonaws.com/runtimes/<arn>/ws` | Backend services with VPC access |

> **Note:** Browser clients cannot access the VPC endpoint directly. Use the CloudFront or ALB route. Obtain `AlbDnsName` from the `coa-dev-serve` CDK stack output.

**Authentication:** JWT Access Token via `Authorization: Bearer <token>` header. See [infra/README.md](../../infra/README.md#serve-stack-coa-dev-serve) for auth configuration details. User identity (`sub` claim) scopes sessions — each user gets isolated conversation history per namespace. Unauthenticated connections can still send single-turn queries but won't have session persistence.

#### Request message

```json
{
  "query": "What is total revenue?",
  "namespace": "demo",
  "profile": {},
  "options": {},
  "requestId": "optional-caller-id",
  "sessionId": "optional-session-to-resume"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `query` | string (1–4000 chars) | yes | Natural language query |
| `namespace` | string (1–128 chars, `^[a-zA-Z0-9][a-zA-Z0-9_-]*$`) | yes | Target namespace |
| `profile` | object | no | User/session context |
| `options` | object | no | Resolution options (e.g., `tierOverride`) |
| `requestId` | string | no | Caller-provided correlation ID (auto-generated if omitted) |
| `sessionId` | string | no | Resume an existing session. If omitted, session is scoped to `user_id + namespace` automatically. |

#### Connected event

Sent after the first message when a session is created or resumed (requires `MEMORY_ID` configured):

```json
{
  "type": "connected",
  "sessionId": "user-uuid_namespace-id",
  "resumedTurns": 3
}
```

| Field | Type | Description |
|-------|------|-------------|
| `type` | `"connected"` | Event discriminator |
| `sessionId` | string | Session identifier (stable across reconnects for same user + namespace) |
| `resumedTurns` | integer | Number of prior conversation turns loaded from history (0 for new session) |

#### Success response

```json
{
  "result": {
    "tier": 0,
    "confidence": { "score": 0.95, "rationale": "..." },
    "synthesizedAnswer": "...",
    "trace": [
      { "step": "...", "status": "success", "durationMs": 42, "parallelGroup": "t3.retrieve", "wallMs": 790 }
    ],
    "metadata": { "totalMs": 1250 }
  },
  "requestId": "abc-123",
  "sessionId": "user-uuid_namespace-id"
}
```

#### Error responses

Errors are returned as JSON frames on the same connection (the connection stays open):

| Error | Status Code | Cause |
|-------|-------------|-------|
| `ParseError` | 400 | Message is not valid JSON |
| `ValidationError` | 400 | Missing/invalid fields (details included) |
| `AccessDenied` | 403 | Caller not authorized for the referenced table(s)/column(s) by the data-level grant profile (SQL firewall deny — terminal, no Tier-3 fallback). Body is generic; the denial reason is logged server-side only. |
| `TimeoutError` | 504 | Resolution exceeded 30s timeout |
| `InternalError` | 500 | Unexpected failure in orchestrator |

Example error frame:

```json
{
  "error": "ValidationError",
  "message": "Invalid request payload",
  "details": [{ "field": "namespace", "type": "missing" }],
  "requestId": "abc-123",
  "statusCode": 400
}
```

#### Connection lifecycle

- Connection remains open after each response — send multiple queries without reconnecting.
- Server never closes the connection due to errors (error frames are sent instead).
- Client closes the connection when done.
- **Multi-turn sessions:** When `MEMORY_ID` is configured, conversation history is persisted in AgentCore Memory (30-day retention). Reconnecting to the same namespace automatically resumes the session with prior context.

#### Example (Python)

```python
from bedrock_agentcore.runtime import AgentCoreRuntimeClient
import websockets, asyncio, json, os

async def main():
    client = AgentCoreRuntimeClient(region="eu-central-1")
    ws_url, headers = client.generate_ws_connection(
        runtime_arn=os.environ["AGENT_ARN"]
    )

    async with websockets.connect(ws_url, additional_headers=headers) as ws:
        await ws.send(json.dumps({"query": "What is revenue?", "namespace": "demo"}))
        response = json.loads(await ws.recv())
        print(response["result"]["synthesizedAnswer"])

asyncio.run(main())
```

## Query Routing

Inbound queries are dispatched through a tiered resolution system:

| Tier | Strategy | Relative Latency | Trigger |
|------|----------|------------------|---------|
| Tier 1 | Deterministic metric lookup | Fast | Exact metric name/synonym match, simple query |
| Tier 2 | Ontology-guided NL-to-SPARQL | Medium | Vector search finds ontology classes/properties |
| Tier 2.5 | Ontology-grounded direct LLM SQL generation | Medium | Tier-2 result confidence below `TIER2_CONFIDENCE_THRESHOLD` (default 0.6) |
| Tier 3 | LLM-powered synthesis | Slow | Complex, multi-metric, or ambiguous queries |

Routing is automatic based on complexity signals (compare, trend, top N, breakdown) and semantic matching scores. Use `tierOverride` in the request `options` field to force a specific tier (1, 2, 2.5, or 3). Tier-2.5-generated SQL passes through the same SQL firewall (terminal 403 on deny) and the same composite executor as Tiers 1 and 2.

### Tier-2 strategy selection (`options.strategy`)

A caller can pin how Tier 2 produces its query via the `strategy` field in request `options`. Setting it disables per-query pruning and honors the caller's intent:

| `options.strategy` | Behavior |
|--------------------|----------|
| `best` | Run all applicable strategies in parallel, return the highest-confidence result |
| `ontop` | Only NL→SPARQL (Ontop / VKG) |
| `nl_to_sql` | Only single-shot NL→SQL |
| `ontop_first` | Sequential: Ontop → NL→SQL fallback |
| `nl_to_sql_first` (**default**) | Sequential: NL→SQL → Ontop fallback |
| `agentic` | **Opt-in only.** A Strands tool-use agent ([`agents/sql_agent.py`](src/coa_serve/agents/sql_agent.py)) that starts from pre-fetched candidate tables and their schemas, discovers any others it needs (`search_tables` / `get_table_schema`), delegates SQL generation (`generate_sql`), then executes + self-corrects (`execute_sql`). It consumes the reusable Tier-2 tools ([`tier2/tools/`](src/coa_serve/tier2/tools/)) and the shared execute-with-authz primitive ([`sql_execution.py`](src/coa_serve/sql_execution.py)) — it has no raw-SQL execution path of its own: `generate_sql` returns a handle and only a generated statement can be executed. It is **never** part of an automatic or fallback chain — it runs solely when pinned explicitly. The loop is bounded by the request deadline and `SERVE_AGENTIC_EXEC_TIMEOUT_S`, not by a turn count. Tuning knobs: `SERVE_AGENTIC_PREFETCH_SCHEMAS`, `SERVE_AGENTIC_EXEC_TIMEOUT_S`, `SERVE_AGENTIC_GRAPH_TRAVERSAL`, `SERVE_AGENTIC_INTENT_REVIEW` (see [Configuration](#configuration)). |

See [docs/query-routing.md](docs/query-routing.md) for the full architecture, decision flow diagram, and threshold configuration.

### Tier-2 answerability filter (mapped classes only)

Tier 2 generates a structured query (NL→SPARQL via Ontop, or NL→SQL) that only resolves against classes with a backing SQL table — i.e. classes carrying the `coa:isMapped` marker written at ingest for R2RML-mapped (structured) classes. Unstructured-induced and foundational-ontology classes have no SQL backing, so exposing them to the prompt makes the LLM author queries that resolve to nothing (silent empty Tier-2 results). Both Tier-2 retrieval paths filter to mapped classes only:

- **Ontop T-Box context** (`tier2/ontop/tbox_context.py`): every class SELECT requires `?class <coa#isMapped> true` (the `_IS_MAPPED_PATTERN` gate) — count/threshold, full-namespace fetch, and entity fetch. Object properties are kept only when **both** domain and range are mapped, so an edge can't reintroduce an unmapped class into the prompt.
- **NL→SQL retrieval** (`tier2/nl_to_sql/sql_generator.py`): after class vector retrieval, hits with no `data_source_id` (unmapped) are dropped before context building; the retrieve trace step reports `unmapped_dropped`. Done client-side because the ontology AOSS index runs on NMSLIB (which rejects filtered k-NN).

Semantics within a **marked** namespace: **absent marker == unmapped == excluded** (no fail-open per class). The marker is produced by the Model layer at ingest (see the ontology-engine "Tier-2 answerability marker" note).

> **Legacy-namespace bridge (compatibility fallback):** a namespace whose ontology was accepted **before** this marker existed carries **zero** `coa:isMapped` triples. To avoid silently breaking Tier-2 for such namespaces, `build()` probes once (a namespace-scoped SPARQL `ASK`) whether the namespace has **any** `coa:isMapped` marker: if it has **none**, the mapped-class gate is **dropped** and all classes are exposed (the pre-marker behavior — Tier-2 keeps working exactly as before). If it has **at least one** marker, the strict per-class gate applies. So a legacy namespace is **not** dark; only namespaces that have started using the marker enforce it. See the `LEGACY-NAMESPACE BRIDGE` block in `tbox_context.py`.
>
> **⏳ This is a temporary bridge — create NEW namespaces.** Added 2026-07-16; **intended for removal once existing namespaces are migrated — revisit by ~2026-10 (≈3 months). If you're reading this later and the bridge is still here, it has likely outlived its purpose — verify all namespaces carry markers and delete it (removal recipe is in the `LEGACY-NAMESPACE BRIDGE` block of `tbox_context.py`).** The fallback exists only so existing/demo namespaces keep working during the transition. **Do new work in a fresh namespace** (it carries markers from the start and enforces correct Tier-2) rather than continuing on a pre-marker namespace — don't build on top of legacy namespaces assuming the fallback stays. Known boundary (intentionally unsupported): within a *single* namespace holding multiple ontologies, re-inducting **one** ontology (adding markers) flips the whole namespace to strict, hiding still-legacy SQL-backed classes in the others — so migrate to a new namespace rather than re-inducting one ontology in place. Watch the `tbox_ismapped_bridge_fallback` / `tbox_context_no_mapped_classes` serve logs and the ingest `mapped_class_count` to observe state.

### Executor dispatch (single-source JDBC vs Athena federation)

SQL produced by Tier 1 (pre-compiled metric templates) and Tier 2 (VKG-generated SQL) executes through a `CompositeQueryExecutor` (`clients/composite_executor.py`) that picks the engine per query:

- **Direct JDBC** (`SourceDBQueryExecutor`, asyncpg, ~20–50ms p50) when ALL of: every table is qualified to exactly one catalog, the query resolves to one explicit data source, and the source's registry entry confirms it is JDBC-capable (`queryEngine == "JDBC"` and `queryable == true` — the `has_jdbc_endpoint` gate).
- **Athena federation** (~500–800ms p50 plus engine spin-up) for everything else: cross-source/multi-catalog queries, Glue-native/S3 sources, bare/unqualified table references, or sources not yet provisioned for direct access.

The gate is conservative by construction: any ambiguity (mixed qualified+bare tables, unresolvable source, registry lookup failure) routes to Athena, so a cross-source query can never be misrouted to a single JDBC database. Set `SCL_DISABLE_JDBC_DISPATCH=true` to force Athena-only dispatch (the composite then behaves exactly like the previous single-executor wiring).

### Tier-3 partial-failure handling

Tier-3 retrieval (vector search + graph traversal) runs sources in parallel with per-source timeouts. A source that fails or exceeds its timeout does not block the response: it is recorded as a **degraded source** (`metadata.degradedSources`, each entry naming the source and failure type), synthesis proceeds with whatever arrived, and the response carries `partial: true` so clients know the answer was built from incomplete context. Timeouts are tunable via `TIER3_VECTOR_TIMEOUT` / `TIER3_GRAPH_TIMEOUT` (per-source) or `TIER3_PER_SOURCE_TIMEOUT_S` (single default for both); raise them for slow/large namespaces, lower them to bound tail latency.

### Few-shot SPARQL examples (Tier 2)

NL-to-SPARQL accuracy can be improved per namespace by publishing curated translation examples: when `ONTOLOGY_BUCKET` is set, `tier2/few_shot_loader.py` loads `ontologies/{namespace}/latest/examples.json` from S3 (the artifact contract defined with the few-shot schema MR), caches it in-memory, and injects the namespace's examples into the translation prompt in place of the generic built-ins. Missing/empty file or fetch error falls back gracefully to the generic examples — the loader never blocks translation. The artifact producer is the Model-layer publish pipeline; until it emits `examples.json` for a namespace, the fallback is the runtime behavior.

## Security Controls

### SQL Firewall

Authorization gate that evaluates every SQL statement before execution. Used by both Tier 1 (pre-compiled metric SQL) and Tier 2 (VKG-generated SQL). Located at `src/coa_serve/tier2/sql_firewall.py`.

The firewall enforces two layers, in this order:

1. **Safety validation** (always enforced):
   - Only `SELECT` statements are allowed; DDL/DML (CREATE, DROP, ALTER, INSERT, UPDATE, DELETE, MERGE, etc.) is rejected via sqlglot AST parsing.
   - `SELECT INTO` is rejected.
   - Dangerous functions are blocked (`pg_read_file`, `pg_sleep`, `dblink`, `lo_import`/`lo_export`, `copy`, etc.).
   - Comment-based smuggling is neutralized before statement-type detection.
   - Parse failures fail closed (`UnsafeSQLError`).

2. **Data-level authorization** (enforced when the caller's grant profile carries restrictions):
   - **`tableAllowlist`** (`list[str]`) — every table referenced in the query must appear in the allowlist. Schema-qualified references (`public.orders`) are matched against the bare name (`orders`). CTE names are excluded.
   - **`columnDenylist`** (`dict[str, list[str]]`) — keys are table names, values are columns that must not be projected from that table.
   - **`SELECT *` / `table.*` over a restricted table is denied** because denied columns cannot be proven excluded. `COUNT(*)` is allowed because it does not project columns.
   - **Comparisons are case-insensitive.** SQL identifiers are case-insensitive for unquoted names; the allowlist/denylist must not be bypassed by casing variations (e.g., `SELECT * FROM ORDERS` against `["orders"]`).
   - **Fail-closed** on parse failures, malformed profile shapes (e.g., `tableAllowlist` is not a list), or unparseable references.

The grant profile is sourced from the caller's `ResourceRoleMapping` record (see [control-plane README](../control-plane/README.md#resourceolemappings-table)) and is passed in via the `profile` field on each query. When the profile carries no `tableAllowlist` or `columnDenylist`, the firewall enforces only safety checks (namespace and action authorization is enforced upstream by the API Gateway authorizer via Cedar policies — see [Two-Layer Authorization](../control-plane/docs/authz-access-matrix.md#data-level-authorization-sql-firewall)).

Profile shape:

```json
{
  "tableAllowlist": ["orders", "customers"],
  "columnDenylist": {
    "customers": ["ssn", "date_of_birth"]
  }
}
```

Example denials:

| Query | Profile | Result |
|---|---|---|
| `SELECT * FROM payroll` | `tableAllowlist: ["orders"]` | Denied — `payroll` not in allowlist |
| `SELECT * FROM customers` | `columnDenylist: {customers: ["ssn"]}` | Denied — `SELECT *` may expose `ssn` |
| `SELECT name FROM customers` | `columnDenylist: {customers: ["ssn"]}` | Allowed — `ssn` not referenced |
| `SELECT SSN FROM customers` | `columnDenylist: {customers: ["ssn"]}` | Denied — case-insensitive match |
| `SELECT COUNT(*) FROM customers` | `columnDenylist: {customers: ["ssn"]}` | Allowed — projects no columns |

### Cedar Authorization

In addition to the SQL firewall's structural checks, every query is evaluated against **Cedar policy** before execution. The evaluator (`tier2/cedar_authorizer.py` + the vendored engine under `authz/`) runs the same `cedarpy`-backed `coa_authorization` engine the control-plane API authorizer uses, against the same shipped seed policies (`authz/seed/*.cedar`: global_admin, global_viewer, and the namespace-scoped data-analyst/steward/owner/maintainer roles). A byte-identity guard test keeps the vendored copy in sync with the control-plane source.

How a decision is made:

1. The caller's identity is taken **only from the validated JWT** — any `userId`/`groups`/`globalRoles`/`resourceRoles` keys in the request body are stripped on both consumer surfaces (WebSocket handler and `invoke` entrypoint), so a client cannot self-escalate by claiming roles.
2. The caller's **resolved roles** (forwarded by a trusted upstream — the control-plane's role resolution; never client-supplied) are evaluated for `SCL::Action::"query"` on the target namespace against the seed policies.
3. **Fail-closed**: no roles → deny; evaluator error → deny; deny is terminal (`AccessDeniedError` → HTTP 403, no Tier-3 fall-through, generic client message with the reason logged server-side only).

Operational knobs (see Configuration): `SCL_DISABLE_CEDAR=true` swaps in the allow-all `NullCedarAuthorizer` (the firewall's structural safety + allowlist/denylist checks still run — posture is "Cedar not consulted," never "authz off"); `SCL_CEDAR_FAIL_OPEN_NO_ROLES=true` is a dev/pre-provisioning convenience that lets role-less callers through Cedar (leave UNSET in production so an unconfigured deployment fails closed).

### Row Limit Enforcement

All executed queries are subject to a `max_rows` cap (default: 10,000):

### WebSocket Message Guard

The WebSocket handler enforces layered protection on all incoming messages:

**1. Size validation** (all messages): Messages exceeding `WS_MAX_MESSAGE_BYTES` (default 128 KB, aligned with AWS API Gateway WS limit) immediately close the connection with RFC 6455 code 1009. Multibyte characters are counted as UTF-8 bytes.

**2. Tiered rate limiting** (per-connection, two independent token buckets):

| Bucket | Burst | Sustained Rate | Applies To |
|--------|-------|----------------|------------|
| Query | 10 | ~10/min (1 token / 6s) | Messages without `action` field (expensive orchestrator operations hitting Neptune, OpenSearch, Bedrock) |
| Action | 30 | ~60/min (1 token / 1s) | Messages with `action` field (lightweight session lifecycle: ping/pong, list/create/delete sessions, get history) |

The buckets are independent: exhausting the query bucket does not affect action messages, and vice versa. This prevents normal session lifecycle operations (connect, ping, restore session) from starving query capacity. When a bucket is exhausted, a 429 `RateLimited` error is returned without closing the connection.

**3. Structural validation** (query messages only): Handled downstream by Pydantic's `InvokeRequest` model, which validates required fields, patterns, and lengths with field-level error detail. The guard does not duplicate schema validation (single source of truth principle).

**Design rationale:** Rate limiting is proportional to cost. Queries trigger the full orchestrator pipeline (multiple backend services, seconds of compute). Actions are O(1) operations (DynamoDB read/write, in-memory pong). Applying the same strict limit to both would degrade UX during reconnect scenarios without meaningful security benefit, since the connection is already authenticated via Bearer token at the CloudFront/ALB layer.

### Row Limit Enforcement

All executed queries are subject to a `max_rows` cap (default: 10,000):

- Queries without `LIMIT` get one appended automatically (on both the JDBC and Athena paths; Athena injection caps only the outer query's LIMIT, never a subquery's)
- Queries with `LIMIT` exceeding `max_rows` are capped to `max_rows`
- Queries with `LIMIT` within `max_rows` pass through unchanged

## Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GRAPH_URI_TEMPLATE` | No | `urn:coa:{namespace}:published` | Graph URI template for ontology resolution. Must contain a `{namespace}` placeholder. |
| `MEMORY_ID` | No | `""` (disabled) | AgentCore Memory ID for conversation session state. When set, enables multi-turn session management (create/resume, history loading, turn storage). Obtain from CDK output of serve-stack. |
| `SCL_DISABLE_JDBC_DISPATCH` | No | unset (JDBC dispatch on) | `true` forces Athena-only execution — the composite executor never routes to direct JDBC. Operational off-switch; restores the previous single-executor behavior. |
| `SCL_DISABLE_CEDAR` | No | unset (Cedar on) | `true` swaps the real Cedar evaluator for the allow-all `NullCedarAuthorizer`. SQL-firewall safety + allowlist/denylist checks still run. |
| `SCL_CEDAR_FAIL_OPEN_NO_ROLES` | No | unset (fail-closed) | `true` lets callers with **no resolved roles** pass Cedar (dev/pre-provisioning E2E convenience). Leave unset in production: no roles → deny. |
| `GROUP_CLAIM_NAME` | No | `groups` | JWT claim holding the caller's IdP group memberships (list or delimited string). Mirrors the control-plane authorizer's setting. |
| `ONTOLOGY_BUCKET` | No | `""` (disabled) | S3 bucket for namespace-scoped few-shot `examples.json` (`ontologies/{namespace}/latest/examples.json`). Unset = generic built-in prompt examples. |
| `TIER3_PER_SOURCE_TIMEOUT_S` | No | `10` | Default per-source Tier-3 retrieval timeout (seconds) for both vector search and graph traversal. Timed-out sources are recorded as degraded; synthesis proceeds. |
| `TIER3_VECTOR_TIMEOUT` | No | falls back to `TIER3_PER_SOURCE_TIMEOUT_S` | Per-source override (seconds) for the vector-search retrieval timeout. |
| `TIER3_GRAPH_TIMEOUT` | No | falls back to `TIER3_PER_SOURCE_TIMEOUT_S` | Per-source override (seconds) for the graph-traversal retrieval timeout. |
| `LEXICAL_RETRIEVER_TIMEOUT_S` | No | `45` | Standard-mode (non-agentic) Tier-3 lexical retriever per-query timeout (seconds). Raise for slow single-shot graphrag traversal strategies (e.g. `topic_beam`) that would otherwise be truncated by the retriever's built-in 15s default. Ignored on the agentic path, which passes its own per-tool budget. An invalid value logs a warning and keeps the 45s default. |
| `SERVE_AGENTIC_EXEC_TIMEOUT_S` | No | `35` (min 5) | Per-query `execute_sql` execution timeout (seconds) inside the Tier-2 agentic loop (`options.strategy="agentic"`). Bounds each tool-driven SQL execution so one slow query can't consume the whole agent budget. Values below 5 are clamped to 5. |
| `SERVE_AGENTIC_PREFETCH_SCHEMAS` | No | `3` (min 0) | Table schemas inlined into the agentic prompt before the first model turn (`options.strategy="agentic"`). One k-NN search on the question supplies the candidate list; the top N candidates' full schemas are inlined so the agent does not spend turns re-requesting them. `0` disables the prefetch and restores pure tool-driven discovery. |
| `SERVE_AGENTIC_GRAPH_TRAVERSAL` | No | unset (disabled) | `1`/`true`/`on`/`yes` offers the agentic loop an extra `explore_graph` tool ([`tier2/tools/graph_tools.py`](src/coa_serve/tier2/tools/graph_tools.py)) that walks the ontology's induced foreign-key edges outward from a seed table, so the agent can find join partners a semantic search never surfaces. Requires a graph client (Neptune) to be wired. The flag changes **tool availability only** — the system prompt is byte-identical either way and never mentions `explore_graph`, so the agent decides whether a walk is worth a turn from the tool's own description. That keeps the flag a clean A/B and the decision AI-driven: an earlier prompt block that both re-described the tool and instructed the agent to call it produced 81 calls across 135 Spider 2.0 questions for +1.1pp EX (p=0.69), because hop-1 FK neighbours are already in `get_table_schema` output. Experimental: a retrieval lever under evaluation, not a measured win, so it ships off. The edges it walks come from the same SPARQL builder the Ontop T-Box context uses (`query_utils.object_properties_sparql`) — traversal only passes a higher `LIMIT`, asks for `rdfs:comment`, and drops the `coa:isMapped` gate, since it returns graph facts rather than authoring SQL. |
| `SERVE_NL2SQL_GRAPH_EXPAND` | No | unset (disabled) | `1`/`true`/`on`/`yes` appends the tables one induced foreign-key hop from the *retrieved* classes — with their columns — to the flat (non-agentic) NL→SQL schema context ([`OntologyGraphTool.expand_from`](src/coa_serve/tier2/tools/graph_tools.py)). Requires a graph client (Neptune) to be wired. This is the same FK walk `SERVE_AGENTIC_GRAPH_TRAVERSAL` offers the agent as a tool, but here it fires on every generation: the flat path retrieves **once** — its 2-shot `correct()` re-generates on an execution error or an empty result, but never re-retrieves — so a table similarity did not rank has no other route into the prompt. The walked tables are labelled as **not** similarity-matched and annotated with the join key, and are appended after — never in place of — the retrieved block, whose text carries sampled values the ontology cannot supply; the writer decides whether to use them. Per-request override: `options.flatGraphExpand`, which is what lets on/off run paired against one image. Experimental and off by default: measured **+6 to +12.5pp retrieval full-hit across three cells** (pooled Spider 2.0, per-DB Spider 2.0, BIRD) and an execution-accuracy gain in all four Spider 2.0 arm×seed comparisons — **+2.2 to +5.2pp** (both cells' paired outcomes pooled: 44 win / 25 loss, p=0.030) — at unchanged latency, all of it at a cap of **8** appended tables (see `SERVE_NL2SQL_GRAPH_EXPAND_MAX_TABLES`, whose default is now 15). Off by default because no single cell reaches significance on its own (p=0.11–0.20) and BIRD is a two-repeat wash (+0.7pp then −1.5pp; pooled −0.4pp, p=0.67 over 2,616 matched pairs). |
| `SERVE_NL2SQL_GRAPH_EXPAND_MAX_TABLES` | No | `15` (min 0) | How many walked tables `SERVE_NL2SQL_GRAPH_EXPAND` may append to the flat NL→SQL schema context. A budget, not a tuned optimum: the walk is unconditional and ordered by FK hop then label — adjacency, not question similarity — so on a wide schema the hop-1 frontier is dozens of tables and an uncapped appended block would outweigh the retrieved one it is meant to supplement. 15 is roughly double the 7 tables retrieval itself contributes (`DEFAULT_RETRIEVAL_K`). Raise it when the missing join table is plausibly further down the FK ordering and the prompt has room; lower it (or set `0`, which appends nothing) to isolate the rest of the flag's behaviour. Non-numeric values fall back to the default and log `nl_to_sql_graph_expand_max_tables_invalid`; the effective cap and whether it bound are on every `nl_to_sql_graph_expand` line (`max_tables`, `capped`). The graph tool's own `MAX_NODE_LIMIT` (200) is the hard ceiling. |
| `SERVE_AGENTIC_INTENT_REVIEW` | No | unset (disabled) | `1`/`true`/`on`/`yes` enables an extra interpretation-reconciliation turn in the agentic loop (the agent restates and reconciles query intent before finalizing SQL). Off by default. |
| `SERVE_NL2SQL_MAX_TOKENS` | No | `4096` (512–32768) | Output-token cap for a Tier-2 flat NL→SQL generation (both the first attempt and the on-error correction). Hitting the cap truncates the query mid-statement, which Bedrock reports as a normal 200 response — so the symptom is an execution error, not a generation error; the `nl_to_sql_generation_truncated` warning names it when it happens. A reasoning model spends this same budget on its thinking blocks, so lower it only to bound worst-case latency or cost. Values outside 512–32768 are clamped (logged as `nl_to_sql_max_tokens_clamped`); an unparseable value keeps the default. |
| `SERVE_EVIDENCE_MAX_CHARS` | No | `500` | Caps the number of characters of caller-supplied evidence text injected into Tier-2 prompts (`capped_evidence()`); longer evidence is truncated to bound prompt size. |
| `WS_MAX_MESSAGE_BYTES` | No | `131072` (128 KB) | Maximum WebSocket message size in bytes. Messages exceeding this close the connection with RFC 6455 code 1009. |
| `WS_RATE_LIMIT_BURST` | No | `10` | Query rate limit: maximum burst tokens per connection. Each query consumes one token. |
| `WS_RATE_LIMIT_REFILL_SECONDS` | No | `6.0` | Query rate limit: seconds to refill one token (~10 queries/min sustained). |
| `WS_ACTION_RATE_LIMIT_BURST` | No | `30` | Action rate limit: maximum burst tokens for lightweight session operations (ping, list, create, delete sessions). |
| `WS_ACTION_RATE_LIMIT_REFILL_SECONDS` | No | `1.0` | Action rate limit: seconds to refill one token (~60 actions/min sustained). |

## Development

```bash
cd packages/context-manager
uv run pytest tests/unit/ -v    # run unit tests
uv run mypy src/                # type check
```

## Local testing

```bash
uv run python -m coa_serve.main
# Server starts on http://localhost:8080
# WebSocket available at ws://localhost:8080/ws
```
