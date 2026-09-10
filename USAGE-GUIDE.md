# COA New-User Usage Guide

A practical walkthrough of the Context Ontology Accelerator (COA): create a
namespace, load data sources, model them (ontology + metrics), connect an
agent, and ask questions.

Two ways to run COA:

| Path | Cost | When to use |
|---|---|---|
| **Local Docker stack** | Free | Learning, demos, development |
| **AWS deployment** | ~$930/mo idle (Neptune + OpenSearch) | Production, integration testing |

If you're new, start with the local Docker stack — it runs everything on your
machine with Ollama as the LLM.

---

## Part 1 — Getting Started (Local Docker Stack)

### Prerequisites

- Docker with Compose
- [Ollama](https://ollama.com) running on the host (or reachable at
  `OLLAMA_BASE_URL` from `.env`)

### Steps

```bash
cd docker
cp .env.example .env             # edit only if you need to (e.g. Ollama host)
ollama pull gemma4:latest        # LLM used for enrichment/induction/synthesis
ollama pull mxbai-embed-large    # embeddings for semantic search

docker compose build
docker compose up -d
docker compose logs -f provisioner   # wait for "provisioning complete", then Ctrl-C
```

Then open **http://localhost:3000** and sign in with
`admin@coa.local` / `Passw0rd!`.

What each port is (you rarely need to touch these):

| URL | What it is |
|---|---|
| `localhost:3000` | Web app (management console + Playground) |
| `localhost:9090` | Local API gateway (stands in for the real API) |
| `localhost:3030` | Fuseki SPARQL endpoint (stands in for Neptune) |
| `localhost:9200` | OpenSearch (vector search) |
| `localhost:5432` | Postgres (demo seed data you can query) |
| `localhost:8002` | MCP server |

Teardown when done: `docker compose down -v` (removes all data).

### Changing models

**Local stack (Ollama).** Model selection is env vars in `docker/.env`
(already wired in `docker-compose.yml`):

| Variable | Default | Used for |
|---|---|---|
| `OLLAMA_CHAT_MODEL` | `gemma4:latest` | Enrichment, induction, document KG extraction, synthesis |
| `OLLAMA_EMBED_MODEL` | `mxbai-embed-large:latest` | All embeddings (must stay the same once data exists) |
| `OLLAMA_TIMEOUT_S` | `120` | Per-request timeout for the Ollama call |

Change them, then `docker compose build && docker compose up -d` to pick the
new values up. After switching the **embedding** model, existing vectors are no
longer comparable — re-ingest document sources (and re-approve tables that rely
on vector search) before trusting retrieval results.

**AWS deployment (Bedrock).** Model IDs come from the SSM config parameter
`/{prefix}/config` (default `/coa/config`), a JSON document read at CDK synth
time. The keys:

| SSM key | Feeds |
|---|---|
| `bedrockChatModelId` | Source enrichment, constraint inference, doc KG extraction |
| `bedrockInductionLlmModelId` | Ontology induction, grounding rerank, descriptions |
| `bedrockLlmModelId` | Serve: NL-to-SPARQL + synthesis |
| `bedrockEmbedModelId` | Every embedding producer/consumer (one shared model) |
| `bedrockEmbedDimensions` | Vector dimension baked into the OpenSearch index at creation |

```bash
aws ssm put-parameter --name "/coa/config" --type String \
  --value '{"bedrockChatModelId":"us.anthropic.claude-haiku-4-5-20251001-v1:0","bedrockEmbedModelId":"us.cohere.embed-v4:0"}'
```

Same caveat as above for embeddings: the dimension is fixed at index creation,
so change `bedrockEmbedModelId` / `bedrockEmbedDimensions` at initial deploy,
or plan a re-ingest/migration afterwards.

### Local document graph (Neo4j)

Document ingestion builds a lexical knowledge graph (entities, facts, topics,
chunks) alongside the vector index. On AWS this graph lands in **Neptune**; on
the local stack it lands in **Neo4j** (container `coa-local-neo4j-1`, browse
http://localhost:7474). The switch is entirely env-driven — no code changes:

- `GRAPH_STORE_URI` — set to `bolt://neo4j:...@neo4j:7687` in compose for the
  kg-build workers and the serve (context-manager) containers. Production
  keeps the `neptune-db://` URI and ignores this path.
- `LLM_PROVIDER=ollama` — routes LLM calls (extraction, synthesis) to the
  host Ollama instead of Bedrock, with `OLLAMA_CHAT_MODEL` /
  `OLLAMA_EMBED_MODEL` selecting the models.
- `OPENSEARCH_AUTH=none` — vanilla OpenSearch has no SigV4 signing.

If Tier-3 document queries fail with a graph-store error, check that the
context-manager container sees `GRAPH_STORE_URI` (`docker compose exec
context-manager env | grep GRAPH`) and that the Neo4j driver is present
(`docker compose exec context-manager python -c "import neo4j"`).

---

## Part 2 — Getting Started (AWS Deployment)

### Prerequisites

Python 3.12, Node 22+, pnpm, uv, Docker, Java 17+ (for Smithy codegen), AWS CLI v2.

```bash
make setup
```

### First deploy

```bash
make deploy-dev
```

This bootstraps CDK/ECR Public if the account is new, and provisions 16 stacks
including the web app, API, Neptune, OpenSearch, and the MCP runtime.

Useful overrides:

```bash
SCL_PREFIX=myproj make deploy-dev          # custom resource-name prefix
SCL_VPC_ID=vpc-0abc123 make deploy-dev     # reuse an existing VPC
```

The web app needs OIDC configured: copy
`packages/web-app/public/runtime-config.example.json` to `runtime-config.json`
and fill in your identity provider's authority and clientId, then `pnpm dev`
for local dev (or deploy it via the stack).

### Deployed URLs / outputs

After deploy, read the stack outputs — notably the API endpoint
(`coa-dev-*` stacks) and the MCP server AgentCore Runtime endpoint
(`coa-dev-mcp` stack).

### Cost control

Destroy the stack when not testing:

```bash
make destroy-dev
```

Optional: scale OpenSearch to zero while keeping the stack up —
`cdk deploy -c aoss_min_ocu=0` (adds ~10 s cold start on first query).

---

## Part 3 — Loading Data (Scan)

Everything below happens **inside a namespace**, the isolated workspace for
your sources, ontologies, metrics, and grants.

### Step 1: Create a namespace

Web app: **Administration → Namespaces → Create namespace**. Note the owner
you assign gets the `namespace-owner` role automatically.

### Step 2: Connect a source

Three kinds of data come in differently:

#### A. Structured databases (JDBC)

Supported engines: PostgreSQL, Redshift, MySQL, SQL Server (direct + federated
querying), Oracle and Snowflake (federated only).

Prerequisites:

1. A read-only user with `SELECT` on catalog views.
2. Credentials stored in AWS Secrets Manager, **tagged with the namespace**:

```bash
aws secretsmanager create-secret \
  --name "coa/jdbc/my-postgres" \
  --secret-string '{"username":"readonly_user","password":"s3cur3!"}' \
  --tags Key=coa:namespace,Value=<namespaceId>
```

3. Network: your database must be reachable from COA's VPC (open the DB port
   to the connector security group).

Register (web app: namespace → **Sources → Connect Source → JDBC Database**;
API: `POST /namespaces/{namespaceId}/sources` with
`sourceType: "DATABASE"` + `jdbcConfiguration`). Key fields:

| Field | Notes |
|---|---|
| `engine` | `POSTGRESQL`, `REDSHIFT`, `MYSQL`, `SQLSERVER`, `ORACLE`, `SNOWFLAKE` |
| `host` / `port` / `databaseName` | Host/port/dB can't be changed later |
| `credentialSecretArn` | The tagged secret ARN (can't be changed later) |
| `schemaFilter` / `schemaExcludeFilter` / `tableFilter` / `tableExcludeFilter` | Regex scoping of discovery |
| `warehouse` | **Required for Snowflake** |
| `metadataEnrichmentEnabled` | `true` (default) runs AI enrichment |

#### B. Glue Data Catalog

If your data is already registered in Glue (e.g. an S3 data lake):

1. Tag the Glue database so COA knows your namespace is authorized:

```bash
aws glue tag-resource \
  --resource-arn arn:aws:glue:us-east-1:111122223333:database/sales \
  --tags-to-add '{"coa:namespace":"<namespaceId>"}'
```

(Use `ALL` as the tag value to share it with every namespace. Cross-account
catalogs need a `crossAccountRoleArn` instead of a tag.)

2. Register via the web app (**Connect Source → Glue Database**) or the API
   with `sourceType: "DATABASE"` + `glueConfiguration`
   (`catalogId`, `region`, `databaseName`, optional `tableFilter`).

The database must already be Athena-queryable — Glue sources are always
queried through Athena (3-part names like `"my_data_lake"."orders"`).

#### C. Documents

- **Local upload** (web app): **Connect Source → Documents → Get Upload
  URLs** → upload PDFs/TXT/DOCX/HTML (≤50 MB each) → **Create Source**.
- **S3 bucket** (API): tag the bucket with `coa:namespace = <namespaceId>`
  first (`aws s3api put-bucket-tagging`), then
  `POST .../sources` with `sourceType: "DOCUMENTS"` +
  `sourceBucketArn` + `s3Prefixes`.

Useful `extractionConfig` options when creating a document source:

```json
{
  "extractionConfig": {
    "enableTableExtraction": true,                 // route PDFs through Textract for tables
    "preferredEntityClassifications": ["Policy", "Claim"],  // pin entity types
    "chunkSize": 1024                              // bigger chunks for dense/tabular docs
  }
}
```

#### D. Other sources (custom connectors)

Anything else (Databricks, SAP, internal APIs) works via an Athena
federation connector Lambda. A ready-made **Databricks SQL Warehouse**
connector ships in `connectors/databricks/` — see its README for the runbook.
For your own, start from `connectors/README.md`.

### Step 3: Watch the scan and approve

After registering, the pipeline runs automatically:
`REGISTERED → SCANNING → ENRICHING → PENDING_REVIEW → APPROVED`
(or `SCAN_FAILED`).

- Poll status in the web app, or
  `GET /namespaces/{namespaceId}/sources/{sourceId}`.
- On `PENDING_REVIEW`, review AI-generated descriptions/keys, then
  **approve** each table (or bulk-approve the whole source). Approval makes
  tables queryable.
- On `SCAN_FAILED`, check the error message, fix credentials/network, and
  `POST .../sources/{sourceId}/rescan` (re-scan is only allowed after a
  failure — it's not a schema-drift refresh yet).
- Steward edits you make (descriptions, keys) survive future re-scans —
  AI-generated metadata does not.

---

## Part 4 — Modeling (Ontology + Metrics)

### Ontology

An ontology gives the system semantics: classes (`Customer`, `Order`),
properties, relationships, and constraints — used for NL→SPARQL→SQL
translation and graph traversal.

- **Automatic induction**: namespace → **Ontology → Induction → Start
  induction**. Select your approved sources; Bedrock infers classes and
  relationships from schemas and metadata. Grounding against a foundational
  ontology (FIBO, Schema.org) is on by default (`groundingMode: ENHANCED`)
  so concepts align rather than duplicate.
- **Upload existing**: same page accepts Turtle `.ttl`, RDF/XML, OWL, JSON-LD.
- Induction is async: poll the job (`pending` → in-progress → `completed` /
  `failed`). `failed` is honest — nothing was silently degraded; re-run or fix
  the cause named in `error`.

### Metrics

A metric is a governed business calculation — the deterministic Tier 1 answer
path.

- Web app: namespace → **Metrics → Create Metric**
  (name, description, data source, source table, SQL expression, dialect).
- API: `POST /namespaces/{namespaceId}/metrics`.
- Validate first with `POST .../metrics/validate` (same body, nothing saved).
- Bulk import via **OSI v1.0** YAML/JSON: **Metrics → Import**.
- DML/DDL is hard-rejected; SQL syntax/column issues are soft warnings
  (the serve-time firewall enforces safety anyway).

---

## Part 5 — Connecting an Agent

Agents always act **on behalf of a user** with that user's permissions — there
is no separate agent identity.

### Option A: MCP (the easy path)

Point your MCP client (Claude Desktop, Cursor, Kiro, Amazon Q) at the COA MCP
server:

- **Local stack:** `http://localhost:8002` (mcp-server container).
- **AWS:** the AgentCore Runtime endpoint from the `coa-dev-mcp` stack
  outputs, over Streamable HTTP.

The server exposes 6 tools:

| Tool | Use |
|---|---|
| `list_metrics` | discover governed metrics |
| `describe_schema` | see classes/properties/tables available |
| `query` | natural-language question → result |
| `translate_sparql` | NL → SPARQL without executing |
| `rag_retrieval` | semantically similar document chunks |
| `graph_traversal` | entity-relationship walking |

Authentication: the user's OIDC Bearer token from the standard 3-legged
Authorization Code + PKCE flow (the platform provisions a public client with
`localhost:9876/oauth/callback` for IDE logins). Every tool call is
authorized against that user's grants — an MCP tool call cannot exceed what
the user can do via REST.

### Option B: Direct REST

For scripts/notebooks, call the API with the user's bearer token:

1. Complete the OIDC Authorization Code + PKCE login (the user does this once).
2. Exchange the code for tokens:

```bash
TOKEN=$(curl -s -X POST "https://your-idp-domain/oauth2/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=authorization_code&client_id=CLIENT_ID&code=AUTH_CODE&redirect_uri=REDIRECT_URI&code_verifier=PKCE_VERIFIER" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['id_token'])")
```

3. Call any endpoint, e.g. query:

```bash
curl -X POST "https://<api>/namespaces/<namespaceId>/query" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"query": "What is total revenue?"}'
```

4. Refresh with `grant_type=refresh_token` instead of re-prompting.

### Granting access

You never grant the agent anything — grant the **user** (or their IdP group):

- `POST /namespaces/{namespaceId}/grants` with a role
  (`namespace-owner`, `maintainer`, `data-steward`, `data-analyst`).
- Add `tableAllowlist` / `allowedMetrics` for least-privilege access; agents
  inherit exactly these limits.

---

## Part 6 — Asking Questions (Serve Layer)

Two interfaces: the **Playground** (web app → Playground, pick a namespace,
chat) or `POST /namespaces/{namespaceId}/query`.

Resolution falls through tiers on a miss:

1. **Tier 1 — Metric resolution.** Exact single-metric match only,
   executed verbatim. *"What is total revenue?"* hits the
   `total_revenue` metric. A narrower question (*"...for the Gold tier?"*)
   falls through to Tier 2 — pass the filter via `options.dimensions` or pin
   `tierOverride: 1` if you want the deterministic metric answer.
2. **Tier 2 — Structured query.** NL → SPARQL → SQL via the ontology (VKG),
   or direct NL-to-SQL from the catalog. All SQL passes the firewall
   (SELECT-only, table allowlists).
3. **Tier 3 — Knowledge retrieval.** Vector search + graph traversal +
   Bedrock synthesis for unstructured/document questions.

Namespace composition auto-skips tiers: document-only namespaces start at
Tier 3; database-only ones skip Tier 3 vector search.

Useful options:

- `{"execute": false}` — get the generated SQL without running it
- `{"tierOverride": 1|2|3}` — force a tier
- `{"maxResults": N}` — cap rows

Also available: `POST .../translate` (NL→SPARQL), `POST .../kb/search`,
`POST .../graph/traverse`, `GET .../schema`.

---

## End-to-end example (local stack)

```bash
# 0. Stack up + signed in at http://localhost:3000

# 1. Create namespace "sales"
#    Administration → Namespaces → Create namespace

# 2. Connect the demo Postgres source
#    Sources → Connect Source → JDBC Database
#    (host: postgres, port: 5432 — see docker/.env for credentials)

# 3. Wait for PENDING_REVIEW → review & approve tables

# 4. Induce the ontology (Ontology → Induction → Start induction)

# 5. Create a metric (Metrics → Create Metric):
#    name: total_revenue
#    expression: SUM(orders.total_amount)
#    source table: orders

# 6. Ask in the Playground:
#    "What is total revenue?"            → Tier 1 (metric)
#    "How many orders did customer X place?"  → Tier 2 (SQL)
#    "What do the returns policy docs say?"   → Tier 3 (documents)

# 7. Point your MCP client at http://localhost:8002 and ask the same
#    questions from your IDE.
```

---

## Troubleshooting quick reference

| Symptom | Likely fix |
|---|---|
| `SCAN_FAILED` right away | Credentials wrong, or security group blocks the connector on the DB port |
| Snowflake scan fails after discovery | `warehouse` missing from `jdbcConfiguration` |
| `403` creating a Glue source | Glue database is missing the `coa:namespace` tag (error message includes the exact tag command) |
| `403` on a document source | Bucket missing the `coa:namespace` tag |
| Enrichment stuck in `ENRICHING` | Bedrock throttling — wait/retry, check `BedrockThrottleCount` |
| `403 Access Denied` on query | Acting user has no grant on the namespace — create one |
| Query returns empty | Nothing modeled yet: approve tables, induce ontology, define metrics |
| Query falls through tiers unexpectedly | Question carries a filter/grouping — pass it via `options.dimensions` or use `tierOverride` |
| `401` from agent/script | Token expired — refresh with the refresh token |
| Local stack: provisioning hangs | `docker compose logs -f provisioner` — also confirm Ollama is up and models are pulled |
| Local stack: Tier-3 query fails with a graph error | Context-manager can't reach the document graph — check `GRAPH_STORE_URI` in the compose env and that Neo4j is healthy (`docker compose ps neo4j`) |
| Local stack: doc ingestion fails at extraction | Ollama unreachable from the container — check `OLLAMA_BASE_URL` (use `http://host.docker.internal:11434` from inside containers) |

For deeper details, see `external-docs/content/`: `sources.md` (every source
type), `serve.md` (resolution tiers), `agent-access.md` (agent auth),
`ontologies.md`, `metrics.md`, and `deploying.md` (AWS stack list).
