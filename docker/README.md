# COA Docker Stack

Run the entire Context Ontology Accelerator locally via Docker Compose — every
AWS dependency replaced by a local equivalent, LLM/embeddings served by the
host's Ollama.

## Quick start

```bash
cd docker
cp .env.example .env            # edit if needed
docker compose build            # all images
docker compose up -d            # infra + apps
docker compose logs -f provisioner   # wait for "provisioning complete"
./e2e/run-e2e.sh                # smoke E2E (API flows)
# Playwright (host):
cd ../packages/web-app && pnpm exec playwright install chromium
cd ../../docker && ./e2e/run-playwright.sh
```

Open http://localhost:3000 — sign in with `admin@coa.local` / `Passw0rd!`.

## Service map

| Service | Port | Replaces |
|---|---|---|
| web-app (nginx) | 3000 | CloudFront + WebStack |
| local-gateway (FastAPI) | 9090 | API Gateway + ~10 Lambda families |
| context-manager | 8081→8080 | AgentCore Runtime (CM) |
| mcp-server | 8002→8000 | AgentCore Runtime (MCP) |
| ontology-engine | 8001 | OntologyStack ECS |
| data-layer | 8083→8080 | DataLayerStack Lambda |
| vkg-router | 8180 | per-namespace Ontop ECS + CloudMap |
| scan-workers | — | Step Functions pipelines + Fargate tasks |
| localstack | 8888→4566 | DynamoDB, S3, SSM, Secrets, SQS, EventBridge, STS |
| keycloak | 8280→8080 | Cognito |
| fuseki | 3030 | Neptune (SPARQL) |
| opensearch | 9200 | OpenSearch Serverless |
| postgres | 5432 | The queryable source DB (demo seed) |

The host's Ollama at `host.docker.internal:11434` serves LLM (default model
tag `gemma4:latest`) and embeddings (`mxbai-embed-large`). Point `OLLAMA_BASE_URL`
in `.env` at a different host if Ollama runs elsewhere (e.g. `http://ollama:11434`
when running it as a compose service too).

## Pull models first

```bash
ollama pull gemma4:latest
ollama pull mxbai-embed-large
```

## Local-only env switches

These override AWS-tuned defaults for the self-managed local services. All are
set in `docker/.env.example`; on a real AWS deployment they stay unset and the
production defaults apply.

| Var | Where | Local value | Why |
|---|---|---|---|
| `NDB_GSP_PATH` | ontology-engine | `/sparql/gsp` | Fuseki serves named-graph GSP without a trailing slash; Neptune default is `/sparql/gsp/`. |
| `OSS_KNN_ENGINE` | ontology-engine, context-manager | `faiss` | Vanilla OpenSearch (unlike AOSS) defaults to NMSLIB, which rejects filtered k-NN. Also switches `space_type` to `l2` (faiss doesn't support `cosinesimil`). |
| `DATA_LAYER_CM_TIMEOUT_S` | data-layer, gateway | `120` | Raises the internal context-manager call timeout past the local stack's slower query path (AWS default 29s matches the API Gateway cap). The gateway needs it too: it imports the data-layer handler in-process for `/query`. |
| `METRICS_EMIT` | ontology-engine | `0` | Skips CloudWatch `PutMetricData` emission (induction cost/heartbeat metrics) — LocalStack has no valid CloudWatch creds, so every emit logs `InvalidClientTokenId` noise. Unset (or `1`) on AWS emits as before. |

Fuseki uses a custom read-write dataset (`docker/fuseki/assembler.ttl`, mounted
read-only into the container) because the stock `secoresearch/fuseki` image
ships a query-only dataset. If you tear down with `docker compose down -v`, the
volume is recreated from that file on next boot — no manual step.

## Teardown

```bash
docker compose down -v    # removes volumes (DDB/S3/Fuseki/PG data)
```
