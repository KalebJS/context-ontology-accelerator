# AGENTS.md

Guidance for AI agents working in the Context Ontology Accelerator (COA) monorepo.

## Overview

COA is a semantic context layer for AWS: knowledge graphs + formal ontologies + rules, served to AI agents via MCP and REST. Monorepo of Python packages, TypeScript CDK infra, and a React web app, orchestrated by Nx.

**Workflow: Scan → Model → Serve.** Scan (source discovery, document ingestion) → Model (ontology induction, metrics, semantic graph) → Serve (SPARQL federation via VKG, graph traversal, tiered NL query resolution, MCP tools).

Access control: namespace-scoped roles (owner, maintainer, data-steward, data-analyst) plus platform-level roles (`platform-admin`, `platform-viewer`), enforced by a Cedar authorizer in the control plane.

## Common Commands

```bash
make setup        # install uv + pnpm, sync workspace, install pre-commit hooks
make generate     # Smithy codegen (only needed after editing models/*.smithy); requires Java 17+ / Gradle
make format       # auto-format (ruff + prettier via Nx) — run before committing
make lint         # ruff + mypy + TS type-check + version check
make test         # unit tests (Nx) + repo-level suite (tests/unit, scripts/agents)
make test-integ   # integration tests (requires a deployed dev stack)
make build        # build all packages (also regenerates root NOTICE)
make deploy-dev   # deploy to a dev AWS account
make destroy-dev  # tear down all dev stacks
```

Run a single package's tests / a single test:

```bash
# One package's unit tests (mirrors its Nx test target)
uv run pytest packages/<pkg>/tests/unit -m unit -q

# One test file or test
uv run pytest packages/<pkg>/tests/unit/test_foo.py -q
uv run pytest packages/<pkg>/tests/unit/test_foo.py::test_name -q

# One package via Nx (lint / test / format)
pnpm nx run <pkg>:lint
pnpm nx run <pkg>:test

# Repo-level cross-cutting tests (not owned by any Nx project)
uv run pytest tests/unit -q
uv run pytest scripts/agents -q
```

## Versioning

Single source of truth is the repo-root `VERSION` file. Never edit a package's version by hand: bump `VERSION`, then run `make version` (propagates to all manifests). `make lint` fails if any manifest has drifted (`scripts/sync_version.py --check`).

## Architecture

### API contracts: Smithy is the source of truth

- `models/src/main/smithy/*.smithy` defines two services: `ControlPlaneService` (management CRUD: namespaces, sources, metrics, ontologies, grants) and `DataLayerService` (`data-layer.smithy` + `serve.smithy`: runtime query/retrieval). Both are served through one API Gateway; the generated OpenAPI specs are merged at deploy time.
- `make generate` produces:
  - TypeScript SDK-style clients → `smithy-generated/*-typescript-client/` (pnpm workspace members, imported by web-app)
  - Pydantic v2 server models (via OpenAPI + openapi-generator) → `smithy-generated/{control-plane,data-layer}-python-server/` (uv workspace members, bundled into Lambdas)
- **Never hand-edit anything under `smithy-generated/`** — change the `.smithy` models and regenerate.
- Pydantic convention: `model_validate()` for parsing untrusted client input (enforces Smithy constraints), `model_construct()` for building responses from trusted/server-generated data (skips re-validation).

### Packages (`packages/`)

| Package | Role |
|---|---|
| `control-plane` | Management-plane API: namespace lifecycle, roles, grants, Cedar authorizer |
| `data-layer` | Runtime REST API; invokes the Serve runtime (context-manager) and formats responses |
| `sources` | Unified source registry (databases + documents): discovery, scanning, review, metadata enrichment |
| `ontology-engine` | Ontology induction, validation, storage, reasoning (HermiT/ELK) |
| `metric-service` | Metric authoring, validation, OSI v1.0 import/export |
| `vkg` | Virtual Knowledge Graph (Ontop-backed SPARQL→SQL federation), runs in ECS/Docker |
| `context-manager` | Serve layer: tiered query orchestration + upstream service clients |
| `mcp-server` | MCP tools for AI agents, hosted on AgentCore Runtime |
| `mcp-proxy` | stdio-to-Streamable-HTTP bridge for local MCP clients |
| `web-app` | React + Cloudscape management console (OIDC auth) |

### Python conventions

- Python 3.12+. Package layout: `packages/<pkg>/src/coa_<name>/` with `api/`, `services/`, `models/`, `utils/`; tests in `tests/unit/` and `tests/integ/`. Pydantic package name is kebab-case (`coa-control-plane`), import name snake_case (`coa_control_plane`).
- Every package has a `project.json` defining Nx `lint`/`test`/`format` targets; new packages must also be registered in root `pyproject.toml` `[tool.uv.workspace]`. See `external-docs/content/package-guide.md` for the full recipe.
- Always depend on and import from `libs/common` (`coa_common`) instead of duplicating utilities: `SCLConfig`/`resolve_region` (config), `setup_logging` (structlog), `SCLError` hierarchy, S3 ops, `DynamoDBDAO`, pipeline constants.
- `libs/ts-shared` mirrors shared types in TypeScript.
- Lint: ruff (line length 120, Google docstring convention — docstrings required under `src/`, exempt in tests/scripts/demos) + mypy strict-ish. `from __future__ import annotations`, type hints on signatures, structlog not print, no bare `except:`.
- Unit tests mock all AWS/external dependencies; integration tests (`-m integ`) hit the real deployed API via shared fixtures, never hardcoded endpoints.

### Infra (`infra/`)

AWS CDK (TypeScript), ~16 stacks deployed in dependency order from `infra/bin/app.ts`. All stacks extend a `CoaStack` base class driven by CDK context variables (`resource_prefix` default `coa`, `env` default `dev`, etc.) — resource naming is `{prefix}-{env}-{name}`. Deploy overrides via env vars, e.g. `SCL_PREFIX=myproj SCL_VPC_ID=vpc-123 make deploy-dev`. Full stack list in `external-docs/content/deploying.md` and `infra/README.md`.

### Serve/query resolution (context-manager)

Queries resolve through tiers that fall through on a miss: **Tier 1** metric resolution (exact single-metric match only, executed verbatim, no filter substitution), **Tier 2** VKG (NL→SPARQL→SQL via ontology) or NL-to-SQL via catalog, **Tier 3** knowledge retrieval (vector search + graph traversal + synthesis). All generated SQL passes a SQL Firewall before execution. `tierOverride` in request options skips to a tier; source-composition gating (document-only vs database-only namespaces) auto-skips tiers.

## Supply chain / hygiene

- `pnpm-workspace.yaml` enforces supply-chain hardening (min release age, pinned overrides, no downgrades). Don't relax it casually.
- `make build` regenerates the root NOTICE from the resolved dependency set (`scripts/supply_chain/cli notice`); commit NOTICE changes when dependencies change.
- Pre-commit hooks run ruff + ruff-format + mypy. If `core.hooksPath` is set (e.g. git-defender), pre-commit install is skipped — run `make lint` before committing.
- This repo is a read-only mirror maintained by an AWS team: don't push; bugs/feedback go through GitHub Issues.

## Key docs

- `external-docs/content/` — published guides: `getting-started.md`, `deploying.md`, `sources.md`, `serve.md`, `ontologies.md`, `metrics.md`, `namespaces.md`, `smithy-codegen.md`, `package-guide.md`, `agent-access.md`, `cedar-policy-authoring.md`
- `packages/control-plane/README.md` — grants and authorization model
- `infra/README.md` — stack list, Lake Formation bootstrap, troubleshooting
