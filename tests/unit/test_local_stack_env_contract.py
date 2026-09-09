# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Static contract test: docker/local stack env vars vs. what handlers read.

The local Docker stack re-implements the CDK environment wiring by hand
(compose ``environment:`` blocks + the gateway's ``_seed_handler_env``).
Any name that drifts from what the production handler reads surfaces only
at runtime, deep inside a request — the failure mode that motivated this
test. Here the contract is enforced statically, in CI:

1. Parse ``docker/docker-compose.yml`` (which merges the gateway's env
   seeding logic by inclusion) for the variables each service defines.
2. Statically scan the package source each local container bundles for
   ``os.environ``/``os.getenv`` reads.
3. Fail when a package reads a service-scoped env var that neither compose
   nor the gateway seeding defines (AND the read has no code default, so
   the handler would crash or silently misconfigure).

Reads WITH a fallback default are exempt (the handler still functions);
``os.environ[...]`` hard reads must be provided or seeded.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).parent.parent.parent
COMPOSE = REPO_ROOT / "docker" / "docker-compose.yml"

# Service name (compose key) → package source dirs bundled into that image.
# Mirrors the COPY lines in each Dockerfile — keep in sync when images change.
SERVICE_SOURCES: dict[str, list[Path]] = {
    "gateway": [
        REPO_ROOT / "packages" / "control-plane" / "src",
        REPO_ROOT / "packages" / "sources" / "src",
        REPO_ROOT / "packages" / "metric-service" / "src",
        REPO_ROOT / "packages" / "ontology-engine" / "src",
        REPO_ROOT / "packages" / "data-layer" / "src",
        REPO_ROOT / "libs" / "common" / "src",
        REPO_ROOT / "docker" / "gateway" / "src",
    ],
    "context-manager": [
        REPO_ROOT / "packages" / "context-manager" / "src",
        REPO_ROOT / "libs" / "common" / "src",
    ],
    "ontology-engine": [
        REPO_ROOT / "packages" / "ontology-engine" / "src",
        REPO_ROOT / "libs" / "common" / "src",
    ],
    "scan-workers": [
        REPO_ROOT / "packages" / "sources" / "src",
        REPO_ROOT / "libs" / "common" / "src",
        REPO_ROOT / "docker" / "scan-workers",
    ],
    "mcp-server": [
        REPO_ROOT / "packages" / "context-manager" / "src",
        REPO_ROOT / "packages" / "mcp-server" / "src",
        REPO_ROOT / "libs" / "common" / "src",
    ],
    "data-layer": [
        REPO_ROOT / "packages" / "data-layer" / "src",
        REPO_ROOT / "libs" / "common" / "src",
    ],
}

# Env vars set/seeded for EVERY service (compose anchors merged into each
# block, plus the gateway's in-process seeding which also covers handlers it
# imports). Keep in sync with docker-compose.yml x-aws-env / x-ollama-env /
# x-oidc-env and _seed_handler_env.
_GLOBAL_ENV = {
    "AWS_REGION",
    "AWS_DEFAULT_REGION",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "LOCALSTACK_ENDPOINT",
    "AWS_ENDPOINT_URL_DYNAMODB",
    "AWS_ENDPOINT_URL_S3",
    "AWS_ENDPOINT_URL_SQS",
    "AWS_ENDPOINT_URL_SSM",
    "AWS_ENDPOINT_URL_SECRETS_MANAGER",
    "AWS_ENDPOINT_URL_EVENTBRIDGE",
    "AWS_ENDPOINT_URL_STS",
    "AWS_ENDPOINT_URL_STEP_FUNCTIONS",
    "AWS_ENDPOINT_URL_ATHENA",
    "AWS_ENDPOINT_URL_GLUE",
    "AWS_ENDPOINT_URL_TEXTRACT",
    "SCL_PREFIX",
    "SCL_ENV",
    "LOG_LEVEL",
    # x-ollama-env
    "LLM_PROVIDER",
    "EMBED_PROVIDER",
    "OLLAMA_BASE_URL",
    "OLLAMA_CHAT_MODEL",
    "OLLAMA_EMBED_MODEL",
    "OLLAMA_TIMEOUT_S",
    # x-oidc-env (gateway/CM/mcp only, but harmless to allow everywhere)
    "KC_ISSUER_INTERNAL",
    "KC_WEB_CLIENT_ID",
    "KC_MCP_CLIENT_ID",
    "KC_ISSUER_PUBLIC",
    # gateway _seed_handler_env (in-process for handlers it imports)
    "ALLOWED_ORIGIN",
    "RESOURCE_PREFIX",
    "TAG_PREFIX",
    "NAMESPACES_TABLE",
    "NAMESPACES_TABLE_NAME",
    "ROLES_TABLE",
    "ROLES_TABLE_NAME",
    "RESOURCE_ROLE_MAPPINGS_TABLE",
    "RESOURCE_ROLE_MAPPINGS_TABLE_NAME",
    "CACHE_INVALIDATION_TABLE_NAME",
    "SOURCES_TABLE",
    "SOURCE_SCAN_JOBS_TABLE",
    "DOC_SOURCES_TABLE",
    "ONTOLOGY_ENGINE_TABLE",
    "METRIC_IMPORT_JOBS_TABLE",
    "METRIC_DEFINITIONS_TABLE",
    "DATAZONE_DOMAIN_ID",
    "PROJECT_ACCESS_ROLE_ARN_SSM",
    "JWKS_ISSUER",
    "JWKS_URI",
    "CLIENT_ID",
    "GROUP_CLAIM_NAME",
    "ATHENA_RESULTS_BUCKET_SSM",
    "ONTOLOGY_ENGINE_ENDPOINT",
    "AGENTCORE_DIRECT_URL",
    "AGENTCORE_RUNTIME_ARN",
    "SOURCES_BUCKET",
    "ONTOLOGY_BUCKET",
    "SCAN_QUEUE_URL",
    "REVIEW_QUEUE_URL",
    "INGESTION_QUEUE_URL",
    "DELETION_STATE_MACHINE_ARN",
    "SSM_PREFIX",
}

# Patterns that extract env reads from Python source. Each returns the var
# name in group 1.
_ENV_PATTERNS = [
    re.compile(r'os\.environ\.get\(\s*"([A-Z][A-Z0-9_]+)"'),
    re.compile(r"os\.environ\.get\(\s*'([A-Z][A-Z0-9_]+)'"),
    re.compile(r'os\.getenv\(\s*"([A-Z][A-Z0-9_]+)"'),
    re.compile(r"os\.getenv\(\s*'([A-Z][A-Z0-9_]+)'"),
    re.compile(r'os\.environ\[\s*"([A-Z][A-Z0-9_]+)"\s*\]'),
    re.compile(r"os\.environ\[\s*'([A-Z][A-Z0-9_]+)'\s*\]"),
    re.compile(r'os\.environ\.setdefault\(\s*"([A-Z][A-Z0-9_]+)"'),
]

# Env vars that are legitimately unset in the local stack — the handler treats
# absence as "feature off" or the local caller always passes the value another
# way. Each entry carries the reason so future drift reviews can re-judge.
OPTIONAL_BY_DESIGN: dict[str, str] = {
    # Scan-pipeline handlers: the local worker (docker/scan-workers/worker.py)
    # always passes these in the event; the os.environ hard reads are the
    # Lambda-部署 fallback only.
    "DATASOURCE_ID": "scan worker passes event values; env is the Lambda fallback",
    "SCAN_JOB_ID": "scan worker passes event values; env is the Lambda fallback",
    "NAMESPACE_ID": "scan worker passes event values; env is the Lambda fallback",
    "SMUS_DOMAIN_ID": "scan worker passes event values; env is the Lambda fallback",
    # Document-upload trigger Lambda — not run in-process locally (doc ingestion
    # is queued directly); the Step Functions state machine does not exist locally.
    "STATE_MACHINE_ARN": "doc-trigger Lambda not in the local flow",
    # Namespace-deletion pipeline invokes the metric/sources API Lambdas by name;
    # local deletion is a known limitation (LocalStack cannot invoke the
    # in-process gateway handlers). Surface later if local delete matters.
    "METRIC_API_FN_NAME": "namespace-delete pipeline; local delete is a known limitation",
    "SOURCES_API_FN_NAME": "namespace-delete pipeline; local delete is a known limitation",
    # None-means-off toggles: absence disables an optional capability.
    "PROJECT_ACCESS_ROLE_ARN": "falls back to the caller's own credentials",
    "VKG_CLUSTER_ARN": "absence → vkg health reports UNKNOWN by design",
    "ATHENA_WORKGROUP": "absence → per-namespace workgroup resolution",
    "ATHENA_OUTPUT_S3": "absence → per-call output location",
    "ATHENA_DATABASE": "absence → 'default' database",
    # Constructor-arg-first clients: production paths always pass the URL as an
    # argument (config.vkg_endpoint etc.); the env read is only the bare-constructor
    # fallback.
    "VKG_SERVICE_URL": "VKGClient base_url arg is always passed on live paths",
    # get()-with-fallback shapes the single-line regex cannot see (default
    # resolved via `or` on the same expression or on the following lines).
    "BEDROCK_CHAT_MODEL_ID": "resolve_chat_model_id falls back to the us. profile id",
    "SERVE_NL2SQL_MAX_TOKENS": "_resolve_max_output_tokens defaults when unset",
    "SERVE_NL2SQL_GRAPH_EXPAND_MAX_TABLES": "_resolve_graph_expand_max_tables defaults when unset",
    "ROLES_CACHE_TTL_SECONDS": "_parse_ttl_seconds defaults when unset",
}

# Standard AWS/runtime vars the platform itself guarantees.
_PLATFORM_ENV = {
    "AWS_REGION",
    "AWS_DEFAULT_REGION",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
    "AWS_LAMBDA_FUNCTION_NAME",
    "AWS_EXECUTION_ENV",
    "PYTHONPATH",
    "PATH",
    "HOME",
    "PORT",
}


def _compose_service_env(service_name: str, compose: dict) -> set[str]:
    """Union of env keys a compose service defines (direct + x-anchor merges)."""
    svc = compose.get("services", {}).get(service_name, {})
    env = svc.get("environment") or {}
    if isinstance(env, list):  # list form: ["KEY=value", ...]
        return {e.split("=", 1)[0] for e in env}
    return set(env.keys())


def _package_env_reads(directories: list[Path]) -> dict[str, list[str]]:
    """Map env var → file paths that read it, across the given source dirs."""
    reads: dict[str, list[str]] = {}
    for directory in directories:
        if not directory.exists():
            continue
        for py in directory.rglob("*.py"):
            rel = str(py.relative_to(REPO_ROOT))
            try:
                text = py.read_text()
            except (OSError, UnicodeDecodeError):
                continue
            for pattern in _ENV_PATTERNS:
                for match in pattern.finditer(text):
                    reads.setdefault(match.group(1), []).append(rel)
    return reads


def test_compose_services_exist() -> None:
    """Guard: the services this test checks are all present in compose."""
    compose = yaml.safe_load(COMPOSE.read_text())
    services = set(compose.get("services", {}).keys())
    missing = set(SERVICE_SOURCES) - services
    assert not missing, f"compose is missing services this test checks: {sorted(missing)}"


def test_local_stack_env_contract() -> None:
    """Every env var a bundled package reads must be defined by its service.

    Fails when a package reads an env var that compose (or gateway seeding,
    for the gateway's in-process handlers) never provides AND the read has no
    inline fallback — i.e. ``os.environ[...]`` or a ``get`` with no default
    argument, which crashes or misconfigures at runtime. Reads with defaults
    are safe by construction and exempt.
    """
    compose = yaml.safe_load(COMPOSE.read_text())

    problems: list[str] = []
    for service, source_dirs in SERVICE_SOURCES.items():
        provided = _GLOBAL_ENV | _PLATFORM_ENV | _compose_service_env(service, compose)
        reads = _package_env_reads(source_dirs)

        # Required = hard read os.environ[X] (crashes) or a .get() with no
        # default argument (single string arg). Soft reads flagged only when
        # the .get() call carries NO default argument.
        hard_pattern = re.compile(
            r'os\.environ\[\s*"([A-Z][A-Z0-9_]+)"\s*\]' r"|os\.environ\[\s*'([A-Z][A-Z0-9_]+)'\s*\]"
        )
        no_default_pattern = re.compile(
            r'os\.environ\.get\(\s*["\']([A-Z][A-Z0-9_]+)["\']\s*\)(?!\s*,)'  # get("X") with no default
        )

        for var, _files in sorted(reads.items()):
            if var in provided or var in OPTIONAL_BY_DESIGN:
                continue
            for directory in source_dirs:
                if not directory.exists():
                    continue
                for py in directory.rglob("*.py"):
                    text = py.read_text(errors="ignore")
                    rel = str(py.relative_to(REPO_ROOT))
                    is_hard = any(m.group(1) == var or m.group(2) == var for m in hard_pattern.finditer(text))
                    is_nodefault = any(m.group(1) == var for m in no_default_pattern.finditer(text))
                    if is_hard or is_nodefault:
                        problems.append(
                            f"{service}: env var {var!r} is read as required in {rel} "
                            f"but compose/gateway seeding never provides it"
                        )
                        break

    assert not problems, "Local stack env contract violations:\n  " + "\n  ".join(problems)
