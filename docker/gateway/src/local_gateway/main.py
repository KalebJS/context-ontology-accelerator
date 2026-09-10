# Copyright Amazon.com, Inc. or its affiliates. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Local gateway — API Gateway + Lambda emulation in one FastAPI app.

Runs every real Lambda handler module in-process. Requests are adapted into
API Gateway proxy events, routed through the same Cedar authorizer used in
production (JWT validation via Keycloak JWKS + Cedar policy evaluation from
DynamoDB), and the Lambda proxy response is translated back to HTTP.

Route table mirrors ApiStack.ssmPathHandlers in infra/bin/app.ts.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import re
from collections.abc import Callable

import boto3
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger("local-gateway")

app = FastAPI(title="coa-local-gateway", docs_url="/docs", openapi_url="/openapi.json")

_allowed_origin = os.environ.get("ALLOWED_ORIGIN", "*")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o for o in _allowed_origin.split(",") if o],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

PREFIX = os.environ.get("SCL_PREFIX", "coa")
ENV = os.environ.get("SCL_ENV", "local")
REGION = os.environ.get("AWS_REGION", "us-east-1")
LOCALSTACK = os.environ.get("LOCALSTACK_ENDPOINT", "http://localstack:4566")

TABLE = {
    "namespaces": f"{PREFIX}-{ENV}-namespaces",
    "roles": f"{PREFIX}-{ENV}-roles",
    "rrm": f"{PREFIX}-{ENV}-resource-role-mappings",
    "cache": f"{PREFIX}-{ENV}-cache-invalidation",
    "sources": f"{PREFIX}-{ENV}-sources",
    "scan_jobs": f"{PREFIX}-{ENV}-source-scan-jobs",
    "ontology": f"{PREFIX}-{ENV}-ontology-engine",
    "import_jobs": f"{PREFIX}-{ENV}-metric-import-jobs",
}


# ── Environment the Lambda handlers expect ─────────────────────────────────
def _seed_handler_env() -> None:
    os.environ.setdefault("AWS_REGION", REGION)
    os.environ.setdefault("ALLOWED_ORIGIN", _allowed_origin)
    os.environ.setdefault("RESOURCE_PREFIX", f"{PREFIX}-{ENV}-")
    os.environ.setdefault("TAG_PREFIX", PREFIX)
    os.environ.setdefault("NAMESPACES_TABLE", TABLE["namespaces"])
    os.environ.setdefault("NAMESPACES_TABLE_NAME", TABLE["namespaces"])
    os.environ.setdefault("ROLES_TABLE", TABLE["roles"])
    os.environ.setdefault("ROLES_TABLE_NAME", TABLE["roles"])
    os.environ.setdefault("RESOURCE_ROLE_MAPPINGS_TABLE", TABLE["rrm"])
    os.environ.setdefault("RESOURCE_ROLE_MAPPINGS_TABLE_NAME", TABLE["rrm"])
    os.environ.setdefault("CACHE_INVALIDATION_TABLE_NAME", TABLE["cache"])
    os.environ.setdefault("SOURCES_TABLE", TABLE["sources"])
    os.environ.setdefault("SOURCE_SCAN_JOBS_TABLE", TABLE["scan_jobs"])
    os.environ.setdefault("DOC_SOURCES_TABLE", TABLE["sources"])
    os.environ.setdefault("ONTOLOGY_ENGINE_TABLE", TABLE["ontology"])
    os.environ.setdefault("METRIC_IMPORT_JOBS_TABLE", TABLE["import_jobs"])
    os.environ.setdefault("METRIC_DEFINITIONS_TABLE", TABLE["ontology"])
    os.environ.setdefault("DATAZONE_DOMAIN_ID", os.environ.get("LOCAL_METADATA_DOMAIN_ID", "local-domain"))
    os.environ.setdefault("PROJECT_ACCESS_ROLE_ARN_SSM", f"/{PREFIX}/smus/dz-project-access-role-arn")
    os.environ.setdefault("JWKS_ISSUER", os.environ.get("KC_ISSUER_PUBLIC", os.environ["KC_ISSUER_INTERNAL"]))
    # Tokens carry the browser-facing issuer (KC_ISSUER_PUBLIC); the JWKS
    # endpoint is fetched container-side via the service-network issuer.
    os.environ.setdefault(
        "JWKS_URI",
        f"{os.environ['KC_ISSUER_INTERNAL'].rstrip('/')}/protocol/openid-connect/certs",
    )
    os.environ.setdefault("CLIENT_ID", os.environ.get("KC_WEB_CLIENT_ID", "coa-web"))
    os.environ.setdefault("GROUP_CLAIM_NAME", "groups")
    os.environ.setdefault("ATHENA_RESULTS_BUCKET_SSM", f"/{PREFIX}/query/athena-results-bucket")
    os.environ.setdefault("ONTOLOGY_ENGINE_ENDPOINT", os.environ["OE_ENDPOINT"])
    os.environ.setdefault("AGENTCORE_DIRECT_URL", os.environ["CM_ENDPOINT"])
    # The data-layer Lambda reads AGENTCORE_RUNTIME_ARN at import to build its
    # AgentCore invoke URL. The local stack overrides the URL via
    # AGENTCORE_DIRECT_URL (above), so a stand-in ARN keeps the import happy.
    os.environ.setdefault("AGENTCORE_RUNTIME_ARN", "local/cm-runtime")
    # Bucket names the handlers reference
    os.environ.setdefault("SOURCES_BUCKET", f"{PREFIX}-{ENV}-sources-data")
    # The sources handler reads BUCKET_NAME (what sources-stack.ts sets in the
    # real Lambda env) for presigned upload URLs; SOURCES_BUCKET alone 500s it.
    os.environ.setdefault("BUCKET_NAME", f"{PREFIX}-{ENV}-sources-data")
    os.environ.setdefault("ONTOLOGY_BUCKET", f"{PREFIX}-{ENV}-ontology-artifacts")
    os.environ.setdefault("SCAN_QUEUE_URL", f"http://localstack:4566/000000000000/{PREFIX}-{ENV}-sources-db-scan-queue")
    os.environ.setdefault(
        "REVIEW_QUEUE_URL", f"http://localstack:4566/000000000000/{PREFIX}-local-sources-bulk-review-queue"
    )
    os.environ.setdefault(
        "INGESTION_QUEUE_URL", f"http://localstack:4566/000000000000/{PREFIX}-{ENV}-sources-doc-ingestion-queue"
    )
    os.environ.setdefault("DELETION_STATE_MACHINE_ARN", f"local:{PREFIX}-{ENV}-ns-deletion")
    os.environ.setdefault("SSM_PREFIX", f"/{PREFIX}")


_seed_handler_env()

# ── Presigned-URL host rewrite ──────────────────────────────────────────────
# generate_presigned_url emits the container-internal LocalStack host
# (localstack:4566), which the user's browser cannot resolve. Rewrite the
# endpoint host:port to the host-mapped LocalStack port (LOCALSTACK_PORT, set
# in docker/.env) so PUTs from the web app work. Safe in local mode only:
# LocalStack does not validate SigV4 signatures, so rewriting the host in the
# URL cannot invalidate anything. This never applies to a real AWS deployment
# — the rewrite exists only inside the local gateway process.
_LOCALSTACK_INTERNAL = os.environ.get("LOCALSTACK_ENDPOINT", "http://localstack:4566")
_external_s3_host: str | None = os.environ.get("LOCALSTACK_EXTERNAL_ENDPOINT")
if not _external_s3_host:
    # Derive from LOCALSTACK_PORT (docker/.env) when available; leave the
    # internal URL untouched otherwise (e.g. container-side E2E runs).
    _port = os.environ.get("LOCALSTACK_PORT")
    if _port:
        _external_s3_host = f"http://localhost:{_port}"

if _external_s3_host:
    from urllib.parse import urlsplit

    _ext, _int = urlsplit(_external_s3_host), urlsplit(_LOCALSTACK_INTERNAL)
    _S3_HOST_REWRITE: tuple[tuple[str, int], tuple[str, int]] | None = (
        (_int.hostname or "localstack", _int.port or 4566),
        (_ext.hostname or "localhost", _ext.port or 8888),
    )
else:
    _S3_HOST_REWRITE = None

import boto3.session  # noqa: E402  (after env seeding)


def _local_boto() -> boto3.session.Session:
    return boto3.session.Session(
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        region_name=REGION,
    )


# ── Route table ─────────────────────────────────────────────────────────────
# Mirrors ApiStack path handlers; (method, resource-template) → handler module.


def _namespace_handler():
    from coa_control_plane.namespace.namespace_api_handler import handler

    return handler


def _roles_platform_handler():
    from coa_control_plane.roles.list_platform_roles_handler import handler

    return handler


def _roles_ns_handler():
    from coa_control_plane.roles.namespace_roles_handler import handler

    return handler


def _grants_handler():
    from coa_control_plane.grants.grants_handler import handler

    return handler


def _sources_handler():
    from coa_sources.api.sources_handler import handler

    return handler


def _metric_handler():
    from coa_metrics.api.metric_api_handler import handler

    return handler


def _ontology_proxy_handler():
    from coa_ontology.api_proxy_handler import handler

    return handler


def _data_layer_handler():
    from coa_data_layer.handler import handler

    return handler


def _authorizer_handler():
    from coa_control_plane.authorization.handler import handler

    return handler


# (method, resource) → loader. Resource templates use API Gateway syntax.
ROUTES: dict[tuple[str, str], Callable[[], Callable]] = {
    ("POST", "/namespaces"): _namespace_handler,
    ("GET", "/namespaces"): _namespace_handler,
    ("GET", "/namespaces/{namespaceId}"): _namespace_handler,
    ("PUT", "/namespaces/{namespaceId}"): _namespace_handler,
    ("DELETE", "/namespaces/{namespaceId}"): _namespace_handler,
    ("PATCH", "/namespaces/{namespaceId}/status"): _namespace_handler,
    ("GET", "/roles"): _roles_platform_handler,
    ("GET", "/namespaces/{namespaceId}/roles"): _roles_ns_handler,
    ("GET", "/namespaces/{namespaceId}/roles/{roleId}"): _roles_ns_handler,
    ("GET", "/namespaces/{namespaceId}/grants"): _grants_handler,
    ("POST", "/namespaces/{namespaceId}/grants"): _grants_handler,
    ("DELETE", "/namespaces/{namespaceId}/grants/{grantId}"): _grants_handler,
    ("GET", "/principals/{principalId}/grants"): _grants_handler,
    ("POST", "/grants"): _grants_handler,
    ("GET", "/grants"): _grants_handler,
    ("DELETE", "/grants/{grantId}"): _grants_handler,
    ("GET", "/namespaces/{namespaceId}/sources"): _sources_handler,
    ("POST", "/namespaces/{namespaceId}/sources"): _sources_handler,
    ("POST", "/namespaces/{namespaceId}/sources/upload-urls"): _sources_handler,
    ("GET", "/namespaces/{namespaceId}/sources/{sourceId}"): _sources_handler,
    ("DELETE", "/namespaces/{namespaceId}/sources/{sourceId}"): _sources_handler,
    ("POST", "/namespaces/{namespaceId}/sources/{sourceId}/rescan"): _sources_handler,
    ("POST", "/namespaces/{namespaceId}/sources/{sourceId}/approve"): _sources_handler,
    ("POST", "/namespaces/{namespaceId}/sources/{sourceId}/reject"): _sources_handler,
    ("GET", "/namespaces/{namespaceId}/sources/{sourceId}/tables"): _sources_handler,
    ("GET", "/namespaces/{namespaceId}/sources/{sourceId}/tables/{tableId}"): _sources_handler,
    ("PUT", "/namespaces/{namespaceId}/sources/{sourceId}/tables/{tableId}/review"): _sources_handler,
    ("PATCH", "/namespaces/{namespaceId}/sources/{sourceId}/tables/{tableId}/metadata"): _sources_handler,
    ("PATCH", "/namespaces/{namespaceId}/sources/{sourceId}/tables/{tableId}/keys"): _sources_handler,
    (
        "PUT",
        "/namespaces/{namespaceId}/sources/{sourceId}/tables/{tableId}/columns/{columnName}/review",
    ): _sources_handler,
    (
        "PATCH",
        "/namespaces/{namespaceId}/sources/{sourceId}/tables/{tableId}/columns/{columnName}/metadata",
    ): _sources_handler,
    ("GET", "/namespaces/{namespaceId}/sources/{sourceId}/scan/{jobId}"): _sources_handler,
    ("PUT", "/namespaces/{namespaceId}/sources/{sourceId}/metadata"): _sources_handler,
    ("GET", "/namespaces/{namespaceId}/metrics"): _metric_handler,
    ("POST", "/namespaces/{namespaceId}/metrics"): _metric_handler,
    ("GET", "/namespaces/{namespaceId}/metrics/{name}"): _metric_handler,
    ("PUT", "/namespaces/{namespaceId}/metrics/{name}"): _metric_handler,
    ("DELETE", "/namespaces/{namespaceId}/metrics/{name}"): _metric_handler,
    ("POST", "/namespaces/{namespaceId}/metrics/validate"): _metric_handler,
    ("POST", "/namespaces/{namespaceId}/bulk-delete-metrics"): _metric_handler,
    ("POST", "/namespaces/{namespaceId}/import-osi"): _metric_handler,
    ("POST", "/namespaces/{namespaceId}/import-osi/upload-url"): _metric_handler,
    ("GET", "/namespaces/{namespaceId}/import-jobs/{jobId}"): _metric_handler,
    ("GET", "/namespaces/{namespaceId}/export-osi"): _metric_handler,
    ("POST", "/namespaces/{namespaceId}/induce"): _ontology_proxy_handler,
    ("GET", "/namespaces/{namespaceId}/induce/jobs"): _ontology_proxy_handler,
    ("GET", "/namespaces/{namespaceId}/induce/jobs/{jobId}"): _ontology_proxy_handler,
    ("GET", "/namespaces/{namespaceId}/induce/datasources"): _ontology_proxy_handler,
    ("GET", "/namespaces/{namespaceId}/induce/datasources/induced"): _ontology_proxy_handler,
    ("GET", "/namespaces/{namespaceId}/proposals"): _ontology_proxy_handler,
    ("GET", "/namespaces/{namespaceId}/proposals/{proposalId}"): _ontology_proxy_handler,
    ("POST", "/namespaces/{namespaceId}/proposals/{proposalId}/accept"): _ontology_proxy_handler,
    ("POST", "/namespaces/{namespaceId}/proposals/{proposalId}/cancel"): _ontology_proxy_handler,
    ("POST", "/namespaces/{namespaceId}/proposals/{proposalId}/upload-url"): _ontology_proxy_handler,
    ("POST", "/namespaces/{namespaceId}/proposals/{proposalId}/validate"): _ontology_proxy_handler,
    ("GET", "/namespaces/{namespaceId}/proposals/{proposalId}/validate/jobs/{jobId}"): _ontology_proxy_handler,
    ("POST", "/namespaces/{namespaceId}/proposals/{proposalId}/infer-constraints"): _ontology_proxy_handler,
    ("GET", "/namespaces/{namespaceId}/proposals/{proposalId}/infer-constraints/jobs/{jobId}"): _ontology_proxy_handler,
    ("POST", "/namespaces/{namespaceId}/proposals/{proposalId}/compile-constraints"): _ontology_proxy_handler,
    ("POST", "/namespaces/{namespaceId}/proposals/{proposalId}/repair-datatypes"): _ontology_proxy_handler,
    ("POST", "/namespaces/{namespaceId}/proposals/update"): _ontology_proxy_handler,
    ("POST", "/namespaces/{namespaceId}/proposals/reject"): _ontology_proxy_handler,
    ("POST", "/namespaces/{namespaceId}/induce/datasources"): _ontology_proxy_handler,
    ("GET", "/namespaces/{namespaceId}/ontology/foundational"): _ontology_proxy_handler,
    ("POST", "/namespaces/{namespaceId}/ontology/foundational/{key}/load"): _ontology_proxy_handler,
    ("GET", "/namespaces/{namespaceId}/graph/search"): _ontology_proxy_handler,
    ("GET", "/namespaces/{namespaceId}/graph/ontology-overview"): _ontology_proxy_handler,
    ("GET", "/namespaces/{namespaceId}/graph/class"): _ontology_proxy_handler,
    ("GET", "/namespaces/{namespaceId}/graph/object-property"): _ontology_proxy_handler,
    ("GET", "/namespaces/{namespaceId}/graph/datatype-property"): _ontology_proxy_handler,
    ("GET", "/namespaces/{namespaceId}/ontologies"): _ontology_proxy_handler,
    ("DELETE", "/namespaces/{namespaceId}/ontologies"): _ontology_proxy_handler,
    ("GET", "/namespaces/{namespaceId}/ontologies/{ontologyId}"): _ontology_proxy_handler,
    ("GET", "/namespaces/{namespaceId}/ontologies/{ontologyId}/download"): _ontology_proxy_handler,
    ("POST", "/namespaces/{namespaceId}/ontologies/upload-url"): _ontology_proxy_handler,
    ("POST", "/namespaces/{namespaceId}/ontologies/{ontologyId}/fetch"): _ontology_proxy_handler,
    ("POST", "/namespaces/{namespaceId}/ontologies/{ontologyId}/ingest-from-s3"): _ontology_proxy_handler,
    ("GET", "/namespaces/{namespaceId}/ontologies/{ontologyId}/ingest-status/{jobId}"): _ontology_proxy_handler,
    ("GET", "/namespaces/{namespaceId}/embeddings"): _ontology_proxy_handler,
    ("POST", "/namespaces/{namespaceId}/embeddings/batch"): _ontology_proxy_handler,
    ("GET", "/namespaces/{namespaceId}/embeddings/search"): _ontology_proxy_handler,
    ("GET", "/namespaces/{namespaceId}/embeddings/by-entity/{entityUri}"): _ontology_proxy_handler,
    ("GET", "/namespaces/{namespaceId}/embeddings/by-ontology/{ontologyId}"): _ontology_proxy_handler,
    ("POST", "/namespaces/{namespaceId}/validate"): _ontology_proxy_handler,
    ("GET", "/namespaces/{namespaceId}/validate/jobs"): _ontology_proxy_handler,
    ("GET", "/namespaces/{namespaceId}/validate/jobs/{jobId}"): _ontology_proxy_handler,
    ("GET", "/system-health"): _ontology_proxy_handler,
    ("POST", "/namespaces/{namespaceId}/query"): _data_layer_handler,
    ("POST", "/namespaces/{namespaceId}/translate"): _data_layer_handler,
    ("POST", "/namespaces/{namespaceId}/kb/search"): _data_layer_handler,
    ("POST", "/namespaces/{namespaceId}/graph/traverse"): _data_layer_handler,
    ("GET", "/namespaces/{namespaceId}/schema"): _data_layer_handler,
}

# Resource templates ordered so literal segments beat greedy ones on match.
_TEMPLATE_RE = re.compile(r"\{[^}]+\}")


def _match_resource(path: str, method: str) -> tuple[str, dict[str, str]] | None:
    """Match an incoming path against the route table templates."""
    candidates: list[tuple[str, dict[str, str]]] = []
    for m, resource in ROUTES:
        if m != method:
            continue
        pattern = "^" + _TEMPLATE_RE.sub(lambda mt: f"(?P<{mt.group(0)[1:-1]}>[^/]+)", resource) + "$"
        match = re.match(pattern, path)
        if match:
            # Rank by number of literal segments (prefer more specific).
            literals = len([s for s in resource.split("/") if s and "{" not in s])
            candidates.append((resource, match.groupdict(), literals))
    if not candidates:
        return None
    candidates.sort(key=lambda c: -c[2])
    return candidates[0][0], candidates[0][1]


# ── Authorizer (invokes the real authorizer Lambda handler in-process) ─────


def _authorize(
    method: str, resource: str, path_params: dict[str, str], headers: dict[str, str]
) -> tuple[bool, dict[str, str]]:
    """Run the control-plane authorizer handler; returns (allowed, context)."""
    # methodArn shape: arn:aws:execute-api:<region>:<acct>:<api-id>/<stage>/<verb><path>
    method_arn = f"arn:aws:execute-api:{REGION}:000000000000:local/prod/{method}/{resource}"
    event = {
        "type": "REQUEST",
        "methodArn": method_arn,
        "httpMethod": method,
        "resource": resource,
        "path": resource,
        "headers": headers,
        "pathParameters": path_params,
        "requestContext": {
            "stage": "prod",
            "http": {"method": method, "path": resource},
        },
    }
    try:
        policy = _authorizer_handler()(event, None)
    except Exception:
        logger.exception("authorizer_failed")
        return False, {}
    effect = policy.get("policyDocument", {}).get("Statement", [{}])[0].get("Effect", "Deny")
    context = policy.get("context", {})
    return effect == "Allow", context


# ── Event adaptation ────────────────────────────────────────────────────────


def _build_event(
    method: str, resource: str, path_params: dict[str, str], query: dict[str, str], headers: dict[str, str], body: bytes
) -> dict:
    body_str: str | None = None
    is_b64 = False
    if body:
        try:
            body_str = body.decode("utf-8")
        except UnicodeDecodeError:
            body_str = base64.b64encode(body).decode()
            is_b64 = True
    lower_headers = {k.lower(): v for k, v in headers.items()}
    return {
        "httpMethod": method,
        "resource": resource,
        "path": resource,
        "pathParameters": path_params,
        "queryStringParameters": {k: v for k, v in query.items()} if query else None,
        "multiValueQueryStringParameters": None,
        "headers": lower_headers,
        "body": body_str,
        "isBase64Encoded": is_b64,
        "requestContext": {
            "stage": "prod",
            "http": {"method": method, "path": resource},
            "authorizer": {},  # filled after authorization
        },
    }


def _lambda_response_to_http(resp: dict) -> Response:
    status = int(resp.get("statusCode", 500))
    body = resp.get("body", "")
    headers = dict(resp.get("headers") or {})
    if resp.get("isBase64Encoded"):
        return Response(content=base64.b64decode(body), status_code=status, headers=headers)
    return Response(content=body, status_code=status, headers=headers)


# Routes whose responses can carry presigned S3 URLs: the sources document
# upload-urls route, and every .../proposals/... route — the OE embeds
# presigned GET URLs in GET /proposals/{id} (ontology_url/r2rml_url from
# presign_proposal_artifact, matches_url from presign_proposal_matches) and
# presigned PUT URLs in POST /proposals/{id}/upload-url (ontology_put_url/
# r2rml_put_url from presign_proposal_artifact_put). The remaining proposal
# routes carry no presigned URLs today but are covered so a future presigned
# field can't regress; bodies without an internal URL pass through untouched.
_UPLOAD_URLS_RESOURCE = "/namespaces/{namespaceId}/sources/upload-urls"
_PROPOSAL_RESOURCE_RE = re.compile(re.escape("/namespaces/{namespaceId}/proposals") + r"(/.*)?$")


def _rewrite_presigned_hosts(resp: dict) -> dict:
    """Rewrite container-internal LocalStack hosts in a presigned-URL response
    to the browser-reachable host (see the _S3_HOST_REWRITE block above).

    Walks the whole JSON body and rewrites every string value that is an
    internal-LocalStack URL (scheme+host+port match against the internal
    half), preserving path/query/fragment — covers uploadUrls[].uploadUrl as
    well as directly embedded presigned fields (ontology_url, r2rml_url,
    matches_url, ontology_put_url, r2rml_put_url). Non-JSON bodies, malformed
    JSON, and bodies without an internal-URL string pass through untouched.
    """
    from urllib.parse import urlsplit, urlunsplit

    (int_host, int_port), (ext_host, ext_port) = _S3_HOST_REWRITE  # type: ignore[misc]
    int_scheme = urlsplit(_LOCALSTACK_INTERNAL).scheme or "http"

    def _swap(url: str) -> str:
        parts = urlsplit(url)
        if (
            parts.scheme == int_scheme
            and (parts.hostname or "") == int_host
            and (parts.port or 4566) == int_port
        ):
            netloc = f"{ext_host}:{ext_port}" if ext_port else ext_host
            return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))
        return url

    changed = False

    def _walk(node: object) -> None:
        nonlocal changed
        if isinstance(node, dict):
            for key, value in node.items():
                if isinstance(value, str) and "://" in value:
                    swapped = _swap(value)
                    if swapped is not value:
                        node[key] = swapped
                        changed = True
                else:
                    _walk(value)
        elif isinstance(node, list):
            for i, value in enumerate(node):
                if isinstance(value, str) and "://" in value:
                    swapped = _swap(value)
                    if swapped is not value:
                        node[i] = swapped
                        changed = True
                else:
                    _walk(value)

    try:
        body = json.loads(resp.get("body") or "{}")
    except (json.JSONDecodeError, ValueError):
        return resp
    _walk(body)
    if not changed:
        return resp
    resp["body"] = json.dumps(body)
    return resp


# ── Routes ──────────────────────────────────────────────────────────────────


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
async def catch_all(request: Request, path: str) -> Response:
    method = request.method
    actual_path = "/" + path

    if method == "OPTIONS":
        return Response(status_code=204)

    # Health endpoint for the container
    if actual_path == "/health":
        return JSONResponse({"status": "ok"})

    match = _match_resource(actual_path, method)
    if not match:
        return JSONResponse({"message": f"No route: {method} {actual_path}"}, status_code=404)
    resource, path_params = match

    # Build lowercase headers, preserving Authorization
    headers = {k.lower(): v for k, v in request.headers.items()}

    # ── Authorize ─────────────────────────────────────────────────────
    allowed, auth_context = _authorize(method, resource, path_params, headers)
    if not allowed:
        return JSONResponse({"message": "Unauthorized"}, status_code=403)

    body = await request.body()
    query = {k: v for k, v in request.query_params.items()}
    event = _build_event(method, resource, path_params, query, headers, body)
    event["requestContext"]["authorizer"] = {
        "principalId": auth_context.get("userId", ""),
        "email": auth_context.get("email", ""),
        "groups": auth_context.get("groups", ""),
        "globalRoles": auth_context.get("globalRoles", ""),
        "claims": {"sub": auth_context.get("sub", ""), "email": auth_context.get("email", "")},
    }

    handler_factory = ROUTES[(method, resource)]
    handler = handler_factory()

    import asyncio

    loop = asyncio.get_running_loop()
    try:
        resp = await loop.run_in_executor(None, lambda: handler(event, None))
    except Exception:
        logger.exception("handler_error route=%s", resource)
        return JSONResponse({"message": "Internal server error"}, status_code=500)

    if _S3_HOST_REWRITE and (resource == _UPLOAD_URLS_RESOURCE or _PROPOSAL_RESOURCE_RE.match(resource)):
        resp = _rewrite_presigned_hosts(resp)

    return _lambda_response_to_http(resp)


@app.get("/_gateway/health")
async def health() -> JSONResponse:
    return JSONResponse({"status": "ok"})
