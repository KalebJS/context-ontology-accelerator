# Copyright Amazon.com, Inc. or its affiliates. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Data-layer HTTP wrapper — serves the existing Lambda handler over FastAPI.

The handler's route map covers POST /namespaces/{id}/{query,translate,...} and
GET /namespaces/{id}/{schema,metrics}; this adapter builds API GW proxy events
from FastAPI requests and forwards the Lambda proxy response as HTTP.

ONTOLOGY_PROXY_ENDPOINT / METRIC_SERVICE_ENDPOINT: the handler invokes the
ontology proxy and metric-service Lambdas via boto3; in the local stack those
are the ontology-engine container and the gateway. We patch the handler's
_lambda_client with a small shim that POSTs the proxy event to the gateway's
/_lambda route, which executes it in-process — no Lambda emulation service
needed.
"""

from __future__ import annotations

import json
import os

import coa_data_layer.handler as dl
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

app = FastAPI(title="coa-data-layer", docs_url=None, redoc_url=None)

GATEWAY = os.environ.get("METRIC_SERVICE_ENDPOINT", "").rstrip("/")
OE_ENDPOINT = os.environ.get("ONTOLOGY_PROXY_ENDPOINT", "").rstrip("/")


def _shim_invoke_lambda(function_name: str, proxy_event: dict) -> dict:
    """Local replacement for boto3 Lambda invoke: run the target handler in-process.

    function_name is an SSM-style ARN in production; locally we route by which
    backend the event targets:
      resource starts with /namespaces/{ns}/schema → ontology proxy handler
      resource starts with /namespaces/{ns}/metrics → metric API handler
    """
    resource = proxy_event.get("resource", "")
    if "/schema" in resource:
        from coa_ontology.api_proxy_handler import handler as ontology_proxy

        ontology_proxy.__globals__["ENDPOINT"] = OE_ENDPOINT
        return ontology_proxy(proxy_event, None)
    if "/metrics" in resource:
        from coa_metrics.api.metric_api_handler import handler as metrics_api

        return metrics_api(proxy_event, None)
    return {"statusCode": 501, "headers": {}, "body": json.dumps({"message": "unknown shim target"})}


class _LambdaShim:
    """Mimics the boto3 Lambda client surface the data-layer handler uses."""

    class _Payload:
        def __init__(self, data: bytes):
            self._data = data

        def read(self) -> bytes:
            return self._data

    def invoke(self, FunctionName: str, InvocationType: str, Payload: bytes) -> dict:  # noqa: N803
        import json as _json

        proxy_event = _json.loads(Payload.decode("utf-8"))
        resp = _shim_invoke_lambda(FunctionName, proxy_event)
        return {"Payload": self._Payload(_json.dumps(resp).encode("utf-8"))}


def _install_shims() -> None:
    import coa_data_layer.handler as dl_mod

    dl_mod._lambda_client = _LambdaShim()  # noqa: SLF001 — intentional local seam


if GATEWAY or OE_ENDPOINT:
    _install_shims()


@app.get("/health")
async def health() -> JSONResponse:
    return JSONResponse({"status": "ok"})


@app.api_route("/namespaces/{namespace_id}/{rest:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def route(namespace_id: str, rest: str, request: Request) -> Response:
    method = request.method
    actual = f"/namespaces/{namespace_id}" + (f"/{rest}" if rest else "")
    resource = _resource_from_path(method, actual)
    if not resource:
        return JSONResponse({"message": f"no route: {method} {actual}"}, status_code=404)
    body = await request.body()
    event = {
        "httpMethod": method,
        "resource": resource,
        "path": actual,
        "pathParameters": {"namespaceId": namespace_id},
        "queryStringParameters": {k: v for k, v in request.query_params.items()} or None,
        "headers": {k.lower(): v for k, v in request.headers.items()},
        "body": body.decode("utf-8") if body else None,
        "isBase64Encoded": False,
        "requestContext": {
            "stage": "data-layer",
            "http": {"method": method, "path": actual},
            "authorizer": {
                "principalId": request.headers.get("x-coa-userid", ""),
                "email": request.headers.get("x-coa-email", ""),
                "groups": request.headers.get("x-coa-groups", ""),
                "globalRoles": request.headers.get("x-coa-globalroles", ""),
                "claims": {},
            },
        },
    }
    import asyncio

    loop = asyncio.get_running_loop()
    try:
        resp = await loop.run_in_executor(None, lambda: dl.handler(event, None))
    except Exception:
        import logging

        logging.getLogger(__name__).exception("data_layer_handler_error")
        return JSONResponse({"message": "Internal server error"}, status_code=500)
    status = int(resp.get("statusCode", 500))
    return Response(content=resp.get("body", ""), status_code=status, headers=dict(resp.get("headers") or {}))


def _resource_from_path(method: str, path: str) -> str | None:
    import re

    import coa_data_layer.handler as dl_mod

    # _ROUTE_MAP keys are (method, resource) tuples
    for m, resource in getattr(dl_mod, "_ROUTE_MAP", {}):
        if m != method:
            continue
        pattern = "^" + re.sub(r"\{[^}]+\}", r"[^/]+", resource) + "$"
        if re.match(pattern, path):
            return resource
    return None
