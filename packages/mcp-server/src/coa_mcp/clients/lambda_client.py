# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Lambda client for direct invocation of backend Lambdas.

Builds synthetic API Gateway v1.0 proxy integration events and invokes
backend Lambdas directly, bypassing API Gateway.
"""

from __future__ import annotations

import asyncio
import json
import os
from uuid import uuid4

import boto3
import structlog
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

logger = structlog.get_logger(__name__)

_LAMBDA_INVOKE_TIMEOUT_S = int(os.environ.get("LAMBDA_INVOKE_TIMEOUT_S", "30"))


class LambdaInvokeError(Exception):
    """Raised when Lambda invocation itself fails (function error, timeout, throttle)."""


class LambdaApiError(Exception):
    """Raised when the Lambda returns a non-2xx statusCode in its API GW response."""

    def __init__(self, status_code: int, body: str, message: str = ""):
        """Capture the failing status code and response body.

        Args:
            status_code: The non-2xx status code returned by the Lambda.
            body: The raw response body.
            message: Optional override for the exception message; a summary is
                derived from the status code and body when omitted.
        """
        self.status_code = status_code
        self.body = body
        super().__init__(message or f"Lambda returned {status_code}: {body[:200]}")


class LambdaClient:
    """Invoke backend Lambdas with synthetic API Gateway proxy events."""

    def __init__(self, region: str):
        """Initialize the boto3 Lambda client with fixed timeout and retry config.

        Args:
            region: AWS region hosting the backend Lambdas.
        """
        self._region = region
        # Sync customer path: explicit connect/read timeouts and fail-fast
        # (max_attempts=1 == no retries) — the caller decides when to retry.
        self._lambda_client = boto3.client(
            "lambda",
            region_name=region,
            config=BotoConfig(
                read_timeout=_LAMBDA_INVOKE_TIMEOUT_S,
                connect_timeout=5,
                retries={"max_attempts": 1},
            ),
        )
        logger.info("lambda_client_initialized", region=region)

    def _build_event(
        self,
        method: str,
        resource: str,
        path_params: dict,
        query_params: dict | None,
        body: dict | None,
        bearer_token: str,
        authorizer_context: dict | None = None,
    ) -> dict:
        """Build a synthetic API Gateway v1.0 proxy integration event."""
        # Substitute path params into resource to get the resolved path
        resolved_path = resource
        for key, value in path_params.items():
            resolved_path = resolved_path.replace(f"{{{key}}}", value)

        request_context: dict = {"stage": "mcp", "requestId": str(uuid4())}
        if authorizer_context:
            request_context["authorizer"] = authorizer_context

        return {
            "httpMethod": method,
            "resource": resource,
            "path": resolved_path,
            "pathParameters": path_params,
            "queryStringParameters": query_params or None,
            "headers": {
                "Authorization": f"Bearer {bearer_token}",
                "Content-Type": "application/json",
            },
            "body": json.dumps(body) if body else None,
            "isBase64Encoded": False,
            "requestContext": request_context,
        }

    def _invoke_sync(self, function_name: str, payload: bytes) -> dict:
        """Synchronous Lambda invoke (called via asyncio.to_thread)."""
        return self._lambda_client.invoke(
            FunctionName=function_name,
            InvocationType="RequestResponse",
            Payload=payload,
        )

    # Local Docker stack: GATEWAY_ENDPOINT routes "Lambda invocations" to the
    # local gateway over HTTP instead of boto3→Lambda (see invoke_api).
    _GATEWAY_ENDPOINT = os.environ.get("GATEWAY_ENDPOINT", "").rstrip("/")

    async def invoke_api(
        self,
        function_name: str,
        method: str,
        resource: str,
        path_params: dict,
        query_params: dict | None = None,
        body: dict | None = None,
        bearer_token: str = "",
        authorizer_context: dict | None = None,
    ) -> dict:
        """Invoke Lambda, parse API GW proxy response, return body as dict.

        Args:
            function_name: Lambda function name or ARN.
            method: HTTP method (GET, POST, etc.).
            resource: API Gateway resource path with placeholders.
            path_params: Parameters to substitute into the resource path.
            query_params: Optional query string parameters.
            body: Optional request body (will be JSON-serialized).
            bearer_token: Bearer token for Authorization header.
            authorizer_context: Optional authorizer context to inject into
                requestContext.authorizer (mirrors API GW custom authorizer output).

        Returns:
            Parsed JSON body from the Lambda's API GW proxy response.

        Raises:
            LambdaInvokeError: When the Lambda invocation itself fails.
            LambdaApiError: When the Lambda returns a non-2xx status code.
        """
        event = self._build_event(method, resource, path_params, query_params, body, bearer_token, authorizer_context)
        payload = json.dumps(event).encode()

        # ── Local Docker stack: route through the local gateway over HTTP ──
        if self._GATEWAY_ENDPOINT:
            import httpx

            # Map the synthetic event back to a real HTTP request against the
            # gateway. The gateway re-derives authorizer context from the JWT,
            # so the bearer token is all that needs forwarding.
            resolved_path = resource
            for key, value in (path_params or {}).items():
                resolved_path = resolved_path.replace(f"{{{key}}}", value)
            url = f"{self._GATEWAY_ENDPOINT}{resolved_path}"
            try:
                async with httpx.AsyncClient(
                    timeout=httpx.Timeout(connect=5.0, read=_LAMBDA_INVOKE_TIMEOUT_S)
                ) as client:
                    resp = await client.request(
                        method,
                        url,
                        params=query_params or None,
                        json=body if body else None,
                        headers={"Authorization": f"Bearer {bearer_token}"},
                    )
            except httpx.HTTPError as exc:
                logger.error("gateway_http_error", url=url, error=str(exc))
                raise LambdaInvokeError(f"Gateway invoke failed: {exc}") from exc
            if resp.status_code >= 400:
                raise LambdaApiError(status_code=resp.status_code, body=resp.text)
            return resp.json() if resp.text else {}

        try:
            response = await asyncio.to_thread(self._invoke_sync, function_name, payload)
        except ClientError as exc:
            logger.error("lambda_invoke_client_error", function=function_name, error=str(exc))
            raise LambdaInvokeError(f"Lambda invoke failed: {exc}") from exc

        # Check for Lambda-level errors (unhandled exceptions, timeouts)
        if "FunctionError" in response:
            error_payload = response["Payload"].read().decode()
            logger.error(
                "lambda_function_error",
                function=function_name,
                error=error_payload[:500],
            )
            raise LambdaInvokeError(f"Lambda function error: {error_payload[:500]}")

        # Parse the response payload
        response_body = json.loads(response["Payload"].read().decode())
        status_code = response_body.get("statusCode", 200)

        if status_code >= 400:
            body_str = response_body.get("body", "")
            logger.warning(
                "lambda_api_error",
                function=function_name,
                status=status_code,
                body=body_str[:200],
            )
            raise LambdaApiError(status_code=status_code, body=body_str)

        # Parse the body JSON from the proxy response
        raw_body = response_body.get("body", "{}")
        if isinstance(raw_body, str):
            return json.loads(raw_body)
        return raw_body
