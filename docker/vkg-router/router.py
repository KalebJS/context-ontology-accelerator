# Copyright Amazon.com, Inc. or its affiliates. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""VKG router — local replacement for per-namespace Ontop ECS services.

In production, the CM rewrites ``://vkg.`` → ``://vkg-{ns}.`` and CloudMap
resolves one ECS service per namespace (each running Ontop + translate-server).

Locally, the CM is configured with ``VKG_ROUTER_MODE=true`` and a single
endpoint. This router:

1. receives POST /sparql/translate?namespace={ns}
2. lazily spawns an Ontop process for that namespace, using ontology
   artifacts (ttl + obda/r2rml + sql schema) downloaded from LocalStack S3
   at ``s3://{ONTOLOGY_BUCKET}/ontologies/{ns}/{version}/``
3. proxies the translation to that Ontop and caches the process per namespace

Health: GET /health → {"status": "ok"} once running (degraded is fine —
per-namespace processes spawn lazily).
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import shutil
import signal
import subprocess
import urllib.parse
import urllib.request

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger("vkg-router")

app = FastAPI(title="coa-vkg-router", docs_url=None, redoc_url=None)

ONTOLOGY_BUCKET = os.environ["ONTOLOGY_BUCKET"]
ONTOLOGY_PREFIX = os.environ.get("ONTOLOGY_PREFIX", "ontologies/")
LOCALSTACK = os.environ.get("LOCALSTACK_ENDPOINT", "http://localstack:4566")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

ARTIFACT_ROOT = "/app/artifacts"
ONTOP_PORT_BASE = 8300  # per-namespace Ontop ports start here
MAX_PROCESSES = 32

_processes: dict[str, dict] = {}  # ns → {"pid", "port", "version"}


def _s3_client():
    import boto3

    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        endpoint_url=LOCALSTACK,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
    )


def _download_artifacts(namespace: str) -> dict:
    """Download the latest ontology artifacts for a namespace from S3."""
    s3 = _s3_client()
    prefix = f"{ONTOLOGY_PREFIX}{namespace}/"
    try:
        resp = s3.list_objects_v2(Bucket=ONTOLOGY_BUCKET, Prefix=prefix, Delimiter="/")
    except Exception as e:
        return {"error": f"s3_list_failed: {e}"}
    versions = [c["Prefix"] for c in resp.get("CommonPrefixes", [])]
    if not versions:
        return {"error": "no_versions"}
    latest = sorted(versions)[-1]  # lexicographically latest version prefix
    dest = f"{ARTIFACT_ROOT}/{namespace}"
    os.makedirs(dest, exist_ok=True)
    objs = s3.list_objects_v2(Bucket=ONTOLOGY_BUCKET, Prefix=latest)
    for obj in objs.get("Contents", []):
        key = obj["Key"]
        rel = key[len(latest) :]
        if not rel or rel.endswith("/"):
            continue
        target = os.path.join(dest, rel)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        s3.download_file(ONTOLOGY_BUCKET, key, target)
    return {"dir": dest, "version": latest.rstrip("/").split("/")[-1]}


def _find_file(directory: str, extensions: tuple[str, ...]) -> str | None:
    for root, _dirs, files in os.walk(directory):
        for f in sorted(files):
            if f.lower().endswith(extensions) and not f.endswith(".obda"):
                if extensions == (".obda", ".r2rml"):
                    continue
                return os.path.join(root, f)
    return None


def _spawn_ontop(namespace: str) -> dict:
    """Spawn an Ontop endpoint process for a namespace; returns its port."""
    if namespace in _processes:
        return {"ok": True, **_processes[namespace]}

    if len(_processes) >= MAX_PROCESSES:
        # Evict oldest
        oldest = next(iter(_processes))
        _stop(oldest)

    info = _download_artifacts(namespace)
    if "error" in info:
        return {"ok": False, "error": info["error"]}

    directory = info["dir"]
    ontology = _find_file(directory, (".ttl", ".owl"))
    mappings = _find_file(directory, (".obda", ".r2rml"))
    if not ontology or not mappings:
        return {"ok": False, "error": "missing_artifacts"}

    # Optional schema.sql → in-memory H2 (matches the ECS image entrypoint)
    schema = _find_file(directory, (".sql",))
    if schema:
        subprocess.run(
            [
                "java",
                "-cp",
                "/opt/ontop/lib/*:/opt/ontop/jdbc/*",
                "org.h2.tools.RunScript",
                "-url",
                f"jdbc:h2:{directory}/vkgdb;DB_CLOSE_DELAY=-1",
                "-user",
                "sa",
                "-script",
                schema,
            ],
            check=False,
            capture_output=True,
        )
    props = os.path.join(directory, "ontop.properties")
    with open(props, "w") as f:
        f.write(
            f"jdbc.url=jdbc:h2:{directory}/vkgdb;DB_CLOSE_DELAY=-1\n"
            "jdbc.driver=org.h2.Driver\njdbc.user=sa\njdbc.password=\n"
        )

    port = ONTOP_PORT_BASE + len(_processes)
    log_file = os.path.join(directory, "ontop.log")
    with open(log_file, "ab") as lf:
        proc = subprocess.Popen(
            [
                "/opt/ontop/ontop",
                "endpoint",
                f"--ontology={ontology}",
                f"--mapping={mappings}",
                f"--properties={props}",
                f"--port={port}",
                "--lazy",
                "--dev",
            ],
            cwd=directory,
            stdout=lf,
            stderr=lf,
        )
    _processes[namespace] = {"pid": proc.pid, "port": port, "version": info["version"], "proc": proc}
    logger.info("ontop_spawned ns=%s port=%d", namespace, port)
    return {"ok": True, **{k: v for k, v in _processes[namespace].items() if k != "proc"}}


def _stop(namespace: str) -> None:
    info = _processes.pop(namespace, None)
    if info:
        with contextlib.suppress(OSError):
            os.kill(info["pid"], signal.SIGTERM)


def _wait_ontop(port: int, timeout: int = 90) -> bool:
    import time

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"http://localhost:{port}/actuator/health", timeout=2) as r:
                if r.status == 200:
                    return True
        except Exception:
            time.sleep(1)
    return False


@app.get("/health")
async def health() -> JSONResponse:
    return JSONResponse({"status": "ok", "namespaces": list(_processes.keys())})


@app.post("/sparql/translate")
async def translate(request: Request, namespace: str = "") -> JSONResponse:
    """Proxy a translation request to the namespace's Ontop process.

    Mirrors the VKG translate-server contract: the request body carries the
    SPARQL query; the response carries SQL, dialect, ontology version and
    datasource routing.
    """
    if not namespace:
        return JSONResponse({"error": "namespace required"}, status_code=400)
    body = await request.body()

    info = _spawn_ontop(namespace)
    if not info.get("ok"):
        return JSONResponse(
            {"error": "vkg_not_available", "detail": info.get("error"), "namespace": namespace},
            status_code=503,
        )

    ontop_port = info["port"]
    if not _wait_ontop(ontop_port):
        return JSONResponse({"error": "ontop_start_timeout", "namespace": namespace}, status_code=503)

    # Forward to Ontop's reformulate endpoint (started with --dev), then wrap
    # in the translate-server response shape.
    try:
        payload = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return JSONResponse({"error": "invalid_json"}, status_code=400)
    sparql = payload.get("sparql") or payload.get("query") or ""
    if not sparql:
        return JSONResponse({"error": "missing sparql"}, status_code=400)

    req = urllib.request.Request(
        f"http://localhost:{ontop_port}/ontop/reformulate",
        data=json.dumps({"sparql": sparql}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            ontop_data = json.loads(resp.read())
    except Exception as e:
        return JSONResponse({"error": "ontop_translate_failed", "detail": str(e)}, status_code=502)

    sql = ontop_data.get("sql") or ""
    projection = ontop_data.get("projection") or {}
    return JSONResponse(
        {
            "sql": sql,
            "dialect": "postgresql",
            "ontologyVersion": info.get("version", "latest"),
            "sourceTableRefs": ontop_data.get("sourceTableRefs", []),
            "datasourceRouting": ontop_data.get("datasourceRouting", {}),
            "projection": projection,
        }
    )


@app.on_event("shutdown")
async def shutdown() -> None:
    for ns in list(_processes):
        _stop(ns)


if __name__ == "__main__":
    import uvicorn

    # Ontop binary presence check (image bundles it).
    ontop = shutil.which("ontop")
    logger.info("vkg_router_starting (ontop=%s, bucket=%s)", ontop, ONTOLOGY_BUCKET)
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8180")))
