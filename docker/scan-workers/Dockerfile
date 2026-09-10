# Scan worker — runs the DB scan / doc ingestion / doc deletion pipelines as
# SQS-polling loops (replaces Step Functions + Fargate in the local stack).
# Build context: repo root.
FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl default-jre-headless && \
    apt-get upgrade -y && rm -rf /var/lib/apt/lists/*

RUN useradd --no-create-home --shell /bin/false appuser

# Shared common library
COPY libs/common/ /tmp/common/
RUN pip install --no-cache-dir /tmp/common/ && rm -rf /tmp/common/

# Generated server models
COPY smithy-generated/control-plane-python-server/ /tmp/cp-server/
RUN pip install --no-cache-dir /tmp/cp-server/ && rm -rf /tmp/cp-server/

# Sources package (handlers + kg_build)
COPY packages/sources/pyproject.toml /tmp/sources/pyproject.toml
COPY packages/sources/src/ /tmp/sources/src/
# DB drivers the discovery dialects import lazily (pg8000, pymysql, pytds,
# oracledb, snowflake) — declared in the package's Lambda requirements.txt,
# not in pyproject dependencies.
COPY packages/sources/requirements.txt /tmp/sources/requirements.txt
# kg-build runtime deps (graphrag-lexical-graph + llama-index OpenSearch
# vector-store stack) — the same pins the standalone kg-build image uses
# (packages/sources/documents/kg-build/requirements.txt). In production these
# live in the dedicated doc-kg-build Fargate image; the local worker runs
# kg_build in-process via subprocess, so they must be present here too.
COPY packages/sources/documents/kg-build/requirements.txt /tmp/kg-build-requirements.txt
RUN pip install --no-cache-dir uv && \
    cd /tmp/sources && sed -i '/\[tool.uv.sources\]/,$ d' pyproject.toml && \
    uv pip install --system --no-cache . && \
    uv pip install --system --no-cache -r requirements.txt && \
    uv pip install --system --no-cache -r /tmp/kg-build-requirements.txt && rm -rf /tmp/sources

# Worker app
COPY docker/scan-workers/worker.py /app/worker.py

ENV PYTHONPATH=/app

USER appuser
CMD ["python", "/app/worker.py"]
