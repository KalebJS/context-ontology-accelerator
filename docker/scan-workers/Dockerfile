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
RUN pip install --no-cache-dir uv && \
    cd /tmp/sources && sed -i '/\[tool.uv.sources\]/,$ d' pyproject.toml && \
    uv pip install --system --no-cache . && \
    uv pip install --system --no-cache -r requirements.txt && rm -rf /tmp/sources

# Worker app
COPY docker/scan-workers/worker.py /app/worker.py

ENV PYTHONPATH=/app

USER appuser
CMD ["python", "/app/worker.py"]
