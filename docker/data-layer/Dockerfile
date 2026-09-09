# Data-layer FastAPI wrapper — serves the existing Lambda handler over HTTP.
# Build context: repo root.
FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    apt-get upgrade -y && rm -rf /var/lib/apt/lists/*

RUN useradd --no-create-home --shell /bin/false appuser

COPY libs/common/ /tmp/common/
RUN pip install --no-cache-dir /tmp/common/ && rm -rf /tmp/common/

COPY smithy-generated/control-plane-python-server/ /tmp/cp-server/
COPY smithy-generated/data-layer-python-server/ /tmp/dl-server/
RUN pip install --no-cache-dir /tmp/cp-server/ /tmp/dl-server/ && rm -rf /tmp/cp-server/ /tmp/dl-server/

COPY packages/data-layer/pyproject.toml /tmp/pkg/pyproject.toml
COPY packages/data-layer/src/ /tmp/pkg/src/
RUN pip install --no-cache-dir uv fastapi "uvicorn[standard]" && \
    cd /tmp/pkg && sed -i '/\[tool.uv.sources\]/,$ d' pyproject.toml && \
    uv pip install --system --no-cache . && rm -rf /tmp/pkg

COPY docker/data-layer/server.py /app/server.py

ENV PYTHONPATH=/app

USER appuser
EXPOSE 8080

HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=10 \
  CMD curl -sf http://localhost:8080/health || exit 1

CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8080"]
