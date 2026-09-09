FROM python:3.12-slim

WORKDIR /app

# Java for ontology-engine deps is not needed here; the gateway only routes to
# the ontology-engine over HTTP. boto3 talks to LocalStack.
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# Shared common library
COPY libs/common/ /tmp/common/
RUN pip install --no-cache-dir /tmp/common/ && rm -rf /tmp/common/

# Generated server models (request models used by handlers)
COPY smithy-generated/control-plane-python-server/ /tmp/cp-server/
COPY smithy-generated/data-layer-python-server/ /tmp/dl-server/
RUN pip install --no-cache-dir /tmp/cp-server/ /tmp/dl-server/ && rm -rf /tmp/cp-server/ /tmp/dl-server/

# Service packages (handlers run in-process)
COPY packages/control-plane/pyproject.toml /tmp/pkg/control-plane/pyproject.toml
COPY packages/control-plane/src/ /tmp/pkg/control-plane/src/
COPY packages/sources/pyproject.toml /tmp/pkg/sources/pyproject.toml
COPY packages/sources/src/ /tmp/pkg/sources/src/
COPY packages/metric-service/pyproject.toml /tmp/pkg/metric-service/pyproject.toml
COPY packages/metric-service/src/ /tmp/pkg/metric-service/src/
COPY packages/data-layer/pyproject.toml /tmp/pkg/data-layer/pyproject.toml
COPY packages/data-layer/src/ /tmp/pkg/data-layer/src/
# The ontology proxy handler (coa_ontology.api_proxy_handler) runs in-process
# here, mirroring the Lambda deployment — only its boto3/urllib deps needed.
COPY packages/ontology-engine/pyproject.toml /tmp/pkg/ontology-engine/pyproject.toml
COPY packages/ontology-engine/src/ /tmp/pkg/ontology-engine/src/

# Gateway app
COPY docker/gateway/pyproject.toml /app/
COPY docker/gateway/src/ /app/src/

# Install service deps: parse each package's pyproject [project.dependencies]
# via uv (handles the same pins the workspace uses).
RUN pip install --no-cache-dir uv && \
    cd /tmp/pkg/control-plane && sed -i '/\[tool.uv.sources\]/,$ d' pyproject.toml && uv pip install --system --no-cache . && \
    cd /tmp/pkg/sources && sed -i '/\[tool.uv.sources\]/,$ d' pyproject.toml && uv pip install --system --no-cache . && \
    cd /tmp/pkg/metric-service && sed -i '/\[tool.uv.sources\]/,$ d' pyproject.toml && uv pip install --system --no-cache . && \
    cd /tmp/pkg/data-layer && sed -i '/\[tool.uv.sources\]/,$ d' pyproject.toml && uv pip install --system --no-cache . && \
    cd /tmp/pkg/ontology-engine && sed -i '/\[tool.uv.sources\]/,$ d' pyproject.toml && uv pip install --system --no-cache --no-deps . && \
    cd /app && sed -i '/\[tool.uv.sources\]/,$ d' pyproject.toml && uv pip install --system --no-cache . && \
    rm -rf /tmp/pkg

ENV PYTHONPATH=/app/src
EXPOSE 9090

HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=5 \
  CMD curl -sf http://localhost:9090/health || exit 1

CMD ["uvicorn", "local_gateway.main:app", "--host", "0.0.0.0", "--port", "9090"]
