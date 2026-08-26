# Build context must be the repo root:
#   docker build -f packages/sources/database/enrichment/Dockerfile .
# Pin to digest to prevent supply-chain attacks via tag mutation.
# python:3.12-slim built 2026-05-19 — includes glibc deb13u3, systemd/sed/libcap2 deb13u1 CVE fixes.
FROM --platform=linux/amd64 public.ecr.aws/docker/library/python:3.12-slim@sha256:7a8b475003c4fe15a2cd4e55e5cfc2f3560bdc9333d624f24cdd6d4340fd7a17

WORKDIR /app

# Install shared common library
COPY libs/common/ /tmp/common/
RUN pip install --no-cache-dir /tmp/common/ && rm -rf /tmp/common/

# Install smithy-generated models
COPY smithy-generated/control-plane-python-server/ /tmp/server/
RUN pip install --no-cache-dir /tmp/server/ && rm -rf /tmp/server/

# Install the database package (includes all dependencies)
COPY packages/sources/ /tmp/sources/
RUN pip install --no-cache-dir /tmp/sources/ && rm -rf /tmp/sources/

USER 1000
ENTRYPOINT ["python", "-m", "coa_sources.database.pipeline.enrichment_handler"]
