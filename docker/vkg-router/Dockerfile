# VKG router — extends the production VKG image (Ontop CLI + H2) with the
# router app. Build context: repo root.
FROM public.ecr.aws/amazoncorretto/amazoncorretto:17-al2023

# Ontop CLI (same pins as packages/vkg/Dockerfile)
ARG ONTOP_VERSION=5.5.0
ARG ONTOP_SHA256=430dff312e68e8ad41d26e6113160ca2365c28c4fe911926193000a33299458f
ARG H2_VERSION=2.2.224
ARG H2_SHA256=b9d8f19358ada82a4f6eb5b174c6cfe320a375b5a9cb5a4fe456d623e6e55497

RUN yum install -y python3 python3-pip unzip shadow-utils findutils && \
  yum update -y --security && \
  curl -fsSL "https://github.com/ontop/ontop/releases/download/ontop-${ONTOP_VERSION}/ontop-cli-${ONTOP_VERSION}.zip" \
  -o /tmp/ontop.zip && \
  echo "${ONTOP_SHA256}  /tmp/ontop.zip" | sha256sum -c - && \
  unzip /tmp/ontop.zip -d /opt/ontop && \
  rm /tmp/ontop.zip && \
  chmod +x /opt/ontop/ontop && \
  mkdir -p /opt/ontop/jdbc && \
  curl -fsSL "https://repo1.maven.org/maven2/com/h2database/h2/${H2_VERSION}/h2-${H2_VERSION}.jar" \
  -o /opt/ontop/jdbc/h2-${H2_VERSION}.jar && \
  echo "${H2_SHA256}  /opt/ontop/jdbc/h2-${H2_VERSION}.jar" | sha256sum -c - && \
  yum clean all && rm -rf /var/cache/yum

ENV PATH="/opt/ontop:${PATH}"

RUN pip3 install --no-cache-dir fastapi uvicorn boto3

RUN useradd -r -s /sbin/nologin ontop && \
  mkdir -p /app/artifacts && \
  chown -R ontop:ontop /app

COPY --chown=ontop:ontop docker/vkg-router/router.py /app/router.py

USER ontop
WORKDIR /app
EXPOSE 8180

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD curl -sf http://localhost:8180/health || exit 1

CMD ["python3", "/app/router.py"]
