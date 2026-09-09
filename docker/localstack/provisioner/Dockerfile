FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir boto3

COPY provision.py /app/provision.py

# Seed Cedar files are mounted from libs/common (read-only).
ENTRYPOINT ["python", "/app/provision.py"]
