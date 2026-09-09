# Copyright Amazon.com, Inc. or its affiliates. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Scan worker — SQS long-poll loop replacing the Step Functions pipelines.

Each pipeline is a fixed sequence of handler invocations, executed inline and
with the same DynamoDB status writes the state machines perform. Queues:

  db-scan        → discovery → federation → enrichment → status COMPLETED
  doc-ingestion  → preprocessing → save results → kg-build → status COMPLETED
  doc-deletion   → s3 cleanup → graph cleanup → delete DDB record

Failure anywhere writes the FAILED/SCAN_FAILED terminal status the state
machines' error chains produce, so sources are never stranded active.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import time
from typing import Any

import boto3
from botocore.config import Config

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger("scan-worker")

PREFIX = os.environ.get("SCL_PREFIX", "coa")
ENV = os.environ.get("SCL_ENV", "local")
REGION = os.environ.get("AWS_REGION", "us-east-1")
LOCALSTACK = os.environ.get("LOCALSTACK_ENDPOINT", "http://localstack:4566")

_CFG = Config(retries={"max_attempts": 5, "mode": "adaptive"}, region_name=REGION)
_session = boto3.Session(
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
    region_name=REGION,
)
sqs = _session.client("sqs", endpoint_url=LOCALSTACK, config=_CFG)
ddb = _session.client("dynamodb", endpoint_url=LOCALSTACK, config=_CFG)

SOURCES_TABLE = f"{PREFIX}-{ENV}-sources"
SCAN_JOBS_TABLE = f"{PREFIX}-{ENV}-source-scan-jobs"
Q_DB_SCAN = f"{PREFIX}-{ENV}-sources-db-scan-queue"
Q_DOC_INGEST = f"{PREFIX}-{ENV}-sources-doc-ingestion-queue"
Q_DOC_DELETE = f"{PREFIX}-{ENV}-sources-doc-deletion-queue"
# Bulk approve/reject (ApproveSource / RejectSource). Production runs this as
# its own Lambda behind the queue; locally the scan-workers container polls it.
Q_BULK_REVIEW = f"{PREFIX}-local-sources-bulk-review-queue"


def _url(name: str) -> str:
    return sqs.get_queue_url(QueueName=name)["QueueUrl"]


def _get_source(source_id: str, namespace_id: str) -> dict:
    resp = ddb.get_item(
        TableName=SOURCES_TABLE,
        Key={"PK": {"S": f"NS#{namespace_id}"}, "SK": {"S": f"SRC#{source_id}"}},
    )
    return resp.get("Item", {})


def _update_source_status(source_id: str, namespace_id: str, status: str, extra: dict | None = None) -> None:
    expr = "SET #s = :s, updatedAt = :u"
    names = {"#s": "status"}
    values: dict[str, Any] = {":s": {"S": status}, ":u": {"S": _now()}}
    if extra:
        for i, (k, v) in enumerate(extra.items()):
            names[f"#a{i}"] = k
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                values[f":a{i}"] = {"N": str(v)}
            else:
                values[f":a{i}"] = {"S": str(v)}
            expr += f", #a{i} = :a{i}"
    ddb.update_item(
        TableName=SOURCES_TABLE,
        Key={"PK": {"S": f"NS#{namespace_id}"}, "SK": {"S": f"SRC#{source_id}"}},
        UpdateExpression=expr,
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
    )


def _update_scan_job(key_pk: str, key_sk: str, status: str, extra: dict | None = None) -> None:
    values: dict[str, Any] = {":s": {"S": status}, ":u": {"S": _now()}}
    expr = "SET #s = :s, updatedAt = :u"
    names = {"#s": "status"}
    if extra:
        for i, (k, v) in enumerate(extra.items()):
            names[f"#a{i}"] = k
            values[f":a{i}"] = {"S": str(v)}
            expr += f", #a{i} = :a{i}"
    ddb.update_item(
        TableName=SCAN_JOBS_TABLE,
        Key={"PK": {"S": key_pk}, "SK": {"S": key_sk}},
        UpdateExpression=expr,
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
    )


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


# ── Pipeline: db scan ───────────────────────────────────────────────────────


def run_db_scan(msg: dict) -> None:
    """Discovery → federation → enrichment → COMPLETED (error chain → FAILED)."""
    source_id = msg.get("datasourceId", "").removeprefix("DS#")
    scan_job_id = msg.get("scanJobId", "")
    scan_job_pk = msg.get("scanJobPK", f"SRC#{source_id}")
    scan_job_sk = msg.get("scanJobSK", "")
    namespace_id = msg.get("namespaceId", "")
    scan_type = msg.get("scanType", "full")

    logger.info("db_scan_start source=%s job=%s", source_id, scan_job_id)
    try:
        from coa_sources.database.pipeline.discovery_handler import handler as discovery
        from coa_sources.database.pipeline.enrichment_handler import handler as enrichment
        from coa_sources.database.pipeline.federation_handler import handler as federation

        _update_source_status(source_id, namespace_id, "DISCOVERING")

        event = {
            "datasourceId": msg.get("datasourceId"),
            "scanJobId": scan_job_id,
            "scanJobPK": scan_job_pk,
            "scanJobSK": scan_job_sk,
            "namespaceId": namespace_id,
            "scanType": scan_type,
        }
        discovery(event, None)
        federation(event, None)

        _update_source_status(source_id, namespace_id, "ENRICHING")
        enrichment(event, None)

        _update_scan_job(scan_job_pk, scan_job_sk, "COMPLETED")
        # Production parity: the enrichment handler sets the source's terminal
        # state itself — PENDING_REVIEW (steward sign-off gates the Model phase)
        # or COMPLETED when metadataEnrichmentEnabled=false. Only fall back to
        # COMPLETED if enrichment didn't write a terminal status (e.g. it
        # short-circuited before the enrichment stage ran).
        current = _get_source(source_id, namespace_id)
        status_val = current.get("status", {})
        current_status = status_val.get("S") if isinstance(status_val, dict) else status_val
        if current_status not in {"PENDING_REVIEW", "COMPLETED"}:
            _update_source_status(source_id, namespace_id, "COMPLETED")
        logger.info("db_scan_completed source=%s", source_id)
    except Exception:
        logger.exception("db_scan_failed source=%s", source_id)
        try:
            if scan_job_sk:
                _update_scan_job(scan_job_pk, scan_job_sk, "FAILED")
            _update_source_status(source_id, namespace_id, "SCAN_FAILED")
        except Exception:
            logger.exception("terminal_status_write_failed source=%s", source_id)


# ── Pipeline: document ingestion ────────────────────────────────────────────


def run_doc_ingestion(msg: dict) -> None:
    """Preprocessing → save results → kg-build → COMPLETED."""
    doc_source_id = msg.get("doc_source_id", "")
    namespace_id = msg.get("namespace_id", "")
    logger.info("doc_ingestion_start source=%s", doc_source_id)
    try:
        from coa_sources.documents.preprocessing.handler import handler as preprocess

        _update_source_status(doc_source_id, namespace_id, "INGESTING")

        result = preprocess(msg)

        if result.get("status") == "SCAN_FAILED":
            _update_source_status(doc_source_id, namespace_id, "SCAN_FAILED")
            logger.error("doc_ingestion_all_failed source=%s", doc_source_id)
            return

        run_kg_build(msg, result)

        _update_source_status(doc_source_id, namespace_id, "COMPLETED")
        logger.info("doc_ingestion_completed source=%s", doc_source_id)
    except Exception:
        logger.exception("doc_ingestion_failed source=%s", doc_source_id)
        try:
            _update_source_status(doc_source_id, namespace_id, "SCAN_FAILED")
        except Exception:
            logger.exception("terminal_status_write_failed source=%s", doc_source_id)


def preprocess(msg: dict) -> dict:
    """Invoke the preprocessing handler (PDF_OCR_ENGINE selects Textract fallback)."""
    from coa_sources.documents.preprocessing.handler import handler as preprocess_handler

    return preprocess_handler(msg, None)


def run_kg_build(msg: dict, preprocess_result: dict) -> None:
    """Run the KG-build container entrypoint in-process (subprocess for isolation)."""
    env = os.environ.copy()
    env.update(
        {
            "DOC_SOURCE_ID": msg.get("doc_source_id", ""),
            "NAMESPACE_ID": msg.get("namespace_id", ""),
            "TENANT_ID": msg.get("tenant_id", ""),
            "STAGING_PREFIX": preprocess_result.get("staging_prefix", ""),
            "EXTRACTION_CONFIG": json.dumps(msg.get("extraction_config") or {}),
        }
    )
    result = subprocess.run(
        ["python", "-m", "coa_sources.documents.kg_build.graph_build"],
        env=env,
        capture_output=True,
        text=True,
        timeout=60 * 60,
    )
    if result.returncode != 0:
        raise RuntimeError(f"kg_build failed rc={result.returncode}: {result.stdout[-2000:]} {result.stderr[-2000:]}")


# ── Pipeline: document deletion ─────────────────────────────────────────────


def run_doc_deletion(msg: dict) -> None:
    """s3 cleanup → graph cleanup → delete DDB record (error chain → DELETE_FAILED)."""
    doc_source_id = msg.get("doc_source_id", "")
    namespace_id = msg.get("namespace_id", "")
    logger.info("doc_deletion_start source=%s", doc_source_id)
    try:
        from coa_sources.documents.deletion.cleanup_handler import handler as cleanup

        cleanup(msg, None)
        run_graph_cleanup(msg)
        ddb.delete_item(
            TableName=SOURCES_TABLE,
            Key={"PK": {"S": f"NS#{namespace_id}"}, "SK": {"S": f"SRC#{doc_source_id}"}},
        )
        logger.info("doc_deletion_completed source=%s", doc_source_id)
    except Exception:
        logger.exception("doc_deletion_failed source=%s", doc_source_id)
        try:
            _update_source_status(doc_source_id, namespace_id, "DELETE_FAILED")
        except Exception:
            logger.exception("terminal_status_write_failed source=%s", doc_source_id)


def run_graph_cleanup(msg: dict) -> None:
    env = os.environ.copy()
    env.update(
        {
            "DOC_SOURCE_ID": msg.get("doc_source_id", ""),
            "NAMESPACE_ID": msg.get("namespace_id", ""),
            "TENANT_ID": msg.get("tenant_id", ""),
        }
    )
    result = subprocess.run(
        ["python", "-m", "coa_sources.documents.kg_build.graph_cleanup"],
        env=env,
        capture_output=True,
        text=True,
        timeout=30 * 60,
    )
    if result.returncode != 0:
        raise RuntimeError(f"graph_cleanup failed rc={result.returncode}: {result.stderr[-2000:]}")


def run_bulk_review(msg: dict) -> None:
    """Process one ApproveSource/RejectSource message via the shared worker."""
    import json as _json

    from coa_sources.database.bulk_review.worker import handler as bulk_review

    logger.info("bulk_review_start source=%s decision=%s", msg.get("sourceId", ""), msg.get("decision", ""))
    # The shared handler consumes an SQS-trigger shape (Records[{body}]);
    # wrap the raw queue body so the local loop drives the SAME code path
    # the Lambda does (parse_message + pipeline + terminal state writes).
    event = {"Records": [{"messageId": "local", "body": _json.dumps(msg)}]}
    bulk_review(event, None)


# ── Queue loops ─────────────────────────────────────────────────────────────

HANDLERS = {
    "db-scan": (Q_DB_SCAN, run_db_scan),
    "doc-ingestion": (Q_DOC_INGEST, run_doc_ingestion),
    "doc-deletion": (Q_DOC_DELETE, run_doc_deletion),
    "bulk-review": (Q_BULK_REVIEW, run_bulk_review),
}


def main() -> None:
    kinds = [
        k for k in os.environ.get("WORKER_QUEUES", "db-scan,bulk-review,doc-ingestion,doc-deletion").split(",") if k
    ]
    logger.info("worker_starting queues=%s", kinds)
    urls = {}
    for k in kinds:
        queue_name, _fn = HANDLERS[k]
        # Queues are created by the provisioner; wait until they appear so the
        # worker survives a LocalStack restart or a provisioner that hasn't
        # finished yet.
        while True:
            try:
                urls[k] = _url(queue_name)
                break
            except sqs.exceptions.QueueDoesNotExist:
                logger.warning("queue_not_found waiting queue=%s", queue_name)
                time.sleep(5)

    while True:
        for k in kinds:
            try:
                resp = sqs.receive_message(
                    QueueUrl=urls[k],
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=10,
                    VisibilityTimeout=3600,
                )
                messages = resp.get("Messages", [])
                if not messages:
                    continue
                m = messages[0]
                try:
                    body = json.loads(m["Body"])
                except json.JSONDecodeError:
                    logger.error("invalid_message_body queue=%s", k)
                    sqs.delete_message(QueueUrl=urls[k], ReceiptHandle=m["ReceiptHandle"])
                    continue
                HANDLERS[k][1](body)
                sqs.delete_message(QueueUrl=urls[k], ReceiptHandle=m["ReceiptHandle"])
            except KeyboardInterrupt:
                raise
            except Exception:
                logger.exception("worker_loop_error queue=%s", k)
                time.sleep(2)


if __name__ == "__main__":
    main()
