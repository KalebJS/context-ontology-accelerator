# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""LocalStack provisioner — creates tables, buckets, SSM params, seeds.

Runs once (or until converged) at compose startup. Mirrors what the CDK
foundation stacks create:
  - AuthnzStack      → roles, resource-role-mappings (+ 2 GSIs), cache-invalidation
                       and seeds built-in Cedar roles + admin group grants
  - NamespaceStack   → namespaces table (+ NameByIndex GSI)
  - SourcesStack     → sources, source-scan-jobs tables, sources-data bucket
  - OntologyStack    → ontology-engine table, ontology-artifacts bucket
  - MetricService    → metric-import-jobs table, athena results/spill buckets
  - SSM parameters   → issuer, client IDs, table names, endpoints
  - SQS queues       → scan + doc ingestion (+ DLQs)
  - EventBridge      → metric lifecycle rules (no-op bus wiring kept minimal)

Idempotent: every create uses if-not-exists semantics; reruns converge.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import boto3
from botocore.config import Config

LOCALSTACK = os.environ.get("LOCALSTACK_ENDPOINT", "http://localstack:4566")
PREFIX = os.environ.get("SCL_PREFIX", "coa")
ENV = os.environ.get("SCL_ENV", "local")
REGION = os.environ.get("AWS_REGION", "us-east-1")
ADMIN_EMAIL = os.environ.get("E2E_USERNAME", "admin@coa.local")
SEED_DIR = Path(os.environ.get("SEED_DIR", "/seeds"))

_CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, region_name=REGION)

session = boto3.Session(
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
    region_name=REGION,
)

ddb = session.client("dynamodb", endpoint_url=LOCALSTACK, config=_CFG)
s3 = session.client("s3", endpoint_url=LOCALSTACK, config=_CFG)
ssm = session.client("ssm", endpoint_url=LOCALSTACK, config=_CFG)
sqs = session.client("sqs", endpoint_url=LOCALSTACK, config=_CFG)
secrets = session.client("secretsmanager", endpoint_url=LOCALSTACK, config=_CFG)

SSM_PREFIX = f"/{PREFIX}"


def log(msg: str) -> None:
    print(f"[provisioner] {msg}", flush=True)


def wait_localstack(timeout: int = 180) -> None:
    deadline = time.time() + timeout
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            ddb.list_tables()
            return
        except Exception as e:  # noqa: BLE001 — retry any startup error
            last_err = e
            time.sleep(2)
    raise RuntimeError(f"LocalStack not reachable after {timeout}s: {last_err}")


def create_table(name: str, *, partition: str, sort: str | None = None, gs: list[dict] | None = None) -> None:
    """Create a DynamoDB table if it does not exist (PAY_PER_REQUEST)."""
    existing = {t for page in ddb.get_paginator("list_tables").paginate() for t in page["TableNames"]}
    if name in existing:
        log(f"table exists: {name}")
        return
    attrs = [{"AttributeName": partition, "AttributeType": "S"}]
    schema = [{"AttributeName": partition, "KeyType": "HASH"}]
    if sort:
        attrs.append({"AttributeName": sort, "AttributeType": "S"})
        schema.append({"AttributeName": sort, "KeyType": "RANGE"})
    kwargs: dict = {
        "TableName": name,
        "AttributeDefinitions": attrs,
        "KeySchema": schema,
        "BillingMode": "PAY_PER_REQUEST",
    }
    if gs:
        # DynamoDB rejects duplicate AttributeDefinitions ("Cannot have two
        # attributes with the same name"), and GSI key schemas commonly share
        # keys (e.g. two GSIs partitioned on namespaceId) — dedupe per table.
        seen_attrs = {partition}
        if sort:
            seen_attrs.add(sort)
        for idx in gs:
            for key in (idx["pk"], idx.get("sk")):
                if not key or key in seen_attrs:
                    continue
                seen_attrs.add(key)
                kwargs["AttributeDefinitions"].append({"AttributeName": key, "AttributeType": "S"})
            gks = [{"AttributeName": idx["pk"], "KeyType": "HASH"}]
            if idx.get("sk"):
                gks.append({"AttributeName": idx["sk"], "KeyType": "RANGE"})
            kwargs.setdefault("GlobalSecondaryIndexes", []).append(
                {
                    "IndexName": idx["name"],
                    "KeySchema": gks,
                    "Projection": {"ProjectionType": "ALL"},
                }
            )
    ddb.create_table(**kwargs)
    waiter = ddb.get_waiter("table_exists")
    waiter.wait(TableName=name, WaiterConfig={"Delay": 1, "MaxAttempts": 60})
    log(f"table created: {name}")


def _apply_browser_bucket_cors(s3_client, bucket: str) -> None:
    """Allow browser presigned S3 access (mirrors SourcesStack bucket CORS).

    The web app hits the bucket directly from a different origin (web app on
    localhost:3000, LocalStack on localhost:<LOCALSTACK_PORT>), so the browser
    enforces CORS:

    * sources-data — presigned PUTs from the Connect Source wizard ("Upload
      failed: Failed to fetch" without this).
    * ontology-artifacts — presigned GETs the proposal detail page fetches
      (ontology/r2rml/matches artifacts are served out-of-band to stay under
      the 6 MB response cap; the page shows "Could not load proposal" when
      the browser blocks the cross-origin response).
    """
    s3_client.put_bucket_cors(
        Bucket=bucket,
        CORSConfiguration={
            "CORSRules": [
                {
                    "AllowedOrigins": ["*"],
                    "AllowedMethods": ["PUT", "GET", "HEAD"],
                    "AllowedHeaders": ["*"],
                    "MaxAgeSeconds": 3000,
                }
            ]
        },
    )


def create_bucket(name: str, with_cors: bool = False, **_ignored) -> None:
    try:
        s3.head_bucket(Bucket=name)
        log(f"bucket exists: {name}")
    except Exception:  # noqa: BLE001
        s3.create_bucket(Bucket=name)
        log(f"bucket created: {name}")
    if with_cors:
        _apply_browser_bucket_cors(s3, name)
        log(f"bucket cors applied: {name}")


def put_ssm(name: str, value: str) -> None:
    try:
        ssm.get_parameter(Name=name)
    except Exception:  # noqa: BLE001
        ssm.put_parameter(Name=name, Type="String", Value=value)
        log(f"ssm put: {name}")
        return
    ssm.put_parameter(Name=name, Type="String", Value=value, Overwrite=True)
    log(f"ssm set: {name}")


def create_queue(name: str, dlq: str | None = None) -> str:
    try:
        url = sqs.get_queue_url(QueueName=name)["QueueUrl"]
    except sqs.exceptions.QueueDoesNotExist:
        attrs = {}
        if dlq:
            dlq_url = create_queue(dlq)
            dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
            attrs = {"RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": 5})}
        url = sqs.create_queue(QueueName=name, Attributes=attrs)["QueueUrl"]
        log(f"queue created: {name}")
        return url
    log(f"queue exists: {name}")
    return url


def seed_cedar_roles() -> None:
    """Seed built-in roles from libs/common Cedar files (mirrors AuthnzStack)."""
    roles = [
        ("platform-admin", "Platform Admin", "GLOBAL", "global_admin.cedar"),
        ("platform-viewer", "Platform Viewer", "GLOBAL", "global_viewer.cedar"),
        ("namespace-owner", "Namespace Owner", "NAMESPACE_TEMPLATE", "namespace_owner.cedar"),
        ("namespace-maintainer", "Namespace Maintainer", "NAMESPACE_TEMPLATE", "namespace_maintainer.cedar"),
        ("data-steward", "Data Steward", "NAMESPACE_TEMPLATE", "namespace_data_steward.cedar"),
        ("data-analyst", "Data Analyst", "NAMESPACE_TEMPLATE", "namespace_data_analyst.cedar"),
        ("default", "Default", "GLOBAL", "default.cedar"),
    ]
    roles_table = f"{PREFIX}-{ENV}-roles"
    for role_id, name, scope, filename in roles:
        path = SEED_DIR / filename
        if not path.exists():
            log(f"WARN: seed file missing: {path}")
            continue
        cedar = path.read_text()
        item = {
            "PK": {"S": "GLOBAL" if scope == "GLOBAL" else "NAMESPACE_TEMPLATE"},
            "SK": {"S": f"ROLE#{role_id}"},
            "name": {"S": name},
            "description": {"S": name},
            "isBuiltIn": {"BOOL": True},
            "cedarPolicy": {"S": cedar},
            "scope": {"S": scope},
            "createdAt": {"S": "2026-01-01T00:00:00Z"},
            "updatedAt": {"S": "2026-01-01T00:00:00Z"},
        }
        ddb.put_item(TableName=roles_table, Item=item)
    log(f"seeded {len(roles)} built-in roles")

    # Group → platform-admin grant (mirrors the IdP claims mapping seed).
    rrm_table = f"{PREFIX}-{ENV}-resource-role-mappings"
    group = os.environ.get("KC_ADMIN_GROUP", "platform-admins")
    item = {
        "PK": {"S": f"PLATFORM::GLOBAL#GROUP::{group}"},
        "SK": {"S": "ROLE#platform-admin"},
        "resourceType": {"S": "PLATFORM"},
        "resourceId": {"S": "GLOBAL"},
        "principalType": {"S": "Group"},
        "principalId": {"S": group},
        "role": {"S": "platform-admin"},
        "principalKey": {"S": f"Group::{group}"},
        "resourceRoleKey": {"S": "PLATFORM::GLOBAL#ROLE#platform-admin"},
        "namespaceKey": {"S": "NS#GLOBAL"},
        "principalRoleKey": {"S": f"Group::{group}#ROLE#platform-admin"},
        "grantedBy": {"S": "system"},
        "grantedAt": {"S": "2026-01-01T00:00:00Z"},
    }
    ddb.put_item(TableName=rrm_table, Item=item)
    log(f"seeded group grant: {group} → platform-admin")

    # Admin user → platform-admin grant (JWT resolves by email principal).
    item = {
        "PK": {"S": f"PLATFORM::GLOBAL#USER::{ADMIN_EMAIL}"},
        "SK": {"S": "ROLE#platform-admin"},
        "resourceType": {"S": "PLATFORM"},
        "resourceId": {"S": "GLOBAL"},
        "principalType": {"S": "User"},
        "principalId": {"S": ADMIN_EMAIL},
        "role": {"S": "platform-admin"},
        "principalKey": {"S": f"User::{ADMIN_EMAIL}"},
        "resourceRoleKey": {"S": "PLATFORM::GLOBAL#ROLE#platform-admin"},
        "namespaceKey": {"S": "NS#GLOBAL"},
        "principalRoleKey": {"S": f"User::{ADMIN_EMAIL}#ROLE#platform-admin"},
        "grantedBy": {"S": "system"},
        "grantedAt": {"S": "2026-01-01T00:00:00Z"},
    }
    ddb.put_item(TableName=rrm_table, Item=item)
    log(f"seeded user grant: {ADMIN_EMAIL} → platform-admin")


def seed_cache_version() -> None:
    table = f"{PREFIX}-{ENV}-cache-invalidation"
    ddb.put_item(
        TableName=table,
        Item={
            "PK": {"S": "CACHE_VERSION"},
            "SK": {"S": "CURRENT"},
            "version": {"N": "1"},
        },
    )
    log("seeded cache version")


def main() -> None:
    log("waiting for LocalStack…")
    wait_localstack()

    t_namespaces = f"{PREFIX}-{ENV}-namespaces"
    t_roles = f"{PREFIX}-{ENV}-roles"
    t_rrm = f"{PREFIX}-{ENV}-resource-role-mappings"
    t_cache = f"{PREFIX}-{ENV}-cache-invalidation"
    t_sources = f"{PREFIX}-{ENV}-sources"
    t_scan_jobs = f"{PREFIX}-{ENV}-source-scan-jobs"
    t_ontology = f"{PREFIX}-{ENV}-ontology-engine"
    t_import_jobs = f"{PREFIX}-{ENV}-metric-import-jobs"

    create_table(t_namespaces, partition="PK", sort="SK", gs=[{"name": "NameByIndex", "pk": "name"}])
    create_table(t_roles, partition="PK", sort="SK")
    create_table(
        t_rrm,
        partition="PK",
        sort="SK",
        gs=[
            {"name": "PrincipalIndex", "pk": "principalKey", "sk": "resourceRoleKey"},
            {"name": "NamespaceGrantsIndex", "pk": "namespaceKey", "sk": "principalRoleKey"},
        ],
    )
    create_table(t_cache, partition="PK", sort="SK")
    # GSI set mirrors infra/lib/stacks/services/sources-stack.ts — list-sources
    # and namespace-deletion preconditions query ByNamespace/BySourceType, and
    # document-source creation queries ByName for name-uniqueness.
    create_table(
        t_sources,
        partition="PK",
        sort="SK",
        gs=[
            {"name": "ByNamespace", "pk": "namespaceId", "sk": "createdAt"},
            {"name": "BySourceType", "pk": "namespaceId", "sk": "sourceTypeCreatedAt"},
            {"name": "ByName", "pk": "namespaceId", "sk": "name"},
        ],
    )
    create_table(t_scan_jobs, partition="PK", sort="SK", gs=[{"name": "ByNamespace", "pk": "namespaceId"}])
    create_table(t_ontology, partition="PK", sort="SK")
    create_table(t_import_jobs, partition="PK", sort="SK")
    # Metadata store (LocalMetadataStore — DataZone stand-in). AssetIdIndex backs
    # the store's by-assetId lookups (revision/delete/get/forms); AssetNameIndex
    # is the name→id index described in local_store.py's storage model.
    create_table(
        f"{PREFIX}-local-metadata",
        partition="PK",
        sort="SK",
        gs=[{"name": "AssetNameIndex", "pk": "GSI1PK"}, {"name": "AssetIdIndex", "pk": "assetId"}],
    )

    b_sources = f"{PREFIX}-{ENV}-sources-data"
    b_ontology = f"{PREFIX}-{ENV}-ontology-artifacts"
    b_athena_results = f"{PREFIX}-{ENV}-athena-results"
    b_athena_spill = f"{PREFIX}-{ENV}-athena-spill"
    # Browser-direct buckets need CORS: sources-data (presigned PUTs from the
    # Connect Source wizard) and ontology-artifacts (presigned GETs the
    # proposal detail page fetches, same cross-origin enforcement). Athena
    # buckets are only touched server-side, so they stay CORS-free.
    for b in (b_sources, b_ontology, b_athena_results, b_athena_spill):
        create_bucket(b, with_cors=(b in (b_sources, b_ontology)))

    # Demo Postgres credential secret (source creation verifies the secret
    # exists and carries a 'coa:namespace' tag). Value = the demo DB creds
    # from docker/.env; the tag value '*' matches any namespace in local mode.
    secret_name = f"{PREFIX}/{ENV}/demo-postgres"
    try:
        secrets.describe_secret(SecretId=secret_name)
        log(f"secret exists: {secret_name}")
    except Exception:  # noqa: BLE001
        secrets.create_secret(
            Name=secret_name,
            SecretString=json.dumps(
                {
                    "username": os.environ.get("DEMO_DB_USER", "coa"),
                    "password": os.environ.get("DEMO_DB_PASSWORD", "coa_demo_pw"),
                }
            ),
            Tags=[{"Key": "coa:namespace", "Value": "*"}],
        )
        log(f"secret created: {secret_name}")

    seed_cedar_roles()
    seed_cache_version()

    # ── SQS queues ─────────────────────────────────────────────────────
    create_queue(f"{PREFIX}-{ENV}-sources-db-scan-queue")
    create_queue(f"{PREFIX}-{ENV}-sources-db-scan-dlq")
    create_queue(f"{PREFIX}-{ENV}-sources-doc-ingestion-queue")
    create_queue(f"{PREFIX}-{ENV}-sources-doc-ingestion-dlq")
    create_queue(f"{PREFIX}-{ENV}-sources-doc-deletion-queue")
    # Bulk approve/reject (ApproveSource / RejectSource) — consumed by the
    # scan-workers container's bulk-review poll loop (see worker.py).
    create_queue(f"{PREFIX}-local-sources-bulk-review-queue")

    # ── SSM parameters (subset the local stack actually reads) ────────
    put_ssm(f"{SSM_PREFIX}/issuer", os.environ.get("KC_ISSUER_INTERNAL", "http://keycloak:8080/realms/coa"))
    put_ssm(f"{SSM_PREFIX}/userpool-client-id", os.environ.get("KC_WEB_CLIENT_ID", "coa-web"))
    put_ssm(f"{SSM_PREFIX}/mcp-client-id", os.environ.get("KC_MCP_CLIENT_ID", "coa-mcp"))
    put_ssm(f"{SSM_PREFIX}/authentication-group-token-name", "groups")
    put_ssm(f"{SSM_PREFIX}/jwks-uri", f"{os.environ.get('KC_ISSUER_INTERNAL', '')}/protocol/openid-connect/certs")
    put_ssm(f"{SSM_PREFIX}/namespace/namespaces-table-name", t_namespaces)
    put_ssm(f"{SSM_PREFIX}/opensearch/endpoint", os.environ.get("OPENSEARCH_URL", "http://opensearch:9200"))
    put_ssm(f"{SSM_PREFIX}/opensearch/collection-name", "vector-store")
    put_ssm(f"{SSM_PREFIX}/opensearch/collection-arn", "local")
    put_ssm(f"{SSM_PREFIX}/query/athena-results-bucket", b_athena_results)
    put_ssm(f"{SSM_PREFIX}/query/athena-spill-bucket", b_athena_spill)
    put_ssm(f"{SSM_PREFIX}/sources/sources-table-name", t_sources)
    # Local stand-in for the shared DataZone project-access role ARN — the
    # namespace service reads this to register its project membership (a no-op
    # against LocalMetadataStore).
    put_ssm(f"{SSM_PREFIX}/smus/dz-project-access-role-arn", "arn:local:iam::000000000000:role/project-access")

    log("provisioning complete")


if __name__ == "__main__":
    sys.exit(main())
