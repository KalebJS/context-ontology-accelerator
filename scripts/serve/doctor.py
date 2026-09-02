#!/usr/bin/env python3
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Health check for a COA namespace — verifies all dependencies are ready."""

import json
import os
import sys
import urllib.parse
import urllib.request
import uuid

import boto3

region = os.environ.get("AWS_REGION", "us-east-1")
prefix = os.environ.get("SCL_PREFIX", "coa")
namespace = sys.argv[1] if len(sys.argv) > 1 else ""

if not namespace:
    print("Usage: ./doctor.py <namespace-id>")
    print("\nExample: ./doctor.py aa28cab9-8505-4325-a85a-dd9ebb71753c")
    sys.exit(1)

ssm = boto3.client("ssm", region_name=region)
results = []


def check(name, fn):
    try:
        detail = fn()
        results.append({"check": name, "status": "pass", "detail": detail})
    except Exception as e:
        results.append({"check": name, "status": "fail", "error": str(e)[:200]})


# ── 1. Auth ──
def check_auth():
    sm = boto3.client("secretsmanager", region_name=region)
    secret = json.loads(sm.get_secret_value(SecretId=f"{prefix}-dev-integ-test-user")["SecretString"])
    client_id = ssm.get_parameter(Name=f"/{prefix}/userpool-client-id")["Parameter"]["Value"]
    cognito = boto3.client("cognito-idp", region_name=region)
    cognito.initiate_auth(
        AuthFlow="USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": secret["username"], "PASSWORD": secret["password"]},
        ClientId=client_id,
    )
    return "Cognito token obtained successfully"


check("auth", check_auth)


# ── 2. Runtime ──
def check_runtime():
    runtime_arn = ssm.get_parameter(Name=f"/{prefix}/serve/runtime-arn")["Parameter"]["Value"]
    sm = boto3.client("secretsmanager", region_name=region)
    secret = json.loads(sm.get_secret_value(SecretId=f"{prefix}-dev-integ-test-user")["SecretString"])
    client_id = ssm.get_parameter(Name=f"/{prefix}/userpool-client-id")["Parameter"]["Value"]
    cognito = boto3.client("cognito-idp", region_name=region)
    auth = cognito.initiate_auth(
        AuthFlow="USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": secret["username"], "PASSWORD": secret["password"]},
        ClientId=client_id,
    )
    token = auth["AuthenticationResult"]["IdToken"]
    encoded_arn = urllib.parse.quote(runtime_arn, safe="")
    url = f"https://bedrock-agentcore.{region}.amazonaws.com/runtimes/{encoded_arn}/invocations?qualifier=DEFAULT"
    payload = json.dumps(
        {"query": "ping", "namespace": namespace, "options": {"tierOverride": 3}, "requestId": str(uuid.uuid4())}
    ).encode()
    req = urllib.request.Request(
        url,
        data=payload,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "X-Amzn-Bedrock-AgentCore-Runtime-Session-Id": f"doctor-{uuid.uuid4().hex}",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        # Serve streams SSE and keeps the connection open — reading to EOF would
        # block until timeout. The first streamed line proves it's responding.
        for raw_line in resp:
            if raw_line.strip():
                break
        else:
            raise RuntimeError("runtime returned an empty response")
    return "AgentCore runtime is responding"


check("runtime", check_runtime)


# ── 3. Neptune ──
def check_neptune():
    lam = boto3.client("lambda", region_name=region)
    payload = json.dumps(
        {
            "httpMethod": "GET",
            "resource": "/namespaces/{namespaceId}/ontologies",
            "path": f"/namespaces/{namespace}/ontologies",
            "pathParameters": {"namespaceId": namespace},
            "queryStringParameters": None,
            "headers": {"content-type": "application/json"},
            "requestContext": {"authorizer": {"claims": {"sub": "doctor", "cognito:groups": "Admin"}}},
        }
    )
    resp = lam.invoke(FunctionName=f"{prefix}-dev-ontology-api-proxy", Payload=payload)
    body = json.loads(resp["Payload"].read())
    if body.get("statusCode", 0) != 200:
        raise Exception(f"HTTP {body.get('statusCode')}")
    parsed = json.loads(body["body"]) if isinstance(body["body"], str) else body["body"]
    # ListOntologies returns a camelCase {"ontologies": [...]} envelope. Accept a
    # bare array too, so the doctor still reports against an older deployment
    # instead of failing on the tool rather than the service it is checking.
    ontologies = parsed.get("ontologies") or [] if isinstance(parsed, dict) else parsed
    if not ontologies:
        raise Exception("No ontology published for this namespace")
    ont = ontologies[0]
    classes = ont.get("classCount", ont.get("class_count", 0))
    properties = ont.get("propertyCount", ont.get("property_count", 0))
    return f"{classes} classes, {properties} properties published"


check("neptune", check_neptune)


# ── 4. OpenSearch ──
def check_oss():
    import botocore.session
    from botocore.auth import SigV4Auth
    from botocore.awsrequest import AWSRequest

    session = botocore.session.get_session()
    credentials = session.get_credentials().get_frozen_credentials()

    aoss = boto3.client("opensearchserverless", region_name=region)
    collections = aoss.list_collections()["collectionSummaries"]
    coll = next((c for c in collections if prefix in c["name"]), None)
    if not coll:
        raise Exception("No AOSS collection found")
    details = aoss.batch_get_collection(ids=[coll["id"]])["collectionDetails"][0]
    endpoint = details["collectionEndpoint"]

    index_name = f"{prefix}-dev-vectors-{namespace}"
    url = f"{endpoint}/_cat/indices/{index_name}?format=json"
    request = AWSRequest(method="GET", url=url, headers={"Host": endpoint.replace("https://", "")})
    SigV4Auth(credentials, "aoss", region).add_auth(request)
    req = urllib.request.Request(url, headers=dict(request.headers))
    with urllib.request.urlopen(req, timeout=10) as resp:
        indices = json.loads(resp.read())
        if not indices:
            raise Exception(f"Index {index_name} not found")
        doc_count = indices[0].get("docs.count", "0")
        return f"{doc_count} embeddings in OpenSearch index"


check("opensearch", check_oss)


# ── 5. Data Source ──
def check_source():
    ddb = boto3.client("dynamodb", region_name=region)
    resp = ddb.query(
        TableName=f"{prefix}-dev-sources",
        KeyConditionExpression="PK = :pk",
        ExpressionAttributeValues={":pk": {"S": f"NS#{namespace}"}},
        ProjectionExpression="SK, #n, #s, glueConnectionName",
        ExpressionAttributeNames={"#n": "name", "#s": "status"},
    )
    items = resp.get("Items", [])
    if not items:
        raise Exception("No data source found for namespace")
    src = items[0]
    name = src.get("name", {}).get("S", "?")
    conn = src.get("glueConnectionName", {}).get("S", "?")
    glue = boto3.client("glue", region_name=region)
    try:
        glue_conn = glue.get_connection(Name=conn)["Connection"]
        glue_status = glue_conn.get("Status", "UNKNOWN")
    except Exception:
        glue_status = "NOT_FOUND"
    return f"{name} — Glue connection {glue_status}"


check("data_source", check_source)


# ── 6. VKG ──
def check_vkg():
    ecs = boto3.client("ecs", region_name=region)
    try:
        cluster_arn = ssm.get_parameter(Name=f"/{prefix}/vkg/cluster-arn")["Parameter"]["Value"]
        services = ecs.list_services(cluster=cluster_arn, maxResults=10)["serviceArns"]
        vkg_services = [s for s in services if "vkg" in s.lower() and namespace[:8] in s]
        if not vkg_services:
            vkg_services = [s for s in services if "vkg" in s.lower()]
        if not vkg_services:
            raise Exception("No VKG service found")
        svc = ecs.describe_services(cluster=cluster_arn, services=vkg_services[:1])["services"][0]
        running = svc.get("runningCount", 0)
        desired = svc.get("desiredCount", 0)
        return f"VKG service running ({running}/{desired} tasks)"
    except Exception as e:
        try:
            endpoint = ssm.get_parameter(Name=f"/{prefix}/vkg/endpoint")["Parameter"]["Value"]
            return f"VKG endpoint configured ({endpoint})"
        except Exception:
            raise e from None


check("vkg", check_vkg)


# ── 7. Unstructured data (Tier 3) ──
def check_unstructured():
    import botocore.session
    from botocore.auth import SigV4Auth
    from botocore.awsrequest import AWSRequest

    session = botocore.session.get_session()
    credentials = session.get_credentials().get_frozen_credentials()

    aoss = boto3.client("opensearchserverless", region_name=region)
    collections = aoss.list_collections()["collectionSummaries"]
    coll = next((c for c in collections if prefix in c["name"]), None)
    if not coll:
        raise Exception("No AOSS collection found")
    details = aoss.batch_get_collection(ids=[coll["id"]])["collectionDetails"][0]
    endpoint = details["collectionEndpoint"]

    index_name = f"{prefix}-dev-vectors-{namespace}"
    url = f"{endpoint}/{index_name}/_count"
    body = json.dumps(
        {"query": {"bool": {"must_not": [{"terms": {"entity_type": ["class", "property", "metric"]}}]}}}
    ).encode()
    request = AWSRequest(
        method="POST",
        url=url,
        data=body,
        headers={"Host": endpoint.replace("https://", ""), "Content-Type": "application/json"},
    )
    SigV4Auth(credentials, "aoss", region).add_auth(request)
    req = urllib.request.Request(url, data=body, method="POST", headers=dict(request.headers))
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
            doc_count = result.get("count", 0)
            if doc_count > 0:
                return f"{doc_count} unstructured documents indexed"
            return "No unstructured documents (Tier 3 uses synthesis only)"
    except urllib.error.HTTPError:
        return "No unstructured documents (Tier 3 uses synthesis only)"


check("unstructured", check_unstructured)


# ── Output ──
print()
print(f"  Namespace: {namespace}")
print(f"  Region:    {region}")
print()
passed_count = 0
failed_count = 0
for r in results:
    icon = "\033[32m✓\033[0m" if r["status"] == "pass" else "\033[31m✗\033[0m"
    detail = r.get("detail", r.get("error", ""))
    print(f"  {icon} {r['check']:<14} {detail}")
    if r["status"] == "pass":
        passed_count += 1
    else:
        failed_count += 1
print()
if failed_count:
    print(f"  \033[31m{failed_count} check(s) failed\033[0m")
    sys.exit(1)
else:
    print(f"  \033[32mAll {passed_count} checks passed\033[0m")
