# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the pre-signed upload URL and S3-based ingest endpoints."""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

pytestmark = pytest.mark.unit

TURTLE = """@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix ex:   <https://example.com/onto#> .

<https://example.com/onto> a owl:Ontology ;
    rdfs:label "Test Ontology" .

ex:Patient a owl:Class ; rdfs:label "Patient" .
ex:treats a owl:ObjectProperty ; rdfs:label "treats" ;
    rdfs:domain ex:Patient ; rdfs:range ex:Patient .
"""


@pytest.fixture()
def client(monkeypatch, tmp_path):
    """TestClient with stores, dynamo, and boto3 mocked."""
    monkeypatch.setenv("WORKBENCH_BACKEND", "opensearch_neptune")
    monkeypatch.setenv("ONTOLOGY_STORAGE_PATH", str(tmp_path / "ontologies"))
    monkeypatch.setenv("ONTOLOGY_ARTIFACTS_BUCKET", "test-bucket")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv(
        "NDB_ENDPOINT",
        "https://test.cluster-x.us-east-1.neptune.amazonaws.com:8182",
    )
    monkeypatch.setenv("NDB_IAM_AUTH", "false")
    monkeypatch.setenv("OSS_ENDPOINT", "https://test.us-east-1.aoss.amazonaws.com")

    mock_graph = MagicMock()
    mock_graph.get_ontology_by_uri = MagicMock(return_value=None)
    mock_graph.get_ontology = MagicMock(return_value=None)
    mock_graph.create_ontology = MagicMock(return_value={"uri": "https://example.com/onto"})
    mock_graph.store_class = MagicMock(return_value={})
    mock_graph.store_property = MagicMock(return_value={})
    mock_graph.update_ontology = MagicMock(return_value={})
    mock_graph.count_classes_and_properties = MagicMock(return_value=None)
    mock_graph.load_turtle = MagicMock(
        return_value={
            "status": "ok",
            "graph_uri": "https://ontology-workbench.local/default/test",
            "bytes_loaded": 100,
        }
    )

    mock_vec = MagicMock()
    mock_vec.store_embeddings_batch = MagicMock(side_effect=lambda items: items)
    mock_vec.delete_embeddings_for_ontology = MagicMock(return_value=0)
    mock_vec._index_for_namespace = MagicMock(side_effect=lambda ns: f"embeddings-{ns}")

    mock_bedrock = MagicMock()
    mock_bedrock.model_id = "fake-titan-v2"
    mock_bedrock.embed_texts = MagicMock(side_effect=lambda texts: [[0.1] * 4 for _ in texts])

    mock_table = MagicMock()
    mock_table.get_item.return_value = {}
    mock_table.put_item.return_value = {}
    mock_table.update_item.return_value = {"Attributes": {}}
    mock_table.query.return_value = {"Items": []}

    mock_s3 = MagicMock()
    mock_s3.generate_presigned_url.return_value = "https://s3.amazonaws.com/presigned"
    mock_s3.get_object.return_value = {"Body": MagicMock(read=lambda: TURTLE.encode("utf-8"))}
    mock_s3.put_object.return_value = {}

    with (
        patch("coa_ontology.stores.build_stores", return_value=(mock_graph, mock_vec)),
        patch("coa_ontology.bedrock_embeddings.BedrockEmbeddingClient", return_value=mock_bedrock),
        patch("coa_ontology.dynamo_store._get_table", return_value=mock_table),
        patch("coa_ontology.dynamo_store.update_ingest_job"),
        patch("coa_ontology.dynamo_store.put_ingest_job"),
        patch("coa_ontology.dynamo_store.get_ontology_registry", return_value=None),
        patch("coa_ontology.dynamo_store._get_s3", return_value=mock_s3),
        patch("boto3.client", return_value=mock_s3),
        patch("boto3.Session", return_value=MagicMock(client=MagicMock(return_value=mock_s3))),
    ):
        from coa_ontology.main import app

        yield TestClient(app), mock_table, mock_s3


class TestGetUploadUrl:
    def test_returns_presigned_url(self, client):
        http, _, _ = client
        resp = http.post(
            "/ontologies/upload-url?namespace=test-ns",
            json={"filename": "my-onto.ttl", "contentType": "text/turtle"},
        )

        assert resp.status_code == 200
        body = resp.json()
        assert body["uploadUrl"] == "https://s3.amazonaws.com/presigned"
        assert "s3Key" in body

    def test_rejects_invalid_filename(self, client):
        http, _, _ = client
        resp = http.post(
            "/ontologies/upload-url?namespace=ns",
            json={"filename": "...", "contentType": "text/turtle"},
        )
        assert resp.status_code == 400

    def test_presign_does_not_sign_content_length(self, client):
        """Regression: ``ContentLength`` must NOT be signed. Signing a fixed
        length forces the browser to PUT exactly that many bytes under SigV4
        or get a 403 (real file size ≠ signed size). It never capped uploads."""
        http, _, mock_s3 = client
        resp = http.post(
            "/ontologies/upload-url?namespace=test-ns",
            json={"filename": "my-onto.ttl", "contentType": "text/turtle"},
        )
        assert resp.status_code == 200
        params = mock_s3.generate_presigned_url.call_args.kwargs["Params"]
        assert "ContentLength" not in params
        assert params["ContentType"] == "text/turtle"


class TestIngestFromS3:
    def test_returns_202(self, client):
        """The ingest-from-s3 endpoint returns 202 (async job)."""
        http, mock_table, _ = client
        mock_table.get_item.return_value = {}

        resp = http.post(
            "/ontologies/https://example.com/onto/ingest-from-s3?namespace=default",
            json={
                "s3_key": "ontology-uploads/default/test-id/test.ttl",
                "title": "Test Ontology",
                "format": "turtle",
                "ontology_type": "user_uploaded",
            },
        )

        assert resp.status_code == 202
        body = resp.json()
        assert "jobId" in body.get("result", body) or "jobId" in body
        # Give the background thread a moment to finish (uses the mocked boto3)
        time.sleep(0.5)
