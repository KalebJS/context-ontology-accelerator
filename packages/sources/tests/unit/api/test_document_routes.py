# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for document_routes.py — DOCUMENTS source route handlers."""

from __future__ import annotations

import json
import os
from unittest.mock import MagicMock, patch

import pytest
from coa_control_plane_server.models.source_type import SourceType

# ---------------------------------------------------------------------------
# Environment setup — must happen before module import
# ---------------------------------------------------------------------------
os.environ.setdefault("SOURCES_TABLE", "test-sources")
os.environ.setdefault("SOURCE_SCAN_JOBS_TABLE", "test-scan-jobs")
os.environ.setdefault("NAMESPACES_TABLE", "test-namespaces")
os.environ.setdefault("INGESTION_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/ingestion-queue")
os.environ.setdefault("BUCKET_NAME", "test-bucket")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

_NAMESPACE_ID = "550e8400-e29b-41d4-a716-446655440000"
_SOURCE_ID = "src-doc-001"

# Import sources_handler FIRST to resolve circular import, then document_routes
import coa_sources.api.sources_handler  # noqa: F401, I001
import coa_sources.api.document_routes as _docr  # noqa: E402, I001

_DOCR = "coa_sources.api.document_routes"
_SH = "coa_sources.api.sources_handler"


@pytest.fixture(autouse=True)
def reset_lazy_clients():
    """Reset module-level lazy client globals between tests to prevent cross-test pollution."""
    from unittest.mock import patch

    import coa_sources.api.namespace_counters as nc
    import coa_sources.api.sources_handler as sh

    sh._dao = None
    sh._sqs = None
    sh._s3 = None
    sh._sfn = None
    sh._scan_dao = None
    sh._ns_dao = None
    nc._ns_dao = None
    with patch("coa_sources.api.document_routes.adjust_namespace_source_count"):
        yield
    sh._dao = None
    sh._sqs = None
    sh._s3 = None
    sh._sfn = None
    sh._scan_dao = None
    sh._ns_dao = None
    nc._ns_dao = None


def _parse(result):
    return result["statusCode"], json.loads(result["body"]) if result.get("body") else {}


def _make_event(body=None):
    return {
        "httpMethod": "POST",
        "resource": "/namespaces/{namespaceId}/sources",
        "pathParameters": {"namespaceId": _NAMESPACE_ID},
        "queryStringParameters": {},
        "body": json.dumps(body) if body else None,
        "requestContext": {},
    }


def _make_doc_req(
    name="my-doc-source",
    source_bucket_arn=None,
    s3_prefixes=None,
    role_arn=None,
    extraction_config=None,
):
    req = MagicMock()
    req.name = name
    req.source_bucket_arn = source_bucket_arn
    req.s3_prefixes = s3_prefixes or ["uploads/"]
    req.role_arn = role_arn
    req.extraction_config = extraction_config
    return req


# ===================================================================
# _create_document_source
# ===================================================================


@pytest.mark.unit
class TestCreateDocumentSource:
    def test_create_upload_source_happy_path(self):
        mock_dao = MagicMock()
        mock_dao.query.return_value = MagicMock(items=[], last_evaluated_key=None)
        mock_sqs = MagicMock()

        with (
            patch(f"{_DOCR}._get_dao", return_value=mock_dao),
            patch(f"{_DOCR}._get_sqs", return_value=mock_sqs),
            patch(f"{_DOCR}._INGESTION_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/ingestion-queue"),
        ):
            status, body = _parse(_docr._create_document_source(_make_doc_req(), _NAMESPACE_ID, _make_event()))

        assert status == 201
        assert "sourceId" in body
        mock_dao.put.assert_called_once()
        mock_sqs.send_message.assert_called_once()

    def test_create_s3_source_happy_path(self):
        mock_dao = MagicMock()
        mock_dao.query.return_value = MagicMock(items=[], last_evaluated_key=None)
        mock_sqs = MagicMock()

        req = _make_doc_req(
            name="s3-source",
            source_bucket_arn="arn:aws:s3:::my-bucket",
            s3_prefixes=["data/"],
        )

        with (
            patch(f"{_DOCR}._get_dao", return_value=mock_dao),
            patch(f"{_DOCR}._get_sqs", return_value=mock_sqs),
            patch(f"{_DOCR}._INGESTION_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/ingestion-queue"),
        ):
            status, body = _parse(_docr._create_document_source(req, _NAMESPACE_ID, _make_event()))

        assert status == 201
        put_item = mock_dao.put.call_args[0][0]
        assert put_item["sourceSubType"] == "S3"
        assert put_item["sourceBucketArn"] == "arn:aws:s3:::my-bucket"

    def test_create_blank_name_returns_400(self):
        status, body = _parse(_docr._create_document_source(_make_doc_req(name="  "), _NAMESPACE_ID, _make_event()))
        assert status == 400
        assert "name" in body["error"]

    def test_create_duplicate_name_returns_409(self):
        existing = MagicMock()
        existing.get = lambda k, d=None: {
            "name": "my-doc-source",
            "status": "COMPLETED",
            "SK": "SRC#existing-id",
        }.get(k, d)

        mock_dao = MagicMock()
        mock_dao.query.return_value = MagicMock(items=[existing], last_evaluated_key=None)

        with patch(f"{_DOCR}._get_dao", return_value=mock_dao):
            status, body = _parse(_docr._create_document_source(_make_doc_req(), _NAMESPACE_ID, _make_event()))

        assert status == 409
        assert "already exists" in body["error"]

    def test_create_ddb_put_fails_returns_500(self):
        from botocore.exceptions import ClientError

        mock_dao = MagicMock()
        mock_dao.query.return_value = MagicMock(items=[], last_evaluated_key=None)
        mock_dao.put.side_effect = ClientError({"Error": {"Code": "InternalError"}}, "PutItem")

        with patch(f"{_DOCR}._get_dao", return_value=mock_dao):
            status, _ = _parse(_docr._create_document_source(_make_doc_req(), _NAMESPACE_ID, _make_event()))

        assert status == 500

    def test_create_sqs_fails_returns_500(self):
        from botocore.exceptions import ClientError

        mock_dao = MagicMock()
        mock_dao.query.return_value = MagicMock(items=[], last_evaluated_key=None)
        mock_sqs = MagicMock()
        mock_sqs.send_message.side_effect = ClientError({"Error": {"Code": "SQSError"}}, "SendMessage")

        with (
            patch(f"{_DOCR}._get_dao", return_value=mock_dao),
            patch(f"{_DOCR}._get_sqs", return_value=mock_sqs),
            patch(f"{_DOCR}._INGESTION_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/ingestion-queue"),
        ):
            status, _ = _parse(_docr._create_document_source(_make_doc_req(), _NAMESPACE_ID, _make_event()))

        assert status == 500

    def test_create_no_ingestion_queue_skips_sqs(self):
        mock_dao = MagicMock()
        mock_dao.query.return_value = MagicMock(items=[], last_evaluated_key=None)

        with (
            patch(f"{_DOCR}._get_dao", return_value=mock_dao),
            patch(f"{_DOCR}._INGESTION_QUEUE_URL", ""),
        ):
            status, _ = _parse(_docr._create_document_source(_make_doc_req(), _NAMESPACE_ID, _make_event()))

        assert status == 201

    def test_create_with_role_arn_stored(self):
        mock_dao = MagicMock()
        mock_dao.query.return_value = MagicMock(items=[], last_evaluated_key=None)
        mock_sqs = MagicMock()

        req = _make_doc_req(
            name="s3-with-role",
            source_bucket_arn="arn:aws:s3:::my-bucket",
            role_arn="arn:aws:iam::123456789012:role/my-role",
        )

        with (
            patch(f"{_DOCR}._get_dao", return_value=mock_dao),
            patch(f"{_DOCR}._get_sqs", return_value=mock_sqs),
            patch(f"{_DOCR}._INGESTION_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/ingestion-queue"),
        ):
            status, body = _parse(_docr._create_document_source(req, _NAMESPACE_ID, _make_event()))

        assert status == 201
        put_item = mock_dao.put.call_args[0][0]
        assert put_item["roleArn"] == "arn:aws:iam::123456789012:role/my-role"

    def test_create_gsi_query_fails_returns_500(self):
        from botocore.exceptions import ClientError

        mock_dao = MagicMock()
        mock_dao.query.side_effect = ClientError({"Error": {"Code": "InternalError"}}, "Query")

        with patch(f"{_DOCR}._get_dao", return_value=mock_dao):
            status, _ = _parse(_docr._create_document_source(_make_doc_req(), _NAMESPACE_ID, _make_event()))

        assert status == 500

    def test_create_stores_tenant_id(self):
        mock_dao = MagicMock()
        mock_dao.query.return_value = MagicMock(items=[], last_evaluated_key=None)
        mock_sqs = MagicMock()

        with (
            patch(f"{_DOCR}._get_dao", return_value=mock_dao),
            patch(f"{_DOCR}._get_sqs", return_value=mock_sqs),
            patch(f"{_DOCR}._INGESTION_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/ingestion-queue"),
        ):
            status, _ = _parse(_docr._create_document_source(_make_doc_req(), _NAMESPACE_ID, _make_event()))

        assert status == 201
        put_item = mock_dao.put.call_args[0][0]
        assert "tenantId" in put_item

    def test_create_stores_extraction_config(self):
        mock_dao = MagicMock()
        mock_dao.query.return_value = MagicMock(items=[], last_evaluated_key=None)
        mock_sqs = MagicMock()

        with (
            patch(f"{_DOCR}._get_dao", return_value=mock_dao),
            patch(f"{_DOCR}._get_sqs", return_value=mock_sqs),
            patch(f"{_DOCR}._INGESTION_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/ingestion-queue"),
        ):
            status, _ = _parse(_docr._create_document_source(_make_doc_req(), _NAMESPACE_ID, _make_event()))

        assert status == 201
        put_item = mock_dao.put.call_args[0][0]
        assert "extractionConfig" in put_item

    def test_create_local_upload_sub_type(self):
        mock_dao = MagicMock()
        mock_dao.query.return_value = MagicMock(items=[], last_evaluated_key=None)
        mock_sqs = MagicMock()

        with (
            patch(f"{_DOCR}._get_dao", return_value=mock_dao),
            patch(f"{_DOCR}._get_sqs", return_value=mock_sqs),
            patch(f"{_DOCR}._INGESTION_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/ingestion-queue"),
        ):
            status, _ = _parse(_docr._create_document_source(_make_doc_req(), _NAMESPACE_ID, _make_event()))

        assert status == 201
        put_item = mock_dao.put.call_args[0][0]
        assert put_item["sourceSubType"] == "LOCAL_UPLOAD"
        assert put_item["docSourceType"] == "upload"


# ===================================================================
# _create_document_source — namespace sourceCount maintenance
# ===================================================================


@pytest.mark.unit
class TestCreateDocumentSourceCounter:
    """The namespace ``sourceCount`` must be incremented when a DOCUMENTS
    source is created.

    The autouse ``reset_lazy_clients`` fixture patches
    ``adjust_namespace_source_count`` globally; each test re-patches it with a
    local handle so the call can be asserted.
    """

    def test_create_increments_namespace_source_count(self):
        mock_dao = MagicMock()
        mock_dao.query.return_value = MagicMock(items=[], last_evaluated_key=None)

        with (
            patch(f"{_DOCR}._get_dao", return_value=mock_dao),
            patch(f"{_DOCR}._get_sqs", return_value=MagicMock()),
            patch(f"{_DOCR}._INGESTION_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/ingestion-queue"),
            patch(f"{_DOCR}.adjust_namespace_source_count") as mock_counter,
        ):
            status, _ = _parse(_docr._create_document_source(_make_doc_req(), _NAMESPACE_ID, _make_event()))

        assert status == 201
        mock_counter.assert_called_once_with(_NAMESPACE_ID, SourceType.DOCUMENTS, 1)

    def test_create_does_not_count_when_ddb_put_fails(self):
        """If the source row never persists the counter must not be touched."""
        from botocore.exceptions import ClientError

        mock_dao = MagicMock()
        mock_dao.query.return_value = MagicMock(items=[], last_evaluated_key=None)
        mock_dao.put.side_effect = ClientError({"Error": {"Code": "ProvisionedThroughputExceeded"}}, "PutItem")

        with (
            patch(f"{_DOCR}._get_dao", return_value=mock_dao),
            patch(f"{_DOCR}._get_sqs", return_value=MagicMock()),
            patch(f"{_DOCR}.adjust_namespace_source_count") as mock_counter,
        ):
            status, _ = _parse(_docr._create_document_source(_make_doc_req(), _NAMESPACE_ID, _make_event()))

        assert status == 500
        mock_counter.assert_not_called()


# ===================================================================
# _handle_upload_urls
# ===================================================================


@pytest.mark.unit
class TestHandleUploadUrls:
    def _make_upload_event(self, body=None):
        return {
            "body": json.dumps(body) if body is not None else None,
            "pathParameters": {"namespaceId": _NAMESPACE_ID},
        }

    def test_upload_urls_happy_path(self):
        mock_s3 = MagicMock()
        mock_s3.generate_presigned_url.return_value = "https://s3.amazonaws.com/presigned-url"

        with (
            patch(f"{_DOCR}._get_s3", return_value=mock_s3),
            patch(f"{_DOCR}._BUCKET_NAME", "test-bucket"),
        ):
            event = self._make_upload_event({"files": [{"filename": "doc.pdf", "contentType": "application/pdf"}]})
            status, body = _parse(_docr._handle_upload_urls(event, _NAMESPACE_ID))

        assert status == 200
        assert "uploadId" in body
        assert "uploadUrls" in body
        assert len(body["uploadUrls"]) == 1
        assert body["uploadUrls"][0]["filename"] == "doc.pdf"
        assert "uploadUrl" in body["uploadUrls"][0]
        # Regression: never sign ContentLength — under SigV4 it forces the
        # browser to PUT exactly that many bytes or get a 403 (see !1018).
        assert "ContentLength" not in mock_s3.generate_presigned_url.call_args.kwargs["Params"]

    def test_upload_urls_multiple_files(self):
        mock_s3 = MagicMock()
        mock_s3.generate_presigned_url.return_value = "https://s3.amazonaws.com/presigned-url"

        with (
            patch(f"{_DOCR}._get_s3", return_value=mock_s3),
            patch(f"{_DOCR}._BUCKET_NAME", "test-bucket"),
        ):
            event = self._make_upload_event(
                {
                    "files": [
                        {"filename": "doc1.pdf", "contentType": "application/pdf"},
                        {"filename": "doc2.txt", "contentType": "text/plain"},
                    ]
                }
            )
            status, body = _parse(_docr._handle_upload_urls(event, _NAMESPACE_ID))

        assert status == 200
        assert len(body["uploadUrls"]) == 2

    def test_upload_urls_no_bucket_returns_500(self):
        with patch(f"{_DOCR}._BUCKET_NAME", ""):
            status, _ = _parse(_docr._handle_upload_urls(self._make_upload_event({"files": []}), _NAMESPACE_ID))
        assert status == 500

    def test_upload_urls_empty_files_returns_400(self):
        with patch(f"{_DOCR}._BUCKET_NAME", "test-bucket"):
            status, body = _parse(_docr._handle_upload_urls(self._make_upload_event({"files": []}), _NAMESPACE_ID))
        assert status == 400
        assert "files" in body["error"]

    def test_upload_urls_invalid_json_returns_400(self):
        with patch(f"{_DOCR}._BUCKET_NAME", "test-bucket"):
            status, _ = _parse(_docr._handle_upload_urls({"body": "not-json"}, _NAMESPACE_ID))
        assert status == 400

    def test_upload_urls_too_many_files_returns_400(self):
        with patch(f"{_DOCR}._BUCKET_NAME", "test-bucket"):
            files = [{"filename": f"doc{i}.pdf", "contentType": "application/pdf"} for i in range(101)]
            status, body = _parse(_docr._handle_upload_urls(self._make_upload_event({"files": files}), _NAMESPACE_ID))
        assert status == 400
        assert "Too many files" in body["error"]

    def test_upload_urls_blank_filename_returns_400(self):
        with patch(f"{_DOCR}._BUCKET_NAME", "test-bucket"):
            event = self._make_upload_event({"files": [{"filename": "", "contentType": "application/pdf"}]})
            status, body = _parse(_docr._handle_upload_urls(event, _NAMESPACE_ID))
        assert status == 400
        assert "filename" in body["error"]

    def test_upload_urls_invalid_filename_chars_returns_400(self):
        with patch(f"{_DOCR}._BUCKET_NAME", "test-bucket"):
            event = self._make_upload_event({"files": [{"filename": "../evil.pdf", "contentType": "application/pdf"}]})
            status, body = _parse(_docr._handle_upload_urls(event, _NAMESPACE_ID))
        assert status == 400

    def test_upload_urls_unsupported_content_type_returns_400(self):
        with patch(f"{_DOCR}._BUCKET_NAME", "test-bucket"):
            event = self._make_upload_event(
                {"files": [{"filename": "script.exe", "contentType": "application/x-msdownload"}]}
            )
            status, body = _parse(_docr._handle_upload_urls(event, _NAMESPACE_ID))
        assert status == 400
        assert "not supported" in body["error"]

    def test_upload_urls_blank_content_type_returns_400(self):
        with patch(f"{_DOCR}._BUCKET_NAME", "test-bucket"):
            event = self._make_upload_event({"files": [{"filename": "doc.pdf", "contentType": ""}]})
            status, body = _parse(_docr._handle_upload_urls(event, _NAMESPACE_ID))
        assert status == 400

    def test_upload_urls_non_dict_file_entry_returns_400(self):
        with patch(f"{_DOCR}._BUCKET_NAME", "test-bucket"):
            event = self._make_upload_event({"files": ["not-a-dict"]})
            status, body = _parse(_docr._handle_upload_urls(event, _NAMESPACE_ID))
        assert status == 400

    def test_upload_urls_s3_error_returns_500(self):
        from botocore.exceptions import ClientError

        mock_s3 = MagicMock()
        mock_s3.generate_presigned_url.side_effect = ClientError({"Error": {"Code": "S3Error"}}, "GeneratePresignedUrl")

        with (
            patch(f"{_DOCR}._get_s3", return_value=mock_s3),
            patch(f"{_DOCR}._BUCKET_NAME", "test-bucket"),
        ):
            event = self._make_upload_event({"files": [{"filename": "doc.pdf", "contentType": "application/pdf"}]})
            status, _ = _parse(_docr._handle_upload_urls(event, _NAMESPACE_ID))

        assert status == 500

    def test_upload_urls_response_has_s3_prefix(self):
        mock_s3 = MagicMock()
        mock_s3.generate_presigned_url.return_value = "https://s3.amazonaws.com/presigned-url"

        with (
            patch(f"{_DOCR}._get_s3", return_value=mock_s3),
            patch(f"{_DOCR}._BUCKET_NAME", "test-bucket"),
        ):
            event = self._make_upload_event({"files": [{"filename": "doc.pdf", "contentType": "application/pdf"}]})
            status, body = _parse(_docr._handle_upload_urls(event, _NAMESPACE_ID))

        assert status == 200
        assert "s3Prefix" in body
        assert _NAMESPACE_ID in body["s3Prefix"]
        assert body["expiresIn"] == 900
