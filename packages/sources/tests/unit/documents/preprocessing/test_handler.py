# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the preprocessing Lambda handler."""

import os
from unittest.mock import call, patch

import pytest

# Add preprocessing source to path for imports

# Set required env vars before importing handler (module-level validation runs at import)
os.environ.setdefault("BUCKET_NAME", "test-bucket")
os.environ.setdefault("DOC_SOURCES_TABLE", "test-table")

from coa_common.constants import validate_id  # noqa: E402

# Common env vars required by handler
_ENV = {"BUCKET_NAME": "test-bucket", "DOC_SOURCES_TABLE": "test-table"}


# ---------------------------------------------------------------------------
# Input validation — validate_id (now in coa_common.constants)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestValidateId:
    def test_valid_simple(self):
        validate_id("valid-id_123", "test")  # should not raise

    def test_valid_alphanumeric(self):
        validate_id("abc123", "test")

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="Invalid test"):
            validate_id("", "test")

    def test_path_traversal_raises(self):
        with pytest.raises(ValueError):
            validate_id("../path/traversal", "test")

    def test_spaces_raise(self):
        with pytest.raises(ValueError):
            validate_id("has spaces", "test")

    def test_dots_raise(self):
        with pytest.raises(ValueError):
            validate_id("has.dots", "test")

    def test_slashes_raise(self):
        with pytest.raises(ValueError):
            validate_id("has/slash", "test")


# ---------------------------------------------------------------------------
# Validation error responses (400)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestHandlerValidation:
    @patch.dict(os.environ, _ENV)
    def test_invalid_namespace_returns_400(self):
        from coa_sources.documents.preprocessing.handler import handler

        result = handler({"namespace_id": "../bad", "doc_source_id": "ok"}, None)
        assert result["status"] == "SCAN_FAILED"
        assert len(result["issues"]) > 0

    @patch.dict(os.environ, _ENV)
    def test_empty_ids_returns_400(self):
        from coa_sources.documents.preprocessing.handler import handler

        result = handler({"namespace_id": "", "doc_source_id": ""}, None)
        assert result["status"] == "SCAN_FAILED"

    @patch.dict(os.environ, _ENV)
    def test_invalid_doc_source_returns_400(self):
        from coa_sources.documents.preprocessing.handler import handler

        result = handler({"namespace_id": "ok", "doc_source_id": "bad/id"}, None)
        assert result["status"] == "SCAN_FAILED"
        assert result["issues"][0]["reason"]


# ---------------------------------------------------------------------------
# Response shape
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestHandlerResponseShape:
    @patch.dict(os.environ, _ENV)
    @patch("coa_sources.documents.preprocessing.handler.list_objects", return_value=[])
    @patch("coa_sources.documents.preprocessing.handler.get_s3_client")
    def test_empty_listing_has_all_keys(self, mock_client, mock_list):
        from coa_sources.documents.preprocessing.handler import handler

        result = handler(
            {"namespace_id": "ns1", "doc_source_id": "ds1", "s3_prefixes": ["ns1/raw/ds1/"]},
            None,
        )
        assert result["status"] == "SCANNING"
        expected_keys = {
            "status",
            "staging_prefix",
            "files_total",
            "files_preprocessed",
            "files_skipped",
            "files_errored",
            "issues",
            "elapsed_seconds",
        }
        assert expected_keys.issubset(result.keys())
        assert isinstance(result["elapsed_seconds"], float)
        assert result["files_total"] == 0


# ---------------------------------------------------------------------------
# s3_prefixes — whole bucket and multi-prefix behaviour
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestS3PrefixBehaviour:
    @patch.dict(os.environ, _ENV)
    @patch("coa_sources.documents.preprocessing.handler.list_objects", return_value=[])
    @patch("coa_sources.documents.preprocessing.handler.get_s3_client")
    def test_empty_prefixes_lists_whole_bucket(self, mock_client, mock_list):
        """No s3_prefixes → list_objects called with empty prefix (whole bucket)."""
        from coa_sources.documents.preprocessing.handler import handler

        handler({"namespace_id": "ns1", "doc_source_id": "ds1"}, None)
        mock_list.assert_called_once()
        args = mock_list.call_args[0]
        assert args[2] == ""  # empty prefix = whole bucket

    @patch.dict(os.environ, _ENV)
    @patch("coa_sources.documents.preprocessing.handler.list_objects", return_value=[])
    @patch("coa_sources.documents.preprocessing.handler.get_s3_client")
    def test_single_prefix_used(self, mock_client, mock_list):
        from coa_sources.documents.preprocessing.handler import handler

        handler({"namespace_id": "ns1", "doc_source_id": "ds1", "s3_prefixes": ["custom/path/"]}, None)
        args = mock_list.call_args[0]
        assert args[2] == "custom/path/"

    @patch.dict(os.environ, _ENV)
    @patch("coa_sources.documents.preprocessing.handler.list_objects", return_value=[])
    @patch("coa_sources.documents.preprocessing.handler.get_s3_client")
    def test_multiple_prefixes_calls_list_objects_per_prefix(self, mock_client, mock_list):
        from coa_sources.documents.preprocessing.handler import handler

        handler(
            {"namespace_id": "ns1", "doc_source_id": "ds1", "s3_prefixes": ["reports/2024/", "reports/2025/"]},
            None,
        )
        assert mock_list.call_count == 2
        prefixes_called = [c[0][2] for c in mock_list.call_args_list]
        assert "reports/2024/" in prefixes_called
        assert "reports/2025/" in prefixes_called

    @patch.dict(os.environ, _ENV)
    @patch("coa_sources.documents.preprocessing.handler.list_objects", return_value=[])
    @patch("coa_sources.documents.preprocessing.handler.get_s3_client")
    def test_staging_prefix_always_derived(self, mock_client, mock_list):
        from coa_sources.documents.preprocessing.handler import handler

        result = handler({"namespace_id": "myns", "doc_source_id": "myds"}, None)
        assert result["staging_prefix"] == "myns/staging/myds/"

    @patch.dict(os.environ, _ENV)
    @patch("coa_sources.documents.preprocessing.handler.upload_metadata")
    @patch("coa_sources.documents.preprocessing.handler.upload_file")
    @patch("coa_sources.documents.preprocessing.handler.read_file_bytes", return_value=b"ok")
    @patch("coa_sources.documents.preprocessing.handler.list_objects")
    @patch("coa_sources.documents.preprocessing.handler.get_s3_client")
    def test_deduplicates_overlapping_prefixes(self, mock_client, mock_list, mock_read, mock_upload, mock_meta):
        """If two prefixes return the same key, it should only be processed once."""
        mock_list.return_value = [{"Key": "reports/doc.txt", "Size": 2}]
        from coa_sources.documents.preprocessing.handler import handler

        result = handler(
            {"namespace_id": "ns1", "doc_source_id": "ds1", "s3_prefixes": ["reports/", "reports/"]},
            None,
        )
        assert result["files_preprocessed"] == 1
        assert mock_upload.call_count == 1


# ---------------------------------------------------------------------------
# Cross-account S3 access
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCrossAccountAccess:
    @patch.dict(os.environ, _ENV)
    @patch("coa_sources.documents.preprocessing.handler.list_objects", return_value=[])
    @patch(
        "coa_sources.documents.preprocessing.handler.parse_bucket_from_arn",
        return_value="external-bucket",
    )
    @patch("coa_sources.documents.preprocessing.handler.get_s3_client")
    def test_source_bucket_arn_uses_external_bucket(self, mock_client, mock_parse, mock_list):
        from coa_sources.documents.preprocessing.handler import handler

        handler(
            {
                "namespace_id": "ns1",
                "doc_source_id": "ds1",
                "source_bucket_arn": "arn:aws:s3:::external-bucket",
                "role_arn": "arn:aws:iam::123456789012:role/coa-cross-account-reader",
            },
            None,
        )
        mock_parse.assert_called_once_with("arn:aws:s3:::external-bucket")
        assert mock_client.call_count == 2
        first_call = mock_client.call_args_list[0]
        assert first_call == call(role_arn="arn:aws:iam::123456789012:role/coa-cross-account-reader")
        list_call_args = mock_list.call_args[0]
        assert list_call_args[1] == "external-bucket"

    @patch.dict(os.environ, _ENV)
    @patch("coa_sources.documents.preprocessing.handler.list_objects", return_value=[])
    @patch("coa_sources.documents.preprocessing.handler.parse_bucket_from_arn", return_value="ext-bucket")
    @patch("coa_sources.documents.preprocessing.handler.get_s3_client")
    def test_cross_account_without_role_arn(self, mock_client, mock_parse, mock_list):
        from coa_sources.documents.preprocessing.handler import handler

        handler(
            {
                "namespace_id": "ns1",
                "doc_source_id": "ds1",
                "source_bucket_arn": "arn:aws:s3:::ext-bucket",
            },
            None,
        )
        first_call = mock_client.call_args_list[0]
        assert first_call == call(role_arn=None)

    @patch.dict(os.environ, _ENV)
    @patch("coa_sources.documents.preprocessing.handler.list_objects", return_value=[])
    @patch("coa_sources.documents.preprocessing.handler.get_s3_client")
    def test_no_source_bucket_uses_our_bucket(self, mock_client, mock_list):
        from coa_sources.documents.preprocessing.handler import handler

        handler({"namespace_id": "ns1", "doc_source_id": "ds1"}, None)
        assert mock_client.call_count == 2
        assert mock_client.call_args_list[0] == call()
        list_call_args = mock_list.call_args[0]
        assert list_call_args[1] == "test-bucket"


# ---------------------------------------------------------------------------
# Empty extraction — the 39-of-78 silent loss
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestEmptyExtractionIsReported:
    """A processor can return "" without raising, and used to be counted a success.

    `unstructured` abandons text extraction on drawing-heavy PDF pages and yields
    zero characters. That empty string was uploaded to staging as a 0-byte object
    and counted in files_preprocessed, so the source reported Skipped=0/Errored=0
    while KG build silently dropped every empty file — 39 of 78 PDFs vanished with
    nothing in the UI to show it.
    """

    @patch.dict(os.environ, _ENV)
    @patch("coa_sources.documents.preprocessing.handler.upload_metadata")
    @patch("coa_sources.documents.preprocessing.handler.upload_file")
    @patch("coa_sources.documents.preprocessing.handler.read_file_bytes", return_value=b"%PDF-1.7")
    @patch("coa_sources.documents.preprocessing.handler.get_page_count", return_value=30)
    @patch("coa_sources.documents.preprocessing.handler.process_pdf", return_value=("", ".md"))
    @patch("coa_sources.documents.preprocessing.handler.list_objects")
    @patch("coa_sources.documents.preprocessing.handler.get_s3_client")
    def test_empty_pdf_is_skipped_not_staged(
        self, mock_client, mock_list, mock_pdf, mock_pages, mock_read, mock_upload, mock_meta
    ):
        mock_list.return_value = [{"Key": "raw/drawing.pdf", "Size": 5000}]
        from coa_sources.documents.preprocessing.handler import handler

        result = handler({"namespace_id": "ns1", "doc_source_id": "ds1"}, None)

        assert result["files_preprocessed"] == 0, "an empty extraction is not a success"
        assert result["files_skipped"] == 1
        assert result["files_errored"] == 0, "empty is skipped, not an error"
        assert mock_upload.call_count == 0, "no 0-byte object may reach staging"
        assert mock_meta.call_count == 0
        assert len(result["issues"]) == 1
        issue = result["issues"][0]
        assert issue["filename"] == "raw/drawing.pdf"
        assert issue["type"] == "skipped"
        assert "No text extracted" in issue["reason"]

    @patch.dict(os.environ, _ENV)
    @patch("coa_sources.documents.preprocessing.handler.upload_metadata")
    @patch("coa_sources.documents.preprocessing.handler.upload_file")
    @patch("coa_sources.documents.preprocessing.handler.read_file_bytes", return_value=b"%PDF-1.7")
    @patch("coa_sources.documents.preprocessing.handler.get_page_count", return_value=1)
    @patch("coa_sources.documents.preprocessing.handler.process_pdf", return_value=("| a | b |", ".md"))
    @patch("coa_sources.documents.preprocessing.handler.list_objects")
    @patch("coa_sources.documents.preprocessing.handler.get_s3_client")
    def test_enable_table_extraction_flag_reaches_process_pdf(
        self, mock_client, mock_list, mock_pdf, mock_pages, mock_read, mock_upload, mock_meta
    ):
        """The flag arrives on the state-machine input as a string in
        extraction_config and must be forwarded as a bool kwarg."""
        mock_list.return_value = [{"Key": "policy.pdf", "Size": 5000}]
        from coa_sources.documents.preprocessing.handler import handler

        result = handler(
            {
                "namespace_id": "ns1",
                "doc_source_id": "ds1",
                "extraction_config": {"enable_table_extraction": "true"},
            },
            None,
        )
        assert result["files_preprocessed"] == 1
        _, kwargs = mock_pdf.call_args
        assert kwargs.get("enable_table_extraction") is True
        # Metadata records which processing method ran, so operators can grep.
        uploaded_metadata = mock_meta.call_args.args[3]
        assert uploaded_metadata["processing_method"] == "textract_analyze_document_tables"

    @patch.dict(os.environ, _ENV)
    @patch("coa_sources.documents.preprocessing.handler.upload_metadata")
    @patch("coa_sources.documents.preprocessing.handler.upload_file")
    @patch("coa_sources.documents.preprocessing.handler.read_file_bytes", return_value=b"%PDF-1.7")
    @patch("coa_sources.documents.preprocessing.handler.get_page_count", return_value=1)
    @patch("coa_sources.documents.preprocessing.handler.process_pdf", return_value=("prose", ".md"))
    @patch("coa_sources.documents.preprocessing.handler.list_objects")
    @patch("coa_sources.documents.preprocessing.handler.get_s3_client")
    def test_enable_table_extraction_defaults_to_false(
        self, mock_client, mock_list, mock_pdf, mock_pages, mock_read, mock_upload, mock_meta
    ):
        """Missing extraction_config → flag is False (backward compatible)."""
        mock_list.return_value = [{"Key": "prose.pdf", "Size": 5000}]
        from coa_sources.documents.preprocessing.handler import handler

        handler({"namespace_id": "ns1", "doc_source_id": "ds1"}, None)
        _, kwargs = mock_pdf.call_args
        assert kwargs.get("enable_table_extraction") is False

    @patch.dict(os.environ, _ENV)
    @patch("coa_sources.documents.preprocessing.handler.upload_metadata")
    @patch("coa_sources.documents.preprocessing.handler.upload_file")
    @patch("coa_sources.documents.preprocessing.handler.read_file_bytes")
    @patch("coa_sources.documents.preprocessing.handler.list_objects")
    @patch("coa_sources.documents.preprocessing.handler.get_s3_client")
    def test_whitespace_only_is_empty(self, mock_client, mock_list, mock_read, mock_upload, mock_meta):
        """A file of only whitespace stages nothing — KG build would skip it anyway."""
        mock_list.return_value = [{"Key": "blank.txt", "Size": 6}]
        mock_read.return_value = b"  \n\t \n"
        from coa_sources.documents.preprocessing.handler import handler

        result = handler({"namespace_id": "ns1", "doc_source_id": "ds1"}, None)

        assert result["files_skipped"] == 1
        assert result["files_preprocessed"] == 0
        assert mock_upload.call_count == 0

    @patch.dict(os.environ, _ENV)
    @patch("coa_sources.documents.preprocessing.handler.upload_metadata")
    @patch("coa_sources.documents.preprocessing.handler.upload_file")
    @patch("coa_sources.documents.preprocessing.handler.read_file_bytes")
    @patch("coa_sources.documents.preprocessing.handler.list_objects")
    @patch("coa_sources.documents.preprocessing.handler.get_s3_client")
    def test_empty_files_do_not_hide_the_ones_that_worked(
        self, mock_client, mock_list, mock_read, mock_upload, mock_meta
    ):
        """The guard is per-file: a real file still stages, and the counts split."""
        mock_list.return_value = [{"Key": "good.txt", "Size": 12}, {"Key": "empty.txt", "Size": 0}]
        mock_read.side_effect = [b"real content", b""]
        from coa_sources.documents.preprocessing.handler import handler

        result = handler({"namespace_id": "ns1", "doc_source_id": "ds1"}, None)

        assert result["files_total"] == 2
        assert result["files_preprocessed"] == 1
        assert result["files_skipped"] == 1
        assert mock_upload.call_count == 1
        assert mock_upload.call_args[0][3] == "real content"

    @patch.dict(os.environ, _ENV)
    @patch("coa_sources.documents.preprocessing.handler.upload_metadata")
    @patch("coa_sources.documents.preprocessing.handler.upload_file")
    @patch("coa_sources.documents.preprocessing.handler.read_file_bytes", return_value=b"text")
    @patch("coa_sources.documents.preprocessing.handler.list_objects")
    @patch("coa_sources.documents.preprocessing.handler.get_s3_client")
    def test_every_file_empty_is_a_scan_failure(self, mock_client, mock_list, mock_read, mock_upload, mock_meta):
        """Nothing extracted from anything is SCAN_FAILED, not a completed scan."""
        mock_list.return_value = [{"Key": "a.txt", "Size": 1}, {"Key": "b.txt", "Size": 1}]
        mock_read.side_effect = [b"", b"   "]
        from coa_sources.documents.preprocessing.handler import handler

        result = handler({"namespace_id": "ns1", "doc_source_id": "ds1"}, None)

        assert result["status"] == "SCAN_FAILED"
        assert result["files_skipped"] == 2
        assert result["files_preprocessed"] == 0

    @patch.dict(os.environ, _ENV)
    @patch("coa_sources.documents.preprocessing.handler.upload_metadata")
    @patch("coa_sources.documents.preprocessing.handler.upload_file")
    @patch("coa_sources.documents.preprocessing.handler.read_file_bytes", return_value=b"hello world")
    @patch("coa_sources.documents.preprocessing.handler.list_objects")
    @patch("coa_sources.documents.preprocessing.handler.get_s3_client")
    def test_success_path_is_untouched(self, mock_client, mock_list, mock_read, mock_upload, mock_meta):
        """A normal extraction is unaffected — same counts, same staging key."""
        mock_list.return_value = [{"Key": "reports/notes.txt", "Size": 11}]
        from coa_sources.documents.preprocessing.handler import handler

        result = handler({"namespace_id": "ns1", "doc_source_id": "ds1"}, None)

        assert result["files_preprocessed"] == 1
        assert result["files_skipped"] == 0
        assert result["issues"] == []
        assert mock_upload.call_count == 1
        assert mock_upload.call_args[0][2].endswith("reports/notes.txt")
