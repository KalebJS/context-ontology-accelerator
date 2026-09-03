# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the credential-secret → namespace binding.

`_validate_credential_secret_binding` is the front-door check that a JDBC
source's `credentialSecretArn` is bound to the registering namespace: an
in-account secret must carry the tag ``coa:namespace == <namespaceId>``, and
anything unverifiable fails closed with a 400.

Imports go through sources_handler first to resolve the documented circular
import with database_routes (see test_database_routes.py).
"""

from __future__ import annotations

import os
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

os.environ.setdefault("SOURCES_TABLE", "test-sources")
os.environ.setdefault("SOURCE_SCAN_JOBS_TABLE", "test-scan-jobs")
os.environ.setdefault("NAMESPACES_TABLE", "test-namespaces")
os.environ.setdefault("SCAN_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/scan-queue")
os.environ.setdefault("SMUS_DOMAIN_ID", "test-domain-id")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

import coa_sources.api.sources_handler  # noqa: F401, I001
import coa_sources.api.database_routes as _dr  # noqa: E402, I001

_ACCOUNT = "111122223333"
_NS = "550e8400-e29b-41d4-a716-446655440000"
_IN_ACCT_ARN = f"arn:aws:secretsmanager:us-east-1:{_ACCOUNT}:secret:ns-a/db-AbCdEf"
_CROSS_ACCT_ARN = "arn:aws:secretsmanager:us-east-1:999988887777:secret:ns-b/db-XyZ123"


@pytest.fixture(autouse=True)
def _pin_account(monkeypatch):
    """Pin the deployment account so no real STS call is made."""
    monkeypatch.setattr(_dr, "_deployment_account_id", lambda: _ACCOUNT)


def _jdbc(secret_arn: str | None) -> SimpleNamespace:
    return SimpleNamespace(credential_secret_arn=secret_arn)


def test_in_account_matching_tag_is_allowed(monkeypatch):
    monkeypatch.setattr(_dr, "_describe_secret_tags", lambda arn, region: {"coa:namespace": _NS})
    assert _dr._validate_credential_secret_binding(_jdbc(_IN_ACCT_ARN), _NS) is None


def test_in_account_wrong_tag_value_is_rejected(monkeypatch):
    monkeypatch.setattr(_dr, "_describe_secret_tags", lambda arn, region: {"coa:namespace": "some-other-namespace"})
    resp = _dr._validate_credential_secret_binding(_jdbc(_IN_ACCT_ARN), _NS)
    assert resp is not None and resp["statusCode"] == 400


def test_in_account_missing_tag_is_rejected(monkeypatch):
    monkeypatch.setattr(_dr, "_describe_secret_tags", lambda arn, region: {"env": "prod"})
    resp = _dr._validate_credential_secret_binding(_jdbc(_IN_ACCT_ARN), _NS)
    assert resp is not None and resp["statusCode"] == 400


def test_describe_secret_denied_fails_closed(monkeypatch):
    def _raise(arn, region):
        raise ClientError({"Error": {"Code": "AccessDeniedException", "Message": "no"}}, "DescribeSecret")

    monkeypatch.setattr(_dr, "_describe_secret_tags", _raise)
    resp = _dr._validate_credential_secret_binding(_jdbc(_IN_ACCT_ARN), _NS)
    assert resp is not None and resp["statusCode"] == 400


def test_cross_account_arn_skips_tag_check(monkeypatch):
    # Cross-account secrets are gated by the customer's resource policy, not tags.
    # DescribeSecret must NOT be attempted (it would fail against another account).
    spy = MagicMock(side_effect=AssertionError("describe_secret must not be called cross-account"))
    monkeypatch.setattr(_dr, "_describe_secret_tags", spy)
    assert _dr._validate_credential_secret_binding(_jdbc(_CROSS_ACCT_ARN), _NS) is None
    spy.assert_not_called()


def test_malformed_arn_fails_closed(monkeypatch):
    # An ARN with no parsable account must be rejected, never treated as
    # cross-account and skipped.
    spy = MagicMock(side_effect=AssertionError("describe_secret must not be called for a malformed ARN"))
    monkeypatch.setattr(_dr, "_describe_secret_tags", spy)
    resp = _dr._validate_credential_secret_binding(_jdbc("not-an-arn"), _NS)
    assert resp is not None and resp["statusCode"] == 400
    spy.assert_not_called()


def test_account_resolution_failure_fails_closed(monkeypatch):
    # If STS can't resolve the deployment account we cannot tell in- from
    # cross-account, so we must fail closed (retryable), not skip the check.
    from botocore.exceptions import ClientError

    def _boom():
        raise ClientError({"Error": {"Code": "Throttling", "Message": "slow down"}}, "GetCallerIdentity")

    monkeypatch.setattr(_dr, "_deployment_account_id", _boom)
    resp = _dr._validate_credential_secret_binding(_jdbc(_IN_ACCT_ARN), _NS)
    assert resp is not None and resp["statusCode"] == 503


def test_no_jdbc_config_is_noop():
    assert _dr._validate_credential_secret_binding(None, _NS) is None


def test_jdbc_without_secret_is_noop(monkeypatch):
    spy = MagicMock(side_effect=AssertionError("describe_secret must not be called without a secret ARN"))
    monkeypatch.setattr(_dr, "_describe_secret_tags", spy)
    assert _dr._validate_credential_secret_binding(_jdbc(None), _NS) is None
    spy.assert_not_called()
