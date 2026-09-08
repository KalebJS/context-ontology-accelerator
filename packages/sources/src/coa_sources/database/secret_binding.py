# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Credential-secret → namespace binding.

A JDBC source's credential secret is bound to the namespaces entitled to it by a
``<prefix>:namespace`` resource tag whose value lists one or more namespace UUIDs
(see :mod:`coa_common.constants` for the key derivation and why the value is
parsed strictly). An **in-account** secret is usable by a source only if the
source's namespace is one of the listed entries.

This module is the single implementation of that rule, because it is enforced at
more than one point and the two must not drift:

* **Registration** (:mod:`coa_sources.api.database_routes`) — the front door. An
  ARN not bound to the namespace is never persisted.
* **Scan time** (:mod:`~coa_sources.database.pipeline.discovery_handler`,
  :mod:`~coa_sources.database.pipeline.federation_handler`) — re-verified against
  the row before the secret is read. Registration alone cannot cover a row
  written before the rule existed, nor a secret whose tag list changed after
  registration; both handlers read the ARN back out of the sources table, so both
  re-check it there.

The tag is read with ``DescribeSecret`` — metadata only, never
``GetSecretValue``. The IAM ``aws:ResourceTag`` conditions on the
discovery/federation/serve roles enforce the same binding at the platform layer,
but those conditions can only assert the tag EXISTS (a shared execution role has
no per-request namespace identity), so the exact membership test lives here.

Every uncertainty fails closed. A malformed ARN, an unresolvable deployment
account, an unreadable secret, an absent tag, a tag that will not parse, and a
tag that does not list the namespace are all refusals — an uncertainty is never
resolved as "cross-account, skip".
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from coa_common import sync_boto_config
from coa_common.constants import namespace_tag_key, parse_namespace_tag

logger = logging.getLogger(__name__)

NAMESPACE_TAG_KEY = namespace_tag_key()

AWS_REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

_deployment_account: str | None = None


@dataclass(frozen=True)
class BindingVerdict:
    """Outcome of a binding check.

    ``ok`` is the only thing a caller must branch on. ``reason`` is a stable
    machine-readable code for logs and tests; ``message`` is caller-facing prose;
    ``retryable`` marks a failure that is an infrastructure hiccup rather than a
    refusal, so an API caller gets a 5xx and a re-scan can succeed unchanged.
    """

    ok: bool
    reason: str
    message: str = ""
    retryable: bool = False


def arn_account(arn: str) -> str:
    """Account-id segment of an ARN, or ``""`` when it has no parsable one."""
    parts = (arn or "").split(":")
    return parts[4] if len(parts) > 5 else ""


def arn_region(arn: str) -> str:
    """Region segment of an ARN, or ``""`` when it has no parsable one."""
    parts = (arn or "").split(":")
    return parts[3] if len(parts) > 5 else ""


def deployment_account_id() -> str:
    """This deployment's AWS account id (cached STS lookup)."""
    global _deployment_account
    if _deployment_account is None:
        sts = boto3.client("sts", region_name=AWS_REGION, config=sync_boto_config())
        _deployment_account = sts.get_caller_identity()["Account"]
    return _deployment_account


def describe_secret_tags(secret_arn: str, region: str) -> dict[str, str]:
    """Return the secret's tags as a ``{key: value}`` map via DescribeSecret.

    Metadata only — this never calls GetSecretValue. Raises ``ClientError`` /
    ``BotoCoreError`` on failure; callers fail closed on those.
    """
    client = boto3.client("secretsmanager", region_name=region, config=sync_boto_config())
    described = client.describe_secret(SecretId=secret_arn)
    return {t["Key"]: t.get("Value", "") for t in described.get("Tags", []) if "Key" in t}


def check_secret_namespace_binding(secret_arn: str | None, namespace_id: str) -> BindingVerdict:
    """Verify ``secret_arn`` is bound to ``namespace_id``.

    Returns an ``ok`` verdict when the namespace is one of the tag's entries, when
    there is no secret to check, or when the secret is cross-account (not bindable
    by a tag this deployment controls — authorization there comes from the
    secret's own resource policy plus the cross-account assume-role, validated by
    connectivity at scan time). Otherwise returns a refusal.
    """
    if not secret_arn:
        return BindingVerdict(ok=True, reason="no-secret")

    # Fail closed on an unattributable ARN rather than treating it as
    # cross-account and skipping (defense in depth — the Smithy pattern normally
    # rejects a malformed ARN upstream, but this must not depend on that).
    secret_account = arn_account(secret_arn)
    if not secret_account:
        return BindingVerdict(
            ok=False,
            reason="malformed-arn",
            message="credentialSecretArn is malformed: cannot determine its AWS account.",
        )

    # Resolving this deployment's account is what tells in-account from
    # cross-account. If STS is unavailable we cannot make that call safely, so we
    # fail closed and retryable instead of skipping the check (which would bypass
    # the binding).
    try:
        deployment_account = deployment_account_id()
    except (ClientError, BotoCoreError):
        logger.warning("deployment_account_resolution_failed", exc_info=True)
        return BindingVerdict(
            ok=False,
            reason="account-unresolved",
            message="Could not verify credentialSecretArn binding (account resolution failed); retry.",
            retryable=True,
        )

    if secret_account != deployment_account:
        return BindingVerdict(ok=True, reason="cross-account")

    region = arn_region(secret_arn) or AWS_REGION
    try:
        tags = describe_secret_tags(secret_arn, region)
    except (ClientError, BotoCoreError):
        logger.warning("credential_secret_binding_unverifiable", exc_info=True)
        return BindingVerdict(
            ok=False,
            reason="unverifiable",
            message=(
                "credentialSecretArn could not be verified. The secret must exist in this "
                "account and be readable (secretsmanager:DescribeSecret) and carry a "
                f"'{NAMESPACE_TAG_KEY}' tag listing '{namespace_id}'."
            ),
        )

    raw_tag = tags.get(NAMESPACE_TAG_KEY)
    if raw_tag is None:
        logger.warning("credential_secret_binding_rejected", extra={"reason": "tag_absent"})
        return BindingVerdict(
            ok=False,
            reason="tag-absent",
            message=(
                f"credentialSecretArn must be bound to this namespace: tag the secret with "
                f"'{NAMESPACE_TAG_KEY}={namespace_id}'. A secret may only be used by the "
                "namespaces it is tagged for; list several by separating their IDs with a space."
            ),
        )

    # A tag we cannot parse is not a binding. Reject rather than guess: the value
    # is written by whoever owns the secret, and the IAM conditions that enforce
    # the same binding downstream match whole space-delimited entries.
    try:
        bound_namespaces = parse_namespace_tag(raw_tag)
    except ValueError as exc:
        logger.warning("credential_secret_binding_malformed", extra={"reason": str(exc)})
        return BindingVerdict(
            ok=False,
            reason="malformed-tag",
            message=(
                f"credentialSecretArn tag '{NAMESPACE_TAG_KEY}' is malformed: {exc} "
                f"Expected one or more namespace UUIDs separated by a single space, "
                f"including '{namespace_id}'."
            ),
        )

    if namespace_id not in bound_namespaces:
        logger.warning(
            "credential_secret_binding_rejected",
            extra={"reason": "namespace_not_listed", "bound_namespace_count": len(bound_namespaces)},
        )
        return BindingVerdict(
            ok=False,
            reason="not-listed",
            message=(
                f"credentialSecretArn must be bound to this namespace: add '{namespace_id}' to the "
                f"secret's '{NAMESPACE_TAG_KEY}' tag (namespace UUIDs separated by a single space). "
                "A secret may only be used by the namespaces it is tagged for."
            ),
        )

    return BindingVerdict(ok=True, reason="bound")


def require_secret_namespace_binding(secret_arn: str | None, namespace_id: str, datasource_id: str) -> None:
    """Scan-pipeline form of :func:`check_secret_namespace_binding`: raise on refusal.

    Used by the pipeline handlers, where there is no caller to return a status
    code to. Raising fails the Lambda, so the scan-pipeline Catch marks the scan
    FAILED and the source SCAN_FAILED — the same treatment a source whose
    credentials no longer work already gets, and the outcome the operator needs to
    see: the secret's tag must list this namespace before the source can scan.
    """
    verdict = check_secret_namespace_binding(secret_arn, namespace_id)
    if verdict.ok:
        return
    raise RuntimeError(
        f"Credential secret is not bound to namespace {namespace_id} ({datasource_id}): "
        f"{verdict.message} [{verdict.reason}]"
    )
