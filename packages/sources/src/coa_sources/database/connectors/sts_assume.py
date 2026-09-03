# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Shared STS AssumeRole for customer-supplied cross-account datasource roles.

Every cross-account assume on the discovery path goes through
``assume_datasource_session`` — the JDBC credential fetch and the Glue catalog
read both call it, so the ExternalId guard below exists exactly once.

The ExternalId is **required**. It is the only thing that binds an assume to the
namespace that asked for it: the role ARN arrives from the API caller, so without
an ExternalId any principal holding ``manageSource`` on any namespace could point
a source at another tenant's ``*-datasource-access-*`` role and read its catalog
metadata or credential secret (confused deputy). ``discovery_handler`` derives the
value from the namespace and never reads it from the request.
"""

from __future__ import annotations

import logging

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# STS caps RoleSessionName at 64 characters.
MAX_SESSION_NAME_LEN = 64


def assume_datasource_session(
    role_arn: str,
    external_id: str,
    region: str,
    session_name: str,
) -> boto3.Session:
    """Assume *role_arn* with *external_id* and return a session for it.

    Raises ``ValueError`` when either is empty. Callers must not fall back to an
    unconditioned assume: an assume with no ExternalId carries no evidence of
    which namespace requested it, which is the whole control.

    *session_name* is truncated to the STS limit and lands in the data owner's
    CloudTrail, so callers should encode the requesting namespace in it.
    """
    if not role_arn:
        raise ValueError("role_arn is required to assume a cross-account datasource role")
    if not external_id:
        raise ValueError("external_id is required to assume a cross-account datasource role")

    sts = boto3.client("sts", region_name=region)
    try:
        creds = sts.assume_role(
            RoleArn=role_arn,
            RoleSessionName=session_name[:MAX_SESSION_NAME_LEN],
            ExternalId=external_id,
        )["Credentials"]
    except ClientError as e:
        # Log the ARN for operators; return only the STS error code to the caller.
        logger.warning(
            "datasource_assume_role_failed",
            extra={"role_arn": role_arn, "session_name": session_name},
        )
        raise ValueError(f"Failed to assume role: {e.response['Error']['Code']}") from e

    logger.info(
        "datasource_assume_role_ok",
        extra={"role_arn": role_arn, "session_name": session_name},
    )
    return boto3.Session(
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
        region_name=region,
    )
