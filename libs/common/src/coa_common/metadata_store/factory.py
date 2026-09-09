# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Metadata store provider factory — one construction seam for all call sites.

``METADATA_STORE_PROVIDER=local`` builds a :class:`LocalMetadataStore`
(DynamoDB-backed); anything else builds the production :class:`SMUSClient`
unchanged. Import this instead of importing SMUSClient directly so the local
Docker stack can redirect every metadata call through one switch.
"""

from __future__ import annotations

import os

from .base import MetadataStoreClient


def build_metadata_store(
    *,
    domain_id: str = "",
    region_name: str | None = None,
    assume_role_arn: str | None = None,
    session_name: str = "metadata-store",
) -> MetadataStoreClient:
    """Build the configured MetadataStoreClient (local or SMUS/DataZone).

    Args:
        domain_id: DataZone domain id (SMUS provider only).
        region_name: AWS region.
        assume_role_arn: Optional cross-account role (SMUS provider).
        session_name: STS session name (SMUS provider).
    """
    provider = os.environ.get("METADATA_STORE_PROVIDER", "smus").lower()
    if provider == "local":
        from .local_store import LocalMetadataStore

        return LocalMetadataStore(region_name=region_name)
    from .smus import SMUSClient

    if not domain_id:
        raise ValueError("domain_id is required for the SMUS metadata store")
    return SMUSClient(
        domain_id=domain_id,
        region_name=region_name,
        assume_role_arn=assume_role_arn,
        session_name=session_name,
    )
