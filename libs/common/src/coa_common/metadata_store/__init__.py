# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Metadata store abstraction and implementations."""

from .base import AssetResult, MetadataStoreClient, ProjectResult, SearchResult
from .catalog_reader import read_approved_catalog
from .exceptions import MetadataStoreError
from .factory import build_metadata_store
from .reader import read_assets_for_datasource
from .smus import SMUSClient

__all__ = [
    "AssetResult",
    "MetadataStoreClient",
    "MetadataStoreError",
    "ProjectResult",
    "SMUSClient",
    "SearchResult",
    "build_metadata_store",
    "read_approved_catalog",
    "read_assets_for_datasource",
]
