# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Connector framework for structured data source metadata discovery."""

from coa_control_plane_server.models.source_sub_type import SourceSubType

from .athena_connector import AthenaConnector
from .base import (
    ConnectionCheck,
    ConnectionTestResult,
    MetadataConnector,
)
from .glue_catalog import GlueCatalogConnector
from .jdbc import JdbcConnector

__all__ = [
    "AthenaConnector",
    "ConnectionCheck",
    "ConnectionTestResult",
    "GlueCatalogConnector",
    "JdbcConnector",
    "MetadataConnector",
    "get_connector",
]

CONNECTOR_REGISTRY: dict[str, type[MetadataConnector]] = {
    SourceSubType.GLUE_DATABASE: GlueCatalogConnector,
    SourceSubType.JDBC_DATABASE: JdbcConnector,
    SourceSubType.ATHENA_CONNECTOR: AthenaConnector,
}


def get_connector(source_type: str) -> MetadataConnector:
    """Return a connector instance for the given source type."""
    cls = CONNECTOR_REGISTRY.get(source_type)
    if not cls:
        raise ValueError(f"Unsupported source type: {source_type}")
    return cls()
