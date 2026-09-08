# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Reads discovered assets from DataZone for enrichment using SMUSClient."""

from __future__ import annotations

import json
import logging
import os

from coa_common.datazone_forms import (
    FORM_TYPE_NAME,
    deserialize_form,
)
from coa_common.domain_models import Table
from coa_common.metadata_store.smus import SMUSClient

logger = logging.getLogger(__name__)

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


def read_assets_for_datasource(domain_id: str, project_id: str, data_source_id: str) -> list[Table]:
    """Search DataZone for assets belonging to a data source and parse into Table objects."""
    if not data_source_id:
        raise ValueError("data_source_id is required for asset filtering")

    # Assets are stored with DS# prefix in their name
    ds_key = f"DS#{data_source_id}" if not data_source_id.startswith("DS#") else data_source_id

    client = SMUSClient(
        domain_id=domain_id,
        region_name=AWS_REGION,
        assume_role_arn=os.getenv("PROJECT_ACCESS_ROLE_ARN") or None,
        session_name="enrichment-reader",
    )
    tables: list[Table] = []

    logger.info(
        "Searching DataZone for assets: domain=%s project=%s datasource=%s",
        domain_id,
        project_id,
        ds_key,
    )

    next_token: str | None = None
    while True:
        try:
            result = client.search_assets(
                project_id=project_id,
                search_text=ds_key,
                max_results=50,
                next_token=next_token,
            )
        except Exception:
            logger.exception("Failed to search assets for datasource %s", data_source_id)
            raise

        logger.info("Search returned %d assets (page)", len(result.items))

        for asset in result.items:
            logger.debug("Processing asset: id=%s name=%s", asset.asset_id, asset.name)
            # Only process assets prefixed with this data source ID
            if not asset.name.startswith(f"{ds_key}:"):
                continue
            table = _parse_asset(client, asset.asset_id, asset.name, data_source_id)
            if table:
                tables.append(table)
            else:
                logger.warning("Could not parse asset %s (%s) into table", asset.asset_id, asset.name)

        next_token = result.next_token
        if not next_token:
            break

    logger.info("Read %d assets for datasource %s", len(tables), data_source_id)
    return tables


def _client(domain_id: str, session_name: str) -> SMUSClient:
    """SMUSClient for a DataZone domain, with the project-access role when set."""
    return SMUSClient(
        domain_id=domain_id,
        region_name=AWS_REGION,
        assume_role_arn=os.getenv("PROJECT_ACCESS_ROLE_ARN") or None,
        session_name=session_name,
    )


def _table_name_from_asset(asset_name: str, ds_key: str) -> str:
    """Bare table name out of a ``DS#{sourceId}:{database}.{table}`` asset name.

    The writer composes asset names as ``DS#{sourceId}:{db}.{table}`` (see
    ``sources_handler``), and the metadata form's ``tableName`` — which is what
    ``Table.name`` and therefore every table index in this package is keyed on — is
    the bare table. Split on the FIRST ``.`` so a table name containing a dot still
    round-trips; a database name containing one would not, and neither Glue nor a
    JDBC schema permits that.
    """
    remainder = asset_name[len(ds_key) + 1 :]
    _, _, table = remainder.partition(".")
    return table or remainder


def read_asset_names_for_datasource(domain_id: str, project_id: str, data_source_id: str) -> dict[str, str]:
    """Map lower-cased table name → DataZone asset id, from search pages ALONE.

    The cheap half of :func:`read_assets_for_datasource`. Existence questions
    ("does this source know table X?", "does it know any table?") are answerable
    from the asset NAME, which the search page already returns — so answering them
    costs one search call per 50 assets and nothing per asset.

    :func:`read_assets_for_datasource` instead issues one ``get_asset_forms`` per
    asset because it must return full ``Table`` objects. Used for an existence
    check that is O(tables) in sequential DataZone round-trips: 88 assets measured
    at 0.186s each is 16s of a 30s Lambda budget, and metric creation was timing
    out behind it (job 10909663). Callers that need columns or review status should
    fetch the ONE asset they care about via :func:`read_table_for_asset`.

    Returns an empty mapping when the source has no assets. Raises when the search
    itself fails, so callers can distinguish "no tables" from "could not look".
    """
    if not data_source_id:
        raise ValueError("data_source_id is required for asset filtering")

    ds_key = f"DS#{data_source_id}" if not data_source_id.startswith("DS#") else data_source_id
    client = _client(domain_id, "catalog-names-reader")

    names: dict[str, str] = {}
    pages = 0
    next_token: str | None = None
    while True:
        result = client.search_assets(
            project_id=project_id,
            search_text=ds_key,
            max_results=50,
            next_token=next_token,
        )
        pages += 1
        for asset in result.items:
            if not asset.name.startswith(f"{ds_key}:"):
                continue
            table = _table_name_from_asset(asset.name, ds_key)
            if table:
                names[table.lower()] = asset.asset_id
        next_token = result.next_token
        if not next_token:
            break

    logger.info(
        "Read %d asset names for datasource %s in %d search page(s)",
        len(names),
        data_source_id,
        pages,
    )
    return names


def read_table_for_asset(domain_id: str, asset_id: str, asset_name: str, data_source_id: str) -> Table | None:
    """Full ``Table`` for ONE asset — the per-asset cost, paid only when needed.

    ``None`` when the asset has no metadata form or its form cannot be parsed,
    matching :func:`read_assets_for_datasource`'s per-asset behaviour.
    """
    return _parse_asset(_client(domain_id, "catalog-asset-reader"), asset_id, asset_name, data_source_id)


def _parse_asset(client: SMUSClient, asset_id: str, asset_name: str, data_source_id: str) -> Table | None:
    """Parse a DataZone asset into a Table object by reading its form."""
    try:
        detail = client.get_asset_forms(asset_id=asset_id)
    except Exception:
        logger.debug("Failed to get asset %s", asset_id, exc_info=True)
        return None

    forms = detail.get("formsOutput", [])
    for form in forms:
        if form.get("formName") == FORM_TYPE_NAME:
            try:
                payload = json.loads(form["content"])
            except (json.JSONDecodeError, TypeError) as e:
                logger.error("Failed to parse form content for asset %s: %s", asset_id, e)
                return None
            table = deserialize_form(payload, data_source_id=data_source_id)
            return table

    return None
