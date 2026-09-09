# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Local (Docker stack) MetadataStoreClient — DynamoDB-backed DataZone stand-in.

Implements the same ABC as :class:`coa_common.metadata_store.SMUSClient`
against the LocalStack DynamoDB tables, so namespace creation / source asset
writes / enrichment reads work unchanged in the local stack.

Selected by ``METADATA_STORE_PROVIDER=local`` at every construction site
(factory helpers live in :mod:`coa_common.metadata_store.factory`).

Storage model (single table, ``METADATA_PROVIDER=local``):
  PK = "PROJ#<project_id>"          SK = "METADATA"        → project record
  PK = "PROJ#<project_id>"          SK = "ASSET#<asset_id>"→ asset record
  PK = "ASSET_NAME#<project_id>"    SK = <asset name>      → name→id index
Forms are stored verbatim in the asset record's ``forms`` attribute — the
same ``formsOutput`` shape ``reader._parse_asset`` expects.
"""

from __future__ import annotations

import json
import os
import uuid
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from coa_common.metadata_store.base import AssetResult, MetadataStoreClient, ProjectResult, SearchResult
from coa_common.metadata_store.exceptions import MetadataStoreError

_RETRY = Config(retries={"max_attempts": 10, "mode": "adaptive"})


def _table() -> str:
    return os.environ.get("LOCAL_METADATA_TABLE", "coa-local-metadata")


def _ddb() -> Any:
    region = os.environ.get("AWS_REGION", "us-east-1")
    endpoint = os.environ.get("LOCALSTACK_ENDPOINT")
    kwargs: dict[str, Any] = {"region_name": region, "config": _RETRY}
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    return boto3.client("dynamodb", **kwargs)


class LocalMetadataStore(MetadataStoreClient):
    """DynamoDB-backed implementation of :class:`MetadataStoreClient`."""

    def __init__(self, table_name: str | None = None, region_name: str | None = None):
        """Bind the backing table (created lazily by the provisioner)."""
        self._table = table_name or _table()
        self._region = region_name or os.environ.get("AWS_REGION", "us-east-1")
        self._ddb = _ddb()

    # ── Projects ──────────────────────────────────────────────────────

    def create_project(self, *, name: str, description: str = "", project_profile_id: str = "") -> ProjectResult:
        """Create a project record with a generated id."""
        project_id = f"local-{uuid.uuid4().hex[:12]}"
        now = "2026-01-01T00:00:00Z"
        try:
            self._ddb.put_item(
                TableName=self._table,
                Item={
                    "PK": {"S": f"PROJ#{project_id}"},
                    "SK": {"S": "METADATA"},
                    "name": {"S": name},
                    "description": {"S": description},
                    "createdAt": {"S": now},
                },
                ConditionExpression="attribute_not_exists(PK)",
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
                raise MetadataStoreError(f"project id collision: {project_id}") from exc
            raise MetadataStoreError(f"create_project failed: {exc}") from exc
        return ProjectResult(project_id=project_id, name=name)

    def delete_project(self, *, project_id: str) -> None:
        """Delete the project record and all its assets."""
        try:
            self._ddb.delete_item(
                TableName=self._table, Key={"PK": {"S": f"PROJ#{project_id}"}, "SK": {"S": "METADATA"}}
            )
            # Cascade assets
            resp = self._ddb.query(
                TableName=self._table,
                KeyConditionExpression="#pk = :pk",
                ExpressionAttributeNames={"#pk": "PK"},
                ExpressionAttributeValues={":pk": {"S": f"PROJ#{project_id}"}},
            )
            for item in resp.get("Items", []):
                sk = item["SK"]["S"]
                if sk.startswith("ASSET#"):
                    self._ddb.delete_item(
                        TableName=self._table, Key={"PK": {"S": f"PROJ#{project_id}"}, "SK": {"S": sk}}
                    )
        except ClientError as exc:
            raise MetadataStoreError(f"delete_project failed: {exc}") from exc

    def get_project(self, *, project_id: str) -> ProjectResult:
        """Fetch a project record by id."""
        try:
            resp = self._ddb.get_item(
                TableName=self._table, Key={"PK": {"S": f"PROJ#{project_id}"}, "SK": {"S": "METADATA"}}
            )
        except ClientError as exc:
            raise MetadataStoreError(f"get_project failed: {exc}") from exc
        item = resp.get("Item")
        if not item:
            raise MetadataStoreError(f"project not found: {project_id}")
        return ProjectResult(project_id=project_id, name=item["name"]["S"])

    # ── Assets ────────────────────────────────────────────────────────

    def create_asset(
        self,
        *,
        project_id: str,
        name: str,
        type_identifier: str,
        description: str = "",
        forms_input: list[dict[str, str]] | None = None,
    ) -> AssetResult:
        """Create an asset record inside the project."""
        asset_id = f"local-{uuid.uuid4().hex[:12]}"
        try:
            self._ddb.put_item(
                TableName=self._table,
                Item={
                    "PK": {"S": f"PROJ#{project_id}"},
                    "SK": {"S": f"ASSET#{asset_id}"},
                    "assetId": {"S": asset_id},
                    # GSI1PK powers the name→id index (see module docstring).
                    "GSI1PK": {"S": f"ASSET_NAME#{project_id}"},
                    "name": {"S": name},
                    "typeIdentifier": {"S": type_identifier},
                    "description": {"S": description},
                    "forms": {"S": _encode_forms(forms_input)},
                    "createdAt": {"S": "2026-01-01T00:00:00Z"},
                },
                ConditionExpression="attribute_not_exists(PK)",
            )
        except ClientError as exc:
            raise MetadataStoreError(f"create_asset failed: {exc}") from exc
        return AssetResult(asset_id=asset_id, name=name, project_id=project_id)

    def create_asset_revision(
        self,
        *,
        asset_id: str,
        name: str,
        description: str = "",
        forms_input: list[dict[str, str]] | None = None,
    ) -> AssetResult:
        """Overwrite the asset's form payload (revision semantics collapsed)."""
        try:
            resp = self._ddb.query(
                TableName=self._table,
                IndexName="AssetIdIndex",
                KeyConditionExpression="assetId = :aid",
                ExpressionAttributeValues={":aid": {"S": asset_id}},
                Limit=1,
            )
            items = resp.get("Items", [])
            if not items:
                raise MetadataStoreError(f"asset not found for revision: {asset_id}")
            item = items[0]
            project_id = item["PK"]["S"].removeprefix("PROJ#")
            self._ddb.update_item(
                TableName=self._table,
                Key={"PK": {"S": item["PK"]["S"]}, "SK": {"S": item["SK"]["S"]}},
                UpdateExpression="SET #n = :n, #d = :d, forms = :f",
                ExpressionAttributeNames={"#n": "name", "#d": "description"},
                ExpressionAttributeValues={
                    ":n": {"S": name},
                    ":d": {"S": description},
                    ":f": {"S": _encode_forms(forms_input)},
                },
            )
        except ClientError as exc:
            raise MetadataStoreError(f"create_asset_revision failed: {exc}") from exc
        return AssetResult(asset_id=asset_id, name=name, project_id=project_id)

    def delete_asset(self, *, asset_id: str) -> None:
        """Delete an asset record (queries by assetId, as create_asset_revision does)."""
        try:
            resp = self._ddb.query(
                TableName=self._table,
                IndexName="AssetIdIndex",
                KeyConditionExpression="assetId = :aid",
                ExpressionAttributeValues={":aid": {"S": asset_id}},
                Limit=1,
            )
            items = resp.get("Items", [])
            if not items:
                return
            item = items[0]
            self._ddb.delete_item(
                TableName=self._table, Key={"PK": {"S": item["PK"]["S"]}, "SK": {"S": item["SK"]["S"]}}
            )
        except ClientError as exc:
            raise MetadataStoreError(f"delete_asset failed: {exc}") from exc

    def get_asset(self, *, asset_id: str) -> AssetResult:
        """Fetch an asset by id."""
        try:
            resp = self._ddb.query(
                TableName=self._table,
                IndexName="AssetIdIndex",
                KeyConditionExpression="assetId = :aid",
                ExpressionAttributeValues={":aid": {"S": asset_id}},
                Limit=1,
            )
            items = resp.get("Items", [])
            if not items:
                raise MetadataStoreError(f"asset not found: {asset_id}")
            item = items[0]
            return AssetResult(
                asset_id=asset_id, name=item["name"]["S"], project_id=item["PK"]["S"].removeprefix("PROJ#")
            )
        except ClientError as exc:
            raise MetadataStoreError(f"get_asset failed: {exc}") from exc

    def search_assets(
        self,
        *,
        project_id: str,
        search_text: str,
        max_results: int = 50,
        next_token: str | None = None,
    ) -> SearchResult:
        """Search assets in a project by name substring (the DS# prefix filter)."""
        try:
            kwargs: dict[str, Any] = {
                "TableName": self._table,
                "KeyConditionExpression": "#pk = :pk",
                "ExpressionAttributeNames": {"#pk": "PK"},
                "ExpressionAttributeValues": {":pk": {"S": f"PROJ#{project_id}"}},
            }
            if next_token:
                kwargs["ExclusiveStartKey"] = _decode_token(next_token)
            resp = self._ddb.query(**kwargs)
            items = resp.get("Items", [])
            assets = [
                AssetResult(
                    asset_id=item["SK"]["S"].removeprefix("ASSET#"), name=item["name"]["S"], project_id=project_id
                )
                for item in items
                if item["SK"]["S"].startswith("ASSET#") and search_text in item["name"]["S"]
            ][:max_results]
            token = _encode_token(resp.get("LastEvaluatedKey")) if resp.get("LastEvaluatedKey") else None
            return SearchResult(items=assets, next_token=token)
        except ClientError as exc:
            raise MetadataStoreError(f"search_assets failed: {exc}") from exc

    def get_asset_forms(self, *, asset_id: str) -> dict[str, Any]:
        """Return the ``formsOutput`` shape ``reader._parse_asset`` expects.

        The stored ``forms`` attribute IS the forms list (JSON string) — the
        same list ``create_asset``/``create_asset_revision`` received as
        ``forms_input``, each entry carrying its own ``formName`` and
        JSON-string ``content``. It is returned verbatim as ``formsOutput``.
        """
        try:
            resp = self._ddb.query(
                TableName=self._table,
                IndexName="AssetIdIndex",
                KeyConditionExpression="assetId = :aid",
                ExpressionAttributeValues={":aid": {"S": asset_id}},
                Limit=1,
            )
            items = resp.get("Items", [])
            if not items:
                raise MetadataStoreError(f"asset not found: {asset_id}")
            try:
                forms = json.loads(items[0].get("forms", {}).get("S", "[]"))
            except (json.JSONDecodeError, ValueError):
                forms = []
            if not isinstance(forms, list):
                forms = []
            return {"formsOutput": forms}
        except ClientError as exc:
            raise MetadataStoreError(f"get_asset_forms failed: {exc}") from exc

    # ── Memberships (no-op in local mode) ─────────────────────────────

    def create_project_membership(
        self, *, project_id: str, user_identifier: str, designation: str = "PROJECT_CONTRIBUTOR"
    ) -> None:
        """No-op — local stack has no DataZone membership concept."""
        return


def _encode_forms(forms_input: list[dict[str, str]] | None) -> str:
    if not forms_input:
        return "[]"
    return json.dumps(forms_input)


def _encode_token(key: dict[str, Any]) -> str:
    return json.dumps({k: v.get("S", "") for k, v in key.items()})


def _decode_token(token: str) -> dict[str, Any]:
    return {k: {"S": v} for k, v in json.loads(token).items()}
