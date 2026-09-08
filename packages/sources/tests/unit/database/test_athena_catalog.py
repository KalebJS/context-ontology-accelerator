# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the CUSTOM_CONNECTOR data-catalog lifecycle.

Three behaviours carry real risk and are tested hardest:

* the ``Parameters`` map, because Athena accepts exactly one of two forms and
  mixing them is rejected — we always send the composite ``function`` key, and
  Athena rewrites it into the pair on read, which the ARN comparison must survive;
* idempotency by get-then-create, because ``CreateDataCatalog`` declares no
  ``AlreadyExistsException`` and a duplicate name arrives as an opaque 400; and
* the refusal to rebind an existing catalog to a different Lambda, which would
  otherwise serve a source from someone else's connector.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError
from coa_sources.database.connectors.athena_catalog import (
    AthenaCatalogConflictError,
    AthenaCatalogError,
    build_catalog_parameters,
    delete_lambda_catalog,
    derive_catalog_name,
    register_lambda_catalog,
)

pytestmark = pytest.mark.unit

_META_ARN = "arn:aws:lambda:us-east-1:111122223333:function:acme-metadata"
_RECORD_ARN = "arn:aws:lambda:us-east-1:111122223333:function:acme-record"


def _client_error(code: str, message: str = "", op: str = "GetDataCatalog") -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": message}}, op)


class FakeAthena:
    """Athena client double covering the three catalog calls."""

    def __init__(self, *, existing: dict | None = None, get_error: Exception | None = None) -> None:
        self.existing = existing
        self.get_error = get_error
        self.create_error: Exception | None = None
        self.delete_error: Exception | None = None
        self.created: list[dict] = []
        self.deleted: list[str] = []

    def get_data_catalog(self, Name):  # noqa: N803 - boto3 casing
        if self.get_error is not None:
            raise self.get_error
        if self.existing is None:
            raise _client_error("InvalidRequestException", f"Data catalog {Name} was not found")
        return {"DataCatalog": self.existing}

    def create_data_catalog(self, **kwargs):
        if self.create_error is not None:
            raise self.create_error
        self.created.append(kwargs)
        return {}

    def delete_data_catalog(self, Name):  # noqa: N803 - boto3 casing
        if self.delete_error is not None:
            raise self.delete_error
        self.deleted.append(Name)
        return {}


class TestDeriveCatalogName:
    def test_is_deterministic_for_a_source_id(self, monkeypatch):
        monkeypatch.setenv("RESOURCE_PREFIX", "coa-dev-")
        assert derive_catalog_name("abc-123") == derive_catalog_name("abc-123")

    def test_differs_between_sources(self, monkeypatch):
        monkeypatch.setenv("RESOURCE_PREFIX", "coa-dev-")
        assert derive_catalog_name("abc-123") != derive_catalog_name("abc-124")

    # Callers hold the id either bare or DS#-prefixed; both must land on the same
    # catalog or delete would look for a name create never registered.
    def test_ds_prefix_is_stripped(self, monkeypatch):
        monkeypatch.setenv("RESOURCE_PREFIX", "coa-dev-")
        assert derive_catalog_name("DS#abc-123") == derive_catalog_name("abc-123")

    # The prefix is what scopes the IAM statement to catalogs this deployment
    # created, so it has to reach the derived name.
    def test_includes_the_sanitized_resource_prefix(self, monkeypatch):
        monkeypatch.setenv("RESOURCE_PREFIX", "coa-dev-")
        assert derive_catalog_name("abc-123").startswith("coadevds_")


class TestBuildCatalogParameters:
    # Athena accepts `function` alone OR the metadata/record pair, never both. Only
    # the composite form is produced, because CustomConnectorConfiguration models one ARN.
    def test_produces_the_composite_form(self):
        assert build_catalog_parameters(_META_ARN) == {"function": _META_ARN}

    def test_emits_neither_half_of_the_split_pair(self):
        """The split keys must not appear, or Athena rejects the mixed map.

        Asserted as their absence rather than by comparing the whole dict, so this
        keeps failing for the right reason if another parameter is ever added.
        """
        params = build_catalog_parameters(_META_ARN)
        assert "metadata-function" not in params
        assert "record-function" not in params

    def test_takes_one_arn_only(self):
        """A second positional argument is a TypeError, not a silently-ignored value.

        The split pair used to be reachable through this function. Pinning the arity
        means restoring it has to be a deliberate signature change rather than
        something a caller can half-do by passing an extra ARN that goes nowhere.
        """
        with pytest.raises(TypeError):
            build_catalog_parameters(_META_ARN, _RECORD_ARN)  # type: ignore[call-arg]


class TestRegisterLambdaCatalog:
    def test_creates_a_lambda_type_catalog(self):
        client = FakeAthena()
        assert register_lambda_catalog(catalog_name="cat1", connector_function_arn=_META_ARN, client=client) is True
        assert client.created == [{"Name": "cat1", "Type": "LAMBDA", "Parameters": {"function": _META_ARN}}]

    # CreateDataCatalog declares no AlreadyExistsException, so idempotency has to
    # be a get-then-create check rather than a caught duplicate error.
    def test_an_equivalent_existing_catalog_is_a_no_op(self):
        client = FakeAthena(existing={"Name": "cat1", "Type": "LAMBDA", "Parameters": {"function": _META_ARN}})
        assert register_lambda_catalog(catalog_name="cat1", connector_function_arn=_META_ARN, client=client) is False
        assert client.created == []

    # The stored form may differ from what we would write today (a split pair for
    # one Lambda), so the comparison is on the ARNs, not the parameter keys.
    def test_an_existing_catalog_matching_on_arns_is_a_no_op(self):
        client = FakeAthena(
            existing={"Type": "LAMBDA", "Parameters": {"metadata-function": _META_ARN, "record-function": _META_ARN}}
        )
        assert register_lambda_catalog(catalog_name="cat1", connector_function_arn=_META_ARN, client=client) is False

    # Fatal rather than reconciled: the name is derived from a freshly generated
    # source id, so a mismatch means an assumption broke, and silently serving a
    # source from someone else's connector would be a disclosure bug.
    def test_refuses_to_rebind_a_catalog_pointing_elsewhere(self):
        client = FakeAthena(
            existing={"Type": "LAMBDA", "Parameters": {"function": "arn:aws:lambda:us-east-1:999:function:other"}}
        )
        # The conflict subclass, so a caller's rollback knows not to delete it.
        with pytest.raises(AthenaCatalogConflictError, match="does not describe this source"):
            register_lambda_catalog(catalog_name="cat1", connector_function_arn=_META_ARN, client=client)
        assert client.created == []

    # A name collision against a GLUE-type catalog has no Parameters at all, so
    # comparing handler ARNs alone would report "different Lambda ([])" for
    # something that is not a Lambda catalog.
    def test_refuses_to_rebind_a_catalog_of_another_type(self):
        client = FakeAthena(existing={"Name": "cat1", "Type": "GLUE"})
        with pytest.raises(AthenaCatalogConflictError, match="GLUE"):
            register_lambda_catalog(catalog_name="cat1", connector_function_arn=_META_ARN, client=client)
        assert client.created == []

    def test_wraps_a_create_failure(self):
        client = FakeAthena()
        client.create_error = _client_error("InvalidRequestException", op="CreateDataCatalog")
        with pytest.raises(AthenaCatalogError):
            register_lambda_catalog(catalog_name="cat1", connector_function_arn=_META_ARN, client=client)

    def test_wraps_a_transport_failure_on_create(self):
        client = FakeAthena()
        client.create_error = EndpointConnectionError(endpoint_url="https://athena")
        with pytest.raises(AthenaCatalogError):
            register_lambda_catalog(catalog_name="cat1", connector_function_arn=_META_ARN, client=client)

    # Swallowing a lookup failure would read as "not registered", and the create
    # that followed would fail with a duplicate-name 400 saying nothing useful.
    @pytest.mark.parametrize(
        "error",
        [
            _client_error("AccessDeniedException"),
            _client_error("InvalidRequestException", "some other problem"),
            EndpointConnectionError(endpoint_url="https://athena"),
        ],
    )
    def test_a_lookup_failure_is_not_read_as_absent(self, error):
        client = FakeAthena(get_error=error)
        with pytest.raises(AthenaCatalogError):
            register_lambda_catalog(catalog_name="cat1", connector_function_arn=_META_ARN, client=client)
        assert client.created == []


class TestDeleteLambdaCatalog:
    def test_deletes_by_name(self):
        client = FakeAthena()
        delete_lambda_catalog(catalog_name="cat1", client=client)
        assert client.deleted == ["cat1"]

    # A retried delete must converge rather than stall on a 404 for work already
    # done. Athena has no dedicated not-found code for catalogs, so both the
    # explicit code and the message-bearing InvalidRequestException count.
    @pytest.mark.parametrize(
        "error",
        [
            _client_error("ResourceNotFoundException", op="DeleteDataCatalog"),
            _client_error("InvalidRequestException", "Data catalog cat1 was not found", "DeleteDataCatalog"),
            _client_error("InvalidRequestException", "catalog does not exist", "DeleteDataCatalog"),
        ],
    )
    def test_an_absent_catalog_is_success(self, error):
        # `existing=None` makes the confirming lookup report it absent too.
        client = FakeAthena()
        client.delete_error = error
        delete_lambda_catalog(catalog_name="cat1", client=client)

    # `_is_not_found` reads error TEXT, because Athena declares no not-found code
    # for data catalogs. A false positive here would report success, the caller
    # would drop the source row, and the catalog would be unfindable — so the
    # message alone is not enough and a lookup has to confirm it.
    def test_a_not_found_message_is_not_trusted_while_the_catalog_still_exists(self):
        client = FakeAthena(existing={"Name": "cat1", "Type": "LAMBDA", "Parameters": {"function": _META_ARN}})
        client.delete_error = _client_error(
            "InvalidRequestException", "workgroup primary was not found", "DeleteDataCatalog"
        )
        with pytest.raises(AthenaCatalogError):
            delete_lambda_catalog(catalog_name="cat1", client=client)

    # The source record is the only handle on the catalog, so a caller that
    # proceeded past a real failure would orphan it permanently.
    @pytest.mark.parametrize(
        "error",
        [
            _client_error("AccessDeniedException", op="DeleteDataCatalog"),
            _client_error("InvalidRequestException", "malformed name", "DeleteDataCatalog"),
            EndpointConnectionError(endpoint_url="https://athena"),
        ],
    )
    def test_a_real_failure_raises(self, error):
        client = FakeAthena()
        client.delete_error = error
        with pytest.raises(AthenaCatalogError):
            delete_lambda_catalog(catalog_name="cat1", client=client)
