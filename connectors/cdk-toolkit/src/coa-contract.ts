// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * The strings COA's IAM policies match on. Every one is load-bearing.
 *
 * <p>Stated here rather than imported, so a copied-out connector still builds. <b>Nothing verifies
 * that these match the policies COA enforces</b> — the risk is accepted knowingly, and recorded here
 * for whoever changes either side.
 *
 * <p>Failure timing is asymmetric. {@link CONNECTOR_TAG_KEY} fails immediately: COA's invoke policy
 * is scoped to it, so a missing tag denies the first scan. The spill three fail only once a response
 * exceeds Athena's 6 MB limit — and a connector that cannot write spill has been observed returning
 * `SUCCEEDED` with zero rows rather than an error.
 */

/** Tag COA's invoke policy matches on. Without it the function cannot be invoked at all. */
export const CONNECTOR_TAG_KEY = "coa:connector";

/** Tag COA's key policy matches on, on the customer-managed spill key. */
export const CONNECTOR_SPILL_KMS_TAG_KEY = "coa:connector-spill";

/** Value both tags carry. */
export const CONNECTOR_TAG_VALUE = "true";

/** Key shape COA's spill-read policy matches. Only the shape; the id segment is yours. */
export const CONNECTOR_SPILL_KEY_GLOB = "connectors/*/spills/*";

/**
 * Where a COA operator reads the two role ARNs, in COA's account.
 *
 * <p>Documented, not read: a connector usually runs in another account and SSM parameters are not
 * readable across accounts, so the values arrive as environment variables instead.
 */
export const COA_ROLE_SSM_PARAMS = {
  /** Runs queries. `/{prefix}/serve/runtime-role-arn`. */
  serve: "/{prefix}/serve/runtime-role-arn",
  /** Runs `DESCRIBE` during a scan. `/{prefix}/sources/db-connector-role-arn`. */
  discovery: "/{prefix}/sources/db-connector-role-arn",
} as const;

/**
 * CloudWatch namespace a connector's own metrics land in, written as Embedded Metric Format on
 * stdout. Must equal `ConnectorMetrics.NAMESPACE` on the Java side — one emits, the other alarms on
 * it, and a disagreement produces an alarm stuck in `INSUFFICIENT_DATA` rather than an error.
 */
export const CONNECTOR_METRIC_NAMESPACE = "COA/Connectors";

/** Dimension carrying the connector's id. Present on every metric. */
export const CONNECTOR_DIMENSION = "Connector";

/**
 * Dimension carrying the Athena catalog a request arrived under. Absent from metrics emitted below
 * the request, so an alarm that needs to fire for those must not name it.
 */
export const CATALOG_DIMENSION = "Catalog";

/**
 * The metrics a connector emits. Mirrors the constants on `ConnectorMetrics` in the Java toolkit;
 * the two are the same contract expressed twice, because nothing can share a constant across the
 * jar and the CDK app.
 */
export const ConnectorMetricName = {
  /** Configuration could not be resolved for the catalog a request arrived under. */
  configResolutionFailures: "ConnectorConfigResolutionFailures",
  /** A connection to the upstream data source could not be opened. */
  warehouseConnectFailures: "ConnectorWarehouseConnectFailures",
  /** Rows returned by one table read. */
  rowsReturned: "ConnectorRowsReturned",
  /** A read was refused for exceeding the connector's row ceiling. */
  tableCeilingExceeded: "ConnectorTableCeilingExceeded",
} as const;
