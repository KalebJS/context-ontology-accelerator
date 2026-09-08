// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
//
// Reusable pieces for a connector's own CDK app.
//
// This is a library, not a framework. It gives you the Lambda, its spill bucket and the
// resource policies COA needs, as one construct you instantiate inside a stack
// you own. Whatever else your source needs — a VPC, an RDS proxy, a secret, a KMS key, a
// cache table — you add to that stack, and nothing here has to know about it.
export {
  AthenaFederationConnector,
  AthenaFederationConnectorProps,
  spillPrefixFor,
  CONNECTOR_FUNCTION_SUFFIX,
  connectorFunctionName,
  RESERVED_ENVIRONMENT_KEYS,
} from "./athena-federation-connector";

export {
  CONNECTOR_TAG_KEY,
  CONNECTOR_SPILL_KMS_TAG_KEY,
  CONNECTOR_TAG_VALUE,
  CONNECTOR_SPILL_KEY_GLOB,
  COA_ROLE_SSM_PARAMS,
  CONNECTOR_METRIC_NAMESPACE,
  CONNECTOR_DIMENSION,
  CATALOG_DIMENSION,
  ConnectorMetricName,
} from "./coa-contract";

export {
  ENV_FILE,
  loadEnvFiles,
  requiredEnv,
  optionalEnv,
  optionalIntEnv,
  functionNamePrefix,
  queryRoleArns,
  QUERY_ROLE_VARS,
  deploymentEnv,
} from "./env";
