#!/usr/bin/env node
// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
import * as path from "path";
import * as cdk from "aws-cdk-lib";
import {
  connectorFunctionName,
  deploymentEnv,
  functionNamePrefix,
  loadEnvFiles,
} from "coa-connector-cdk";
import { CONNECTOR_ID, DatabricksConnectorStack } from "../lib/databricks-connector-stack";

// Two locations, named rather than found by searching upwards: this app's own directory, then the shared
// one at connectors/. Most specific first, and nothing overwrites a variable that is already set, so an
// app-local .env beats the shared file and a pipeline's exported values beat both.
const appDir = path.join(__dirname, "..");
const connectorsRoot = path.join(appDir, "..", "..");
loadEnvFiles(appDir, connectorsRoot);

const app = new cdk.App();

// Stack and Lambda share a name, from the same call, so a second deployment of this connector into one
// account cannot reuse this stack and replace it. FUNCTION_NAME_PREFIX is what tells two such deployments
// apart.
const prefix = functionNamePrefix();
const stackName = connectorFunctionName(CONNECTOR_ID, prefix);
new DatabricksConnectorStack(app, stackName, {
  stackName,
  env: deploymentEnv(),
  functionNamePrefix: prefix,
});
