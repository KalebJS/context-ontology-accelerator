// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
import * as path from "path";
import * as cdk from "aws-cdk-lib";
import * as cloudwatch from "aws-cdk-lib/aws-cloudwatch";
import * as kms from "aws-cdk-lib/aws-kms";
import * as secretsmanager from "aws-cdk-lib/aws-secretsmanager";
import {
  AthenaFederationConnector,
  ConnectorMetricName,
  functionNamePrefix,
  optionalEnv,
  optionalIntEnv,
  queryRoleArns,
  requiredEnv,
} from "coa-connector-cdk";
import { Construct } from "constructs";

/**
 * The connector's id. Names the stack and the Lambda (`databricks-coa-connector`), so a second connector
 * deployed into the same account cannot collide with this one.
 */
export const CONNECTOR_ID = "databricks";

/** Handler class in the connector's fat jar. */
export const HANDLER = "dev.coa.databricks.DatabricksCompositeHandler";

/**
 * 3008 MB and 120 s, both above the construct's 1024 MB / 90 s defaults.
 *
 * 120 s is headroom for a warm read, not for a warehouse resume. A stopped SQL Warehouse resumes in the
 * background, taking seconds for serverless and minutes for classic and pro, and Athena re-invokes a
 * failed connector, so waiting turns one query into several billed Lambda-minutes and several resumes.
 * The connector fails fast instead (see `DatabricksErrors`).
 *
 * The memory is sized for the read rather than the metadata path. Athena's federation protocol cannot
 * express aggregation, so a `GROUP BY` reads every predicate-matching row out of the warehouse, which is
 * the normal case on this route rather than an edge case.
 */
export const MEMORY_SIZE_MB = 3008;
export const TIMEOUT_SECONDS = 120;

/** The fat jar, relative to this app. Built by `pnpm run package` before deploy. */
export const DEFAULT_JAR_PATH = path.join(
  __dirname,
  "..",
  "..",
  "target",
  "databricks-connector-1.0.0.jar",
);

/**
 * The push-down names the connector recognises.
 *
 * Restated from `PushdownCapabilities.KNOWN` in Java, with nothing enforcing that the two agree, because
 * this app has to build when copied out of the repository. Same trade as `coa-contract.ts`. A name added
 * on one side and not the other is caught by the README's table rather than by a compiler.
 */
export const PUSHDOWN_NAMES = ["filter", "limit", "topn"] as const;

/**
 * The row ceiling the connector applies when `DATABRICKS_MAX_ROWS_PER_TABLE` is unset.
 *
 * Restated from `Settings.DEFAULT_MAX_ROWS_PER_TABLE` in Java for the same reason as
 * {@link PUSHDOWN_NAMES}: this app has to build when copied out of the repository. Only the
 * `ConnectorRowsReturned` alarm's threshold depends on it, so a drift makes that alarm fire early or
 * late rather than changing what the connector does.
 */
export const DEFAULT_MAX_ROWS_PER_TABLE = 2_000_000;

/**
 * `ConnectorDatabase`'s value when `DATABRICKS_SCHEMA` is unset.
 *
 * Not schema-shaped: a consumer who pastes it into an Athena catalog registration or COA's onboarding
 * form should get an obvious error rather than a lookup for a schema called `all`.
 */
export const UNPINNED_DATABASE_OUTPUT = "<unpinned: every schema in the catalog>";

/**
 * Lower-cases a Unity Catalog identifier, as `ConnectionConfig.Builder` does on the Java side.
 *
 * Not cosmetic. `ConnectionConfig` folds the catalog and schema because Unity Catalog stores them that
 * way, and `DatabricksMetadataHandler.readerFor` rejects any schema name that does not match. Without
 * the same fold here, `DATABRICKS_SCHEMA=Sales` deploys cleanly, publishes `Sales` as the
 * `ConnectorDatabase` output, and every consumer of that output gets
 * `Unknown schema: "Sales". This connector serves only "sales"`.
 */
function ucIdentifier(value: string): string {
  return value.toLowerCase();
}

/**
 * A bare SQL identifier, mirroring `ConnectionConfig.IDENTIFIER` on the Java side.
 *
 * Checked here as well as there because `DATABRICKS_SCHEMA` is optional: a value the Java pattern rejects
 * would deploy cleanly and fail at the first query, and at that point it is indistinguishable from the
 * unpinned mode, which also looks like "the schema I set is not being used".
 */
const UC_IDENTIFIER = /^[a-z_][a-z0-9_]*$/;

/**
 * Reads an optional Unity Catalog identifier: lower-cased, shape-checked, `undefined` when absent.
 *
 * Blank counts as absent, matching `ConnectionConfig.Builder.schema`. CDK, the console and a shell
 * disagree about whether an unset variable arrives missing or empty, and an operator means the same thing
 * by both.
 */
function optionalUcIdentifier(name: string): string | undefined {
  const raw = optionalEnv(name);
  if (raw === undefined || raw.trim() === "") {
    return undefined;
  }
  const value = ucIdentifier(raw.trim());
  if (!UC_IDENTIFIER.test(value)) {
    throw new Error(
      `${name}="${raw}" is not a bare SQL identifier: a letter or underscore, then letters, ` +
        `digits or underscores. Unity Catalog allows a quoted name containing more, but such a ` +
        `name is not addressable through both of Athena's parsers, so the connector refuses it.`,
    );
  }
  return value;
}

/**
 * Environment variables whose absence has to fail synth, in the README's order.
 *
 * `DATABRICKS_SCHEMA` is not among them; see {@link OPTIONAL_CONNECTOR_ENV_VARS}. A separate list from
 * the union below, so the "missing coordinate fails synth" test iterates exactly the variables that claim
 * to be required rather than being loosened to tolerate one that is not.
 */
export const REQUIRED_CONNECTOR_ENV_VARS = [
  "DATABRICKS_WORKSPACE_HOSTNAME",
  "DATABRICKS_HTTP_PATH",
  "DATABRICKS_CATALOG",
  "CREDENTIAL_SECRET_ARN",
] as const;

/**
 * Environment variables the connector reads but does not require. `DATABRICKS_SCHEMA` pins it to one
 * Unity Catalog schema; unset, it serves every schema in `DATABRICKS_CATALOG` and the credential's own
 * grants are the only boundary.
 */
export const OPTIONAL_CONNECTOR_ENV_VARS = ["DATABRICKS_SCHEMA"] as const;

/** Every environment variable this stack reads and passes to the connector. */
export const CONNECTOR_ENV_VARS = [
  ...REQUIRED_CONNECTOR_ENV_VARS,
  ...OPTIONAL_CONNECTOR_ENV_VARS,
] as const;

/** Properties for {@link DatabricksConnectorStack}. */
export interface DatabricksConnectorStackProps extends cdk.StackProps {
  /** Overrides the jar location, so tests need no Maven build. */
  readonly jarPath?: string;

  /**
   * Overrides the principals granted invoke and spill read. Defaults to the COA roles named by
   * `SERVE_ROLE_ARN` and `DISCOVERY_ROLE_ARN`.
   */
  readonly queryRoleArns?: readonly string[];

  /** Prefix for the Lambda name, defaulting to `FUNCTION_NAME_PREFIX`. */
  readonly functionNamePrefix?: string;
}

/**
 * Deploys the Databricks connector: the Lambda, its own spill bucket and CMK, and read access to one
 * credential secret.
 *
 * Three things the example connector's stack does not do, each because this connector reaches an external
 * system. It grants the function read on one secret, named by ARN, since a credential must never be a
 * Lambda environment variable: those are readable by anyone holding `lambda:GetFunctionConfiguration` and
 * they land in the CloudFormation template in plain text. It grants `kms:Decrypt` on a customer-managed
 * key when the secret uses one, which no existing grant covers and whose absence shows up as
 * access-denied on every read rather than at deploy. And it sizes the function for the read (see
 * {@link MEMORY_SIZE_MB}).
 *
 * It does not create the Athena data catalog. That belongs to whichever account runs the queries, which
 * for this topology is usually not the account the Lambda lives in; the construct outputs the exact
 * `create-data-catalog` command instead.
 */
export class DatabricksConnectorStack extends cdk.Stack {
  public readonly connector: AthenaFederationConnector;

  constructor(scope: Construct, id: string, props: DatabricksConnectorStackProps = {}) {
    super(scope, id, props);

    const credentialSecretArn = requiredEnv(
      "CREDENTIAL_SECRET_ARN",
      "A Secrets Manager secret holding {\"token\": ...} for a personal access token, or\n" +
        "{\"client_id\": ..., \"client_secret\": ...} for OAuth machine-to-machine. The shape\n" +
        "selects the auth mode; the connector refuses a secret carrying both.",
    );

    this.connector = new AthenaFederationConnector(this, "Connector", {
      connectorId: CONNECTOR_ID,
      handler: HANDLER,
      jarPath: props.jarPath ?? DEFAULT_JAR_PATH,
      queryRoleArns: props.queryRoleArns ?? queryRoleArns(),
      functionNamePrefix: props.functionNamePrefix ?? functionNamePrefix(),
      memorySize: MEMORY_SIZE_MB,
      timeout: cdk.Duration.seconds(TIMEOUT_SECONDS),
      environment: connectorEnvironment(credentialSecretArn),
      alarmTopicArn: optionalEnv("ALARM_TOPIC_ARN"),
      description:
        "Athena Query Federation connector for one Databricks SQL Warehouse: one Unity Catalog\n" +
        "catalog, and either one pinned schema or every schema within it",
    });

    this.createConnectorAlarms();

    // fromSecretCompleteArn, not fromSecretNameV2: the six-character suffix has to be part of the grant,
    // or the policy covers every secret whose name is a prefix of this one.
    const credential = secretsmanager.Secret.fromSecretCompleteArn(
      this,
      "Credential",
      credentialSecretArn,
    );
    credential.grantRead(this.connector.connectorFunction);

    // A secret encrypted with a customer-managed key needs kms:Decrypt on BOTH sides: this grant, and a
    // statement in the key's own policy. The second half is the key owner's and cannot be written here.
    const credentialKeyArn = optionalEnv("CREDENTIAL_KMS_KEY_ARN");
    if (credentialKeyArn !== undefined) {
      kms.Key.fromKeyArn(this, "CredentialKey", credentialKeyArn).grantDecrypt(
        this.connector.connectorFunction,
      );
    }

    this.publishOutputs(credentialSecretArn);
  }

  /**
   * Alarms on the four metrics this connector emits itself, which the three Lambda alarms in the construct
   * cannot see: each of these failures is caught, reported and counted, so the invocation **succeeds** and
   * shows up in neither the error rate nor the duration.
   *
   * <p>All four are wired through the construct's `addAlarm`, so they reach `ALARM_TOPIC_ARN` if one was
   * given and notify nobody if not — see {@link AthenaFederationConnectorProps.alarmTopicArn}.
   */
  private createConnectorAlarms(): void {
    const period = cdk.Duration.minutes(5);

    // Fleet-wide rather than per catalog. A pinned single-endpoint deployment has one catalog, and naming
    // it here would mean an alarm that silently stops matching the day a second COA source is registered
    // against the same connector.
    this.connector.addAlarm("ConfigResolutionFailuresAlarm", {
      alarmName: `${this.connector.functionName}-config-resolution-failures`,
      alarmDescription:
        "The connector could not resolve its configuration. Check the four required DATABRICKS_* " +
        "variables against the cold-start log line, which names the host, catalog and schema it read.",
      metric: this.connector.connectorMetric(ConnectorMetricName.configResolutionFailures, {
        period: cdk.Duration.minutes(15),
        statistic: "Sum",
      }),
      threshold: 0,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
      evaluationPeriods: 1,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    });

    // Five, not one: a warehouse that has scaled to zero refuses the first connections of a resume, and a
    // threshold of one would page on every idle period ending.
    this.connector.addAlarm("WarehouseConnectFailuresAlarm", {
      alarmName: `${this.connector.functionName}-warehouse-connect-failures`,
      alarmDescription:
        "The connector cannot reach the SQL Warehouse. Distinguish a stopped warehouse (recovers on " +
        "resume) from a rejected credential (DATABRICKS_AUTHENTICATION_FAILED in the log, so the " +
        "secret has expired or rotated) from a network fault.",
      metric: this.connector.connectorMetric(ConnectorMetricName.warehouseConnectFailures, {
        period,
        statistic: "Sum",
      }),
      threshold: 5,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
      evaluationPeriods: 1,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    });

    // Any breach at all. The query failed, and the user saw it — this alarm exists to say which table so
    // the answer is a filter or an exclusion rather than a raised ceiling.
    this.connector.addAlarm("TableCeilingExceededAlarm", {
      alarmName: `${this.connector.functionName}-table-ceiling-exceeded`,
      alarmDescription:
        "A query was refused for exceeding DATABRICKS_MAX_ROWS_PER_TABLE. The connector's log names " +
        "the table. Narrow the predicate or set tableExcludeFilter; raising the ceiling means raising " +
        "memory and timeout with it.",
      metric: this.connector.connectorMetric(ConnectorMetricName.tableCeilingExceeded, {
        period,
        statistic: "Sum",
      }),
      threshold: 0,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
      evaluationPeriods: 1,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    });

    // The leading indicator, and the only one here that fires before anything has failed: p95 approaching
    // the ceiling means the next slightly-wider question breaches it. Two periods, because one aggregate
    // query against a large table is a fact about that question rather than a trend.
    const ceiling = optionalIntEnv("DATABRICKS_MAX_ROWS_PER_TABLE") ?? DEFAULT_MAX_ROWS_PER_TABLE;
    this.connector.addAlarm("RowsReturnedAlarm", {
      alarmName: `${this.connector.functionName}-rows-returned-p95`,
      alarmDescription:
        `p95 rows per read is within 20% of the ${ceiling}-row ceiling. Either push-down has ` +
        "regressed or the workload has turned aggregate-heavy; the two look identical from Athena's " +
        "side, so check the advertised push-down before concluding the workload changed.",
      metric: this.connector.connectorMetric(ConnectorMetricName.rowsReturned, {
        period,
        statistic: "p95",
      }),
      threshold: Math.round(ceiling * 0.8),
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
      evaluationPeriods: 2,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    });
  }

  /**
   * Outputs the integration tests and the onboarding steps read. Declared at stack level rather than on
   * the construct: CDK prefixes a construct's output with its path and a hash, and a test looking up a
   * mangled key that has since changed skips while looking healthy.
   */
  private publishOutputs(credentialSecretArn: string): void {
    new cdk.CfnOutput(this, "ConnectorFunctionArn", {
      value: this.connector.connectorFunction.functionArn,
      description: "Register this ARN as an Athena LAMBDA data catalog",
    });
    // Lower-cased, like the environment variables above: this output is what the README tells an operator
    // to paste into the Athena catalog registration and COA's onboarding form, and the connector only
    // answers to the folded name. Always published, so a consumer never has to handle a missing key, but
    // when unpinned it names the mode rather than a schema the connector does not serve. The caller then
    // picks one per COA source, from `SHOW DATABASES IN <catalog>`.
    const schema = optionalUcIdentifier("DATABRICKS_SCHEMA");
    new cdk.CfnOutput(this, "ConnectorDatabase", {
      value: schema ?? UNPINNED_DATABASE_OUTPUT,
      description: schema
        ? "The single Unity Catalog schema this connector exposes; Athena calls it a schema too"
        : "This connector is unpinned and exposes every schema in DatabricksCatalog; " +
          "run SHOW DATABASES against the registered Athena catalog to list them",
    });
    new cdk.CfnOutput(this, "DatabricksCatalog", {
      value: ucIdentifier(requiredEnv("DATABRICKS_CATALOG")),
      description: "Unity Catalog catalog this connector reads",
    });
    new cdk.CfnOutput(this, "CredentialSecretArn", {
      value: credentialSecretArn,
      description: "Secret the connector reads its Databricks credential from",
    });
    new cdk.CfnOutput(this, "SpillBucket", {
      value: this.connector.spillBucket?.bucketName ?? "<none>",
      description: "Bucket the connector spills responses over 6 MB to",
    });
    new cdk.CfnOutput(this, "SpillKeyArn", {
      value: this.connector.spillKey?.keyArn ?? "<none>",
      description: "Customer-managed key encrypting the spill bucket",
    });
  }
}

/**
 * The connector's environment: three required coordinates, the secret's ARN, and three optional settings
 * (the schema pin and two operational ones).
 *
 * The credential's *value* is absent. `CREDENTIAL_SECRET_ARN` is a pointer; a Lambda environment variable
 * is readable by anyone with `lambda:GetFunctionConfiguration` and appears in the CloudFormation template
 * in plain text.
 */
function connectorEnvironment(credentialSecretArn: string): Record<string, string> {
  const environment: Record<string, string> = {
    DATABRICKS_WORKSPACE_HOSTNAME: requiredEnv(
      "DATABRICKS_WORKSPACE_HOSTNAME",
      "The workspace host, with no scheme and no port, on any cloud: " +
        "dbc-xxxxxxxx-xxxx.cloud.databricks.com (AWS), " +
        "adb-xxxxxxxxxxxxxxxx.x.azuredatabricks.net (Azure), " +
        "xxxxxxxxxxxxxxxx.x.gcp.databricks.com (GCP)",
    ),
    DATABRICKS_HTTP_PATH: requiredEnv(
      "DATABRICKS_HTTP_PATH",
      "The SQL Warehouse's HTTP path, from its Connection details tab:\n" +
        "/sql/1.0/warehouses/<warehouse id>",
    ),
    DATABRICKS_CATALOG: ucIdentifier(
      requiredEnv("DATABRICKS_CATALOG", "The Unity Catalog catalog to read."),
    ),
    CREDENTIAL_SECRET_ARN: credentialSecretArn,
  };

  // Set, the connector serves that schema and refuses every other name, a containment boundary it
  // enforces itself on top of the credential's Unity Catalog grants. Unset, it enumerates the catalog's
  // schemas and those grants are the only boundary. Omitted rather than set empty when unpinned: an empty
  // Lambda environment variable reads in the console as a value someone cleared by mistake.
  const schema = optionalUcIdentifier("DATABRICKS_SCHEMA");
  if (schema !== undefined) {
    environment.DATABRICKS_SCHEMA = schema;
  }

  // Validated here rather than in Java: a non-integer deploys untouched, the connector falls back to its
  // default, and the operator believes they raised the ceiling.
  const maxRows = optionalIntEnv("DATABRICKS_MAX_ROWS_PER_TABLE");
  if (maxRows !== undefined) {
    environment.DATABRICKS_MAX_ROWS_PER_TABLE = String(maxRows);
  }

  // Unset advertises nothing, which is the shipped default and the safe one, since an advertisement is a
  // guarantee Athena holds the connector to.
  //
  // Validated here for the same reason the row ceiling is. PushdownCapabilities ignores an unrecognised
  // name rather than refusing to start over a typo in a tuning knob, so `filtr,limit` would deploy clean,
  // advertise only `limit`, and leave the operator reading a full-scan bill with no signal anywhere.
  const pushdown = optionalEnv("DATABRICKS_ADVERTISE_PUSHDOWN");
  if (pushdown !== undefined) {
    const names = pushdown
      .split(",")
      .map((name) => name.trim().toLowerCase())
      .filter((name) => name.length > 0);
    const known: readonly string[] = PUSHDOWN_NAMES;
    const unknown = names.filter((name) => !known.includes(name));
    if (unknown.length > 0) {
      throw new Error(
        `DATABRICKS_ADVERTISE_PUSHDOWN contains ${unknown.map((n) => `"${n}"`).join(", ")}, ` +
          `which the connector does not recognise and would silently ignore. ` +
          `Valid names: ${PUSHDOWN_NAMES.join(", ")}. ` +
          `Complex-expression push-down is deliberately not offered — see databricks/README.md.`,
      );
    }
    if (names.length === 0) {
      throw new Error(
        "DATABRICKS_ADVERTISE_PUSHDOWN is set but names nothing. Unset it to advertise nothing, " +
          `or list one or more of: ${PUSHDOWN_NAMES.join(", ")}.`,
      );
    }
    environment.DATABRICKS_ADVERTISE_PUSHDOWN = names.join(",");
  }

  return environment;
}
