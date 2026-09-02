// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
import * as fs from "fs";
import * as path from "path";
import * as cdk from "aws-cdk-lib";
import { Match, Template } from "aws-cdk-lib/assertions";
import { CONNECTOR_TAG_KEY, CONNECTOR_TAG_VALUE, CONNECTOR_SPILL_KMS_TAG_KEY } from "coa-connector-cdk";
import {
  CONNECTOR_ENV_VARS,
  OPTIONAL_CONNECTOR_ENV_VARS,
  REQUIRED_CONNECTOR_ENV_VARS,
  DEFAULT_JAR_PATH,
  DatabricksConnectorStack,
  DatabricksConnectorStackProps,
  HANDLER,
  MEMORY_SIZE_MB,
  TIMEOUT_SECONDS,
  UNPINNED_DATABASE_OUTPUT,
} from "../lib/databricks-connector-stack";

// A stand-in for the fat JAR, so the tests do not require `mvn package` to have run. CDK stages a
// `.jar` as an archive asset without inspecting its contents.
const FAKE_JAR = path.join(__dirname, "..", "cdk.out", "test-fixture.jar");

const SERVE_ROLE = "arn:aws:iam::999988887777:role/scl-dev-serve-role";
const DISCOVERY_ROLE = "arn:aws:iam::999988887777:role/scl-dev-sources-db-connector";
const SECRET_ARN =
  "arn:aws:secretsmanager:us-east-1:123456789012:secret:databricks-connector-pat-AbCdEf";

const TOUCHED = [
  ...CONNECTOR_ENV_VARS,
  "DATABRICKS_MAX_ROWS_PER_TABLE",
  "DATABRICKS_ADVERTISE_PUSHDOWN",
  "CREDENTIAL_KMS_KEY_ARN",
  "FUNCTION_NAME_PREFIX",
  "ALARM_TOPIC_ARN",
];
let saved: Record<string, string | undefined>;

beforeAll(() => {
  fs.mkdirSync(path.dirname(FAKE_JAR), { recursive: true });
  fs.writeFileSync(FAKE_JAR, "not really a jar");
});

beforeEach(() => {
  saved = {};
  for (const name of TOUCHED) {
    saved[name] = process.env[name];
    delete process.env[name];
  }
  process.env.DATABRICKS_WORKSPACE_HOSTNAME = "dbc-a1b2345c-d6e7.cloud.databricks.com";
  process.env.DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/a1b234c567d8e9fa";
  process.env.DATABRICKS_CATALOG = "workspace";
  process.env.DATABRICKS_SCHEMA = "coa_dbx_test";
  process.env.CREDENTIAL_SECRET_ARN = SECRET_ARN;
});

afterEach(() => {
  for (const name of TOUCHED) {
    if (saved[name] === undefined) {
      delete process.env[name];
    } else {
      process.env[name] = saved[name];
    }
  }
});

function synth(props: Partial<DatabricksConnectorStackProps> = {}): Template {
  const app = new cdk.App();
  const stack = new DatabricksConnectorStack(app, "databricks-coa-connector", {
    env: { account: "123456789012", region: "us-east-1" },
    jarPath: FAKE_JAR,
    queryRoleArns: [SERVE_ROLE, DISCOVERY_ROLE],
    ...props,
  });
  return Template.fromStack(stack);
}

/**
 * Every literal KMS ARN the function's identity policy names. The spill key is not one: its Resource is an
 * `Fn::GetAtt` rather than a string, because this stack creates it.
 */
function externalKmsResources(template: Template): string[] {
  const policies = Object.values(template.findResources("AWS::IAM::Policy"));
  const found: string[] = [];
  for (const policy of policies) {
    const statements = policy.Properties?.PolicyDocument?.Statement ?? [];
    for (const statement of statements) {
      const resource = statement.Resource;
      if (typeof resource === "string" && resource.startsWith("arn:aws:kms:")) {
        found.push(resource);
      }
    }
  }
  return found;
}

/** The connector function's environment variables. */
function connectorEnvironment(template: Template): Record<string, unknown> {
  const functions = template.findResources("AWS::Lambda::Function");
  const variables = Object.values(functions)
    .map((resource) => resource.Properties?.Environment?.Variables)
    .find((vars) => vars?.spill_prefix !== undefined);
  expect(variables).toBeDefined();
  return variables as Record<string, unknown>;
}

describe("the connector Lambda", () => {
  it("deploys with this connector's handler under a name that cannot collide", () => {
    synth().hasResourceProperties("AWS::Lambda::Function", {
      Handler: HANDLER,
      FunctionName: "databricks-coa-connector",
      Runtime: "java17",
    });
  });

  it("is sized 3008 MB and 120 s, not the construct's defaults", () => {
    // The construct defaults to 1024 MB and 90 s. Federation cannot express aggregation, so a GROUP BY
    // reads every predicate-matching row out of the warehouse through this function.
    synth().hasResourceProperties("AWS::Lambda::Function", {
      MemorySize: MEMORY_SIZE_MB,
      Timeout: TIMEOUT_SECONDS,
    });
    expect(MEMORY_SIZE_MB).toBe(3008);
    expect(TIMEOUT_SECONDS).toBe(120);
  });

  it("carries the --add-opens flag Arrow needs on Java 17", () => {
    // Without it, metadata calls succeed and every read fails with "Failed to initialize MemoryUtil". The
    // construct sets it; this asserts that setting the connector's own variables did not displace it.
    expect(connectorEnvironment(synth()).JAVA_TOOL_OPTIONS).toBe(
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
    );
  });

  it("carries the coa:connector tag COA's invoke policy matches", () => {
    // Without the tag nothing can invoke the function and the first scan is denied. COA scopes
    // invoke on the tag, not on the name.
    synth().hasResourceProperties("AWS::Lambda::Function", {
      Tags: Match.arrayWith([{ Key: CONNECTOR_TAG_KEY, Value: CONNECTOR_TAG_VALUE }]),
    });
  });

  it("is the ONLY Lambda in the template carrying that tag", () => {
    // `Tags.of(scope)` applies to every taggable child, so a tag applied one level too high makes another
    // function Athena-invocable. This stack has a second function, the bucket's auto-delete-objects custom
    // resource, so the assertion is not vacuous.
    const functions = Object.entries(synth().findResources("AWS::Lambda::Function"));
    expect(functions.length).toBeGreaterThan(1);
    const tagged = functions.filter(([, resource]) =>
      ((resource.Properties?.Tags ?? []) as { Key?: string }[]).some(
        (tag) => tag.Key === CONNECTOR_TAG_KEY,
      ),
    );
    expect(tagged.map(([logicalId]) => logicalId)).toHaveLength(1);
  });

  it("points at the jar the Maven build actually produces", () => {
    // Read out of the pom rather than restated: comparing DEFAULT_JAR_PATH against a literal copied FROM
    // it asserts nothing about Maven, so bumping <version> or renaming <artifactId> stays green and fails
    // at deploy.
    const pom = fs.readFileSync(path.join(__dirname, "..", "..", "pom.xml"), "utf8");
    const parent = /<parent>([\s\S]*?)<\/parent>/.exec(pom)?.[1] ?? "";
    const version = /<version>([^<]+)<\/version>/.exec(parent)?.[1];
    const artifactId = /<artifactId>([^<]+)<\/artifactId>/.exec(
      pom.slice(pom.indexOf("</parent>")),
    )?.[1];

    expect(artifactId).toBe("databricks-connector");
    expect(version).toBeDefined();
    expect(DEFAULT_JAR_PATH).toBe(
      path.join(__dirname, "..", "..", "target", `${artifactId}-${version}.jar`),
    );
  });
});

describe("the connector's environment", () => {
  it("lower-cases the catalog and schema, as the connector does", () => {
    // ConnectionConfig folds the case and readerFor rejects anything else, so DATABRICKS_SCHEMA=Sales
    // deploys cleanly and every query gets `Unknown schema: "Sales". This connector serves only "sales"`.
    process.env.DATABRICKS_CATALOG = "MainCatalog";
    process.env.DATABRICKS_SCHEMA = "Sales";
    const variables = connectorEnvironment(synth());
    expect(variables.DATABRICKS_CATALOG).toBe("maincatalog");
    expect(variables.DATABRICKS_SCHEMA).toBe("sales");
  });

  it("passes all five coordinates", () => {
    const variables = connectorEnvironment(synth());
    expect(variables.DATABRICKS_WORKSPACE_HOSTNAME).toBe("dbc-a1b2345c-d6e7.cloud.databricks.com");
    expect(variables.DATABRICKS_HTTP_PATH).toBe("/sql/1.0/warehouses/a1b234c567d8e9fa");
    expect(variables.DATABRICKS_CATALOG).toBe("workspace");
    expect(variables.DATABRICKS_SCHEMA).toBe("coa_dbx_test");
    expect(variables.CREDENTIAL_SECRET_ARN).toBe(SECRET_ARN);
  });

  describe("DATABRICKS_SCHEMA is optional", () => {
    it("omits the variable entirely when unset, rather than setting it empty", () => {
      // Absent, not empty. An empty Lambda environment variable reads in the console as a value someone
      // cleared by mistake, and the Java side treats it as unset anyway.
      delete process.env.DATABRICKS_SCHEMA;
      const variables = connectorEnvironment(synth());
      expect(variables.DATABRICKS_SCHEMA).toBeUndefined();
      expect("DATABRICKS_SCHEMA" in variables).toBe(false);
    });

    it("treats a blank value as unset, because a shell and CDK disagree about which it sends", () => {
      for (const blank of ["", "   "]) {
        process.env.DATABRICKS_SCHEMA = blank;
        expect(connectorEnvironment(synth()).DATABRICKS_SCHEMA).toBeUndefined();
      }
    });

    it("still requires the catalog, which cannot travel in a request", () => {
      // An Athena federated catalog has one namespace level below the registered name and this connector
      // spends it on the UC schema, so the UC catalog has to be configuration.
      delete process.env.DATABRICKS_SCHEMA;
      delete process.env.DATABRICKS_CATALOG;
      expect(() => synth()).toThrow(/DATABRICKS_CATALOG/);
    });

    it("rejects a malformed schema at synth rather than at the first query", () => {
      // Optional must not mean unvalidated. At query time a malformed pin is indistinguishable from the
      // unpinned mode: both look like "the schema I set is being ignored".
      for (const bad of ["my-schema", "main.sub", "main;x", "1sales"]) {
        process.env.DATABRICKS_SCHEMA = bad;
        expect(() => synth()).toThrow(/DATABRICKS_SCHEMA/);
      }
    });
  });

  it("never carries a credential value, only the secret's ARN", () => {
    // A Lambda environment variable is readable by anyone with lambda:GetFunctionConfiguration and lands
    // in the CloudFormation template in plain text.
    const rendered = JSON.stringify(synth().toJSON());
    for (const forbidden of ["dapi", "client_secret", "OAuth2Secret", "PWD="]) {
      expect(rendered).not.toContain(forbidden);
    }
  });

  it("advertises no push-down by default", () => {
    // An advertisement is a guarantee Athena holds the connector to, so silence is the safe default.
    expect(connectorEnvironment(synth()).DATABRICKS_ADVERTISE_PUSHDOWN).toBeUndefined();
  });

  it("passes a push-down opt-in through when it is set", () => {
    process.env.DATABRICKS_ADVERTISE_PUSHDOWN = "filter,limit";
    expect(connectorEnvironment(synth()).DATABRICKS_ADVERTISE_PUSHDOWN).toBe("filter,limit");
  });

  it("normalises the push-down opt-in's spacing and case", () => {
    process.env.DATABRICKS_ADVERTISE_PUSHDOWN = " FILTER , limit ,topn";
    expect(connectorEnvironment(synth()).DATABRICKS_ADVERTISE_PUSHDOWN).toBe("filter,limit,topn");
  });

  it("rejects an unrecognised push-down name at synth rather than ignoring it at run time", () => {
    // PushdownCapabilities ignores an unknown name rather than refusing to start over a typo in a tuning
    // knob, so `filtr,limit` would deploy clean, advertise only `limit`, and leave the operator reading a
    // full-scan bill with no signal.
    process.env.DATABRICKS_ADVERTISE_PUSHDOWN = "filtr,limit";
    expect(() => synth()).toThrow(/"filtr"/);
    expect(() => synth()).toThrow(/filter, limit, topn/);
  });

  it("rejects the complex push-down name specifically, since it cannot work", () => {
    process.env.DATABRICKS_ADVERTISE_PUSHDOWN = "complex";
    expect(() => synth()).toThrow(/Complex-expression push-down is deliberately not offered/);
  });

  it("rejects a push-down opt-in that names nothing", () => {
    process.env.DATABRICKS_ADVERTISE_PUSHDOWN = " , ,";
    expect(() => synth()).toThrow(/names nothing/);
  });

  it("sets no row ceiling by default, so the connector's own default stands", () => {
    expect(connectorEnvironment(synth()).DATABRICKS_MAX_ROWS_PER_TABLE).toBeUndefined();
  });

  it("rejects a non-integer row ceiling at synth rather than at run time", () => {
    // Left to the Java side, a typo deploys untouched, the connector falls back to its default, and the
    // operator believes they changed the ceiling.
    process.env.DATABRICKS_MAX_ROWS_PER_TABLE = "2_000_000";
    expect(() => synth()).toThrow(/positive integer/);
  });

  it("fails synth with an actionable message when a required coordinate is missing", () => {
    for (const name of REQUIRED_CONNECTOR_ENV_VARS) {
      const saved = process.env[name];
      delete process.env[name];
      expect(() => synth()).toThrow(new RegExp(name));
      process.env[name] = saved;
    }
  });

  it("synths without any of the optional variables", () => {
    // The counterpart to the test above, so "optional" is asserted rather than just omitted from the
    // required list. The two lists together account for every variable the stack reads.
    for (const name of OPTIONAL_CONNECTOR_ENV_VARS) {
      delete process.env[name];
    }
    expect(() => synth()).not.toThrow();
  });
});

describe("the credential grant", () => {
  it("grants the function read on exactly the named secret", () => {
    synth().hasResourceProperties("AWS::IAM::Policy", {
      PolicyDocument: Match.objectLike({
        Statement: Match.arrayWith([
          Match.objectLike({
            Action: Match.arrayWith(["secretsmanager:GetSecretValue"]),
            Resource: SECRET_ARN,
          }),
        ]),
      }),
    });
  });

  it("scopes the grant to the complete ARN, suffix included", () => {
    // fromSecretNameV2 would grant every secret whose name is a prefix of this one, because Secrets
    // Manager appends a random six-character suffix to every ARN.
    const rendered = JSON.stringify(synth().toJSON());
    expect(rendered).toContain("databricks-connector-pat-AbCdEf");
    expect(rendered).not.toContain("databricks-connector-test-pat-??????");
  });

  it("grants no kms:Decrypt on any external key when the secret uses the AWS-managed one", () => {
    // The only KMS statement should be the spill key's, whose Resource is an Fn::GetAtt on a key this
    // stack creates rather than a literal ARN.
    expect(externalKmsResources(synth())).toEqual([]);
  });

  it("grants kms:Decrypt on a customer-managed key when one is named", () => {
    // No existing grant covers this, and without it every read fails with access-denied at run time rather
    // than at deploy. The rendered Action is a bare string rather than a one-element array, because
    // grantDecrypt on an imported key adds exactly one action.
    const keyArn = "arn:aws:kms:us-east-1:123456789012:key/00000000-0000-0000-0000-000000000000";
    process.env.CREDENTIAL_KMS_KEY_ARN = keyArn;
    synth().hasResourceProperties("AWS::IAM::Policy", {
      PolicyDocument: Match.objectLike({
        Statement: Match.arrayWith([
          Match.objectLike({ Action: "kms:Decrypt", Resource: keyArn }),
        ]),
      }),
    });
    expect(externalKmsResources(synth())).toEqual([keyArn]);
  });
});

describe("spill", () => {
  it("gets its own bucket and its own customer-managed key", () => {
    // Never a shared bucket: one connector's read grant would cover another's spilled rows, and spilled
    // rows are query results.
    const template = synth();
    template.resourceCountIs("AWS::S3::Bucket", 1);
    template.resourceCountIs("AWS::KMS::Key", 1);
  });

  it("tags the spill key coa:connector-spill, which COA's key policy matches", () => {
    // Fails only above 6 MB, where a misconfigured connector looks healthy. Spill is the normal case here,
    // because aggregate reads exceed 6 MB routinely.
    synth().hasResourceProperties("AWS::KMS::Key", {
      Tags: Match.arrayWith([{ Key: CONNECTOR_SPILL_KMS_TAG_KEY, Value: CONNECTOR_TAG_VALUE }]),
    });
  });

  it("spills under connectors/databricks/spills, which is what COA's read grant matches", () => {
    expect(connectorEnvironment(synth()).spill_prefix).toBe("connectors/databricks/spills");
  });

  it("encrypts each block client-side as well", () => {
    expect(connectorEnvironment(synth()).disable_spill_encryption).toBe("false");
  });
});

describe("COA's two roles", () => {
  it("grants invoke to serve AND discovery", () => {
    // Two roles, because two COA components call Athena: serve runs the queries, discovery runs DESCRIBE,
    // which is the only way the @pk/@fk tags are read. Grant serve alone and the connector answers SELECT
    // perfectly while no declared key reaches COA.
    const template = synth();
    for (const role of [SERVE_ROLE, DISCOVERY_ROLE]) {
      template.hasResourceProperties("AWS::Lambda::Permission", {
        Action: "lambda:InvokeFunction",
        Principal: role,
      });
    }
    template.resourceCountIs("AWS::Lambda::Permission", 2);
  });

  it("registers no Athena data catalog: that belongs to the querying account", () => {
    synth().resourceCountIs("AWS::Athena::DataCatalog", 0);
  });
});

describe("outputs", () => {
  const EXPECTED = [
    "ConnectorFunctionArn",
    "ConnectorDatabase",
    "DatabricksCatalog",
    "CredentialSecretArn",
    "SpillBucket",
    "SpillKeyArn",
  ];

  it("publishes every key by its exact name", () => {
    // Declared at stack level so the key is the logical id verbatim. A construct's own outputs come back
    // hash-suffixed, and a test resolving a mangled key finds nothing and skips while looking healthy.
    const outputs = synth().findOutputs("*");
    for (const key of EXPECTED) {
      expect(Object.keys(outputs)).toContain(key);
    }
  });

  it("exports none of them", () => {
    const outputs = synth().findOutputs("*");
    for (const output of Object.values(outputs)) {
      expect(output.Export).toBeUndefined();
    }
    expect(Object.keys(outputs).length).toBeGreaterThan(0);
  });

  it("names the schema the connector exposes, so a test can find it without guessing", () => {
    synth().hasOutput("ConnectorDatabase", { Value: "coa_dbx_test" });
    synth().hasOutput("DatabricksCatalog", { Value: "workspace" });
  });

  it("still publishes ConnectorDatabase when unpinned, saying so rather than naming a schema", () => {
    // Always present, so a consumer never has to handle a missing key, but not schema-shaped: an unpinned
    // connector has no single database, and a value like "all" pasted into an Athena catalog registration
    // would fail as an obscure lookup rather than an obvious mistake.
    delete process.env.DATABRICKS_SCHEMA;
    const template = synth();
    expect(Object.keys(template.findOutputs("*"))).toContain("ConnectorDatabase");
    template.hasOutput("ConnectorDatabase", { Value: UNPINNED_DATABASE_OUTPUT });
    // The catalog is still concrete — it is what SHOW DATABASES is run against.
    template.hasOutput("DatabricksCatalog", { Value: "workspace" });
  });

  it("publishes the lower-cased names, so the outputs match what the connector answers to", () => {
    // These outputs are what the README tells an operator to paste into the Athena catalog registration
    // and COA's onboarding form, and the raw casing points at a schema the connector rejects.
    process.env.DATABRICKS_CATALOG = "MainCatalog";
    process.env.DATABRICKS_SCHEMA = "Sales";
    const template = synth();
    template.hasOutput("ConnectorDatabase", { Value: "sales" });
    template.hasOutput("DatabricksCatalog", { Value: "maincatalog" });
    // And the output agrees with the environment variable rather than coinciding with it.
    expect(connectorEnvironment(template).DATABRICKS_SCHEMA).toBe("sales");
  });
});

describe("connector alarms", () => {
  const ALARM_TOPIC = "arn:aws:sns:us-east-1:123456789012:coa-connector-alarms";

  it("alarms on all four metrics the connector emits, plus the three Lambda ones", () => {
    // Seven, not three: each of the four below is a *caught* failure, so the invocation succeeds and
    // appears in neither the Lambda error rate nor its duration.
    synth().resourceCountIs("AWS::CloudWatch::Alarm", 7);
  });

  it.each([
    ["ConnectorConfigResolutionFailures", "config-resolution-failures", 0],
    ["ConnectorWarehouseConnectFailures", "warehouse-connect-failures", 5],
    ["ConnectorTableCeilingExceeded", "table-ceiling-exceeded", 0],
  ])("alarms on %s in the namespace the jar emits into", (metricName, suffix, threshold) => {
    synth().hasResourceProperties("AWS::CloudWatch::Alarm", {
      AlarmName: `databricks-coa-connector-${suffix}`,
      Namespace: "COA/Connectors",
      MetricName: metricName,
      Threshold: threshold,
      ComparisonOperator: "GreaterThanThreshold",
      Dimensions: [{ Name: "Connector", Value: "databricks" }],
    });
  });

  it("thresholds rows-returned against the default ceiling when none is configured", () => {
    // 80% of 2,000,000 — the leading indicator, so it has to fire before the ceiling refuses a query.
    synth().hasResourceProperties("AWS::CloudWatch::Alarm", {
      AlarmName: "databricks-coa-connector-rows-returned-p95",
      MetricName: "ConnectorRowsReturned",
      ExtendedStatistic: "p95",
      Threshold: 1600000,
      EvaluationPeriods: 2,
    });
  });

  it("moves the rows-returned threshold with a configured ceiling", () => {
    process.env.DATABRICKS_MAX_ROWS_PER_TABLE = "50000";

    synth().hasResourceProperties("AWS::CloudWatch::Alarm", {
      AlarmName: "databricks-coa-connector-rows-returned-p95",
      Threshold: 40000,
    });
  });

  it("does not dimension on Catalog, which would stop matching once a second source is registered", () => {
    const alarms = synth().findResources("AWS::CloudWatch::Alarm");
    const connectorAlarms = Object.values(alarms).filter(
      (alarm) => alarm.Properties?.Namespace === "COA/Connectors",
    );

    expect(connectorAlarms).toHaveLength(4);
    for (const alarm of connectorAlarms) {
      expect(alarm.Properties.Dimensions).toEqual([
        { Name: "Connector", Value: "databricks" },
      ]);
    }
  });

  it("notifies nobody without ALARM_TOPIC_ARN, and every alarm with it", () => {
    for (const alarm of Object.values(synth().findResources("AWS::CloudWatch::Alarm"))) {
      expect(alarm.Properties.AlarmActions).toBeUndefined();
    }

    process.env.ALARM_TOPIC_ARN = ALARM_TOPIC;
    const withTopic = Object.values(synth().findResources("AWS::CloudWatch::Alarm"));

    expect(withTopic).toHaveLength(7);
    for (const alarm of withTopic) {
      expect(alarm.Properties.AlarmActions).toEqual([ALARM_TOPIC]);
    }
  });

  it("names alarms with the function-name prefix, so two deployments do not collide", () => {
    process.env.FUNCTION_NAME_PREFIX = "sales-";

    synth().hasResourceProperties("AWS::CloudWatch::Alarm", {
      AlarmName: "sales-databricks-coa-connector-table-ceiling-exceeded",
    });
  });
});
