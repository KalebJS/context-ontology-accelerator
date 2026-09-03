// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
import * as fs from "fs";
import * as cdk from "aws-cdk-lib";
import * as cloudwatch from "aws-cdk-lib/aws-cloudwatch";
import * as cloudwatchActions from "aws-cdk-lib/aws-cloudwatch-actions";
import * as iam from "aws-cdk-lib/aws-iam";
import * as kms from "aws-cdk-lib/aws-kms";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as logs from "aws-cdk-lib/aws-logs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as sns from "aws-cdk-lib/aws-sns";
import { Construct } from "constructs";
import {
  CONNECTOR_DIMENSION,
  CONNECTOR_METRIC_NAMESPACE,
  CONNECTOR_SPILL_KMS_TAG_KEY,
  CONNECTOR_TAG_KEY,
  CONNECTOR_TAG_VALUE,
} from "./coa-contract";

/**
 * The key prefix a connector spills under: `connectors/{connectorId}/spills`.
 *
 * <p>Derived, not a setting. It is both what COA's spill-read grant matches and what this stack
 * grants the connector write access to, so the two cannot disagree — and disagreement is expensive:
 * <b>a connector that cannot write its spill returns `SUCCEEDED` with zero rows, not an error.</b>
 *
 * <p>Named after COA's layout rather than a conventional segment like `athena-federation-spill`,
 * which every published connector uses — a grant on that would reach spill data in unrelated
 * buckets. The id segment also lets COA grant per connector rather than by one wildcard.
 */
export function spillPrefixFor(connectorId: string): string {
  return `connectors/${connectorId}/spills`;
}

/**
 * Suffix every connector's Lambda name carries. A convention only — it grants nothing, since COA
 * scopes invoke on the {@link CONNECTOR_TAG_KEY} tag with a wildcard function name.
 */
export const CONNECTOR_FUNCTION_SUFFIX = "-coa-connector";

/** Lambda's own limit on a function name. */
const MAX_FUNCTION_NAME_LENGTH = 64;

/**
 * The intersection of what Lambda accepts in a function name and what CloudFormation accepts in a
 * stack name, because one call names both. Lambda alone would allow underscores and a leading digit;
 * CloudFormation would reject the stack after the jar had already been built and staged.
 */
const FUNCTION_NAME_PATTERN = /^[A-Za-z][A-Za-z0-9-]*$/;

/**
 * The connector's Lambda name: `{prefix}{connectorId}{@link CONNECTOR_FUNCTION_SUFFIX}`.
 *
 * <p>Exported so an app names its stack with the same call. They must not drift: a second
 * deployment reusing the first's stack name replaces that connector rather than adding one.
 *
 * @throws Error if the result cannot name both a Lambda and a CloudFormation stack.
 */
export function connectorFunctionName(
  connectorId: string,
  prefix?: string,
): string {
  const name = `${prefix ?? ""}${connectorId}${CONNECTOR_FUNCTION_SUFFIX}`;
  if (!FUNCTION_NAME_PATTERN.test(name)) {
    throw new Error(
      `"${name}" cannot name both a Lambda and a CloudFormation stack: it must start with a ` +
        `letter and contain only letters, digits and hyphens — no underscores. Check the prefix ` +
        `${JSON.stringify(prefix ?? "")} and the connector id "${connectorId}".`,
    );
  }
  if (name.length > MAX_FUNCTION_NAME_LENGTH) {
    throw new Error(
      `Function name "${name}" is ${name.length} characters; Lambda allows ` +
        `${MAX_FUNCTION_NAME_LENGTH}. The suffix "${CONNECTOR_FUNCTION_SUFFIX}" is fixed, so ` +
        `shorten the prefix or the connector id.`,
    );
  }
  return name;
}

/** Environment variables this construct sets itself; a caller may not also set them. */
export const RESERVED_ENVIRONMENT_KEYS = [
  "JAVA_TOOL_OPTIONS",
  "spill_bucket",
  "spill_prefix",
  "disable_spill_encryption",
] as const;

/** Properties for {@link AthenaFederationConnector}. */
export interface AthenaFederationConnectorProps {
  /**
   * The connector's id — its folder name under `connectors/`. Every unique resource name derives
   * from it, so a second connector cannot reshape the first one's stack.
   */
  readonly connectorId: string;

  /**
   * Tells two deployments of the *same* connector apart when they share an account. A prefix rather
   * than a settable name so the suffix convention survives; permissions do not depend on either.
   *
   * <p>Give the stack the same prefix — a second deployment reusing the first's stack name replaces
   * it. {@link connectorFunctionName} derives both from one call.
   */
  readonly functionNamePrefix?: string;

  /** Fully-qualified handler class, e.g. `dev.coa.example.ExampleCompositeHandler`. */
  readonly handler: string;

  /** Path to the fat JAR. Too large for inline code, so it ships as an S3 asset. */
  readonly jarPath: string;

  /**
   * ARNs of every COA role that reaches this connector through Athena — normally two: **serve**,
   * which runs the queries, and **discovery**, which runs `DESCRIBE` and so reads the `@pk` /
   * `@fk` tags. Omit discovery and `SELECT` works while no declared key ever reaches COA.
   *
   * <p>Each gets `lambda:InvokeFunction` and spill read as **resource** policies, which is what
   * makes the connector usable cross-account — that side is the only one this stack owns.
   *
   * <p>All principals get the same grant. `DESCRIBE` cannot spill, so discovery does not strictly
   * need spill read, but a metadata-only grant breaks silently the moment discovery samples rows.
   *
   * <p>Empty deploys a connector nobody external can invoke; useful only for probing.
   */
  readonly queryRoleArns?: readonly string[];

  /** Lambda runtime. Defaults to `java17`. */
  readonly runtime?: lambda.Runtime;

  /** Invocation timeout. Defaults to 90 seconds. */
  readonly timeout?: cdk.Duration;

  /** Memory. Defaults to 1024 MB — enough to buffer a block before it spills. */
  readonly memorySize?: number;

  /**
   * Whether the connector gets a spill bucket. Defaults to `"create"`.
   *
   * <p>`"none"` sets no `spill_bucket` — legitimate for a source that cannot exceed 6 MB, but know
   * the failure mode: metadata and inline responses keep working, and a response needing to spill
   * has been observed returning `SUCCEEDED` with **zero rows**, no exception, nothing logged.
   *
   * <p>Whether you can rule that out depends on the connector, not the data volume: one advertising
   * no `LIMIT` push-down makes Athena request whole tables, so even `LIMIT 1000` can spill.
   *
   * <p>No option to share a bucket: one connector's read grant would cover another's spilled rows.
   */
  readonly spill?: "create" | "none";

  /**
   * Connector-specific environment variables, from the connector's own stack — which is what keeps
   * this construct free of any one connector's settings.
   */
  readonly environment?: Record<string, string>;

  /** Lambda description. */
  readonly description?: string;

  /**
   * Whether the connector gets the three Lambda health alarms — throttles, error rate, and duration
   * against its own timeout. Defaults to `"create"`.
   *
   * <p>`"none"` for a deployment whose operator watches the function some other way. Note what is
   * lost: one connector is shared by every source pointed at it, so a throttle is not one source
   * degrading but all of them at once, and nothing else in COA can see that.
   */
  readonly alarms?: "create" | "none";

  /**
   * SNS topic every alarm on this connector notifies. Optional, and **an alarm without it notifies
   * nobody** — it changes state in the console and that is all.
   *
   * <p>Left optional rather than created here because the topic is where an alarm meets a rota, and
   * this construct has no way to know whose. Supply one before treating the connector as operated.
   */
  readonly alarmTopicArn?: string;
}

/**
 * One Athena Query Federation connector: a Lambda and, by default, its own spill bucket and the
 * customer-managed key encrypting it.
 *
 * <p>COA's IAM is scoped to four things a deployment must get exactly right — a tag on the function,
 * the spill key prefix, a tagged customer-managed key, and three resource policies. Their failure
 * timing is asymmetric, which is the argument for a construct over a runbook: the
 * {@code coa:connector} tag denies the first scan, loudly, while the spill three fail only above
 * 6 MB, where a misconfigured connector looks healthy. A document can be half-followed; a template
 * that always emits all four cannot.
 *
 * <p>Each connector gets its own spill bucket, or none — never a shared one, since one connector's
 * read grant would cover another's spilled rows. The key is created here rather than accepted
 * because COA's key policy is scoped to a tag, and `aws/s3` can neither be tagged nor have its
 * policy edited.
 */
export class AthenaFederationConnector extends Construct {
  /** The connector Lambda. Register this ARN as an Athena `LAMBDA` data catalog. */
  public readonly connectorFunction: lambda.Function;

  /** The connector's own spill bucket, unless `spill: "none"` was chosen. */
  public readonly spillBucket?: s3.Bucket;

  /** The customer-managed key encrypting {@link spillBucket}, created alongside it. */
  public readonly spillKey?: kms.Key;

  /** Every alarm on this connector, the three below plus any the connector's own stack added. */
  public readonly alarms: cloudwatch.Alarm[] = [];

  /** Where alarms notify, when {@link AthenaFederationConnectorProps.alarmTopicArn} was given. */
  public readonly alarmTopic?: sns.ITopic;

  /**
   * The function's name as a literal string.
   *
   * <p>Not the same as `connectorFunction.functionName`, which is a CloudFormation token even when the
   * name was supplied — using that one in an alarm name yields an `Fn::Join` in the template, which
   * resolves correctly but cannot be read by anyone looking for the alarm.
   */
  public readonly functionName: string;

  private readonly connectorId: string;

  constructor(scope: Construct, id: string, props: AthenaFederationConnectorProps) {
    super(scope, id);
    this.connectorId = props.connectorId;

    if (!fs.existsSync(props.jarPath)) {
      throw new Error(
        `Connector JAR not found at ${props.jarPath}. Build it first:\n` +
          `  cd connectors && mvn -q -B package -pl ${props.connectorId} -am\n` +
          `or from the connector's CDK app, where deploy builds it: pnpm run deploy`,
      );
    }

    // The spill prefix is fixed because COA's spill-read policy matches its shape. The name's
    // suffix is only a convention — invoke is scoped on the coa:connector tag applied below.
    // See spillPrefixFor and CONNECTOR_FUNCTION_SUFFIX.
    const functionName = connectorFunctionName(
      props.connectorId,
      props.functionNamePrefix,
    );
    const spillPrefix = spillPrefixFor(props.connectorId);
    this.functionName = functionName;

    if ((props.spill ?? "create") === "create") {
      // Customer-managed because COA's key policy is scoped to a tag. SSE-KMS is required.
      this.spillKey = new kms.Key(this, "SpillKey", {
        description: `Spill encryption for the "${props.connectorId}" COA connector`,
        enableKeyRotation: true,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      });
      cdk.Tags.of(this.spillKey).add(CONNECTOR_SPILL_KMS_TAG_KEY, CONNECTOR_TAG_VALUE);

      // One-day expiry: spill data is scratch, and anything older than its query is garbage.
      this.spillBucket = new s3.Bucket(this, "SpillBucket", {
        blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
        enforceSSL: true,
        encryption: s3.BucketEncryption.KMS,
        encryptionKey: this.spillKey,
        // Without this a CMK turns every spilled block into billable KMS traffic.
        bucketKeyEnabled: true,
        versioned: false,
        lifecycleRules: [
          { id: "expire-spill", expiration: cdk.Duration.days(1), enabled: true },
        ],
        // Retaining it would leave a bucket behind on every `cdk destroy`.
        removalPolicy: cdk.RemovalPolicy.DESTROY,
        autoDeleteObjects: true,
      });
    }

    const environment: Record<string, string> = {
      // MANDATORY on Java 17+. Arrow reaches into java.nio internals, and without this
      // metadata calls succeed while every read fails with "Failed to initialize
      // MemoryUtil" — which presents as "discovery works, queries are broken".
      JAVA_TOOL_OPTIONS: "--add-opens=java.base/java.nio=ALL-UNNAMED",
      spill_prefix: spillPrefix,
      // Explicit rather than left to the SDK's default. The key is generated per query and rides
      // on the Split, so it costs no KMS calls and needs no grant — client-side encryption of the
      // block, independent of the bucket's SSE-KMS above.
      disable_spill_encryption: "false",
    };
    if (this.spillBucket !== undefined) {
      environment.spill_bucket = this.spillBucket.bucketName;
    }
    // Refused rather than merged: either outcome reads as "the spill configuration is ignored".
    for (const [key, value] of Object.entries(props.environment ?? {})) {
      const reserved: readonly string[] = RESERVED_ENVIRONMENT_KEYS;
      if (reserved.includes(key)) {
        throw new Error(
          `Connector "${props.connectorId}" sets environment variable "${key}", which this ` +
            `construct manages. Remove it from the connector's stack. Managed keys: ` +
            `${reserved.join(", ")}.`,
        );
      }
      environment[key] = value;
    }

    this.connectorFunction = new lambda.Function(this, "Function", {
      functionName,
      runtime: props.runtime ?? lambda.Runtime.JAVA_17,
      handler: props.handler,
      // fromAsset on a .jar uploads the archive as-is to the CDK asset bucket; the JAR is
      // far too large to inline into the template.
      code: lambda.Code.fromAsset(props.jarPath),
      timeout: props.timeout ?? cdk.Duration.seconds(90),
      memorySize: props.memorySize ?? 1024,
      environment,
      // An explicit log group rather than `logRetention`, which is deprecated and provisions
      // a custom-resource Lambda to set retention after the fact.
      logGroup: new logs.LogGroup(this, "LogGroup", {
        // Named where anyone would look. Left to CDK it gets a generated name, and every
        // "check the connector's CloudWatch logs" instruction leads nowhere — which matters most
        // on exactly the paths that fail without an error.
        logGroupName: `/aws/lambda/${functionName}`,
        retention: logs.RetentionDays.ONE_WEEK,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      }),
      description:
        props.description ??
        `Athena Query Federation connector "${props.connectorId}"`,
    });

    // The control that fails fast: COA's invoke policy matches this tag, so without it nothing
    // can invoke the function and the first scan is denied.
    cdk.Tags.of(this.connectorFunction).add(CONNECTOR_TAG_KEY, CONNECTOR_TAG_VALUE);

    if (this.spillBucket !== undefined && this.spillKey !== undefined) {
      // Scoped to the prefix; grantReadWrite on a KMS bucket also grants this role the key access
      // it needs to write.
      this.spillBucket.grantReadWrite(this.connectorFunction, `${spillPrefix}/*`);
      // Named explicitly too: it is the grant COA's contract calls for, and a reader will look for
      // it rather than infer it from grantReadWrite's side effects.
      this.spillKey.addToResourcePolicy(
        new iam.PolicyStatement({
          sid: "ConnectorGenerateSpillDataKey",
          principals: [this.connectorFunction.grantPrincipal],
          actions: ["kms:GenerateDataKey"],
          resources: ["*"],
        }),
      );
      // The SDK's SpillLocationVerifier calls HeadBucket before returning splits, which needs s3:ListBucket — a
      // bucket-level call grantReadWrite's object pattern does not cover on every CDK version.
      this.connectorFunction.addToRolePolicy(
        new iam.PolicyStatement({
          sid: "SpillBucketLocate",
          actions: ["s3:GetBucketLocation", "s3:ListBucket"],
          resources: [this.spillBucket.bucketArn],
        }),
      );
    }

    this.grantQueryAccess(props.queryRoleArns ?? [], spillPrefix);

    if (props.alarmTopicArn !== undefined) {
      this.alarmTopic = sns.Topic.fromTopicArn(this, "AlarmTopic", props.alarmTopicArn);
    }
    if ((props.alarms ?? "create") === "create") {
      this.createLambdaAlarms(props.timeout ?? cdk.Duration.seconds(90));
    }

    new cdk.CfnOutput(this, "ConnectorFunctionArn", {
      value: this.connectorFunction.functionArn,
      description:
        "Register this ARN as an Athena LAMBDA data catalog in the querying account",
    });
    // A command, not an AWS::Athena::DataCatalog: the catalog belongs to whichever account runs
    // the queries, which is usually not this one. Underscores because catalog names reject hyphens.
    new cdk.CfnOutput(this, "RegisterCatalogCommand", {
      value:
        `aws athena create-data-catalog --name ${props.connectorId.replace(/-/g, "_")} ` +
        `--type LAMBDA --parameters function=${this.connectorFunction.functionArn}`,
      description: "Run this in the account that will query the connector",
    });
    new cdk.CfnOutput(this, "SpillBucketName", {
      value:
        this.spillBucket?.bucketName ??
        "<none: a response over 6 MB has been observed returning SUCCEEDED with zero rows>",
      description: "spill_bucket the connector writes large responses to",
    });
  }

  /**
   * One of the connector's own EMF metrics, dimensioned to this connector.
   *
   * <p>Use this rather than building a {@link cloudwatch.Metric} by hand: the namespace and the
   * dimension name have to match what the jar emits, and a mismatch is invisible — the alarm sits in
   * `INSUFFICIENT_DATA` forever rather than failing.
   *
   * @param catalog restricts the metric to one Athena catalog. Omit for the fleet-wide view, which is
   *                the only one available for metrics emitted below the request — a connect failure,
   *                for instance, has no catalog on it.
   */
  public connectorMetric(
    metricName: string,
    props: cloudwatch.MetricOptions & { readonly catalog?: string } = {},
  ): cloudwatch.Metric {
    const { catalog, ...metricOptions } = props;
    return new cloudwatch.Metric({
      namespace: CONNECTOR_METRIC_NAMESPACE,
      metricName,
      dimensionsMap: {
        [CONNECTOR_DIMENSION]: this.connectorId,
        ...(catalog !== undefined ? { Catalog: catalog } : {}),
      },
      ...metricOptions,
    });
  }

  /**
   * Adds an alarm, wired to {@link alarmTopic} if there is one and recorded in {@link alarms}.
   *
   * <p>Exists so a connector's own stack cannot add an alarm that notifies nobody while the three
   * created here do — the topic is held by this construct, and forgetting to attach it produces an
   * alarm that looks configured and pages no one.
   */
  public addAlarm(id: string, props: cloudwatch.AlarmProps): cloudwatch.Alarm {
    const alarm = new cloudwatch.Alarm(this, id, props);
    if (this.alarmTopic !== undefined) {
      alarm.addAlarmAction(new cloudwatchActions.SnsAction(this.alarmTopic));
    }
    this.alarms.push(alarm);
    return alarm;
  }

  /**
   * The three alarms that apply to any connector, whatever it talks to.
   *
   * <p>Thresholds are deliberately not tunable. Each is anchored to something structural — zero, one
   * percent, or a fraction of this function's own timeout — rather than to a workload, so there is
   * nothing for a caller to know better.
   */
  private createLambdaAlarms(timeout: cdk.Duration): void {
    const period = cdk.Duration.minutes(5);

    // Any throttle at all. One connector serves every source pointed at it, so this is not one
    // source degrading — it is all of them, simultaneously, and COA sees only failed queries.
    this.addAlarm("ThrottlesAlarm", {
      alarmName: `${this.functionName}-throttles`,
      alarmDescription:
        "The connector is being throttled. Every COA source pointed at it is affected at once; " +
        "raise reserved concurrency.",
      metric: this.connectorFunction.metricThrottles({ period, statistic: "Sum" }),
      threshold: 0,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
      evaluationPeriods: 1,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    });

    // A rate, not a count: a connector serving a busy schema and one serving a quiet one cannot share
    // an absolute threshold, and the quiet one is where a count set for the busy one never fires.
    this.addAlarm("ErrorRateAlarm", {
      alarmName: `${this.functionName}-error-rate`,
      alarmDescription:
        "Over 1% of connector invocations failed. Check the connector's log group for the " +
        "classified error prefix before looking at COA.",
      metric: new cloudwatch.MathExpression({
        expression: "100 * errors / invocations",
        usingMetrics: {
          errors: this.connectorFunction.metricErrors({ period, statistic: "Sum" }),
          invocations: this.connectorFunction.metricInvocations({ period, statistic: "Sum" }),
        },
        label: "Error rate (%)",
        period,
      }),
      threshold: 1,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
      evaluationPeriods: 1,
      // An idle connector divides by zero, which yields no data rather than an error.
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    });

    // Against this function's own timeout rather than a fixed number of seconds, because the timeout
    // is a per-connector decision and a timed-out invocation is the worst diagnosis available: it
    // names no table, suggests no action, and Athena retries it.
    const warningMillis = Math.round(timeout.toMilliseconds() * 0.8);
    this.addAlarm("DurationAlarm", {
      alarmName: `${this.functionName}-duration-p99`,
      alarmDescription:
        `p99 invocation duration is within 20% of the ${timeout.toSeconds()}s timeout. Past it, ` +
        "queries fail as timeouts that name nothing and are retried at full cost.",
      metric: this.connectorFunction.metricDuration({ period, statistic: "p99" }),
      threshold: warningMillis,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
      evaluationPeriods: 1,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    });
  }

  /**
   * Lets COA's querying principals reach this connector. Three resource policies per principal,
   * because a cross-account principal needs an allow from both sides and this stack owns one:
   *
   * <ul>
   *   <li>the Lambda's, so Athena can invoke as that principal. Both roles need it — granting only
   *       discovery lets registration and induction pass, then fails the first query;</li>
   *   <li>the bucket's, `s3:GetObject` under the spill prefix, since spill objects are read with the
   *       <i>querying</i> role's credentials;</li>
   *   <li>the key's, `kms:Decrypt`, since the bucket is SSE-KMS.</li>
   * </ul>
   *
   * <p>The key condition is `kms:ViaService`, not `aws:CalledVia`: under bucket-level SSE-KMS the
   * immediate KMS caller is S3, not Athena, so `aws:CalledVia` would depend on undocumented
   * behaviour and fail closed on every spilled query.
   *
   * <p>Written explicitly rather than via `bucket.grantRead()`, which on a KMS bucket also calls
   * `grantDecrypt()` and would add an <b>unconditioned</b> `kms:Decrypt`, defeating that condition.
   */
  private grantQueryAccess(
    queryRoleArns: readonly string[],
    spillPrefix: string,
  ): void {
    const viaS3 = `s3.${cdk.Stack.of(this).region}.amazonaws.com`;

    // Deduplicated here, not only in env.ts: one role may do both COA jobs, and grantInvoke derives
    // its construct id from the principal, so the same ARN twice fails synth with a duplicate-id
    // error rather than anything that names the cause.
    // Sorted as well as deduplicated. A Set iterates in insertion order, so this is not about
    // determinism within a run — it is so the index in the Sids below does not depend on the order
    // an operator happened to list the ARNs in, which would otherwise rewrite the bucket and key
    // policies on a no-op deploy.
    const unique = [...new Set(queryRoleArns)].sort();

    unique.forEach((roleArn, index) => {
      const principal = new iam.ArnPrincipal(roleArn);
      this.connectorFunction.grantInvoke(principal);

      const spillBucket = this.spillBucket;
      const spillKey = this.spillKey;
      if (spillBucket === undefined || spillKey === undefined) {
        // Nothing to read: with no bucket, a response that would have spilled never becomes an
        // object anyone could fetch.
        return;
      }

      spillBucket.addToResourcePolicy(
        new iam.PolicyStatement({
          sid: `CoaSpillRead${index}`,
          principals: [principal],
          actions: ["s3:GetObject"],
          resources: [spillBucket.arnForObjects(`${spillPrefix}/*`)],
        }),
      );

      spillKey.addToResourcePolicy(
        new iam.PolicyStatement({
          sid: `CoaSpillDecrypt${index}`,
          principals: [principal],
          actions: ["kms:Decrypt"],
          // A key policy's resource is the key it is attached to; "*" means exactly that.
          resources: ["*"],
          conditions: { StringEquals: { "kms:ViaService": viaS3 } },
        }),
      );
    });
  }
}
