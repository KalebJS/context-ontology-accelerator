// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * Bedrock ARN helpers shared by every stack that names a model.
 *
 * Model IDs reach the stacks from deploy config and may be EITHER a geographic
 * inference profile (`us.`/`eu.`/`apac.`/`jp.`/`global.`) or a bare in-region
 * model id — some models publish geo profiles for only a subset of regions, so
 * a bare id is the only option in those regions. The two forms have different
 * ARN shapes, and getting it wrong is silent until runtime: an environment
 * variable naming a nonexistent resource fails on first invoke, and an IAM
 * grant built on the wrong shape fails closed with AccessDenied.
 */

/**
 * True when a model id is a geographic inference profile rather than a bare
 * foundation model.
 *
 * A profile carries a leading geo segment (`jp.anthropic.claude-sonnet-4-6`) so
 * it has three or more dot-separated segments; a bare id has two
 * (`cohere.embed-v4:0`). Counting segments rather than allow-listing geo
 * prefixes means a newly launched geo needs no code change — the same
 * hardcoded-region assumption this family of helpers exists to remove.
 */
export function isInferenceProfileId(modelId: string): boolean {
  return modelId.split(".").length >= 3;
}

/**
 * Build the ARN for a Bedrock model, choosing the resource type by id form.
 *
 * A foundation model is AWS-owned and its ARN carries an EMPTY account field;
 * an inference profile is account-scoped. Pass `"*"` for `region` and/or
 * `account` when building an IAM resource pattern rather than a concrete ARN.
 */
export function bedrockModelArn(
  modelId: string,
  region: string,
  account: string,
): string {
  return isInferenceProfileId(modelId)
    ? `arn:aws:bedrock:${region}:${account}:inference-profile/${modelId}`
    : `arn:aws:bedrock:${region}::foundation-model/${modelId}`;
}
