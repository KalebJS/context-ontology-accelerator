// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import {
  bedrockModelArn,
  isInferenceProfileId,
} from "../../lib/utils/bedrock-utils";

describe("bedrock-utils", () => {
  describe("isInferenceProfileId", () => {
    it.each([
      "us.anthropic.claude-sonnet-5",
      "eu.anthropic.claude-sonnet-4-6",
      "jp.anthropic.claude-haiku-4-5-20251001-v1:0",
      // apac. is a real geo (observed in ap-northeast-1) — an allow-list that
      // forgot it would misclassify these as bare foundation models.
      "apac.anthropic.claude-sonnet-4-20250514-v1:0",
      "global.cohere.embed-v4:0",
    ])("treats %s as an inference profile", (id) => {
      expect(isInferenceProfileId(id)).toBe(true);
    });

    it.each([
      "cohere.embed-v4:0",
      "anthropic.claude-sonnet-5",
      "amazon.titan-embed-text-v2:0",
    ])("treats %s as a bare foundation model", (id) => {
      expect(isInferenceProfileId(id)).toBe(false);
    });
  });

  describe("bedrockModelArn", () => {
    it("builds an account-scoped ARN for an inference profile", () => {
      expect(
        bedrockModelArn("jp.anthropic.claude-sonnet-4-6", "ap-northeast-1", "123456789012"),
      ).toBe(
        "arn:aws:bedrock:ap-northeast-1:123456789012:inference-profile/jp.anthropic.claude-sonnet-4-6",
      );
    });

    it("builds an ARN with an EMPTY account field for a foundation model", () => {
      // Foundation models are AWS-owned; an account in this position makes the
      // ARN name a resource that does not exist.
      expect(
        bedrockModelArn("cohere.embed-v4:0", "ap-northeast-1", "123456789012"),
      ).toBe("arn:aws:bedrock:ap-northeast-1::foundation-model/cohere.embed-v4:0");
    });

    it("supports wildcards for IAM resource patterns", () => {
      expect(bedrockModelArn("cohere.embed-v4:0", "*", "*")).toBe(
        "arn:aws:bedrock:*::foundation-model/cohere.embed-v4:0",
      );
      expect(bedrockModelArn("us.anthropic.claude-sonnet-5", "*", "123456789012")).toBe(
        "arn:aws:bedrock:*:123456789012:inference-profile/us.anthropic.claude-sonnet-5",
      );
    });
  });
});
