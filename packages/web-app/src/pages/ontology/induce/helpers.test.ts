// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from "vitest";
import {
  proposalScope,
  proposalScopeSortKey,
  proposalSourceTypeLabel,
} from "./helpers";

describe("proposalSourceTypeLabel", () => {
  it("maps the source-type enum to a human label", () => {
    expect(proposalSourceTypeLabel("STRUCTURED")).toBe("Structured");
    expect(proposalSourceTypeLabel("UNSTRUCTURED")).toBe("Unstructured");
  });

  it("treats a missing source_type as structured, matching the backend's asymmetric backfill", () => {
    expect(proposalSourceTypeLabel(undefined)).toBe("Structured");
  });
});

describe("proposalScope", () => {
  it("reports table count for structured runs", () => {
    expect(proposalScope({ tables_processed: 46 }, "STRUCTURED")).toBe(
      "46 tables",
    );
  });

  it("reports class count for unstructured runs (and ignores tables_processed there)", () => {
    // Unstructured proposals carry class_count but not tables_processed.
    // Even if some pathological row had both, the source_type decides
    // which metric is meaningful — mixing them would be misleading.
    expect(
      proposalScope({ class_count: 18, tables_processed: 999 }, "UNSTRUCTURED"),
    ).toBe("18 classes");
  });

  it("pluralises correctly at 1", () => {
    expect(proposalScope({ tables_processed: 1 }, "STRUCTURED")).toBe(
      "1 table",
    );
    expect(proposalScope({ class_count: 1 }, "UNSTRUCTURED")).toBe("1 class");
  });

  it("preserves a legitimate zero", () => {
    expect(proposalScope({ tables_processed: 0 }, "STRUCTURED")).toBe(
      "0 tables",
    );
  });

  it("treats a missing source_type as structured, matching the backend's asymmetric backfill", () => {
    expect(proposalScope({ tables_processed: 12 }, undefined)).toBe(
      "12 tables",
    );
  });

  it("returns null when the relevant metric is absent, non-numeric, or metadata missing", () => {
    expect(proposalScope({}, "STRUCTURED")).toBeNull();
    expect(proposalScope({ tables_processed: "12" }, "STRUCTURED")).toBeNull();
    expect(
      proposalScope({ class_count: Number.NaN }, "UNSTRUCTURED"),
    ).toBeNull();
    expect(proposalScope(undefined, "STRUCTURED")).toBeNull();
  });

  it("rejects negative counts as corrupted data rather than rendering '-5 tables'", () => {
    expect(proposalScope({ tables_processed: -5 }, "STRUCTURED")).toBeNull();
    expect(proposalScope({ class_count: -1 }, "UNSTRUCTURED")).toBeNull();
  });
});

describe("proposalScopeSortKey", () => {
  it("returns the numeric metric for the source type", () => {
    expect(proposalScopeSortKey({ tables_processed: 46 }, "STRUCTURED")).toBe(
      46,
    );
    expect(proposalScopeSortKey({ class_count: 18 }, "UNSTRUCTURED")).toBe(18);
  });

  it("sinks rows with no metric to the bottom on ascending sort", () => {
    expect(proposalScopeSortKey({}, "STRUCTURED")).toBe(-1);
    expect(proposalScopeSortKey(undefined, "UNSTRUCTURED")).toBe(-1);
  });

  it("treats a negative count as no metric (-1), consistent with proposalScope", () => {
    expect(proposalScopeSortKey({ tables_processed: -5 }, "STRUCTURED")).toBe(
      -1,
    );
  });
});
