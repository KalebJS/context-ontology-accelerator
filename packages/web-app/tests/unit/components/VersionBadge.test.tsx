// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from "vitest";
import { render, screen } from "@testing-library/react";
import React from "react";
import { VersionBadge } from "../../../src/components/VersionBadge";
import {
  RuntimeConfigContext,
  RuntimeContext,
} from "../../../src/components/RuntimeContext";

const baseContext: RuntimeContext = {
  region: "us-east-1",
  authority: "https://cognito.example.com",
  clientId: "test-client",
  oidcConfig: {
    authority: "https://cognito.example.com",
    clientId: "test-client",
  },
};

describe("VersionBadge", () => {
  it("renders the deployed version prefixed with 'v'", () => {
    render(
      <RuntimeConfigContext.Provider
        value={{ ...baseContext, version: "0.2.2" }}
      >
        <VersionBadge />
      </RuntimeConfigContext.Provider>,
    );
    expect(screen.getByText("v0.2.2")).toBeInTheDocument();
  });

  it("renders nothing when no version is present in the runtime config", () => {
    const { container } = render(
      <RuntimeConfigContext.Provider value={baseContext}>
        <VersionBadge />
      </RuntimeConfigContext.Provider>,
    );
    expect(container).toBeEmptyDOMElement();
  });

  it("renders nothing when there is no runtime context", () => {
    const { container } = render(<VersionBadge />);
    expect(container).toBeEmptyDOMElement();
  });

  it("renders nothing for an empty-string version", () => {
    const { container } = render(
      <RuntimeConfigContext.Provider value={{ ...baseContext, version: "" }}>
        <VersionBadge />
      </RuntimeConfigContext.Provider>,
    );
    expect(container).toBeEmptyDOMElement();
  });

  it("renders nothing for a whitespace-only version", () => {
    const { container } = render(
      <RuntimeConfigContext.Provider value={{ ...baseContext, version: "   " }}>
        <VersionBadge />
      </RuntimeConfigContext.Provider>,
    );
    expect(container).toBeEmptyDOMElement();
  });

  it("renders a pre-release / build-metadata semver verbatim", () => {
    render(
      <RuntimeConfigContext.Provider
        value={{ ...baseContext, version: "0.2.2-beta.1+build.123" }}
      >
        <VersionBadge />
      </RuntimeConfigContext.Provider>,
    );
    expect(screen.getByText("v0.2.2-beta.1+build.123")).toBeInTheDocument();
  });
});
