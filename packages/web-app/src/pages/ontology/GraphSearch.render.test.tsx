// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import React from "react";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { describe, it, expect, vi, beforeEach } from "vitest";
import { GraphSearchPage } from "./GraphSearch";
import { BreadcrumbProvider } from "@components/BreadcrumbProvider";
import {
  CLASS_FETCH_CHUNK_SIZE,
  CLASS_DISPLAY_PAGE_SIZE,
} from "@api-hooks/use-graph-classes";
import type {
  GraphSearchHit,
  GraphSearchResult,
  GraphVertex,
  OntologyOverviewResult,
  OntologyRecord,
} from "../../services/ontology-engine";

// Two classes in one ontology: Policy is a sub-class of Claim. The taxonomy
// comes from the ontology-overview (subClassOf), NOT a per-class getClass
// fan-out.
const CLAIM = "http://ex.org/o#Claim";
const POLICY = "http://ex.org/o#Policy";
const CLASSES: GraphSearchHit[] = [
  { uri: CLAIM, label: "Claim", kind: "class", graph_uris: ["wb/ns/o"] },
  { uri: POLICY, label: "Policy", kind: "class", graph_uris: ["wb/ns/o"] },
];
const CLASS_PAGE: GraphSearchResult = { hits: CLASSES, total_count: 2 };
// The List view + graph seed are now keyed off the ontology registry (complete
// set of ontology ids), not a capped wildcard class fetch.
const ONTOLOGY_RECORDS: OntologyRecord[] = [
  {
    ontologyId: "o",
    title: "Insurance",
    ontologyType: "induced",
  } as OntologyRecord,
];
const OVERVIEW: OntologyOverviewResult = {
  ontology_id: "o",
  classes: [
    { uri: CLAIM, label: "Claim", comment: "A claim.", subClassOf: [] },
    { uri: POLICY, label: "Policy", comment: null, subClassOf: [CLAIM] },
  ],
};
// The bounded seed the backend returns for sample=true: the root + its
// (capped) descendants, no properties. Here Claim (root) + Policy (child).
const SAMPLE_OVERVIEW: OntologyOverviewResult = {
  ontology_id: "o",
  classes: [
    { uri: CLAIM, label: "Claim", comment: "A claim.", subClassOf: [] },
    { uri: POLICY, label: "Policy", comment: null, subClassOf: [CLAIM] },
  ],
};
const CLAIM_FULL: GraphVertex = {
  uri: CLAIM,
  kind: "class",
  labels: ["Claim"],
  comments: ["A claim."],
  alt_labels: [],
  graph_uris: ["wb/ns/o"],
  edges: [],
};

const searchEntities = vi.fn();
const searchEntitiesPaged = vi.fn();
const getClass = vi.fn();
const getOntologyOverview = vi.fn();
const listOntologies = vi.fn();

vi.mock("../../services/ontology-engine", () => ({
  searchEntities: (...args: unknown[]) => searchEntities(...args),
  searchEntitiesPaged: (...args: unknown[]) => searchEntitiesPaged(...args),
  getClass: (...args: unknown[]) => getClass(...args),
  getObjectProperty: vi.fn(),
  getDatatypeProperty: vi.fn(),
  getOntologyOverview: (...args: unknown[]) => getOntologyOverview(...args),
  listOntologies: (...args: unknown[]) => listOntologies(...args),
}));

vi.mock("@components/ontology/PendingReviewBanner", () => ({
  PendingReviewBanner: () => null,
}));

// Stub the React Flow canvas but surface the nodes it receives so the test can
// assert what the graph renders.
vi.mock("../../components/graph/OntologyGraphFlow", () => ({
  OntologyGraphFlow: (props: {
    nodes: Array<{ id: string; label: string }>;
    onNodeSelect?: (n: { id: string; label: string }) => void;
  }) => (
    <div data-testid="graph-flow">
      {props.nodes.map((n) => (
        <button
          key={n.id}
          data-testid="graph-node"
          onClick={() => props.onNodeSelect?.(n)}
        >
          {n.label}
        </button>
      ))}
    </div>
  ),
}));

// A STABLE client instance — the real useApiClient is useMemo-memoized, so its
// reference is stable across renders. A fresh object per render would re-trigger
// every apiClient-dependent effect and mask the behavior under test.
const stableApiClient = { get: vi.fn(), getText: vi.fn() };
vi.mock("@components/ApiClientProvider", () => ({
  useApiClient: () => stableApiClient,
}));

function renderPage() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={["/namespaces/ns/ontology/graph"]}>
        <BreadcrumbProvider>
          <Routes>
            <Route
              path="/namespaces/:namespaceId/ontology/graph"
              element={<GraphSearchPage />}
            />
          </Routes>
        </BreadcrumbProvider>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe("GraphSearchPage — initial Graph view builds from ontology-overview", () => {
  beforeEach(() => {
    searchEntities.mockReset();
    searchEntitiesPaged.mockReset();
    getClass.mockReset();
    getOntologyOverview.mockReset();
    listOntologies.mockReset();
    searchEntities.mockResolvedValue(CLASSES);
    // The List view pages classes via searchEntitiesPaged.
    searchEntitiesPaged.mockResolvedValue(CLASS_PAGE);
    // Mount (List descriptions) fetches the FULL overview; the Graph view
    // fetches a BOUNDED sample. Return per-mode fixtures so the test can prove
    // the Graph view uses sample mode.
    getOntologyOverview.mockImplementation(
      (
        _client: unknown,
        _ns: string,
        _id: string,
        opts?: { sample?: boolean },
      ) => Promise.resolve(opts?.sample ? SAMPLE_OVERVIEW : OVERVIEW),
    );
    getClass.mockResolvedValue(CLAIM_FULL);
    // The registry drives both description enrichment and the graph seed.
    listOntologies.mockResolvedValue(ONTOLOGY_RECORDS);
  });

  it("seeds the graph from a bounded sample, without a per-class getClass fan-out", async () => {
    renderPage();

    // Mount loads the registry, then a FULL overview per ontology for the
    // List view's descriptions.
    await waitFor(() => expect(listOntologies).toHaveBeenCalled());
    await waitFor(() => expect(getOntologyOverview).toHaveBeenCalled());

    // Toggle to Graph — it fetches a BOUNDED sample (sample=true), not the full
    // class set, and builds the taxonomy from just that.
    await userEvent.click(screen.getByText("Graph"));
    await waitFor(() =>
      expect(getOntologyOverview).toHaveBeenCalledWith(
        expect.anything(),
        "ns",
        expect.any(String),
        { sample: true },
      ),
    );
    await waitFor(() =>
      expect(screen.getByTestId("graph-flow")).toBeInTheDocument(),
    );
    expect(screen.getByText("Claim")).toBeInTheDocument();

    // The key win: opening the Graph view did NOT fan out getClass over every
    // class (that was the ~100-request / deadlocking path). No getClass yet.
    expect(getClass).not.toHaveBeenCalled();
  });

  it("lazily fetches the full vertex when a graph node is selected", async () => {
    renderPage();
    await waitFor(() => expect(getOntologyOverview).toHaveBeenCalled());
    await userEvent.click(screen.getByText("Graph"));
    await waitFor(() =>
      expect(screen.getByTestId("graph-flow")).toBeInTheDocument(),
    );

    // Selecting a node upgrades its partial (taxonomy-only) vertex to the full
    // one via getClass, so the detail panel gets relationships/attributes.
    await userEvent.click(screen.getByText("Claim"));
    await waitFor(() =>
      expect(getClass).toHaveBeenCalledWith(expect.anything(), "ns", CLAIM),
    );
  });
});

describe("GraphSearchPage — List view ontology filter", () => {
  beforeEach(() => {
    searchEntities.mockReset();
    searchEntitiesPaged.mockReset();
    getClass.mockReset();
    getOntologyOverview.mockReset();
    listOntologies.mockReset();
    searchEntitiesPaged.mockResolvedValue(CLASS_PAGE);
    getOntologyOverview.mockResolvedValue(OVERVIEW);
    getClass.mockResolvedValue(CLAIM_FULL);
    listOntologies.mockResolvedValue(ONTOLOGY_RECORDS);
  });

  it("defaults the class query to the induced ontology, and clears the filter when 'All ontologies' is chosen", async () => {
    renderPage();

    // The List view is the default; once the registry loads, the paged class
    // query is scoped to the induced ontology ("o").
    await waitFor(() =>
      expect(searchEntitiesPaged).toHaveBeenCalledWith(
        expect.anything(),
        "ns",
        "*",
        expect.objectContaining({ kind: "class", ontology_id: "o" }),
      ),
    );

    // Target the filter trigger by its aria label (unique in the DOM) rather
    // than by visible text — "Insurance" also appears in the results table.
    const filterTrigger = screen.getByRole("button", {
      name: /Restrict classes by ontology/i,
    });
    // The selector shows the induced ontology's title as the default.
    expect(filterTrigger).toHaveTextContent("Insurance");

    // Switch to "All ontologies" → the query re-runs without an ontologyId.
    searchEntitiesPaged.mockClear();
    await userEvent.click(filterTrigger);
    await userEvent.click(screen.getByText("All ontologies"));

    await waitFor(() =>
      expect(searchEntitiesPaged).toHaveBeenCalledWith(
        expect.anything(),
        "ns",
        "*",
        expect.objectContaining({ kind: "class", ontology_id: undefined }),
      ),
    );
  });
});

describe("GraphSearchPage — List view paging across chunks", () => {
  // Chunk 1 is a FULL fetch chunk so its rows back several display pages; the
  // server reports more remain (total_count > chunk size). Ontology "o" is the
  // induced default, so the List view queries it without extra setup.
  const CHUNK1_ROWS: GraphSearchHit[] = Array.from(
    { length: CLASS_FETCH_CHUNK_SIZE },
    (_, i) => ({
      uri: `http://ex.org/o#C${i}`,
      label: `C${String(i).padStart(3, "0")}`,
      kind: "class",
      graph_uris: ["wb/ns/o"],
    }),
  );
  // Total spans exactly one extra display page (chunk1 + 20 → 6 pages of 20).
  const TOTAL = CLASS_FETCH_CHUNK_SIZE + CLASS_DISPLAY_PAGE_SIZE;
  const LAST_PAGE = TOTAL / CLASS_DISPLAY_PAGE_SIZE; // 6
  const CHUNK2_ROWS: GraphSearchHit[] = Array.from(
    { length: CLASS_DISPLAY_PAGE_SIZE },
    (_, i) => ({
      uri: `http://ex.org/o#C${CLASS_FETCH_CHUNK_SIZE + i}`,
      label: `C${String(CLASS_FETCH_CHUNK_SIZE + i).padStart(3, "0")}`,
      kind: "class",
      graph_uris: ["wb/ns/o"],
    }),
  );
  const CHUNK1: GraphSearchResult = { hits: CHUNK1_ROWS, total_count: TOTAL };
  const CHUNK2: GraphSearchResult = { hits: CHUNK2_ROWS, total_count: TOTAL };
  const PAGED_RECORDS: OntologyRecord[] = [
    { ontologyId: "o", title: "Insurance", ontologyType: "induced" },
  ] as OntologyRecord[];
  const PAGED_OVERVIEW: OntologyOverviewResult = {
    ontology_id: "o",
    classes: [],
  };

  /** Narrow the paged-search opts to its numeric offset without a type assertion. */
  function offsetOf(opts: unknown): number {
    if (typeof opts === "object" && opts !== null && "offset" in opts) {
      const { offset }: { offset?: unknown } = opts;
      return typeof offset === "number" ? offset : 0;
    }
    return 0;
  }

  let releaseChunk2: (r: GraphSearchResult) => void = () => {};

  beforeEach(() => {
    searchEntities.mockReset();
    searchEntitiesPaged.mockReset();
    getClass.mockReset();
    getOntologyOverview.mockReset();
    listOntologies.mockReset();
    getOntologyOverview.mockResolvedValue(PAGED_OVERVIEW);
    listOntologies.mockResolvedValue(PAGED_RECORDS);
    // Chunk 1 resolves immediately; chunk 2 (offset 100) is deferred so the test
    // controls when it lands — this is the ~10s stall seen in production.
    const chunk2 = new Promise<GraphSearchResult>((res) => {
      releaseChunk2 = res;
    });
    searchEntitiesPaged.mockImplementation(
      (_c: unknown, _n: unknown, _q: unknown, opts: unknown) =>
        offsetOf(opts) === 0 ? Promise.resolve(CHUNK1) : chunk2,
    );
  });

  it("shows a loading state, not a false empty, on a page whose chunk is still fetching", async () => {
    const { container } = renderPage();
    const view = within(container);
    // Chunk 1 fully rendered: its last row is on the last chunk-1-backed page.
    await view.findByText("C000");
    // The known total gives 6 display pages even before chunk 2 loads.
    const lastPageButton = await view.findByRole("button", {
      name: String(LAST_PAGE),
    });

    // Jump to the last display page — its rows (chunk 2) have not arrived.
    await userEvent.click(lastPageButton);

    // The regression: the table must read as loading, and must NOT claim the
    // namespace is empty.
    expect(await view.findByText("Loading classes…")).toBeInTheDocument();
    expect(
      view.queryByText("No classes found in this namespace yet."),
    ).toBeNull();

    // When the deferred chunk lands, the real rows appear.
    releaseChunk2(CHUNK2);
    await view.findByText(`C${String(TOTAL - 1).padStart(3, "0")}`);
  });

  it("prefetches the next chunk one page early without blanking visible rows", async () => {
    const { container } = renderPage();
    const view = within(container);
    await view.findByText("C000");
    const page5 = await view.findByRole("button", { name: "5" });
    searchEntitiesPaged.mockClear();

    // Page 5 is the last page backed by chunk 1 (rows 80..99). Landing here must
    // kick off the offset-100 fetch while page-5 rows stay on screen.
    await userEvent.click(page5);

    // Explicit timeout: this assertion polls for a state transition driven by
    // React effect scheduling (click -> setState -> effect -> fetchNextPage),
    // which the default 1000ms waitFor budget sometimes misses under full-suite
    // parallel load (many vitest workers contending for CPU) even though the
    // prefetch always fires — confirmed by running this test in isolation vs.
    // under the full suite, and flaking only under the latter. Longer timeout,
    // not a mock/logic change: the fix is tolerance for CI scheduling jitter,
    // not the assertion's correctness.
    await waitFor(
      () =>
        expect(
          searchEntitiesPaged.mock.calls.some(
            (c) => offsetOf(c[3]) === CLASS_FETCH_CHUNK_SIZE,
          ),
        ).toBe(true),
      { timeout: 5000 },
    );
    // Rows are NOT replaced by a spinner: a page-5 row is visible, no loading text.
    expect(view.getByText("C099")).toBeInTheDocument();
    expect(view.queryByText("Loading classes…")).toBeNull();
  });

  it("keeps the empty state for a genuinely empty namespace", async () => {
    searchEntitiesPaged.mockReset();
    searchEntitiesPaged.mockResolvedValue({ hits: [], total_count: 0 });
    const { container } = renderPage();
    const view = within(container);
    // The empty text appears and STAYS (loading resolves to empty, not the
    // false-empty spinner). waitFor rather than a single-instant assertion so a
    // slow initial-load frame under parallel test load isn't read as a failure.
    //
    // The explicit timeout matters: findByText defaults to 1000ms, which the
    // shared CI runner exceeds on the first paint (the suite takes ~2m there vs
    // ~12s locally), so this failed in CI while passing 11/11 locally. Matches
    // the 5000ms the paging test above already uses.
    await view.findByText(
      "No classes found in this namespace yet.",
      undefined,
      {
        timeout: 5000,
      },
    );
    await waitFor(
      () => expect(view.queryByText("Loading classes…")).toBeNull(),
      { timeout: 5000 },
    );
    expect(
      view.getByText("No classes found in this namespace yet."),
    ).toBeInTheDocument();
  });
});
