// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import React, { useEffect, useMemo, useRef, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { useQueryClient } from "@tanstack/react-query";
import Alert from "@cloudscape-design/components/alert";
import Badge from "@cloudscape-design/components/badge";
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import ColumnLayout from "@cloudscape-design/components/column-layout";
import Container from "@cloudscape-design/components/container";
import Flashbar from "@cloudscape-design/components/flashbar";
import Header from "@cloudscape-design/components/header";
import KeyValuePairs from "@cloudscape-design/components/key-value-pairs";
import Link from "@cloudscape-design/components/link";
import Modal from "@cloudscape-design/components/modal";
import Pagination from "@cloudscape-design/components/pagination";
import SpaceBetween from "@cloudscape-design/components/space-between";
import Spinner from "@cloudscape-design/components/spinner";
import StatusIndicator from "@cloudscape-design/components/status-indicator";
import Table from "@cloudscape-design/components/table";
import Tabs from "@cloudscape-design/components/tabs";
import Select from "@cloudscape-design/components/select";
import TextFilter from "@cloudscape-design/components/text-filter";
import type { StatusIndicatorProps } from "@cloudscape-design/components/status-indicator";
import { useCollection } from "@cloudscape-design/collection-hooks";
import {
  useGetSource,
  useDeleteSource,
  useRescanSource,
  useListSourceTables,
  useApproveSource,
  useRejectSource,
  useGetSourceScanJob,
} from "@api-hooks";
import {
  ReviewDecision,
  ReviewSourceTableCommand,
  ReviewStatus,
  SourceStatus,
} from "@coa/control-plane-client";
import type { TableSummary } from "@coa/control-plane-client";
import { useControlPlaneClient } from "@components/ControlPlaneClientProvider";
import { ButtonWithHint } from "@components/ButtonWithHint";
import { sourceStatusType, sourceStatusLabel } from "@utils/source-status";
import { formatTimestamp } from "@utils/helpers";

const TABLES_PAGE_SIZE = 25;
const METADATA_FRESHNESS_THRESHOLD_DAYS = 30;

// ── Status helpers ────────────────────────────────────────────────────────────

const ACTIVE_STATUSES = new Set([
  "REGISTERED",
  "SCANNING",
  "ENRICHING",
  "DELETING",
  "APPROVING",
  "REJECTING",
]);

// Human labels for the DATABASE sub-types. A lookup rather than a ternary so a
// sub-type without an entry shows its own name instead of being mislabelled as
// one of the others.
const DATABASE_SUBTYPE_LABELS: Record<string, string> = {
  GLUE_DATABASE: "Glue database",
  JDBC_DATABASE: "JDBC database",
  ATHENA_CONNECTOR: "Athena connector",
};

// ── Degraded-scan annotation ──────────────────────────────────────────────────
//
// Discovery for the ATHENA_CONNECTOR sub-type reads one table at a time (a
// DESCRIBE per table), so a single unreadable table is dropped while the scan
// as a whole still SUCCEEDS. The scan job then reports `tablesFailed` — the
// count of listed-but-unreadable tables, absent rather than zero when the scan
// was clean, so its presence alone marks the scan degraded — and
// `failedTables`, the affected tables as `database.table`.
//
// `failedTables` is a capped diagnostic sample and may be shorter than
// `tablesFailed`, so the count is always taken from `tablesFailed` and never
// derived from the list's length.
//
// Both members ARE modelled on `GetSourceScanJobOutput`, but they are read
// defensively off the response rather than by typed field access, mirroring
// `normalizeResponse` in `@api-hooks/normalize-response` — the package's idiom
// for values the API assembles as a raw dict rather than through a model.
//
// The `typeof === "number"` check is load-bearing, not decoration. DynamoDB
// numbers arrive as `Decimal` through boto3's resource interface and serialize to
// JSON as STRINGS unless the handler coerces them, so a regression on the API
// side must read here as "not degraded" rather than as a truthy count.

interface DegradedScan {
  tablesFailed: number;
  failedTables: string[];
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function readDegradedScan(scanJob: unknown): DegradedScan | undefined {
  if (!isRecord(scanJob)) return undefined;
  const count = scanJob.tablesFailed;
  if (typeof count !== "number" || count <= 0) return undefined;
  const listed = scanJob.failedTables;
  return {
    tablesFailed: count,
    failedTables: Array.isArray(listed)
      ? listed.filter((entry): entry is string => typeof entry === "string")
      : [],
  };
}

const ValueWithLabel: React.FC<{
  label: string;
  children: React.ReactNode;
}> = ({ label, children }) => (
  <div>
    <Box variant="awsui-key-label">{label}</Box>
    <div>{children}</div>
  </div>
);

// ── Component ─────────────────────────────────────────────────────────────────

export const SourceDetail: React.FC = () => {
  const { namespaceId, sourceId } = useParams<{
    namespaceId: string;
    sourceId: string;
  }>();
  const navigate = useNavigate();
  const [showDeleteModal, setShowDeleteModal] = useState(false);
  const [flash, setFlash] = useState<
    { id: string; type: "success" | "error"; content: string }[]
  >([]);

  const { data, isLoading, error, refetch } = useGetSource(
    namespaceId ?? "",
    sourceId ?? "",
  );

  const {
    mutate: deleteSource,
    isPending: isDeleting,
    error: deleteError,
  } = useDeleteSource(namespaceId ?? "", sourceId ?? "", {
    onSuccess: () => navigate(`/namespaces/${namespaceId}/sources`),
  });

  const {
    mutate: rescan,
    isPending: isRescanning,
    error: rescanError,
  } = useRescanSource(namespaceId ?? "", sourceId ?? "", {
    onSuccess: () => {
      setFlash([
        {
          id: String(Date.now()),
          type: "success",
          content: "Re-scan triggered.",
        },
      ]);
      refetch();
    },
  });

  const source = data?.body;
  const isDatabase = source?.sourceType === "DATABASE";

  // Tables — only fetched for DATABASE sources via the new sources-api tables endpoint
  const { data: tablesData, isLoading: tablesLoading } = useListSourceTables(
    namespaceId ?? "",
    isDatabase ? (sourceId ?? "") : "",
  );
  const approveMutation = useApproveSource(namespaceId ?? "", sourceId ?? "");
  const rejectMutation = useRejectSource(namespaceId ?? "", sourceId ?? "");
  const controlPlaneClient = useControlPlaneClient();
  const queryClient = useQueryClient();
  const [selectedTables, setSelectedTables] = useState<TableSummary[]>([]);
  const [isBatchReviewing, setIsBatchReviewing] = useState(false);

  // Refetch tables list when source transitions out of APPROVING/REJECTING
  const prevStatusRef = useRef(source?.status);
  useEffect(() => {
    const prev = prevStatusRef.current;
    const curr = source?.status;
    prevStatusRef.current = curr;
    if (
      (prev === SourceStatus.APPROVING || prev === SourceStatus.REJECTING) &&
      curr !== prev
    ) {
      queryClient.invalidateQueries({
        queryKey: ["sourceTables", namespaceId, sourceId],
      });
    }
  }, [source?.status, queryClient, namespaceId, sourceId]);

  // Scan-job record for the most recent scan of a database source. Fetched for
  // every scan, not just failed ones: a scan can succeed and still have dropped
  // individual tables, and that count lives only on the scan job.
  const { data: scanJobData } = useGetSourceScanJob(
    namespaceId ?? "",
    sourceId ?? "",
    isDatabase ? (source?.databaseDetails?.lastScanJobId ?? "") : "",
  );

  // useCollection must be called unconditionally (Rules of Hooks) — before any early returns.
  // Sort: tables with unapproved columns first, then fully approved, alphabetical within each group.
  const tables = React.useMemo(() => {
    const raw = tablesData?.items ?? [];
    return [...raw].sort((a, b) => {
      const aFull =
        (a.columnsApproved ?? 0) >= (a.columnCount ?? 0) &&
        (a.columnCount ?? 0) > 0;
      const bFull =
        (b.columnsApproved ?? 0) >= (b.columnCount ?? 0) &&
        (b.columnCount ?? 0) > 0;
      if (aFull !== bFull) return aFull ? 1 : -1;
      return (a.name ?? "").localeCompare(b.name ?? "");
    });
  }, [tablesData]);
  const [statusFilter, setStatusFilter] = useState<string | null>(null);
  const filteredTables = statusFilter
    ? tables.filter((t) => t.reviewStatus === statusFilter)
    : tables;
  const {
    items: pagedTables,
    collectionProps,
    filterProps,
    paginationProps,
  } = useCollection(filteredTables, {
    filtering: {
      empty: (
        <Box textAlign="center" color="text-status-inactive" padding="l">
          No tables found.
        </Box>
      ),
      noMatch: (
        <Box textAlign="center" color="text-status-inactive" padding="l">
          No tables match the filter.
        </Box>
      ),
    },
    pagination: { pageSize: TABLES_PAGE_SIZE },
    sorting: {
      defaultState: {
        sortingColumn: { sortingField: "reviewStatus" },
        isDescending: false,
      },
    },
  });

  // Warn when approved metadata is older than threshold — schema may have drifted.
  const isMetadataStale = useMemo(() => {
    const s = source?.status;
    const lastScanAt = source?.databaseDetails?.lastScanAt;
    if (s !== SourceStatus.APPROVED || !lastScanAt) return false;
    const lastScan = new Date(
      lastScanAt instanceof Date ? lastScanAt.toISOString() : lastScanAt,
    );
    const ageDays = Math.floor(
      (Date.now() - lastScan.getTime()) / (1000 * 60 * 60 * 24),
    );
    return ageDays > METADATA_FRESHNESS_THRESHOLD_DAYS;
  }, [source?.status, source?.databaseDetails?.lastScanAt]);

  if (isLoading) {
    return (
      <Box textAlign="center" padding="xxl">
        <Spinner size="large" />
      </Box>
    );
  }

  if (error) {
    return (
      <Alert type="error" header="Failed to load source">
        {error.message}
      </Alert>
    );
  }

  if (!source) return null;

  const isDocuments = source.sourceType === "DOCUMENTS";
  const isActive = ACTIVE_STATUSES.has(source.status ?? "");
  const status = source.status ?? "";

  // Type-specific detail objects
  const dbDetails = source.databaseDetails;
  const docDetails = source.documentDetails;

  // Present only when the last scan succeeded but lost individual tables.
  const degradedScan = readDegradedScan(scanJobData);

  const preprocessingIssues = Array.isArray(docDetails?.preprocessingIssues)
    ? docDetails.preprocessingIssues
    : [];
  const extractionConfig = docDetails?.extractionConfig;

  const handleApproveAll = () => {
    approveMutation.mutate(
      {},
      {
        onSuccess: () =>
          setFlash([
            {
              id: String(Date.now()),
              type: "success",
              content:
                "Bulk approve started. Status will update when complete.",
            },
          ]),
        onError: (e) =>
          setFlash([
            { id: String(Date.now()), type: "error", content: e.message },
          ]),
      },
    );
  };

  const handleRejectAll = () => {
    rejectMutation.mutate(
      {},
      {
        onSuccess: () =>
          setFlash([
            {
              id: String(Date.now()),
              type: "success",
              content: "Bulk reject started. Status will update when complete.",
            },
          ]),
        onError: (e) =>
          setFlash([
            { id: String(Date.now()), type: "error", content: e.message },
          ]),
      },
    );
  };

  const handleBatchReviewTables = async (decision: ReviewDecision) => {
    if (selectedTables.length === 0) return;
    setIsBatchReviewing(true);
    const label =
      decision === ReviewDecision.APPROVED ? "approved" : "rejected";
    try {
      const results = await Promise.allSettled(
        selectedTables.map((t) =>
          controlPlaneClient.send(
            new ReviewSourceTableCommand({
              namespaceId: namespaceId!,
              sourceId: sourceId!,
              tableId: t.tableId!,
              decision,
            }),
          ),
        ),
      );
      const succeeded = results.filter((r) => r.status === "fulfilled").length;
      const failed = results.filter((r) => r.status === "rejected").length;
      if (failed === 0) {
        setFlash([
          {
            id: String(Date.now()),
            type: "success",
            content: `${succeeded} table(s) ${label}.`,
          },
        ]);
      } else {
        setFlash([
          {
            id: String(Date.now()),
            type: "error",
            content: `${succeeded} of ${selectedTables.length} table(s) ${label}. ${failed} failed.`,
          },
        ]);
      }
      setSelectedTables([]);
      queryClient.invalidateQueries({
        queryKey: ["sourceTables", namespaceId, sourceId],
      });
      queryClient.invalidateQueries({
        queryKey: ["source", namespaceId, sourceId],
      });
    } finally {
      setIsBatchReviewing(false);
    }
  };

  const isTransient =
    status === SourceStatus.APPROVING || status === SourceStatus.REJECTING;

  return (
    <SpaceBetween size="l">
      {flash.length > 0 && (
        <Flashbar
          items={flash.map((f) => ({
            ...f,
            dismissible: true,
            onDismiss: () =>
              setFlash((prev) => prev.filter((x) => x.id !== f.id)),
          }))}
        />
      )}

      <Header
        variant="h1"
        actions={
          <SpaceBetween direction="horizontal" size="xs">
            {isDatabase && (
              <>
                <ButtonWithHint
                  hint="Marks all pending tables and columns in this source as rejected. Already-approved or already-rejected items are not affected."
                  onClick={handleRejectAll}
                  loading={rejectMutation.isPending}
                  disabled={
                    isTransient ||
                    status === SourceStatus.APPROVED ||
                    tables.length === 0
                  }
                >
                  Reject source
                </ButtonWithHint>
                <ButtonWithHint
                  hint={
                    status === SourceStatus.APPROVAL_FAILED
                      ? "Retries approving all pending tables and columns in this source. Already-rejected items are not affected."
                      : "Approves all pending tables and columns in this source, making them queryable. Already-rejected items are not affected."
                  }
                  variant="primary"
                  onClick={handleApproveAll}
                  loading={approveMutation.isPending}
                  disabled={
                    isTransient ||
                    status === SourceStatus.APPROVED ||
                    tables.length === 0
                  }
                >
                  {status === SourceStatus.APPROVAL_FAILED
                    ? "Retry approve source"
                    : "Approve source"}
                </ButtonWithHint>
              </>
            )}
            <Button
              onClick={() => setShowDeleteModal(true)}
              disabled={isActive}
            >
              Delete
            </Button>
            <Button
              onClick={() => rescan()}
              loading={isRescanning}
              disabled={status !== SourceStatus.SCAN_FAILED}
            >
              Re-scan
            </Button>
            <Button
              onClick={() => navigate(`/namespaces/${namespaceId}/sources`)}
            >
              Back to sources
            </Button>
          </SpaceBetween>
        }
      >
        {source.name}
      </Header>

      {rescanError && (
        <Alert type="error" header="Failed to start re-scan">
          {rescanError.message}
        </Alert>
      )}

      {status === SourceStatus.APPROVAL_FAILED && (
        <Alert type="error" header="Bulk approval failed">
          The background worker encountered an error. You can retry by clicking
          &quot;Retry approve source&quot;.
        </Alert>
      )}

      {status === SourceStatus.REJECTION_FAILED && (
        <Alert type="error" header="Bulk rejection failed">
          The background worker encountered an error. You can retry by clicking
          &quot;Reject source&quot;.
        </Alert>
      )}

      {isTransient && (
        <Alert type="info" header="Processing">
          {status === SourceStatus.APPROVING
            ? "Bulk approval is in progress. This page will update automatically."
            : "Bulk rejection is in progress. This page will update automatically."}
        </Alert>
      )}

      {docDetails?.errorMessage && (
        <Alert type="error" header="Error">
          {docDetails.errorMessage}
        </Alert>
      )}

      {isDatabase &&
        status === SourceStatus.SCAN_FAILED &&
        scanJobData?.errorMessage && (
          <Alert type="error" header="Scan failed">
            {scanJobData.errorMessage}
          </Alert>
        )}

      {isDatabase && degradedScan && (
        <Alert
          type="warning"
          header="Scan completed, but metadata is incomplete"
        >
          <SpaceBetween size="xs">
            <Box variant="p">
              The last scan succeeded but could not read{" "}
              {degradedScan.tablesFailed === 1
                ? "1 of the tables"
                : `${degradedScan.tablesFailed} of the tables`}{" "}
              it listed. Those tables have no columns, no comments and no
              declared keys below, and any AI-generated descriptions were
              produced over that gap — do not review this source as complete.
              Re-scan once the tables are readable.
            </Box>
            {degradedScan.failedTables.length > 0 && (
              <Box variant="p">
                {degradedScan.failedTables.length < degradedScan.tablesFailed
                  ? `Affected tables (${degradedScan.failedTables.length} of ${degradedScan.tablesFailed} shown): `
                  : "Affected tables: "}
                <Box variant="code">{degradedScan.failedTables.join(", ")}</Box>
              </Box>
            )}
          </SpaceBetween>
        </Alert>
      )}

      {isDatabase && isMetadataStale && (
        <Alert type="warning" header="Metadata may be stale">
          This source was last scanned more than 30 days ago. The underlying
          schema may have changed since approval. Consider refreshing the source
          to ensure ontology induction uses up-to-date metadata.
        </Alert>
      )}

      {/* ── DATABASE layout — matches DataSourceDetail ── */}
      {isDatabase && (
        <>
          <Container header={<Header variant="h2">Source summary</Header>}>
            <KeyValuePairs
              columns={4}
              items={[
                {
                  label: "Source type",
                  value:
                    DATABASE_SUBTYPE_LABELS[source.sourceSubType ?? ""] ??
                    source.sourceSubType ??
                    "—",
                },
                {
                  label: "Status",
                  value: (
                    <StatusIndicator type={sourceStatusType(status)}>
                      {sourceStatusLabel(status)}
                    </StatusIndicator>
                  ),
                },
                {
                  label: "Tables discovered",
                  value: String(dbDetails?.tablesDiscovered ?? 0),
                },
                {
                  label: "Tables approved",
                  value: `${dbDetails?.tablesApproved ?? 0} / ${dbDetails?.tablesDiscovered ?? 0}`,
                },
                {
                  label: "Metadata enrichment",
                  value:
                    dbDetails?.metadataEnrichmentEnabled === false
                      ? "Disabled"
                      : "Enabled",
                },
                {
                  label: "Last scan",
                  value: dbDetails?.lastScanAt
                    ? formatTimestamp(dbDetails.lastScanAt)
                    : "—",
                },
                { label: "Created", value: formatTimestamp(source.createdAt) },
                { label: "Source ID", value: source.sourceId },
              ]}
            />
          </Container>

          <Tabs
            tabs={[
              {
                id: "tables",
                label: `Tables (${tables.length})`,
                content: (
                  <SpaceBetween size="m">
                    {(tablesData?.skippedAssets ?? 0) > 0 && (
                      <Alert type="warning">
                        {tablesData?.skippedAssets === 1
                          ? "1 table could not be loaded due to a data issue."
                          : `${tablesData?.skippedAssets} tables could not be loaded due to data issues.`}{" "}
                        The list below may be incomplete.
                      </Alert>
                    )}
                    <Table
                      {...collectionProps}
                      variant="container"
                      selectionType="multi"
                      selectedItems={selectedTables}
                      onSelectionChange={({ detail }) =>
                        setSelectedTables(detail.selectedItems)
                      }
                      loading={tablesLoading}
                      items={pagedTables}
                      trackBy="tableId"
                      columnDefinitions={[
                        {
                          id: "name",
                          header: "Table",
                          cell: (item: TableSummary) => (
                            <Link
                              onFollow={(e) => {
                                e.preventDefault();
                                navigate(
                                  `/namespaces/${namespaceId}/sources/${sourceId}/tables/${item.tableId}`,
                                );
                              }}
                            >
                              {item.tableId}
                            </Link>
                          ),
                        },
                        {
                          id: "database",
                          header: "Database",
                          cell: (item: TableSummary) => item.database,
                        },
                        {
                          id: "columns",
                          header: "Columns",
                          cell: (item: TableSummary) => {
                            const total = item.columnCount ?? 0;
                            const approved = item.columnsApproved ?? 0;
                            const allApproved = approved >= total && total > 0;
                            return (
                              <StatusIndicator
                                type={allApproved ? "success" : "warning"}
                              >
                                {`${approved}/${total}`}
                              </StatusIndicator>
                            );
                          },
                        },
                        {
                          id: "enriched",
                          header: "Enriched",
                          cell: (item: TableSummary) =>
                            item.enrichmentSource ? (
                              <Badge color="blue">
                                {item.enrichmentSource}
                              </Badge>
                            ) : (
                              "—"
                            ),
                        },
                        {
                          id: "status",
                          header: "Review status",
                          sortingField: "reviewStatus",
                          cell: (item: TableSummary) => {
                            const s = item.reviewStatus as string;
                            const type =
                              s === ReviewStatus.APPROVED
                                ? "success"
                                : s === ReviewStatus.REJECTED
                                  ? "error"
                                  : "warning";
                            const label =
                              s === ReviewStatus.APPROVED
                                ? "Approved"
                                : s === ReviewStatus.REJECTED
                                  ? "Rejected"
                                  : "Pending review";
                            return (
                              <StatusIndicator type={type}>
                                {label}
                              </StatusIndicator>
                            );
                          },
                        },
                      ]}
                      filter={
                        <SpaceBetween direction="horizontal" size="xs">
                          <TextFilter
                            {...filterProps}
                            filteringPlaceholder="Find tables"
                            filteringAriaLabel="Filter tables"
                          />
                          <Select
                            selectedOption={
                              statusFilter
                                ? { value: statusFilter, label: statusFilter }
                                : { value: "", label: "All statuses" }
                            }
                            onChange={({ detail }) =>
                              setStatusFilter(
                                detail.selectedOption.value || null,
                              )
                            }
                            options={[
                              { value: "", label: "All statuses" },
                              {
                                value: ReviewStatus.PENDING_REVIEW,
                                label: "Pending review",
                              },
                              {
                                value: ReviewStatus.APPROVED,
                                label: "Approved",
                              },
                              {
                                value: ReviewStatus.REJECTED,
                                label: "Rejected",
                              },
                            ]}
                            placeholder="Filter by status"
                          />
                        </SpaceBetween>
                      }
                      pagination={<Pagination {...paginationProps} />}
                      header={
                        <Header
                          variant="h2"
                          counter={`(${tables.length})`}
                          actions={
                            <SpaceBetween direction="horizontal" size="xs">
                              <Button
                                onClick={() =>
                                  handleBatchReviewTables(
                                    ReviewDecision.REJECTED,
                                  )
                                }
                                loading={isBatchReviewing}
                                disabled={selectedTables.length === 0}
                              >
                                Reject selected
                              </Button>
                              <Button
                                onClick={() =>
                                  handleBatchReviewTables(
                                    ReviewDecision.APPROVED,
                                  )
                                }
                                loading={isBatchReviewing}
                                disabled={selectedTables.length === 0}
                              >
                                Approve selected
                              </Button>
                            </SpaceBetween>
                          }
                        >
                          Tables
                        </Header>
                      }
                      empty={
                        <Box
                          textAlign="center"
                          color="text-status-inactive"
                          padding="l"
                        >
                          {status === "SCANNING" || status === "ENRICHING"
                            ? "Tables will appear here once the scan completes."
                            : status === "REGISTERED"
                              ? "Scan has not started yet."
                              : "No tables discovered."}
                        </Box>
                      }
                    />
                  </SpaceBetween>
                ),
              },
              {
                id: "history",
                label: "Scan history",
                content: (
                  <ScanHistoryTable
                    createdAt={source.createdAt}
                    updatedAt={source.updatedAt}
                    lastScanAt={dbDetails?.lastScanAt}
                    status={status}
                    tablesDiscovered={dbDetails?.tablesDiscovered ?? 0}
                    tablesApproved={dbDetails?.tablesApproved ?? 0}
                    namespaceId={namespaceId ?? ""}
                    sourceId={sourceId ?? ""}
                    lastScanJobId={dbDetails?.lastScanJobId}
                  />
                ),
              },
              {
                id: "settings",
                label: "Settings",
                content: (
                  <Container
                    header={<Header variant="h2">Connection settings</Header>}
                  >
                    {dbDetails?.glueConfiguration && (
                      <KeyValuePairs
                        columns={2}
                        items={[
                          {
                            label: "Catalog ID",
                            value: String(
                              dbDetails.glueConfiguration.catalogId ?? "—",
                            ),
                          },
                          {
                            label: "Region",
                            value: String(
                              dbDetails.glueConfiguration.region ?? "—",
                            ),
                          },
                          {
                            label: "Database",
                            value: String(
                              dbDetails.glueConfiguration.databaseName ?? "—",
                            ),
                          },
                          {
                            label: "Table filter",
                            value: String(
                              dbDetails.glueConfiguration.tableFilter ??
                                "(none)",
                            ),
                          },
                        ]}
                      />
                    )}
                    {dbDetails?.jdbcConfiguration && (
                      <KeyValuePairs
                        columns={2}
                        items={[
                          {
                            label: "Engine",
                            value: String(
                              dbDetails.jdbcConfiguration.engine ?? "—",
                            ),
                          },
                          {
                            label: "Host",
                            value: String(
                              dbDetails.jdbcConfiguration.host ?? "—",
                            ),
                          },
                          {
                            label: "Port",
                            value: String(
                              dbDetails.jdbcConfiguration.port ?? "—",
                            ),
                          },
                          {
                            label: "Database",
                            value: String(
                              dbDetails.jdbcConfiguration.databaseName ?? "—",
                            ),
                          },
                          {
                            label: "Schema filter",
                            value: String(
                              dbDetails.jdbcConfiguration.schemaFilter ??
                                "(none)",
                            ),
                          },
                          {
                            label: "Table filter",
                            value: String(
                              dbDetails.jdbcConfiguration.tableFilter ??
                                "(none)",
                            ),
                          },
                          {
                            label: "Auth",
                            value: "Secrets Manager",
                          },
                        ]}
                      />
                    )}
                    {dbDetails?.athenaConfiguration && (
                      <KeyValuePairs
                        columns={2}
                        items={[
                          {
                            // One ARN, because AthenaConfiguration models one: the
                            // connector Lambda serves both the metadata and record
                            // paths. Athena's split metadata/record pair is not
                            // offered anywhere — see that Smithy shape for why.
                            label: "Connector function ARN",
                            value: String(
                              dbDetails.athenaConfiguration
                                .connectorFunctionArn ?? "—",
                            ),
                          },
                          {
                            label: "Database",
                            value: String(
                              dbDetails.athenaConfiguration.databaseName ?? "—",
                            ),
                          },
                          {
                            label: "Table filter",
                            value: String(
                              dbDetails.athenaConfiguration.tableFilter ??
                                "(none)",
                            ),
                          },
                          {
                            label: "Table exclude filter",
                            value: String(
                              dbDetails.athenaConfiguration
                                .tableExcludeFilter ?? "(none)",
                            ),
                          },
                        ]}
                      />
                    )}
                  </Container>
                ),
              },
            ]}
          />
        </>
      )}

      {/* ── DOCUMENTS layout — matches DocSourceDetail ── */}
      {isDocuments && (
        <>
          <Container header={<Header variant="h2">Overview</Header>}>
            <ColumnLayout columns={3} variant="text-grid">
              <ValueWithLabel label="Status">
                <StatusIndicator type={sourceStatusType(status)}>
                  {sourceStatusLabel(status)}
                </StatusIndicator>
              </ValueWithLabel>
              <ValueWithLabel label="Source">
                {source.sourceSubType === "LOCAL_UPLOAD"
                  ? "File upload"
                  : "S3 bucket"}
              </ValueWithLabel>
              {source.sourceSubType !== "LOCAL_UPLOAD" && (
                <>
                  <ValueWithLabel label="S3 Prefixes">
                    {docDetails?.s3Prefixes &&
                    docDetails.s3Prefixes.length > 0 ? (
                      docDetails.s3Prefixes.map((p, i) => (
                        <div key={i}>
                          <Box variant="code">{p}</Box>
                        </div>
                      ))
                    ) : (
                      <span>— (entire bucket)</span>
                    )}
                  </ValueWithLabel>
                  <ValueWithLabel label="Source Bucket ARN">
                    {String(docDetails?.sourceBucketArn ?? "—")}
                  </ValueWithLabel>
                  <ValueWithLabel label="Cross-account Role">
                    {String(docDetails?.roleArn ?? "—")}
                  </ValueWithLabel>
                </>
              )}
              <ValueWithLabel label="Created">
                {formatTimestamp(source.createdAt)}
              </ValueWithLabel>
              <ValueWithLabel label="Last Updated">
                {formatTimestamp(source.updatedAt)}
              </ValueWithLabel>
            </ColumnLayout>
          </Container>

          <Container
            header={<Header variant="h2">Ingestion Statistics</Header>}
          >
            <ColumnLayout columns={3} variant="text-grid">
              <ValueWithLabel label="Total Files">
                {String(docDetails?.filesTotal ?? "—")}
              </ValueWithLabel>
              <ValueWithLabel label="Skipped">
                {String(docDetails?.filesSkipped ?? "—")}
              </ValueWithLabel>
              <ValueWithLabel label="Errored">
                {String(docDetails?.filesErrored ?? "—")}
              </ValueWithLabel>
              <ValueWithLabel label="Documents Processed">
                {String(docDetails?.documentsProcessed ?? "—")}
              </ValueWithLabel>
              <ValueWithLabel label="Text Chunks AI Processed">
                {String(docDetails?.chunksLLM ?? "—")}
              </ValueWithLabel>
              <ValueWithLabel label="Text Chunks Embeddings">
                {String(docDetails?.chunksEmbed ?? "—")}
              </ValueWithLabel>
              <ValueWithLabel label="Text Chunks Added To Graph">
                {String(docDetails?.chunksGraph ?? "—")}
              </ValueWithLabel>
            </ColumnLayout>
          </Container>

          {preprocessingIssues.length > 0 && (
            <Table
              header={
                <Header
                  variant="h2"
                  counter={`(${preprocessingIssues.length})`}
                >
                  File Issues
                </Header>
              }
              columnDefinitions={[
                {
                  id: "filename",
                  header: "Filename",
                  cell: (item) => item.filename,
                },
                {
                  id: "type",
                  header: "Type",
                  cell: (item) => (
                    <StatusIndicator
                      type={item.type === "error" ? "error" : "warning"}
                    >
                      {item.type}
                    </StatusIndicator>
                  ),
                },
                { id: "reason", header: "Reason", cell: (item) => item.reason },
              ]}
              items={preprocessingIssues}
            />
          )}

          {extractionConfig && (
            <Container header={<Header variant="h2">Advanced Options</Header>}>
              <ColumnLayout columns={2} variant="text-grid">
                <ValueWithLabel label="Versioning">
                  {(extractionConfig.enableVersioning as boolean)
                    ? "Enabled"
                    : "Disabled"}
                </ValueWithLabel>
                <ValueWithLabel label="Delete Previous Versions">
                  {(extractionConfig.deletePrevVersions as boolean)
                    ? "Yes"
                    : "No"}
                </ValueWithLabel>
              </ColumnLayout>
            </Container>
          )}
        </>
      )}

      {deleteError && (
        <Alert type="error" header="Failed to delete source">
          {deleteError.message}
        </Alert>
      )}

      {showDeleteModal && (
        <Modal
          visible
          onDismiss={() => setShowDeleteModal(false)}
          header={isDocuments ? "Delete document source" : "Delete data source"}
          footer={
            <Box float="right">
              <SpaceBetween direction="horizontal" size="xs">
                <Button
                  variant="link"
                  onClick={() => setShowDeleteModal(false)}
                >
                  Cancel
                </Button>
                <Button
                  variant="normal"
                  loading={isDeleting}
                  onClick={() => {
                    setShowDeleteModal(false);
                    deleteSource();
                  }}
                >
                  Delete
                </Button>
              </SpaceBetween>
            </Box>
          }
        >
          <SpaceBetween size="m">
            <Box>
              {isDocuments ? (
                <>
                  Permanently delete <b>{source.name}</b>? This will remove all
                  associated files from S3, the DynamoDB record, and the
                  knowledge graph data from Neptune and OpenSearch.
                </>
              ) : (
                <>
                  Permanently delete <b>{source.name}</b>? This will remove all
                  associated data.
                </>
              )}
            </Box>
            <Alert type="warning">This action cannot be undone.</Alert>
          </SpaceBetween>
        </Modal>
      )}
    </SpaceBetween>
  );
};

// ── Scan history (database sources) ──────────────────────────────────────────
//
// The detail record carries enough fields (createdAt / updatedAt / lastScanAt
// plus the current status) to reconstruct a lightweight scan-history view
// without a dedicated audit-log table. This mirrors the activity tab in
// coa-ui (see ../../../../coa-ui/app/src/pages/SourceDetail.tsx) and
// gives stewards a chronological summary of what the platform did to the
// source. When a real event store lands later we can swap the derived rows
// for paginated audit entries without changing the public component shape.

interface ScanHistoryEntry {
  id: string;
  at: string; // ISO timestamp (normalized from Date | string)
  kind: "registered" | "scan" | "review" | "deletion";
  type: StatusIndicatorProps["type"];
  label: string;
  detail: string;
}

function toIso(value: Date | string | undefined): string | undefined {
  if (!value) return undefined;
  return value instanceof Date ? value.toISOString() : value;
}

function buildScanHistory(args: {
  createdAt?: Date | string;
  updatedAt?: Date | string;
  lastScanAt?: Date | string;
  status: string;
  tablesDiscovered: number;
  tablesApproved: number;
  errorMessage?: string;
  /** Tables lost to a per-table read failure while the scan itself succeeded. */
  tablesFailed?: number;
}): ScanHistoryEntry[] {
  // Normalize Smithy ``Date`` timestamps to ISO strings so the rest of the
  // function can build stable ids and sort lexicographically.
  const createdAt = toIso(args.createdAt);
  const updatedAt = toIso(args.updatedAt);
  const lastScanAt = toIso(args.lastScanAt);
  const events: ScanHistoryEntry[] = [];

  if (createdAt) {
    events.push({
      id: `created-${createdAt}`,
      at: createdAt,
      kind: "registered",
      type: "info",
      label: "Source registered",
      detail: "Source created and queued for initial scan.",
    });
  }

  if (lastScanAt) {
    if (args.status === SourceStatus.SCAN_FAILED) {
      events.push({
        id: `scan-${lastScanAt}`,
        at: lastScanAt,
        kind: "scan",
        type: "error",
        label: "Scan failed",
        detail: args.errorMessage
          ? `Error: ${args.errorMessage}`
          : "The most recent scan did not complete. Use Re-scan to try again.",
      });
    } else if (
      args.status === SourceStatus.PENDING_REVIEW ||
      args.status === SourceStatus.APPROVING ||
      args.status === SourceStatus.REJECTING ||
      args.status === SourceStatus.APPROVED ||
      args.status === SourceStatus.APPROVAL_FAILED ||
      args.status === SourceStatus.REJECTION_FAILED
    ) {
      const scanned = `Scanned ${args.tablesDiscovered} table${args.tablesDiscovered === 1 ? "" : "s"}.`;
      const lost = args.tablesFailed ?? 0;
      events.push({
        id: `scan-${lastScanAt}`,
        at: lastScanAt,
        kind: "scan",
        // A scan that dropped tables completed, but not cleanly — flag it so
        // the activity log doesn't read as a clean success.
        type: lost > 0 ? "warning" : "success",
        label: "Scan completed",
        detail:
          lost > 0
            ? `${scanned} ${lost} table${lost === 1 ? "" : "s"} could not be read and ${lost === 1 ? "was" : "were"} skipped.`
            : scanned,
      });
    } else if (
      args.status === SourceStatus.SCANNING ||
      args.status === SourceStatus.ENRICHING
    ) {
      events.push({
        id: `scan-${lastScanAt}`,
        at: lastScanAt,
        kind: "scan",
        type: "in-progress",
        label: "Scan in progress",
        detail: "The scan or enrichment pipeline is still running.",
      });
    }
  }

  if (
    args.status === SourceStatus.APPROVED &&
    args.tablesApproved > 0 &&
    updatedAt
  ) {
    events.push({
      id: `review-${updatedAt}`,
      at: updatedAt,
      kind: "review",
      type: "success",
      label: "All tables approved",
      detail: `${args.tablesApproved} of ${args.tablesDiscovered} tables approved.`,
    });
  }

  if (args.status === SourceStatus.DELETING && updatedAt) {
    events.push({
      id: `delete-${updatedAt}`,
      at: updatedAt,
      kind: "deletion",
      type: "in-progress",
      label: "Deletion in progress",
      detail: "The source and all associated data are being removed.",
    });
  }

  // Most recent first so stewards see the latest state at the top.
  return events.sort((a, b) => (a.at < b.at ? 1 : -1));
}

interface ScanHistoryTableProps {
  createdAt?: Date | string;
  updatedAt?: Date | string;
  lastScanAt?: Date | string;
  status: string;
  tablesDiscovered: number;
  tablesApproved: number;
  namespaceId: string;
  sourceId: string;
  lastScanJobId?: string;
}

const ScanHistoryTable: React.FC<ScanHistoryTableProps> = (props) => {
  // Fetch the scan job for its errorMessage when the scan failed, and for its
  // per-table failure count when it succeeded — a scan that completed can still
  // have dropped tables, and only the scan job records that.
  const { data: scanJob } = useGetSourceScanJob(
    props.namespaceId,
    props.sourceId,
    props.lastScanJobId ?? "",
  );
  const tablesFailed = readDegradedScan(scanJob)?.tablesFailed;

  const events = React.useMemo(
    () =>
      buildScanHistory({
        createdAt: props.createdAt,
        updatedAt: props.updatedAt,
        lastScanAt: props.lastScanAt,
        status: props.status,
        tablesDiscovered: props.tablesDiscovered,
        tablesApproved: props.tablesApproved,
        errorMessage: scanJob?.errorMessage,
        tablesFailed,
      }),
    [
      props.createdAt,
      props.updatedAt,
      props.lastScanAt,
      props.status,
      props.tablesDiscovered,
      props.tablesApproved,
      scanJob?.errorMessage,
      tablesFailed,
    ],
  );

  return (
    <Table<ScanHistoryEntry>
      variant="container"
      items={events}
      trackBy="id"
      columnDefinitions={[
        {
          id: "at",
          header: "Time",
          isRowHeader: true,
          cell: (e) => formatTimestamp(e.at),
          minWidth: 180,
        },
        {
          id: "status",
          header: "Status",
          cell: (e) => (
            <StatusIndicator type={e.type}>{e.label}</StatusIndicator>
          ),
          minWidth: 200,
        },
        {
          id: "detail",
          header: "Details",
          cell: (e) => e.detail,
        },
      ]}
      header={
        <Header variant="h2" counter={`(${events.length})`}>
          Activity
        </Header>
      }
      empty={
        <Box textAlign="center" color="text-status-inactive" padding="l">
          No activity yet.
        </Box>
      }
    />
  );
};
