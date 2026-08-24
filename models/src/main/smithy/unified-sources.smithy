// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
// Unified source registry — CRUD operations for the unified sources experience.
//
// The service definition lives in control-plane.smithy (ControlPlaneService).
$version: "2"

namespace com.amazon.semanticcontext.unifiedsources

use com.amazon.semanticcontext.common#DisplayName
use com.amazon.semanticcontext.common#IamRoleArn
use com.amazon.semanticcontext.common#PaginatedInput
use com.amazon.semanticcontext.common#PaginatedOutput
use com.amazon.semanticcontext.common#S3BucketArn
use com.amazon.semanticcontext.common#S3Prefix
use com.amazon.semanticcontext.common#Uuid

/// Table identifier (database.table format).
@length(min: 1, max: 512)
@pattern("^[a-zA-Z0-9_\\-:.]+$")
string TableId

/// Column name identifier.
@length(min: 1, max: 256)
@pattern("^[a-zA-Z_][a-zA-Z0-9_]*$")
string ColumnName

/// STS ExternalId used when assuming a cross-account access role
/// (confused-deputy protection). Must match the ExternalId on the role's
/// trust policy. See AWS STS ExternalId character/length constraints.
@length(min: 2, max: 1224)
@pattern("^[a-zA-Z0-9+=,.@:\\/-]+$")
string ExternalId

/// Review status for enriched metadata.
enum ReviewStatus {
    /// Awaiting steward review.
    PENDING_REVIEW

    /// Reviewed and approved.
    APPROVED

    /// Reviewed and rejected.
    REJECTED
}

/// Decision a steward can make on enriched metadata.
enum ReviewDecision {
    /// Approve the metadata.
    APPROVED

    /// Reject the metadata.
    REJECTED
}

/// AWS account ID (12 digits).
@pattern("^\\d{12}$")
@length(min: 12, max: 12)
string AwsAccountId

/// Glue Data Catalog identifier: a 12-digit account id, or `account:catalogName`
/// for a nested catalog.
@pattern("^\\d{12}(:[a-zA-Z0-9_/-]+)?$")
@length(min: 12, max: 256)
string GlueCatalogId

/// Athena DataCatalog name (1-256 chars, alphanumeric + underscore).
@length(min: 1, max: 256)
@pattern("^[a-zA-Z][a-zA-Z0-9_]*$")
string AthenaDataCatalogName

/// Amazon Redshift Serverless workgroup name (3-64 chars, lowercase
/// alphanumeric and hyphens; must start with a letter — Redshift Serverless
/// naming rules).
@length(min: 3, max: 64)
@pattern("^[a-z][a-z0-9-]*$")
string RedshiftWorkgroupName

/// AWS region identifier.
@length(min: 1, max: 64)
string AwsRegion

/// Database hostname or endpoint.
@length(min: 1, max: 512)
string Hostname

/// Database port number.
@range(min: 1, max: 65535)
integer Port

/// Database name.
@length(min: 1, max: 256)
string DatabaseName

/// Regex filter pattern for schema/table filtering.
@length(min: 1, max: 1024)
string FilterPattern

/// Secrets Manager ARN for database credentials.
@pattern("^arn:(aws|aws-us-gov):secretsmanager:[a-z0-9-]+:\\d{12}:secret:.+$")
@length(min: 20, max: 2048)
string SecretArn

/// AWS Lambda function ARN, optionally qualified with a version number, an
/// alias, or `$LATEST`. Format:
/// arn:{partition}:lambda:{region}:{account-id}:function:{name}[:{qualifier}]
///
/// A full ARN is required: Lambda's partial-ARN
/// (`{account-id}:function:{name}`) and name-only forms are deliberately
/// rejected, because the connector runs in the customer's account and an
/// unqualified name would resolve against this deployment's account instead.
@pattern("^arn:aws[a-z-]*:lambda:[a-z0-9-]+:\\d{12}:function:[a-zA-Z0-9_-]+(:(\\$LATEST|[a-zA-Z0-9_-]+))?$")
@length(min: 20, max: 2048)
string LambdaFunctionArn

/// Database engine type for JDBC connections.
enum DatabaseEngine {
    /// PostgreSQL database engine.
    POSTGRESQL

    /// MySQL database engine.
    MYSQL

    /// Amazon Redshift database engine.
    REDSHIFT

    /// Oracle database engine.
    ORACLE

    /// Microsoft SQL Server database engine.
    SQLSERVER

    /// Snowflake database engine.
    SNOWFLAKE
}

/// Engine type for the underlying data behind a Glue database.
enum ExecutionEngine {
    /// PostgreSQL execution engine.
    POSTGRESQL

    /// MySQL execution engine.
    MYSQL

    /// Amazon Redshift execution engine.
    REDSHIFT

    /// Oracle execution engine.
    ORACLE

    /// Microsoft SQL Server execution engine.
    SQLSERVER

    /// Snowflake execution engine.
    SNOWFLAKE

    /// Amazon DynamoDB execution engine.
    DYNAMODB

    /// S3-backed Apache Iceberg execution engine.
    S3_ICEBERG
}

/// How the consumption layer should execute queries against a source.
enum QueryEngine {
    /// Query via Amazon Athena — native Glue catalog, or a JDBC federated catalog.
    ATHENA

    /// Query the database directly over JDBC (no Athena).
    JDBC

    /// Query Glue/Iceberg tables via Amazon Redshift Serverless using the
    /// `awsdatacatalog` auto-mount (an alternative execution engine to Athena
    /// for Glue-backed sources; opt-in per source via
    /// GlueConfiguration.executionEngine).
    REDSHIFT
}

/// JDBC database connection configuration.
structure JdbcConfiguration {
    @required
    engine: DatabaseEngine

    @required
    host: Hostname

    @required
    port: Port

    @required
    databaseName: DatabaseName

    schemaFilter: FilterPattern

    schemaExcludeFilter: FilterPattern

    tableFilter: FilterPattern

    tableExcludeFilter: FilterPattern

    credentialSecretArn: SecretArn

    crossAccountRoleArn: IamRoleArn

    /// Optional STS ExternalId required by the cross-account role's trust
    /// policy. Only meaningful alongside crossAccountRoleArn.
    externalId: ExternalId

    /// Snowflake only: the virtual warehouse used to run INFORMATION_SCHEMA
    /// queries during discovery (Snowflake requires an active warehouse).
    warehouse: String

    /// Snowflake only: optional Snowflake RBAC role name to assume for the
    /// session (a Snowflake construct — not an AWS IAM role; see crossAccountRoleArn).
    role: String
}

/// Glue Data Catalog database connection configuration.
structure GlueConfiguration {
    /// Glue Data Catalog id: a 12-digit AWS account id (the account's root
    /// catalog) or a nested catalog id of the form `account:catalogName`
    /// (federated / S3 Tables / Redshift-backed / cross-account catalogs).
    @required
    catalogId: GlueCatalogId

    @required
    region: AwsRegion

    @required
    databaseName: DatabaseName

    tableFilter: FilterPattern

    tableExcludeFilter: FilterPattern

    crossAccountRoleArn: IamRoleArn

    /// Optional STS ExternalId required by the cross-account role's trust
    /// policy. Only meaningful alongside crossAccountRoleArn.
    externalId: ExternalId

    /// Nested Athena/Glue catalog name when the database lives in a federated or
    /// other non-root catalog. When set, the source is queried as
    /// `AwsDataCatalog.<athenaDataCatalogName>.<databaseName>.<table>`; when
    /// omitted, as `AwsDataCatalog.<databaseName>.<table>`.
    athenaDataCatalogName: AthenaDataCatalogName

    /// Execution engine for this Glue/Iceberg source's serve-time queries.
    /// Defaults to ATHENA when omitted. Set REDSHIFT to route queries through
    /// Amazon Redshift Serverless (`awsdatacatalog` auto-mount); requires
    /// `redshiftWorkgroup` to be set.
    executionEngine: GlueExecutionEngine

    /// Redshift Serverless workgroup used to execute queries when
    /// `executionEngine` is REDSHIFT. Ignored (and unnecessary) for ATHENA.
    redshiftWorkgroup: RedshiftWorkgroupName
}

/// Custom Athena Query Federation connector configuration — the
/// ATHENA_CONNECTOR sub-type. The connector is a Lambda the customer authors
/// and deploys in their own account, which this service registers as a
/// Lambda-backed Athena data catalog in its own account; both discovery and
/// serve-time queries run through Athena against that catalog.
///
/// Unlike GlueConfiguration there is no Glue database behind the source, so
/// there is no catalogId and no cross-account IAM role to assume: the customer
/// grants access on the connector Lambda's own resource policy instead. There
/// is also no region member — the connector must live in this deployment's
/// region (see metadataFunctionArn).
structure AthenaConfiguration {
    /// ARN of the connector Lambda that handles metadata requests — and record
    /// requests too when recordFunctionArn is omitted, i.e. a composite
    /// handler. The ARN's region must equal this deployment's own region,
    /// because Athena can only invoke a data-source connector co-located with
    /// the query.
    @required
    metadataFunctionArn: LambdaFunctionArn

    /// ARN of a separate record-handler Lambda, for a connector deployed as a
    /// split metadata/record pair. Omit for the common composite connector, in
    /// which case metadataFunctionArn serves both paths.
    recordFunctionArn: LambdaFunctionArn

    /// The single database inside the connector's catalog that this source
    /// exposes. Exactly one database is resolved per source, so a connector
    /// serving several databases is onboarded once per database.
    @required
    databaseName: DatabaseName

    /// Include-filter applied to table names within databaseName. Omit to
    /// discover every table.
    tableFilter: FilterPattern

    /// Exclude-filter applied to table names within databaseName, evaluated
    /// after tableFilter.
    tableExcludeFilter: FilterPattern
}

/// Execution engine choice for a Glue/Iceberg source. A deliberately narrow
/// enum (only the engines valid for a Glue source) so the onboarding UI/API
/// cannot select an engine — e.g. direct JDBC — that doesn't apply to Glue.
enum GlueExecutionEngine {
    /// Execute via Amazon Athena (default).
    ATHENA

    /// Execute via Amazon Redshift Serverless `awsdatacatalog` auto-mount.
    REDSHIFT
}

/// Business-facing metadata for a table or column returned in responses:
/// description, synonyms, glossary terms, tags, enrichment source, review
/// status, and AI-inference confidence.
structure BusinessMetadataOutput {
    /// Human-readable description of the table or column.
    description: String

    /// Alternative names or aliases.
    synonyms: StringList

    /// Associated business glossary terms.
    glossaryTerms: StringList

    /// Free-form classification tags.
    tags: StringList

    /// Origin of this metadata (e.g. AI-inferred, steward-specified, catalog-existing).
    enrichmentSource: String

    reviewStatus: ReviewStatus

    /// AI-inference confidence (0.0-1.0) for AI-generated metadata; absent or 0
    /// for steward-specified or catalog-existing metadata.
    confidence: Float
}

/// The primary-key definition reported for a table: its key columns plus the
/// source (e.g. discovered vs. steward-specified) and inference confidence.
structure PrimaryKeyOutput {
    /// Ordered column names composing the primary key.
    columns: StringList

    /// Origin of the key (e.g. discovered vs. steward-specified).
    source: String

    /// Inference confidence (0.0-1.0) for AI-inferred keys.
    confidence: Float
}

/// A foreign-key relationship reported for a table: the local column and the
/// target table/column it references, with the source and inference confidence.
structure ForeignKeyOutput {
    /// Local column holding the foreign-key reference.
    column: String

    /// Referenced table.
    targetTable: String

    /// Referenced column in the target table.
    targetColumn: String

    /// Origin of the key (e.g. discovered vs. steward-specified).
    source: String

    /// Inference confidence (0.0-1.0) for AI-inferred keys.
    confidence: Float
}

list ForeignKeyList {
    member: ForeignKeyOutput
}

/// Metadata describing a single column of a source table: its name, data type,
/// nullability, partition-key flag, description, business metadata, and any
/// sampled distinct values.
structure ColumnMetadata {
    /// Column name.
    @required
    name: String

    /// Column data type (source-native type string).
    @required
    dataType: String

    /// Whether the column permits null values.
    nullable: Boolean

    /// Whether the column is a partition key.
    isPartitionKey: Boolean

    /// Human-readable description of the column.
    description: String

    businessMetadata: BusinessMetadataOutput

    /// Sampled distinct values for low-cardinality categorical columns
    /// (capped, best-effort). Empty for high-cardinality, non-string, or
    /// unsampled columns. Used by the serve NL→SQL layer to hint the LLM
    /// with correct enum literals for WHERE clauses, and shown in the UI.
    distinctValues: StringList
}

list ColumnMetadataList {
    member: ColumnMetadata
}

/// Technical metadata for a table: column count, partition keys, storage
/// format, and physical location.
structure TechnicalMetadataOutput {
    /// Number of columns in the table.
    columnCount: Integer

    /// Names of the table's partition-key columns.
    partitionKeys: StringList

    /// Storage/serialization format.
    format: String

    /// Physical storage location (e.g. S3 URI).
    location: String
}

/// Steward-supplied overrides applied to discovered metadata: description,
/// synonyms, glossary terms, and tags.
structure MetadataOverrides {
    /// Override description for the table or column.
    description: String

    /// Override synonyms or aliases.
    synonyms: StringList

    /// Override business glossary terms.
    glossaryTerms: StringList

    /// Override classification tags.
    tags: StringList
}

/// Steward-supplied primary key. Replaces the table's primary key; an empty
/// columns list clears it. Stored with source = STEWARD_SPECIFIED.
structure PrimaryKeyInput {
    /// Column names composing the primary key; empty list clears it.
    @required
    columns: StringList
}

/// Steward-supplied foreign key relationship. Stored with
/// source = STEWARD_SPECIFIED.
structure ForeignKeyInput {
    @required
    column: ColumnName

    /// Referenced table.
    @required
    targetTable: String

    /// Referenced column in the target table.
    targetColumn: String
}

list ForeignKeyInputList {
    member: ForeignKeyInput
}

/// A summary record for a source table in list/get responses: identifiers,
/// database, column and approved-column counts, review status, and enrichment
/// source.
structure TableSummary {
    /// Unique table identifier (database.table format).
    @required
    tableId: String

    /// Table name.
    @required
    name: String

    /// Database the table belongs to.
    @required
    database: String

    /// Number of columns in the table.
    columnCount: Integer

    /// Number of columns with reviewStatus=APPROVED. Allows the UI to show
    /// approval progress (columnsApproved / columnCount) without fetching
    /// per-column detail.
    columnsApproved: Integer

    @required
    reviewStatus: ReviewStatus

    /// Origin of the table's business metadata.
    enrichmentSource: String
}

list TableSummaryList {
    member: TableSummary
}

list StringList {
    member: String
}

/// Bedrock inference profile or foundation model ARN.
@pattern("^arn:(aws|aws-us-gov):bedrock:[a-z0-9-]*:[0-9]*:(inference-profile|foundation-model)/.+$")
@length(min: 20, max: 2048)
string BedrockModelArn

/// Extraction mode — mirrors ExtractionMode in constants.py.
enum ExtractionMode {
    /// Extract documents in a single continuous pass.
    CONTINUOUS = "continuous"

    /// Extract documents as separated units.
    SEPARATED = "separated"
}

/// Configuration for document extraction behaviour.
structure ExtractionConfig {
    extractionMode: ExtractionMode

    /// Whether to use Bedrock batch inference for extraction.
    useBatchInference: Boolean

    /// Whether to version extracted documents.
    enableVersioning: Boolean

    /// Whether to extract propositions from chunks.
    enablePropositionExtraction: Boolean

    /// Whether to delete previous document versions on re-ingest.
    deletePrevVersions: Boolean

    bedrockModelArn: BedrockModelArn
}

list S3PrefixList {
    member: S3Prefix
}

/// Type of per-file preprocessing issue.
enum PreprocessingIssueType {
    /// The file was skipped during preprocessing.
    SKIPPED = "skipped"

    /// The file errored during preprocessing.
    ERROR = "error"
}

/// A single file-level preprocessing issue recorded during ingestion.
structure PreprocessingIssue {
    /// Name of the file the issue applies to.
    @required
    filename: String

    @required
    type: PreprocessingIssueType

    /// Human-readable explanation of the issue.
    @required
    reason: String
}

list PreprocessingIssueList {
    member: PreprocessingIssue
}

/// A single file to upload, identified by filename and MIME content type.
structure UploadFileRequest {
    /// Name of the file to upload.
    @required
    filename: String

    /// MIME content type of the file.
    @required
    contentType: String
}

list UploadFileRequestList {
    member: UploadFileRequest
}

/// A pre-signed S3 PUT URL for a single file.
structure UploadUrl {
    /// Name of the file this URL is for.
    @required
    filename: String

    /// Pre-signed S3 PUT URL for uploading the file.
    @required
    uploadUrl: String
}

list UploadUrlList {
    member: UploadUrl
}

// =============================================================================
// Enums
// =============================================================================
/// Top-level category of a unified source.
enum SourceType {
    /// A structured database source.
    DATABASE

    /// An unstructured documents source.
    DOCUMENTS
}

/// Specific connector sub-type within a source category.
enum SourceSubType {
    /// Structured: AWS Glue Data Catalog database
    GLUE_DATABASE

    /// Structured: JDBC-connected relational database
    JDBC_DATABASE

    /// Structured: customer-authored AWS Athena Query Federation SDK connector
    /// (a Lambda in the customer's account), registered as a Lambda-backed
    /// Athena data catalog and queried through Athena
    ATHENA_CONNECTOR

    /// Unstructured: S3 bucket prefix
    S3

    /// Unstructured: direct file upload
    LOCAL_UPLOAD
}

/// Unified lifecycle status for all source types.
enum SourceStatus {
    /// Source registered, pipeline not yet started
    REGISTERED

    /// Pipeline is running (schema discovery or document ingestion)
    SCANNING

    /// LLM entity extraction in progress (document sources only)
    SCANNING_ENTITY_EXTRACTION

    /// Knowledge graph build in progress (document sources only)
    SCANNING_KG_BUILD

    /// AI metadata enrichment in progress (database sources only)
    ENRICHING

    /// Enrichment complete, awaiting human review (database sources only)
    PENDING_REVIEW

    /// Bulk approve in progress — async worker is writing DataZone revisions
    /// (database sources only)
    APPROVING

    /// Async bulk approve worker failed; client may retry by calling
    /// ApproveSource again (database sources only)
    APPROVAL_FAILED

    /// Bulk reject in progress — async worker is writing DataZone revisions
    /// (database sources only)
    REJECTING

    /// Async bulk reject worker failed; client may retry by calling
    /// RejectSource again (database sources only)
    REJECTION_FAILED

    /// Processing completed successfully (document sources)
    COMPLETED

    /// All tables reviewed and approved (database sources only)
    APPROVED

    /// Bulk reject completed — the source and all its tables/columns are
    /// rejected. Terminal: no further review transitions are allowed; the
    /// steward must re-onboard a new source (database sources only)
    REJECTED

    /// Processing failed
    SCAN_FAILED

    /// Deletion in progress
    DELETING

    /// Fully deleted
    DELETED

    /// Deletion failed
    DELETE_FAILED
}

/// Status of a database scan job execution.
enum SourceScanJobStatus {
    /// Scan job is running.
    IN_PROGRESS

    /// Scan job is discovering tables and schema.
    DISCOVERING

    /// Scan job is enriching metadata.
    ENRICHING

    /// Scan job completed successfully.
    COMPLETED

    /// Scan job failed.
    FAILED

    /// Scan job was cancelled.
    CANCELLED
}

// =============================================================================
// Core structures — shared across multiple operations
// =============================================================================
/// Summary representation of a unified source (used in list responses).
/// Contains only the fields common to all source types.
structure SourceSummary {
    /// Unique source identifier.
    @required
    sourceId: String

    @required
    namespaceId: Uuid

    /// Display name of the source.
    @required
    name: String

    @required
    sourceType: SourceType

    @required
    sourceSubType: SourceSubType

    @required
    status: SourceStatus

    /// Timestamp when the source was created.
    @required
    createdAt: Timestamp

    /// Timestamp when the source was last updated.
    updatedAt: Timestamp

    /// Number of tables discovered in the most recent scan (DATABASE sources only).
    tablesDiscovered: Integer
}

list SourceSummaryList {
    member: SourceSummary
}

// ── Create structures ─────────────────────────────────────────────────────────
/// Top-level create request. Exactly one of databaseSource or documentSource
/// must be set, matching the sourceType discriminator.
structure CreateSourceInput {
    @required
    sourceType: SourceType

    /// Required when sourceType = DATABASE.
    databaseSource: CreateDatabaseSourceInput

    /// Required when sourceType = DOCUMENTS.
    documentSource: CreateDocumentSourceInput
}

/// Database source creation fields.
/// Exactly one of glueConfiguration, jdbcConfiguration or athenaConfiguration
/// must be provided — it selects the sub-type.
structure CreateDatabaseSourceInput {
    @required
    name: DisplayName

    /// Glue Data Catalog config. Required for GLUE_DATABASE sub-type.
    glueConfiguration: GlueConfiguration

    /// JDBC connection config. Required for JDBC_DATABASE sub-type.
    jdbcConfiguration: JdbcConfiguration

    /// Custom Athena federation connector config. Required for the
    /// ATHENA_CONNECTOR sub-type.
    athenaConfiguration: AthenaConfiguration

    /// Enable AI-powered business metadata enrichment (descriptions,
    /// synonyms, glossary terms, tags). Defaults to true. When false the
    /// scan pipeline skips the enrichment step and the source transitions
    /// directly from SCANNING to PENDING_REVIEW with discovered technical
    /// metadata only.
    metadataEnrichmentEnabled: Boolean
}

/// Document source creation fields.
/// For S3 sources: provide sourceBucketArn (and optionally s3Prefixes, roleArn).
/// For upload sources: provide s3Prefixes pointing to the upload session prefix.
structure CreateDocumentSourceInput {
    @required
    name: DisplayName

    /// S3 key prefixes to ingest. Required for upload flow; optional for S3.
    s3Prefixes: S3PrefixList

    /// ARN of the S3 bucket. Required for S3 sub-type.
    sourceBucketArn: S3BucketArn

    /// IAM role for cross-account S3 access.
    roleArn: IamRoleArn

    extractionConfig: ExtractionConfig
}

/// Create source response.
structure CreateSourceOutput {
    /// Unique identifier of the newly created source.
    @required
    sourceId: String

    @required
    status: SourceStatus

    /// Identifier of the scan job started for the source, if any.
    scanJobId: String

    /// Timestamp when the source was created.
    @required
    createdAt: Timestamp
}

// ── Get structures ────────────────────────────────────────────────────────────
/// Full source record returned by GetSource.
/// Uses sourceType as a discriminator — branch on it to access type-specific detail:
///   DATABASE  → databaseDetails is populated; documentDetails is absent.
///   DOCUMENTS → documentDetails is populated; databaseDetails is absent.
structure GetSourceOutput {
    /// Unique source identifier.
    @required
    sourceId: String

    @required
    namespaceId: Uuid

    /// Display name of the source.
    @required
    name: String

    @required
    sourceType: SourceType

    @required
    sourceSubType: SourceSubType

    @required
    status: SourceStatus

    /// Timestamp when the source was created.
    @required
    createdAt: Timestamp

    /// Timestamp when the source was last updated.
    updatedAt: Timestamp

    /// Populated when sourceType = DATABASE. Absent for DOCUMENTS sources.
    databaseDetails: DatabaseSourceDetail

    /// Populated when sourceType = DOCUMENTS. Absent for DATABASE sources.
    documentDetails: DocumentSourceDetail
}

/// Type-specific detail for DATABASE sources.
structure DatabaseSourceDetail {
    glueConfiguration: GlueConfiguration

    jdbcConfiguration: JdbcConfiguration

    /// Populated for the ATHENA_CONNECTOR sub-type; absent otherwise.
    athenaConfiguration: AthenaConfiguration

    /// Whether AI-powered business metadata enrichment runs for this source.
    /// Set at creation time and persisted with the source record. When false
    /// the scan pipeline skips the enrichment step and the source transitions
    /// from SCANNING straight to PENDING_REVIEW.
    metadataEnrichmentEnabled: Boolean

    /// Preferred execution engine for single-source queries against this source.
    /// Read-only and system-set at creation: `JDBC` for direct (low-latency)
    /// execution, set only for engines with an implemented direct dialect
    /// (PostgreSQL/Redshift today); otherwise `ATHENA` (native Glue, and JDBC
    /// engines without a direct path yet). The consumption layer overrides this
    /// to Athena for any query spanning multiple sources.
    queryEngine: QueryEngine

    /// Number of tables discovered in the most recent scan.
    tablesDiscovered: Integer

    /// Number of tables with reviewStatus=APPROVED.
    tablesApproved: Integer

    /// Identifier of the most recent scan job.
    lastScanJobId: String

    /// Timestamp of the most recent scan.
    lastScanAt: Timestamp

    /// Name of the Glue Connection that backs Athena federated queries
    /// for this data source. Read-only and system-managed: populated
    /// only for JDBC sub-types (PostgreSQL, MySQL, Redshift, Oracle, SQL
    /// Server, Snowflake) on first scan and removed when the source is
    /// deleted. Null for S3/Iceberg-backed Glue databases — those are
    /// queried via Athena's native AwsDataCatalog.
    ///
    /// To query the source via Athena, pair this with the namespace's
    /// `athenaWorkgroupName` and the `athenaDataCatalogName` below. See
    /// the Athena Queryability section of `packages/sources/README.md`
    /// for examples.
    glueConnectionName: String

    /// Name of the managed Glue federated catalog registered for this
    /// source. Read-only and system-managed. Populated only for JDBC
    /// sub-types (null for S3/Iceberg). It is a nested catalog under
    /// Athena's default `AwsDataCatalog`, so query it as:
    ///
    ///     SELECT * FROM "AwsDataCatalog"."<athenaDataCatalogName>"."<schema>"."<table>"
    ///
    /// Run the query inside the namespace's Athena workgroup (see
    /// NamespaceDetail.athenaWorkgroupName). This catalog name equals
    /// the `glueConnectionName`; both are derived from
    /// `{resource-prefix}ds_{sourceId}`.
    athenaDataCatalogName: String
}

/// Type-specific detail for DOCUMENTS sources.
structure DocumentSourceDetail {
    /// S3 key prefixes ingested for this source.
    s3Prefixes: S3PrefixList

    sourceBucketArn: S3BucketArn

    roleArn: IamRoleArn

    extractionConfig: ExtractionConfig

    /// Total number of files considered for ingestion.
    filesTotal: Integer

    /// Number of files skipped during preprocessing.
    filesSkipped: Integer

    /// Number of files that errored during preprocessing.
    filesErrored: Integer

    /// Error message if processing failed.
    errorMessage: String

    /// Per-file preprocessing issues recorded during ingestion.
    preprocessingIssues: PreprocessingIssueList

    /// Distinct source documents opened for chunk extraction by the KG build,
    /// read from the per-source metrics record (PK=METRICS#{ns}, SK=KGBUILD#{ds}).
    /// Null until the KG build has run and written metrics.
    documentsProcessed: Integer

    /// Count of LLM extraction prompts run on chunks (topic + proposition each
    /// fire once per chunk, so this is ~= extractors x chunks; NOT distinct).
    /// Null until the KG build has run.
    chunksLLM: Integer

    /// Distinct chunks whose embeddings were written to the OSS chunk vector
    /// index. Bounded by the true chunk count. Null until the KG build has run.
    chunksEmbed: Integer

    /// Distinct chunk nodes inserted into the Neptune graph. Bounded by the true
    /// chunk count. Null until the KG build has run.
    chunksGraph: Integer
}

// =============================================================================
// Operations
// =============================================================================
// ── Source CRUD ───────────────────────────────────────────────────────────────
/// List all sources in a namespace across all source types.
@http(method: "GET", uri: "/namespaces/{namespaceId}/sources")
@readonly
@paginated(inputToken: "nextToken", outputToken: "nextToken", pageSize: "maxResults", items: "items")
operation ListSources {
    input := {
        @required
        @httpLabel
        namespaceId: Uuid

        /// Filter by source type (DATABASE or DOCUMENTS).
        @httpQuery("sourceType")
        sourceType: SourceType

        /// Pagination token from a previous response.
        @httpQuery("nextToken")
        nextToken: String

        /// Maximum number of items to return per page.
        @httpQuery("maxResults")
        maxResults: Integer
    }

    output := with [PaginatedOutput] {
        /// Page of source summaries.
        @required
        items: SourceSummaryList
    }
}

/// Create a new source.
/// The body must contain sourceType plus exactly one of:
///   databaseSource  — when sourceType = DATABASE
///   documentSource  — when sourceType = DOCUMENTS
@http(method: "POST", uri: "/namespaces/{namespaceId}/sources")
operation CreateSource {
    input: CreateSourceOperationInput
    output: CreateSourceOperationOutput
}

@input
structure CreateSourceOperationInput {
    @required
    @httpLabel
    namespaceId: Uuid

    @required
    @httpPayload
    body: CreateSourceInput
}

@output
structure CreateSourceOperationOutput {
    @required
    @httpPayload
    body: CreateSourceOutput
}

/// Get a single source by ID.
@http(method: "GET", uri: "/namespaces/{namespaceId}/sources/{sourceId}")
@readonly
operation GetSource {
    input: GetSourceOperationInput
    output: GetSourceOperationOutput
}

@input
structure GetSourceOperationInput {
    @required
    @httpLabel
    namespaceId: Uuid

    /// Identifier of the source to fetch.
    @required
    @httpLabel
    sourceId: String
}

@output
structure GetSourceOperationOutput {
    @required
    @httpPayload
    body: GetSourceOutput
}

/// Delete a source and all associated data.
@http(method: "DELETE", uri: "/namespaces/{namespaceId}/sources/{sourceId}")
operation DeleteSource {
    input := {
        @required
        @httpLabel
        namespaceId: Uuid

        /// Identifier of the source to delete.
        @required
        @httpLabel
        sourceId: String
    }

    output := {
        /// Identifier of the deleted source.
        sourceId: String

        status: SourceStatus
    }
}

/// Re-trigger the scan/ingestion pipeline for an existing source.
@http(method: "POST", uri: "/namespaces/{namespaceId}/sources/{sourceId}/rescan")
operation RescanSource {
    input := {
        @required
        @httpLabel
        namespaceId: Uuid

        /// Identifier of the source to rescan.
        @required
        @httpLabel
        sourceId: String
    }

    output := {
        /// Identifier of the rescanned source.
        sourceId: String

        status: SourceStatus
    }
}

/// Update mutable metadata fields on an existing source (DATABASE only).
@http(method: "PUT", uri: "/namespaces/{namespaceId}/sources/{sourceId}/metadata")
operation UpdateSourceMetadata {
    input := {
        @required
        @httpLabel
        namespaceId: Uuid

        /// Identifier of the source to update.
        @required
        @httpLabel
        sourceId: String

        name: DisplayName

        glueConfiguration: GlueConfiguration

        jdbcConfiguration: JdbcConfiguration

        athenaConfiguration: AthenaConfiguration

        /// Whether AI-powered metadata enrichment runs for this source.
        metadataEnrichmentEnabled: Boolean
    }

    output := {
        /// Identifier of the updated source.
        @required
        sourceId: String

        @required
        status: SourceStatus

        /// Timestamp when the source was last updated.
        @required
        updatedAt: Timestamp
    }
}

/// Generate pre-signed S3 upload URLs for DOCUMENTS sources (upload flow).
@http(method: "POST", uri: "/namespaces/{namespaceId}/sources/upload-urls", code: 200)
operation GetSourceUploadUrls {
    input := {
        @required
        @httpLabel
        namespaceId: Uuid

        /// Files to generate pre-signed upload URLs for.
        @required
        files: UploadFileRequestList
    }

    output := {
        @required
        uploadId: Uuid

        @required
        s3Prefix: S3Prefix

        /// Pre-signed S3 PUT URLs, one per requested file.
        @required
        uploadUrls: UploadUrlList

        /// Seconds until the generated URLs expire.
        @required
        expiresIn: Integer
    }
}

// ── Table operations (DATABASE sources) ──────────────────────────────────────
/// List tables discovered for a DATABASE source with enrichment/review summary.
@http(method: "GET", uri: "/namespaces/{namespaceId}/sources/{sourceId}/tables")
@readonly
@paginated(inputToken: "nextToken", outputToken: "nextToken", pageSize: "maxResults", items: "items")
operation ListSourceTables {
    input := with [PaginatedInput] {
        @required
        @httpLabel
        namespaceId: Uuid

        /// Identifier of the source whose tables to list.
        @required
        @httpLabel
        sourceId: String

        /// Pagination token from a previous response.
        @httpQuery("nextToken")
        nextToken: String

        /// Maximum number of items to return per page.
        @httpQuery("maxResults")
        maxResults: Integer

        @httpQuery("reviewStatus")
        reviewStatus: ReviewStatus
    }

    output := with [PaginatedOutput] {
        /// Page of table summaries.
        @required
        items: TableSummaryList

        /// Number of assets skipped due to retrieval or parsing failures.
        /// Present only when one or more assets could not be loaded, indicating
        /// a degraded (partial) response.
        skippedAssets: Integer
    }
}

/// Get full table metadata including all columns with business metadata.
@http(method: "GET", uri: "/namespaces/{namespaceId}/sources/{sourceId}/tables/{tableId}")
@readonly
operation GetSourceTable {
    input := {
        @required
        @httpLabel
        namespaceId: Uuid

        /// Identifier of the source containing the table.
        @required
        @httpLabel
        sourceId: String

        /// Identifier of the table to fetch.
        @required
        @httpLabel
        tableId: String
    }

    output: GetSourceTableOutput
}

@output
structure GetSourceTableOutput {
    /// Unique table identifier (database.table format).
    @required
    tableId: String

    /// Table name.
    @required
    name: String

    /// Database the table belongs to.
    @required
    database: String

    @required
    reviewStatus: ReviewStatus

    /// Metadata for every column in the table.
    @required
    columns: ColumnMetadataList

    businessMetadata: BusinessMetadataOutput

    primaryKey: PrimaryKeyOutput

    /// Foreign-key relationships reported for the table.
    foreignKeys: ForeignKeyList

    technicalMetadata: TechnicalMetadataOutput
}

// ── Resource-oriented review endpoints ────────────────────────────────────────
//
// Replaces the deprecated ReviewSourceMetadata "god endpoint" with six
// single-purpose operations. Bulk approve/reject is async (202) to avoid the
// API Gateway 29s timeout for sources with 75+ tables. Per-table and
// per-column review/edit are synchronous and fast (~400ms) because they
// operate on a single asset.
//
// Cascade semantics:
//   ApproveSource cascades APPROVED to all PENDING tables and PENDING columns.
//   ReviewSourceTable APPROVED cascades to PENDING columns on that table only.
//   Neither cascade overrides REJECTED status — explicit re-approval required.
//
// Idempotency:
//   All review endpoints are idempotent. Approving an already-approved
//   resource is a 200 no-op. The async bulk endpoints transition status
//   PENDING_REVIEW → APPROVING via a conditional DynamoDB update so duplicate
//   SQS messages cannot start two workers.
//
// Edit vs. review separation:
//   UpdateSourceTableMetadata / UpdateSourceColumnMetadata sets
//   enrichmentSource = STEWARD_EDITED and does NOT change reviewStatus.
//   "Edit + Approve" UX is two calls: PATCH /metadata, then PUT /review.
/// Approve all PENDING tables and columns of a DATABASE source asynchronously.
///
/// Returns 202 immediately and transitions the source's status to APPROVING.
/// A background worker writes DataZone asset revisions for any asset whose
/// review status actually changes. On completion the source's status becomes
/// APPROVED; on failure it becomes APPROVAL_FAILED. Clients poll the source's
/// status via GetSource until it leaves the APPROVING state.
///
/// Returns 409 if the source is not in PENDING_REVIEW or APPROVAL_FAILED.
@http(method: "POST", uri: "/namespaces/{namespaceId}/sources/{sourceId}/approve", code: 202)
operation ApproveSource {
    input := {
        @required
        @httpLabel
        namespaceId: Uuid

        /// Identifier of the source to approve.
        @required
        @httpLabel
        sourceId: String
    }

    output := {
        /// Identifier of the source being approved.
        @required
        sourceId: String

        @required
        status: SourceStatus
    }
}

/// Reject all PENDING tables and columns of a DATABASE source asynchronously.
///
/// Returns 202 immediately and transitions the source's status to REJECTING.
/// A background worker writes DataZone asset revisions, marking each
/// non-REJECTED column as REJECTED. On success the source returns to
/// PENDING_REVIEW (rejection does not approve the source overall — the
/// steward can re-review). On failure it becomes REJECTION_FAILED. Clients
/// poll the source's status via GetSource until it leaves the REJECTING state.
///
/// Returns 409 if the source is not in PENDING_REVIEW or REJECTION_FAILED.
@http(method: "POST", uri: "/namespaces/{namespaceId}/sources/{sourceId}/reject", code: 202)
operation RejectSource {
    input := {
        @required
        @httpLabel
        namespaceId: Uuid

        /// Identifier of the source to reject.
        @required
        @httpLabel
        sourceId: String
    }

    output := {
        /// Identifier of the source being rejected.
        @required
        sourceId: String

        @required
        status: SourceStatus
    }
}

/// Review a single table — set reviewStatus to APPROVED or REJECTED.
///
/// Cascades to PENDING columns on that table only (REJECTED columns are not
/// touched). DataZone asset revision is only written when the resulting
/// status differs from the existing one.
@http(method: "PUT", uri: "/namespaces/{namespaceId}/sources/{sourceId}/tables/{tableId}/review")
@idempotent
operation ReviewSourceTable {
    input := {
        @required
        @httpLabel
        namespaceId: Uuid

        /// Identifier of the source containing the table.
        @required
        @httpLabel
        sourceId: String

        @required
        @httpLabel
        tableId: TableId

        @required
        decision: ReviewDecision
    }

    output := {
        /// Identifier of the reviewed table.
        @required
        tableId: String

        @required
        reviewStatus: ReviewStatus
    }
}

/// Edit a single table's business metadata (description, synonyms, glossary
/// terms, tags). Sets enrichmentSource = STEWARD_EDITED.
///
/// Does NOT change reviewStatus. Use ReviewSourceTable to approve/reject.
@http(method: "PATCH", uri: "/namespaces/{namespaceId}/sources/{sourceId}/tables/{tableId}/metadata")
operation UpdateSourceTableMetadata {
    input := {
        @required
        @httpLabel
        namespaceId: Uuid

        /// Identifier of the source containing the table.
        @required
        @httpLabel
        sourceId: String

        @required
        @httpLabel
        tableId: TableId

        @required
        overrides: MetadataOverrides
    }

    output := {
        /// Identifier of the updated table.
        @required
        tableId: String

        @required
        reviewStatus: ReviewStatus
    }
}

/// Review a single column on a table — set reviewStatus to APPROVED or REJECTED.
///
/// DataZone asset revision is only written when the resulting status differs
/// from the existing one.
@http(method: "PUT", uri: "/namespaces/{namespaceId}/sources/{sourceId}/tables/{tableId}/columns/{columnName}/review")
@idempotent
operation ReviewSourceColumn {
    input := {
        @required
        @httpLabel
        namespaceId: Uuid

        /// Identifier of the source containing the column.
        @required
        @httpLabel
        sourceId: String

        @required
        @httpLabel
        tableId: TableId

        @required
        @httpLabel
        columnName: ColumnName

        @required
        decision: ReviewDecision
    }

    output := {
        /// Identifier of the table containing the column.
        @required
        tableId: String

        /// Name of the reviewed column.
        @required
        columnName: String

        @required
        reviewStatus: ReviewStatus
    }
}

/// Edit a single column's business metadata. Sets
/// enrichmentSource = STEWARD_EDITED. Does NOT change reviewStatus.
@http(
    method: "PATCH"
    uri: "/namespaces/{namespaceId}/sources/{sourceId}/tables/{tableId}/columns/{columnName}/metadata"
)
operation UpdateSourceColumnMetadata {
    input := {
        @required
        @httpLabel
        namespaceId: Uuid

        /// Identifier of the source containing the column.
        @required
        @httpLabel
        sourceId: String

        @required
        @httpLabel
        tableId: TableId

        @required
        @httpLabel
        columnName: ColumnName

        @required
        overrides: MetadataOverrides
    }

    output := {
        /// Identifier of the table containing the column.
        @required
        tableId: String

        /// Name of the updated column.
        @required
        columnName: String

        @required
        reviewStatus: ReviewStatus
    }
}

/// Edit a table's primary key and foreign key relationships. Steward intent is
/// authoritative: affected keys are stored with source = STEWARD_SPECIFIED,
/// overriding deterministic or AI-inferred keys. Provide `primaryKey` and/or
/// `foreignKeys` — an omitted field is left unchanged; an empty list clears it.
/// Key columns are validated against the table's columns. Does NOT change
/// reviewStatus.
@http(method: "PATCH", uri: "/namespaces/{namespaceId}/sources/{sourceId}/tables/{tableId}/keys")
operation UpdateSourceTableKeys {
    input := {
        @required
        @httpLabel
        namespaceId: Uuid

        /// Identifier of the source containing the table.
        @required
        @httpLabel
        sourceId: String

        @required
        @httpLabel
        tableId: TableId

        primaryKey: PrimaryKeyInput

        /// Foreign-key relationships to set for the table.
        foreignKeys: ForeignKeyInputList
    }

    output := {
        /// Identifier of the updated table.
        @required
        tableId: String

        @required
        reviewStatus: ReviewStatus
    }
}

// ── Scan job (DATABASE sources) ───────────────────────────────────────────────
/// Get the status and results of a scan job for a DATABASE source.
@http(method: "GET", uri: "/namespaces/{namespaceId}/sources/{sourceId}/scan/{jobId}")
@readonly
operation GetSourceScanJob {
    input := {
        @required
        @httpLabel
        namespaceId: Uuid

        /// Identifier of the source the scan job belongs to.
        @required
        @httpLabel
        sourceId: String

        /// Identifier of the scan job to fetch.
        @required
        @httpLabel
        jobId: String
    }

    output: GetSourceScanJobOutput
}

@output
structure GetSourceScanJobOutput {
    /// Unique scan job identifier.
    @required
    jobId: String

    /// Identifier of the source scanned.
    @required
    sourceId: String

    @required
    status: SourceScanJobStatus

    /// Number of tables discovered by the scan.
    tablesDiscovered: Integer

    /// Number of tables added since the previous scan.
    tablesAdded: Integer

    /// Number of tables removed since the previous scan.
    tablesRemoved: Integer

    /// Number of tables modified since the previous scan.
    tablesModified: Integer

    /// Timestamp when the scan job started.
    @required
    startedAt: Timestamp

    /// Timestamp when the scan job completed.
    completedAt: Timestamp

    /// Error message if the scan job failed.
    errorMessage: String
}
