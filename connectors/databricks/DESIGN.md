# Databricks SQL Warehouse connector — design notes

Why this connector is built the way it is, and what was measured rather than assumed. None of it is
needed to deploy or operate the connector — [`README.md`](README.md) is the runbook, and it is
self-contained. Read this when you are changing the connector, reviewing it, or asking why some
decision went the way it did.

## Contents

- [Why one deployment is one endpoint](#why-one-deployment-is-one-endpoint)
- [Why identifiers must be bare and lower-case](#why-identifiers-must-be-bare-and-lower-case)
- [Driver licence](#driver-licence)
- [Push-down: what is and is not advertised](#push-down-what-is-and-is-not-advertised)
- [Measured performance and cost](#measured-performance-and-cost)
- [What it does under the hood](#what-it-does-under-the-hood)

## Why one deployment is one endpoint

The UC catalog cannot travel in a request, and that is structural rather than a simplification. An
Athena federated catalog has exactly **one** namespace level below the registered catalog name, and
this connector spends it on the UC *schema* — so `catalog.schema.table` addressing has nowhere left to
put the UC catalog. Flattening it into a `catalog__schema` schema name was rejected: that name appears
in neither Databricks nor the ontology, so nobody could look it up.

`ConnectionConfigProvider` is nevertheless an interface. It resolves the configuration for the Athena
catalog name a request arrived under, and this connector's implementation reads the environment once
and ignores the argument, because one deployment is one endpoint. The seam exists because Athena passes
the registered catalog name to a connector verbatim on every call path, and that name is the
discriminator a future multiplexed deployment would resolve on — so adding one is a new class rather
than a reshaping of every call site.

## Why identifiers must be bare and lower-case

`DATABRICKS_CATALOG` and `DATABRICKS_SCHEMA` must both match `^[a-z_][a-z0-9_]*$` after folding. That
is stricter than Unity Catalog, though less so than it first appears: measured, UC **rejects** a table
name containing a space, period or forward slash outright, and Delta rejects a space in a column name
unless Column Mapping is enabled. What UC does allow, and what therefore has to be quoted in generated
SQL, is a hyphen or a reserved word — Databricks says so itself:
`[INVALID_IDENTIFIER] The unquoted identifier odd-name-table is invalid and must be back quoted`.

**The reason for refusing such a name is Athena rather than Databricks.** Athena's `SHOW`/`DESCRIBE`
parser accepts backticks and rejects double quotes, while its `SELECT` parser does the reverse — so a
name needing quotes is not addressable through both. Refusing it is the only option that cannot
mis-address a table at query time.

The fold is applied because Unity Catalog stores catalog, schema and **table** names lower-cased. It
does **not** lower-case **column** names — see
[Table names are lower-cased; column names are not](#table-names-are-lower-cased-column-names-are-not).
The same fold is applied independently in the CDK app, so `DATABRICKS_SCHEMA=Sales` cannot deploy
cleanly and then fail every query against a connector that only answers to `sales`.

## Driver licence

**This connector bundles `com.databricks:databricks-jdbc:3.4.2`, which is Apache-2.0 licensed and may
be redistributed in a Lambda deployment package.**

Read that version number carefully, because Databricks publishes **two different drivers under the
same `groupId` *and* the same `artifactId`**, distinguishable only by version:

| Versions | Driver | Licence | Redistributable here |
| --- | --- | --- | --- |
| `3.x`, and `0.9.x-oss` / `1.0.x-oss` before it | Open-source Databricks JDBC driver, [github.com/databricks/databricks-jdbc](https://github.com/databricks/databricks-jdbc) | **Apache License 2.0** | **Yes** |
| `2.6.x`, `2.7.x`, `2.8.x` | Simba-derived Databricks JDBC Driver | [Databricks JDBC Driver License](https://databricks.com/jdbc-odbc-driver-license) | Not evaluated — do not use |

`2.8.3` is *numerically lower* than `3.0.1` but is the proprietary line, and a Maven version range or
a well-meaning dependency bump could cross between them without anyone noticing. The version is
pinned in a named property in `pom.xml` with that warning attached.

Verified from the artifacts themselves, not from documentation: the `3.4.2` POM declares
`Apache License, Version 2.0` and points at the GitHub repository; the `2.7.3` POM declares
`Databricks JDBC Driver License`. The `3.4.2` jar contains no Simba code and relocates every bundled
dependency under `com.databricks.internal.*`, so it introduces no class conflicts with the federation
SDK's Arrow.

`athena-jdbc` and the federation SDK are both Apache-2.0. Attribution files (`META-INF/LICENSE`,
`META-INF/NOTICE`) are preserved in the shaded jar.

## Push-down: what is and is not advertised

**As shipped, this connector advertises nothing** — and, measured against a live warehouse, that costs
nothing. This section was rewritten after the end-to-end pass, because the premise it previously rested
on turned out to be false.

### What was measured

A deployed connector, a registered Athena catalog, and the same six query shapes run twice: once with
an empty capability map and once advertising `filter,limit,topn`. Evidence from two independent
channels — the statements Databricks recorded in `system.query.history`, and the connector's own
`Read N rows` log line.

With **nothing advertised**, the statements that reached the warehouse were:

```
SELECT `region_code`, `order_num` FROM `workspace`.`coa_dbx_test`.`orders` WHERE (`region_code` = ?)
SELECT `order_num`               FROM `workspace`.`coa_dbx_test`.`orders` LIMIT 1
SELECT `order_num`               FROM `workspace`.`coa_dbx_test`.`orders` LIMIT 3
SELECT `order_num`               FROM `workspace`.`coa_dbx_test`.`orders` ORDER BY `order_num` DESC NULLS LAST LIMIT 2
```

The predicate, the `LIMIT` and the top-N `ORDER BY ... NULLS LAST LIMIT` were **all** pushed into the
warehouse with the capability map empty. Advertising `filter,limit,topn` produced **byte-identical
statements** and identical row counts across all six shapes.

| Query shape | Rows off the warehouse, nothing advertised | Advertising `filter,limit,topn` |
| --- | --- | --- |
| `WHERE region_code='EU'` (2 of 5 rows) | 2 | 2 |
| `WHERE region_code='APAC'` (1 of 5) | 1 | 1 |
| `LIMIT 1` | 1 | 1 |
| `LIMIT 3` | 3 | 3 |
| `ORDER BY order_num DESC LIMIT 2` | 2 | 2 |
| `WHERE region_code='EU' LIMIT 1` | 2 | 2 |

### What that means, and the claim it corrects

**Athena populates `Constraints.getSummary()`, `getLimit()` and `getOrderByClause()` regardless of the
capability map.** Those are part of the base `ReadRecords` payload, not something the advertisement
unlocks. Since this connector's query builder reads all three unconditionally, the predicate and the
limit reach the warehouse either way.

> An earlier version of this section — and the design it came from — claimed that *"Athena pushes
> nothing into a connector that advertises nothing: it requests whole tables and applies the predicate
> and the `LIMIT` itself."* **That is wrong**, at least for simple single-table predicates, integer
> limits and top-N. It is the premise the whole feature was justified on, so it is worth stating
> plainly rather than quietly fixing.

One shape behaves differently and is worth knowing: with **both** a predicate and a limit
(`WHERE region_code='EU' LIMIT 1`), Athena pushes the predicate but **not** the limit — 2 rows, in both
configurations. It applies the limit itself after filtering.

### Why the default is still silence

Because the advertisement bought nothing measurable, and it is still a guarantee:

- **No measured benefit.** Identical SQL, identical rows off the warehouse, in every shape tested.
- **Not advertising is the safer of two equal options.** With an empty map Athena re-applies the
  predicate and the limit on its own side after the connector returns. That is a correctness safety
  net — if the builder ever spelled a predicate in a way that under-filtered, Athena would still return
  the right answer — and it costs Athena CPU rather than warehouse reads.
- **An advertisement is a promise Athena will hold the connector to.** Advertise `limit` and Athena may
  stop applying it; if the record path ever dropped `Constraints.getLimit()` the query would return too
  many rows with nothing erroring.

So: `DATABRICKS_ADVERTISE_PUSHDOWN` remains unset by default. Setting it is supported and harmless on
the evidence above, but on that same evidence it is not worth doing.

```bash
DATABRICKS_ADVERTISE_PUSHDOWN=filter,limit,topn pnpm run deploy   # supported; no measured effect
```

| Name | Advertises | Measured effect |
| --- | --- | --- |
| `filter` | Predicates: comparison, range, `IN`, null checks | None — pushed anyway |
| `limit` | `LIMIT n` with an integer constant | None — pushed anyway |
| `topn` | `ORDER BY ... LIMIT n` | None — pushed anyway |

Those three are the whole list. An unrecognised name is rejected at synth by the CDK app, so a typo
cannot deploy and silently advertise less than you asked for.

**Complex-expression push-down is not offered, and could not be made to work by a flag.** The
connector uses `JdbcSplitQueryBuilder`'s single-argument constructor, which installs
`DefaultJdbcFederationExpressionParser` — whose `mapFunctionToDataSourceSyntax` is an unconditional
`throw` in 2025.15.1: *"Subclass does not yet support complex expressions."* (verified in that
release's source and bytecode). Offering a switch for it would guarantee a run-time failure the first
time Athena pushed a function expression, so there is no switch. Supporting it means writing a
Databricks-specific `FederationExpressionParser` that maps each `FunctionName` Athena may send to
Spark SQL's spelling — future work, not configuration.

**Aggregation is not on the list either, and cannot be.** See
[The aggregation caveat](README.md#the-aggregation-caveat).

## Measured performance and cost

From the end-to-end pass against a Serverless PRO **Small** warehouse (10-minute auto-stop) and a
5-row `orders` table, so these are floor figures — they measure orchestration, not data volume.

| Measurement | Value | Source |
| --- | --- | --- |
| Lambda init (cold start) | **1.44 – 1.51 s** (4 samples) | Lambda `Init Duration` |
| Lambda duration, metadata call | 2 – 5 ms | Lambda `REPORT` |
| Lambda duration, row read | 0.55 – 1.34 s | Lambda `REPORT` |
| Peak Lambda memory | **281 MB** of 3008 MB | Lambda `REPORT` |
| Athena planning | 0.53 – 0.78 s | `QueryPlanningTimeInMillis` |
| **Athena total, warm Lambda + warm warehouse** | **8.3 – 12.7 s** | `TotalExecutionTimeInMillis` |
| Athena total, cold Lambda | ~18 s wall clock | measured end to end |
| Warehouse statement duration, average | 784 ms | `system.query.history` |

**The warm end-to-end figure is the surprising one, and it does not meet the design target.** The LLD
budgets p50 3 s / p95 6 s for a warm single-source query; the measured floor on a five-row table is
8.3 s, with 12.7 s seen. The time is not in the Lambda (milliseconds for metadata, under 1.4 s for the
read) and not in Athena planning (under 0.8 s) — it is Athena's federation orchestration, which makes
a separate Lambda invocation per protocol step and schedules each one. **Anyone quoting a latency
budget for this route should start from ~8 s, not ~6 s, and treat it as roughly independent of table
size.** Memory is heavily over-provisioned at 3008 MB for peak 281 MB; it is sized for the aggregate
read described in [The aggregation caveat](README.md#the-aggregation-caveat), not for these queries.

### Cost, and why a per-query DBU figure is the wrong unit

No measured DBU figure is available: `system.billing.usage` lags by hours and had no rows for the test
window. `system.billing.list_prices` is readable and gives **$0.70 per DBU** for US East/West serverless
SQL.

What the pass does establish is that **for a sparse query pattern the auto-stop tail dominates, not the
rows scanned.** The whole exercise — 135 statements including a full discovery pass — consumed **105.9 s**
of warehouse statement time. A single query keeps a 10-minute-auto-stop warehouse alive for 600 s. So
the billed unit for one occasional COA question is ten minutes of warehouse uptime, roughly **6× all the
statement time this entire test generated**.

That matters because it inverts the advice for two different usage patterns:

- **Sparse/interactive** — a steward asking occasional questions. Cost is dominated by resumes and idle
  tails. Push-down is irrelevant; a shorter auto-stop, or accepting the resume latency, is the lever.
- **Dense/aggregate** — the case [The aggregation caveat](README.md#the-aggregation-caveat) describes.
  Cost is dominated by rows read out of the warehouse, and the row ceiling is the lever.

The connector's own statement profile, for sizing a discovery pass:

| Statement | Count | Average |
| --- | --- | --- |
| Row reads (`SELECT`) | 100 | 826 ms |
| `SHOW TABLES` | 26 | 230 ms |
| `information_schema.columns` | 1 | 8610 ms (first, cold metastore) |
| `information_schema.tables` | 2 | 1771 ms |
| Constraint reads (PK + FK) | 6 | ~850 ms |

Note the first `information_schema.columns` read cost **8.6 s** against a cold metastore and dropped to
sub-second afterwards — so the first table of a first scan is far slower than the rest, which is worth
knowing before concluding a scan has hung.

## What it does under the hood

### Enumeration uses `SHOW TABLES`, not `information_schema.tables`

Creating a materialized view or a streaming table **also creates internal side tables**, and
`information_schema.tables` lists them as ordinary user tables:

```
__materialization_mat_96ea77da_..._order_counts_mv_1   MANAGED
event_log_96ea77da_...                                 MANAGED
```

Measured on a small fixture schema: `information_schema.tables` returned **sixteen** rows where
`SHOW TABLES` returned **eleven**. Four of the five extra rows are those internal side tables and the
fifth is a shallow clone. Left in, the side tables would be discovered, enriched by an LLM, and
presented to a steward as real tables — a quarter of the ontology being Databricks' own bookkeeping.
**No column of that view distinguishes them**; across all fifteen of its columns they are shaped
exactly like a genuine `MANAGED` table.

`SHOW TABLES IN <catalog>.<schema>` omits them, so it is the authority on what exists.
`information_schema.tables` is still read for each object's `table_type`, and the answer is the
intersection. Filtering on the `__` and `event_log_` name prefixes was rejected: undocumented internal
naming, and a customer table legitimately called `event_log_2026` would vanish.

### `table_type` is never `'BASE TABLE'`

The ANSI spelling does not exist in Unity Catalog. Databricks returns `MANAGED`, `EXTERNAL`, `VIEW`,
`MATERIALIZED_VIEW`, `STREAMING_TABLE`, `FOREIGN`, `MANAGED_SHALLOW_CLONE` and
`EXTERNAL_SHALLOW_CLONE`.

This connector exposes the first five. `FOREIGN` is excluded because it is a table federated into Unity
Catalog from somewhere else, and reading it through two federation layers is slower and less faithful
than onboarding its own source; the two shallow-clone types are excluded because their rows duplicate
another table's, and including them would put the same facts in the ontology twice under two names.

The failure mode of getting this wrong is worth knowing, because it is not the obvious one. A filter of
`table_type IN ('BASE TABLE', 'VIEW')` does not return nothing: `'VIEW'` *is* a Databricks value, so
measured on that fixture schema it returns **one** row. A steward sees a source that scanned
successfully and contains views but no tables, which reads as a permissions problem rather than as a
dialect bug.

### Table names are lower-cased; column names are not

Measured, and neither half is documented:

- `CREATE TABLE MixedCaseTable` → `information_schema.tables.table_name` = `mixedcasetable`
- `CustomerName STRING` → `information_schema.columns.column_name` = `CustomerName`

So catalog, schema and table names are compared against lower-case literals, and a **column name is
carried through exactly as returned**. Lower-casing it would generate SQL naming a column that does not
exist.

### `ordinal_position` counts from a different base in different views

Measured: `information_schema.columns.ordinal_position` is **0-based**;
`information_schema.key_column_usage.ordinal_position` is **1-based**. Both are only used for
`ORDER BY` here, which is base-agnostic. Joining the two views on `ordinal_position` would be off by
one — silently, pairing each key column with its neighbour.

### Constraints use the ANSI join

The obvious join — `key_column_usage` to `constraint_column_usage` on `constraint_name` — produces an
N x N cartesian product for a composite key and pairs child columns with the wrong parents. It is not
obviously wrong, because for a single-column key N x N is 1 x 1 and the answer is right. Measured
against a two-column foreign key it returns **four rows, two of them wrong**.

This connector walks `referential_constraints` to the referenced constraint and matches
`kcu.position_in_unique_constraint = ref.ordinal_position`, which is the ANSI-defined pairing and the
only one correct for a composite key. It was verified live, once, by hand — the record is the connector
LLD's §8.1. **Nothing in the committed test suite asserts it**, because the integration suites that did
were removed with the workspace they needed; see [Tests](README.md#tests). So the reason for the
complexity is now asserted in a comment rather than checked, which is exactly the shape of thing that
gets simplified back into a bug. Rebuilding that assertion is the first thing to do on acquiring a
workspace.

### Comment tags are the connector's channel only

A column comment in Unity Catalog is set with `COMMENT ON COLUMN`, which anyone with `MODIFY` can run.
So a comment reading `"customer surrogate key @pk"` would mint a primary key Unity Catalog never
declared, and `"@fk(payroll.ssn)"` would assert a relationship into a table that may not exist. By the
time COA sees it, a hand-written tag is byte-identical to a generated one.

This connector therefore **strips any live `@pk` or `@fk(...)` out of the comment before forwarding
it**, and emits tags only from `information_schema`. Near misses are left alone, because COA leaves
them alone: `@PK`, `@pkey`, `@pk=x`, `@pk(x)` and `owner bob@pk.example.com` are prose on both sides —
removing them here would delete text a customer wrote and COA would have stored.

There is a second step, and it is the one that stops a bad comment taking down a scan. COA's parser
*keeps* a malformed tag — an `@fk(` that never closes — as feedback to whoever wrote it, so the strip
step keeps it too. But the toolkit's encoder refuses **any** `@fk(` in prose, closed or not, and
refuses by throwing: its guard exists to stop a *connector author* hand-writing a tag. Here the prose
is a *customer's*, which COA cannot ask to be corrected and whose author will never see the complaint.
A single Unity Catalog comment of `'Line total @fk(orders.order_id'` was therefore enough to fail that
table's `DESCRIBE` permanently and take the whole schema's scan with it. So the connector neutralises
the surviving token, keeps the prose, and logs it.

### Credentials never touch the JDBC URL

The Databricks JDBC URL is a `;`-delimited property list, so anything concatenated into it can be split
by a `;`: an injected `SSL=0` downgrades TLS, `ProxyHost` redirects an authenticated session, and
`LogPath` with `LogLevel=6` writes connection details — credentials included — to the Lambda's disk.

The URL this connector builds is exactly `jdbc:databricks://<host>:443`. Everything else — HTTP path,
catalog, schema, credential, every hardening property — is set on a `java.util.Properties` object,
which the driver reads as a map and never parses. The configuration patterns are the second layer.

### Five driver defaults are overridden

| Property | Default | Here | Why |
| --- | --- | --- | --- |
| `TemporarilyUnavailableRetry` | `1` | `0` | Retries a stopped warehouse for up to 900 s, outliving the 120 s invocation timeout |
| `socketTimeout` | 900 s | 90 s | Same: a timeout above the invocation timeout can never fire |
| `EnableTelemetry` | `1` | `0` | The driver reports usage to Databricks by default |
| `LogLevel` | OFF | `0`, pinned | Driver logging is how a connection string containing `PWD=` reaches disk |
| `IgnoreTransactions` | `0` | `1` | Makes the inherited read loop's `setAutoCommit(false)` and `commit()` no-ops instead of two extra warehouse round trips per read |

### Logging

The module ships an SLF4J binding (`slf4j-simple`) with levels pinned in
`src/main/resources/simplelogger.properties`. Without a binding, every log line from the connector
**and from the federation SDK** is discarded — the reference connector prints
`No SLF4J providers were found` on every invocation.

Enabling logging is a data-exposure change, not pure operability, so the levels are pinned rather than
defaulted: the connector at INFO, and the federation SDK and `athena-jdbc` at WARN, because the SDK's
request logging and `JdbcSplitQueryBuilder`'s statement logging carry constraint literals derived from
the user's question. The connector logs one cold-start line naming the endpoint and one row count per
read, and never the credential, the JDBC URL, or a predicate value. Every error message is passed
through a redactor that replaces the value of any credential-bearing property.

The Databricks driver is a separate case: it bundles its **own relocated** SLF4J bound to a
`java.util.logging` provider, so `simplelogger.properties` cannot reach it. `LogLevel=0` on the
connection is the control that works.

`JAVA_TOOL_OPTIONS` cannot be used for any of this — the CDK construct owns it outright and refuses a
stack that sets it.

### What is inherited from `athena-jdbc`

The **record path**: `JdbcRecordHandler`'s read loop and its typed per-column extractors for eleven
Arrow types, and `JdbcSplitQueryBuilder`'s translation of Athena's constraint model into a prepared
statement — projection list, `WHERE` from each column's value set, typed parameter binding,
identifier quoting, `ORDER BY` and `LIMIT`.

The **metadata path is not inherited**, because it builds a table's schema from JDBC result-set
metadata, which carries neither comments nor constraints, and never queries the catalog for them.
Comments are the channel declared keys travel through, so a metadata handler that cannot read them
cannot deliver the feature.

The **multiplexing handlers are not inherited** either. They route on the catalog name exactly as a
multi-endpoint deployment would, but they build their routing table once at construction from
environment variables named per catalog — against Lambda's 4 KB total environment limit and with a hard
ceiling of 100 catalogs.

The published `athena-jdbc` artifact is a 46 MB shaded uber-jar carrying its own copy of the federation
SDK, Arrow, the AWS SDK, Netty, log4j-core, Bouncy Castle and HikariCP. Unfiltered, every one of those
competes by path with the artifact that legitimately provides it and the shade plugin keeps whichever
it saw first. `pom.xml` filters it to the 39 classes this connector actually extends.
