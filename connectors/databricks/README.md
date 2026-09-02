# Databricks SQL Warehouse connector

An Athena Query Federation connector for **one** Databricks SQL Warehouse and **one** Unity Catalog
catalog — either one schema inside it or every schema inside it. You deploy it into your own account
and onboard it in COA as a `CUSTOM_CONNECTOR` source by handing COA the **Lambda's ARN**. COA registers
the Athena data catalog itself, in its own account.

What it gives COA that a generic connector does not: **declared primary and foreign keys**. It reads
Unity Catalog's own `information_schema` and emits each declared key as a `@pk` / `@fk` comment tag,
so relationships in the ontology come from what your data engineers declared rather than from
guessing at column-name overlap.

> Read [`connectors/README.md`](../README.md) first if you have not. It explains what a connector is,
> the eleven non-obvious constraints of the federation SDK, and the spill mechanism — none of which is
> repeated here.
>
> [`DESIGN.md`](DESIGN.md) is the companion to this file: the bundled driver's licence trap, what
> push-down was measured to do, the latency and cost figures, and how the `information_schema` reads
> work. None of it is needed to deploy or operate the connector.

## Contents

**Runbook:** [Quick start](#quick-start) · [What one deployment covers](#what-one-deployment-covers) ·
[Configuration](#configuration) · [Creating the credential secret](#creating-the-credential-secret) ·
[Unity Catalog grants](#unity-catalog-grants) · [Deploy](#deploy) ·
[Register it in COA](#register-it-in-coa)

**Before you commit to it, and when it breaks:** [The aggregation caveat](#the-aggregation-caveat) ·
[Limits](#limits) · [Types](#types) · [Tests](#tests) · [Monitoring](#monitoring) ·
[When it goes wrong](#when-it-goes-wrong)

**Design and measurement**, in [`DESIGN.md`](DESIGN.md):
[Driver licence](DESIGN.md#driver-licence) ·
[Push-down](DESIGN.md#push-down-what-is-and-is-not-advertised) ·
[Measured performance and cost](DESIGN.md#measured-performance-and-cost) ·
[What it does under the hood](DESIGN.md#what-it-does-under-the-hood)

## Quick start

The shortest path from nothing to a queryable source, for evaluating the connector against a schema you
already have. Every step links to the section that explains it; read those before deploying anything you
intend to keep, because two of the choices below are deliberately the *weaker* ones.

You need: a SQL Warehouse you can reach, `CAN USE` on it, permission to grant Unity Catalog privileges,
and a CDK-bootstrapped AWS account in **COA's own region**.

```bash
# 1. A personal access token is the fastest credential to get hold of. Prefer OAuth M2M for
#    anything lasting - see "Creating the credential secret".
#    In Databricks: Settings -> Developer -> Access tokens -> Generate new token.
aws secretsmanager create-secret --name databricks-connector-pat \
  --secret-string '{"token":"<dapi...>"}'          # note the returned ARN

# 2. Grant the token's user the three Unity Catalog privileges. SELECT is the one people miss,
#    and omitting it lets a scan succeed and every query fail - see "Unity Catalog grants".
#    Run in a Databricks SQL editor:
#      GRANT USE CATALOG ON CATALOG <catalog>          TO `<principal>`;
#      GRANT USE SCHEMA  ON SCHEMA  <catalog>.<schema> TO `<principal>`;
#      GRANT SELECT      ON SCHEMA  <catalog>.<schema> TO `<principal>`;

# 3. Deploy. Four required variables, all from the warehouse's Connection details tab.
cd connectors
cp .env.example .env && pnpm install
export AWS_REGION=<COA's region>                   # in your shell, not just .env
export DATABRICKS_WORKSPACE_HOSTNAME=dbc-a1b2345c-d6e7.cloud.databricks.com
export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/a1b234c567d8e9fa
export DATABRICKS_CATALOG=main
export DATABRICKS_SCHEMA=sales                     # optional, but pin it while evaluating
export CREDENTIAL_SECRET_ARN=<the ARN from step 1>
cd databricks/cdk && pnpm run deploy               # prints ConnectorFunctionArn

# 4. Register it in COA, with that ARN and the schema name.
#    POST /namespaces/{namespaceId}/sources - see "Register it in COA" for the body.
```

Then scan the source and check that primary and foreign keys arrived. If they did not, the **discovery**
role was not granted invoke, so nothing ever ran `DESCRIBE` — queries will work regardless, which is what
makes it easy to miss. [Checking the tags arrived](#checking-the-tags-arrived-in-your-own-account) shows
how to verify from your own account before involving COA.

**What this skips, and why you should not ship it as-is.** Step 1 uses a personal access token, which
carries a human identity and dies with the human; [OAuth machine-to-machine](#oauth-machine-to-machine--preferred)
is the one to use. Step 3 pins `DATABRICKS_SCHEMA`, which is right for evaluation but is a one-schema
deployment — [Pinning to one schema](#pinning-to-one-schema) explains the security trade-off in leaving
it unset. And before committing to the connector at all, read [The aggregation caveat](#the-aggregation-caveat):
`GROUP BY` is computed in Athena, not in the warehouse, and that shapes which questions are affordable.

## What one deployment covers

One deployment serves **one workspace, one warehouse, one Unity Catalog catalog and one credential**,
all fixed at deploy time. Within that catalog it serves either **one schema or all of them**, depending
on `DATABRICKS_SCHEMA` — see [Pinning to one schema](#pinning-to-one-schema). The UC catalog cannot
travel in a request, and that is structural rather than a simplification;
[`DESIGN.md`](DESIGN.md#why-one-deployment-is-one-endpoint) explains why.

Consequences to plan for:

| You want | You deploy |
| --- | --- |
| A second UC schema in the same catalog | Nothing — leave `DATABRICKS_SCHEMA` unset and this connector serves every schema in the catalog, one COA source per schema. Or a second copy, if you want each pinned |
| A second UC catalog | A second copy of this connector, with `FUNCTION_NAME_PREFIX` set |
| A second warehouse, even in the same workspace | A second copy. `DATABRICKS_HTTP_PATH` is required and names exactly one warehouse |
| A second workspace | A second copy |
| A second credential — a different service principal, or a PAT alongside an M2M secret | A second copy. `CREDENTIAL_SECRET_ARN` names one secret, and that secret's shape is what selects the auth mode |
| Two COA environments against one warehouse | Nothing, if both are in this region: each COA deployment registers **its own** Athena data catalog against the same Lambda, so list both deployments' serve and discovery roles in `queryRoleArns` and one copy serves both. A COA deployment in another region needs its own copy — see [Deploy](#deploy) |

### Pinning to one schema

`DATABRICKS_SCHEMA` is **optional**, and the choice is a security decision rather than a convenience
one.

| | `DATABRICKS_SCHEMA` set | `DATABRICKS_SCHEMA` unset |
| --- | --- | --- |
| `SHOW DATABASES` returns | just that schema | every schema in the catalog it can serve — excluding `information_schema`, and excluding any name that is not a bare SQL identifier (each one logged at WARN, so a missing schema is diagnosable) |
| A query naming another schema | refused by the connector, with a message naming the pin | served, if the schema exists and the credential can read it |
| Containment boundary | the connector **and** the credential's UC grants | the credential's UC grants **only** |
| COA sources per deployment | one | one per schema, each selecting its schema with `databaseName` |

**Unset is the more convenient default and the weaker one.** Pinning is enforced by this connector
itself, independently of Unity Catalog: a pinned connector refuses `SELECT * FROM cat.other.t` even if
its credential could read `other`. Unpinned, that refusal is gone and the credential's own grants are
all that stands between a query and every schema in the catalog. So if you leave it unset, **scope the
credential's principal to exactly the schemas you intend to expose** — grant `USE SCHEMA` and `SELECT`
per schema rather than at catalog level, and do not reuse a broadly-granted service principal.

Unset also does not widen what a *single* COA source sees: a source is still one schema, chosen by its
`databaseName`. What changes is how many sources one deployment can back.

A twenty-schema catalog is therefore twenty COA **sources** against **one** deployment. Each of those
sources gets its own Athena data catalog, registered by COA in COA's own account — so the account and
region whose **data-catalog cap** you need headroom in is COA's, not yours, and that cap is shared with
every other federated source COA holds. Check it before onboarding at scale.

Nothing about a deployed connector can be corrected in place except through a redeploy: the
coordinates are Lambda environment variables. Changing the warehouse, the schema or the secret ARN is
`pnpm run deploy` again, which is a function-configuration update rather than anything destructive.

## Configuration

**Four** required environment variables on the function, and four optional ones.

Validation is deliberately not uniform, and the difference is worth knowing before you debug a value
that appears to be ignored:

- The four coordinates below, **and** the optional `DATABRICKS_SCHEMA`, are validated on read.
  A bad one fails Lambda initialisation with a message naming the variable, so a misconfiguration
  surfaces on the first invocation of a cold container rather than as a per-query error.
- `DATABRICKS_MAX_ROWS_PER_TABLE` and `DATABRICKS_ADVERTISE_PUSHDOWN` **fall back silently instead.**
  `Settings` treats an unusable value as absent on purpose: a typo in an operational knob should not
  present as "the connector is broken". The CDK app therefore rejects both at synth, so a value set
  through `pnpm run deploy` *is* checked — but a value set straight onto the function is not, and a
  non-integer row ceiling leaves the operator believing they raised it.

| Variable | Required | Example | Notes |
| --- | --- | --- | --- |
| `DATABRICKS_WORKSPACE_HOSTNAME` | yes | `dbc-a1b2345c-d6e7.cloud.databricks.com` | No scheme, no port. AWS-commercial workspaces only — see [Limits](#limits) |
| `DATABRICKS_HTTP_PATH` | yes | `/sql/1.0/warehouses/a1b234c567d8e9fa` | From the warehouse's **Connection details** tab. Case-sensitive |
| `DATABRICKS_CATALOG` | yes | `main` | The Unity Catalog catalog. Lower-cased on read |
| `CREDENTIAL_SECRET_ARN` | yes | `arn:aws:secretsmanager:...:secret:dbx-AbCdEf` | A pointer. **Never the credential itself** |
| `DATABRICKS_SCHEMA` | no | `sales` | Pins the connector to one UC schema; unset, it serves every schema in the catalog. Lower-cased on read; also the Athena schema name. See [Pinning to one schema](#pinning-to-one-schema) |
| `DATABRICKS_MAX_ROWS_PER_TABLE` | no | `2000000` | Row ceiling. Falls back silently. See [Limits](#limits) |
| `DATABRICKS_ADVERTISE_PUSHDOWN` | no | *(unset)* | Falls back silently. See [Push-down](DESIGN.md#push-down-what-is-and-is-not-advertised). Unset is correct unless you have measured |

### Deploy-time variables, not function ones

These are read by the CDK app at synth and are **never passed into the function's environment**.
Setting either directly on the Lambda has no effect at all.

| Variable | Required | Example | What it does |
| --- | --- | --- | --- |
| `CREDENTIAL_KMS_KEY_ARN` | no | `arn:aws:kms:...:key/...` | Only when the credential secret uses a customer-managed key. Grants the function's execution role `kms:Decrypt` on that key (`databricks-connector-stack.ts`). The **other half** of that grant is a statement in the key's own policy, which is the key owner's to write and which this stack cannot write |
| `FUNCTION_NAME_PREFIX` | no | `sales-` | Prefixes the stack and the function name, so a second copy of this connector can share an account with the first. **Concatenated verbatim** — include your own separator, or `sales` yields `salesdatabricks-coa-connector`. Letters, digits and hyphens only; no underscores, since the same string names a CloudFormation stack |
| `ALARM_TOPIC_ARN` | no | `arn:aws:sns:...:coa-connector-alarms` | Where the seven alarms below notify. **Without it the alarms are still created and notify nobody** — they change state in the console and that is all. See [Monitoring](#monitoring) |

`SERVE_ROLE_ARN`, `DISCOVERY_ROLE_ARN` and `AWS_REGION` are shared by every connector in this folder
and are documented in [`connectors/README.md`](../README.md#deployment-facts-come-from-the-environment).

**Catalog and schema must be bare SQL identifiers** — a letter or underscore, then letters, digits or
underscores — and both are **lower-cased on read**, because Unity Catalog stores catalog, schema and
table names lower-cased. A catalog or schema whose real name needs quoting is refused rather than
half-supported; [`DESIGN.md`](DESIGN.md#why-identifiers-must-be-bare-and-lower-case) has the reason and
the measurements. Note that UC does **not** fold **column** names, which is a separate trap.

## Creating the credential secret

**The secret's JSON shape selects the authentication mode.** Nothing declares the mode separately, so
you cannot say one thing and store another. A secret carrying *both* shapes is refused rather than
resolved by precedence: which one won would be invisible, and the wrong answer is a live credential
going unused while a stale one authenticates.

### OAuth machine-to-machine — preferred

A service principal's credential carries no human identity, which is the reason to prefer it. **It does
expire**, though — see the warning below; an earlier version of this section claimed otherwise and was
wrong.

Create the service principal in the Databricks UI (*Settings → Identity and access → Service
principals*), then generate a secret. The UI is *Secrets → Generate secret*; the API call, verified
against a live workspace, is:

```bash
# NOTE the path and the host. This is the endpoint that works.
curl -X POST \
  "https://<workspace-host>/api/2.0/accounts/servicePrincipals/<numeric-id>/credentials/secrets" \
  -H "Authorization: Bearer <admin token>"
```

Two details that cost an hour if you guess:

- It is the **workspace** host, not `accounts.cloud.databricks.com`, despite the `/accounts/` in the
  path.
- The `<numeric-id>` is the service principal's **numeric id**, not its application/client id. The two
  more obvious spellings — `/api/2.0/oauth2/service-principal-secrets` and
  `/api/2.0/oauth2/servicePrincipals/{id}/secrets` — both return **404**.

The value that goes in `client_id` is the **application (client) id**, a UUID — not the numeric id used
in the URL above.

```bash
aws secretsmanager create-secret \
  --name databricks-connector-m2m \
  --secret-string '{"client_id":"<application id, a UUID>","client_secret":"<generated secret>"}'
```

> **Service principal secrets expire, by default after 90 days.** Record the expiry date when you
> create one, and rotate before it. The failure mode is worth knowing precisely, because it is the one
> the two credential-related error prefixes exist to tell apart:
>
> | Situation | Prefix you get |
> | --- | --- |
> | Secret expired, or wrong `client_secret` | **`DATABRICKS_AUTHENTICATION_FAILED`** — the secret was read fine; Databricks rejected it at the token exchange. Measured: the driver reports `invalid_client` |
> | Connector cannot read the secret at all (no `GetSecretValue`, or missing `kms:Decrypt`) | **`CONNECTOR_CREDENTIAL_UNREADABLE`** |
>
> So an expired M2M secret is **not** a `CONNECTOR_CREDENTIAL_UNREADABLE`. If you see that one, the
> problem is IAM in your own account, not Databricks. (The rejection path was measured with a
> deliberately wrong client secret; an expired secret fails the same token exchange, so it lands on the
> same prefix. Expiry itself was not waited out.)

### Personal access token

```bash
# In Databricks: Settings -> Developer -> Access tokens -> Generate new token.
aws secretsmanager create-secret \
  --name databricks-connector-pat \
  --secret-string '{"token":"<dapi...>"}'
```

Rotating either is `aws secretsmanager put-secret-value` with no redeploy: the connector caches the
parsed credential per container for five minutes with jitter, so a rotation takes effect within that
window rather than waiting for containers to recycle.

Extra keys are ignored, so a `comment` or a rotation timestamp in the secret is fine. Anything that is
neither shape is refused with a message naming the secret's ARN and never echoing its value.

## Unity Catalog grants

The credential's principal needs **three** grants, and the third one is the one people miss:

```sql
GRANT USE CATALOG ON CATALOG  <catalog>          TO `<principal>`;
GRANT USE SCHEMA  ON SCHEMA   <catalog>.<schema> TO `<principal>`;
GRANT SELECT      ON SCHEMA   <catalog>.<schema> TO `<principal>`;
```

**`SELECT` is not optional, and omitting it fails in the worst available shape.** Unity Catalog's
`information_schema` is visible with `BROWSE` or `USE SCHEMA` alone, so without `SELECT` a table is
**discovered normally** — it appears in the scan, enters the ontology, gets enriched, and reaches a
steward for review — and then fails the first time anyone queries it. There is no point before serve
at which the missing grant is visible.

`information_schema` results are also **privilege-filtered**, which produces the mirror-image problem:
an under-privileged principal yields a *successful* scan of a *subset* of the schema, indistinguishable
from a schema that is genuinely that small. If a scan finds fewer tables than you expect, check the
grants before looking anywhere else.

`SELECT` on the schema covers tables added later. Granting per table means re-granting, and this
connector has no re-scan path.

## Deploy

Prerequisites: JDK 11+, Maven, Node 20.12+, pnpm, AWS credentials, and a **CDK-bootstrapped** account
and region — the jar ships as an S3 asset.

```bash
cd connectors
cp .env.example .env          # SERVE_ROLE_ARN and DISCOVERY_ROLE_ARN, read from SSM
pnpm install                  # once, at connectors/

export AWS_REGION=<COA's own region>   # in your SHELL. See the note below
export DATABRICKS_WORKSPACE_HOSTNAME=dbc-a1b2345c-d6e7.cloud.databricks.com
export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/a1b234c567d8e9fa
export DATABRICKS_CATALOG=main
export DATABRICKS_SCHEMA=sales
export CREDENTIAL_SECRET_ARN=arn:aws:secretsmanager:<region>:111122223333:secret:dbx-AbCdEf

cd databricks/cdk
pnpm test && pnpm run synth   # synth builds the jar first, via presynth
pnpm run deploy
```

### The region is COA's, and it is enforced

**Deploy this connector into the region COA itself is deployed in.** Not a region you prefer, and not
merely "the same region as some Athena catalog": COA's. Source creation **rejects** a
`connectorFunctionArn` whose region differs from the COA deployment's with a **400**, because the IAM
grant that lets Athena invoke your function is region-pinned and a cross-region connector would fail at
query time. A COA deployment in a second region needs its own copy of this connector in that region.
See [Custom Connector Sources](../../external-docs/content/custom-connector-sources.md#the-connector-lambda-must-be-in-this-deployments-region).

**Export `AWS_REGION` in your shell, not only in `.env`.** The CDK CLI sets `CDK_DEFAULT_REGION` from
the active profile on every invocation and that wins over the file — so a `.env` naming one region
with a profile defaulting to another deploys to the profile's, succeeds, and leaves a connector
nothing will ever query. Check the region in the synth output before deploying.

### Two role ARNs

Two COA components reach a connector: **serve** runs the queries and **discovery** runs `DESCRIBE`,
which is the only way the `@pk` / `@fk` tags are read at all. Grant serve but not discovery and the
connector answers `SELECT` perfectly while no declared key is ever found.

### What the stack creates

- The Lambda — **3008 MB, 120 s**, deliberately above the construct's 1024 MB / 90 s defaults.
- Its **log group**, `/aws/lambda/<function-name>` — so
  `/aws/lambda/databricks-coa-connector` by default, and
  `/aws/lambda/<FUNCTION_NAME_PREFIX>databricks-coa-connector` with a prefix set. Named explicitly
  rather than left to CDK, because every "check the connector's logs" instruction below depends on it
  being where you would look. **Retention is 7 days**, which is short enough to matter: a scan whose
  behaviour you want to explain a fortnight later has no log left, and the connector's logs are the
  only record of a dropped foreign key or a neutralised malformed tag. Raise it in the construct, or
  export the lines you care about, before investigating anything historical.
- Its own spill bucket, and its own KMS key tagged `coa:connector-spill=true`.
- The `coa:connector` tag on the function — which is what COA's invoke policy matches on, not the name.
- Read access to the one credential secret, and `kms:Decrypt` on its key if `CREDENTIAL_KMS_KEY_ARN`
  was set.
- The resource policies COA's two roles need.

It creates **no** Athena data catalog: COA registers its own, in its own account. See
[Register it in COA](#register-it-in-coa).

### If you deploy this into COA's own account

The topology this connector is built for is cross-account, and that is the one to prefer. Deploying it
into COA's **own** account also works and needs no resource policies, but two constraints apply and
both fail late:

- **Do not let the function name carry COA's resource prefix.** COA's roles hold an explicit `Deny` on
  `lambda:InvokeFunction` for in-account functions matching `<prefix>-*` when the call arrives through
  Athena — it exists to stop an Athena user-defined function reaching COA's own Lambdas, and it cannot
  tell your connector apart by name. `FUNCTION_NAME_PREFIX` is what would cause this: set it to
  anything that is not COA's resource prefix.
- **Spill is not supported in this topology, and this stack always creates a spill bucket.** COA's
  wildcard spill-read permission deliberately excludes COA's own account, so a spill bucket there
  cannot be read and any response over 6 MB fails with `AccessDenied`. Responses under 6 MB are
  unaffected, which is what makes it easy to miss — and
  [The aggregation caveat](#the-aggregation-caveat) means spill is the *normal* case on this route
  rather than an edge case. Treat same-account deployment as usable for a small pinned schema and
  unsuitable for anything that aggregates.

Both are documented on COA's side in
[Custom Connector Sources](../../external-docs/content/custom-connector-sources.md#deploying-the-connector-into-this-deployments-own-account).

### A second copy

Deploying a second copy needs `FUNCTION_NAME_PREFIX` to tell the two apart; it prefixes both the stack
and the function name. You need one for a second **catalog, workspace, warehouse or credential** — see
[What one deployment covers](#what-one-deployment-covers). You do **not** need one for a second schema
in the same catalog, which is what `DATABRICKS_SCHEMA` being optional bought.

## Register it in COA

**You hand COA two things: the connector Lambda's ARN, and one database name.** There is no
catalog-name input, because COA registers the Athena data catalog itself, in its own account, under a
name it derives from the source — so a caller-chosen name could collide with another source's.

`POST /namespaces/{namespaceId}/sources`:

```json
{
  "sourceType": "DATABASE",
  "databaseSource": {
    "name": "acme-databricks-sales",
    "customConnectorConfiguration": {
      "connectorFunctionArn": "arn:aws:lambda:<region>:111122223333:function:databricks-coa-connector",
      "databaseName": "sales"
    }
  }
}
```

| Field | Required | What to put |
| --- | --- | --- |
| `connectorFunctionArn` | **yes** | The **full** ARN of this connector's Lambda, from the stack's `ConnectorFunctionArn` output. Partial and name-only forms are rejected. Its region must equal COA's |
| `databaseName` | **yes** | The UC schema this source exposes, lower-cased. When pinned that is `DATABRICKS_SCHEMA`; when unpinned, pick one per source from `SHOW DATABASES` |
| `tableFilter` | no | Regex — only matching tables are discovered |
| `tableExcludeFilter` | no | Regex — matching tables are excluded, after the include filter |

Those four are the whole configuration. **There is no catalog-name field, no region field and no
cross-account role.** The registered catalog name comes back read-only on the source detail as
`databaseDetails.athenaDataCatalogName`, and that is the value to use as a query prefix. The full
request and response schema, the resource policies, and the spill requirements are in
[Custom Connector Sources](../../external-docs/content/custom-connector-sources.md#register-the-source).

COA discovers the source through Athena `SHOW`/`DESCRIBE`, which it already implements — there is no
Databricks-specific code on COA's side.

If COA finds no keys, the **discovery** role was not granted invoke, so nothing ever ran `DESCRIBE`.
`SELECT` works throughout, which is what makes it hard to spot.

### Checking the tags arrived, in your own account

Optional, and worth doing before you hand anything to COA: register an Athena data catalog of your own,
in the account you want to query from, purely so you can run the `DESCRIBE` self-check yourself. **This
is not a COA prerequisite** — COA neither reads nor needs this catalog.

The stack prints a command for it:

```bash
aws athena create-data-catalog --name databricks --type LAMBDA \
  --parameters function=arn:aws:lambda:<region>:111122223333:function:databricks-coa-connector
```

Three things about that output are worth knowing before you paste it:

- **The output key is mangled.** It is declared on the construct, and CDK prefixes a
  construct-declared output with the construct's path and a hash — so the key in the deploy log is
  `ConnectorRegisterCatalogCommandDF9B53BE`, not `RegisterCatalogCommand`. Grep the deploy output for
  `create-data-catalog`.
- **The name is derived from `connectorId` alone**, so it is always `--name databricks`. It knows
  nothing about `FUNCTION_NAME_PREFIX`, the catalog or the schema — it will not say
  `databricks_sales`.
- **So two copies print the identical command.** The second `create-data-catalog` collides, because
  catalog names are account-global. Rename the second by hand.

Then the check itself — the one thing that fails silently:

```sql
DESCRIBE databricks.sales.orders;
-- the comment column must show your prose plus @pk / @fk(...) where Unity Catalog declares keys
```

Catalog names are account-global and cannot contain hyphens, so pick one per connector and per
querying account.

## The aggregation caveat

**Athena's federation protocol cannot express aggregation.** There is no capability to advertise and
no code that would help. A `GROUP BY`, a `COUNT`, a `SUM` — any aggregate — is computed **in Athena**,
which means every predicate-matching row is read out of your warehouse first.

Concretely: `SELECT region, SUM(total) FROM orders GROUP BY region` over a hundred-million-row table
reads a hundred million rows out of the warehouse, through a Lambda, into Athena, to return a handful.
The same question in Databricks' own SQL editor reads none of them out.

That is **billed warehouse compute**, and it is yours. It is a permanent property of reaching Databricks
through Athena federation — not a defect, not a temporary state, and not something a future version of
this connector fixes. It belongs in your cost model before it appears on a bill.

What bounds it:

- **The predicate is pushed**, so a `WHERE` clause reduces what is read even though the `GROUP BY` does
  not. This needs no configuration and costs nothing —
  [it happens whether or not the connector advertises anything](DESIGN.md#push-down-what-is-and-is-not-advertised).
- **The row ceiling** caps the worst case — see limit 5 below.
- **Spill is the normal case on this route, not an edge case**, because aggregate reads exceed Athena's
  6 MB response limit routinely. That is why the stack always creates a spill bucket.

## Limits

1. **A stopped warehouse fails fast rather than waiting.** A warehouse that is not running answers
   with a temporarily-unavailable error and resumes in the background — seconds for serverless,
   minutes for classic and pro. The driver would retry that for up to 900 seconds by default, which
   outlives the 120-second invocation timeout, so the retry is turned off and the error is labelled
   `DATABRICKS_WAREHOUSE_NOT_RUNNING`. Retry the query shortly. Failing fast is also cheaper: Athena
   re-invokes a failed connector, so a connector that blocks turns one query into several billed
   Lambda-minutes and several warehouse resumes.
2. **`information_schema` visibility is not `SELECT`.** See [Unity Catalog grants](#unity-catalog-grants).
3. **Declared constraints are informational only.** Unity Catalog validates neither uniqueness nor
   referential integrity, so a declared primary key may contain duplicates and a declared foreign key
   may not resolve. This connector reports what the catalog declares and verifies nothing — that is a
   property of Databricks, not of the transport. Constraints also require Delta Lake and DBR 13.3 LTS
   or later, and a foreign key must reference a primary key or unique constraint. An estate that never
   declared any yields none, and COA falls back to inferring relationships.
4. **A foreign key whose parent is outside the exposed schema is dropped, with a warning logged.**
   The `@fk(table.column)` tag has no slot for a schema and COA resolves it inside the source's own
   schema, so emitting such a tag would either dangle or — if a table of that name happens to exist in
   that schema — assert a **wrong relationship** in the ontology. This holds even when the connector is
   *unpinned* and does serve the parent's schema as a separate Athena schema: the limit is the tag's,
   not the connector's. An omission that
   is logged beats a wrong answer that is not. The same applies to a parent whose `table_type` this
   connector does not expose, and to one the credential's principal cannot see. Grep the connector's
   log for `Dropping the declared foreign key` if a relationship you expected is missing.

   **A least-privilege credential may hide such a key rather than drop it, and the log then says
   nothing.** Measured across two credentials on the same schema: `information_schema` is
   privilege-filtered at the level of the *constraint row*, so a principal with no grant on the
   referenced schema does not see the `referential_constraints` entry at all. The outcome is the same —
   no tag is emitted — but there is no warning to grep for, because from the connector's point of view
   the constraint does not exist. A broadly-granted credential logs the drop; a narrowly-granted one is
   silent. Both are safe; only one is diagnosable.
5. **A row ceiling, defaulting to two million rows per table per query.** Past it the connector fails
   with `DATABRICKS_TABLE_TOO_LARGE` naming the table, rather than timing out — a timeout names no
   table, suggests no action, and is retried. Raise it with `DATABRICKS_MAX_ROWS_PER_TABLE`, and raise
   the memory and timeout with it. **There is no byte ceiling**: row width is not knowable before the
   read, so a very wide table can still exhaust the invocation below the row ceiling. Lower the row
   ceiling for such a schema.
6. **One split per table.** A single invocation streams a whole table, so the constraint is invocation
   duration rather than contention. Splitting on a numeric or date column would parallelise this; it
   is not implemented.
7. **No nullability.** Athena's `Column` type has no field for it and `DESCRIBE` returns name, type and
   comment, so it cannot reach COA on this route. The comment channel is not an alternative: COA's
   parser strips a tag only when it recognises one, so an unrecognised `@notnull` would be left in the
   stored description as literal text — visibly worse than absence. Columns arrive nullable.
8. **No table-level comments.** Nothing in Athena surfaces one for a `LAMBDA` catalog. Column comments
   do arrive.
9. **Azure and GCP workspaces are refused.** `azuredatabricks.net` and `gcp.databricks.com` are
   architecturally supported and untested, so the hostname pattern rejects them rather than
   half-supporting them. Widen it deliberately, with a test behind it.
10. **No Lake Formation.** A Lambda-backed Athena catalog is not a Glue Data Catalog object, so there is
   nothing to grant on. Your central Lake Formation policy does not apply to this source. COA's own SQL
   firewall enforces column policy on this route and is the only access control on it.
11. **Cross-catalog joins do not work**, and that is platform-wide rather than specific to this
    connector: a Lambda-backed catalog is always its own top-level catalog, and no COA SQL generator
    emits a catalog or database qualifier for any source type. Single-source questions work.

## Types

| Databricks | Arrow | Notes |
| --- | --- | --- |
| `BOOLEAN` | `BIT` | |
| `TINYINT` / `BYTE` | `TINYINT` | |
| `SMALLINT` / `SHORT` | `SMALLINT` | |
| `INT` / `INTEGER` | `INT` | |
| `BIGINT` / `LONG` | `BIGINT` | |
| `FLOAT` / `REAL` | `FLOAT4` | |
| `DOUBLE` | `FLOAT8` | |
| `DECIMAL(p,s)` / `NUMERIC` | `Decimal(p,s)` | Precision and scale preserved — read from `full_data_type`, not `data_type` |
| `STRING` / `VARCHAR(n)` / `CHAR(n)` | `VARCHAR` | |
| `BINARY` | `VARBINARY` | |
| `DATE` | `DATEDAY` | Epoch day |
| `TIMESTAMP` / `TIMESTAMP_LTZ` | `DATEMILLI` | Epoch millis, UTC |
| `TIMESTAMP_NTZ` | `DATEMILLI` | Carries no zone, so it is **read as UTC** |
| `ARRAY` / `MAP` / `STRUCT` / `VARIANT` / `OBJECT` / `GEOMETRY` / `GEOGRAPHY` / `INTERVAL` | `VARCHAR` | Flattened to the value's string rendering |
| anything Databricks adds later | `VARCHAR` | Falls back rather than failing the whole table |

Complex types are flattened because the SDK's own `BlockUtils.setValue` covers scalars only — handed a
struct, list or map vector it throws `Unknown type Struct` at read time, after deploy. The driver
returns them as their string rendering, which is usable in a projection and not usable as a predicate
target.

## Tests

```bash
cd connectors
mvn -B test -pl databricks -am     # unit tests
cd databricks/cdk && pnpm test     # the stack
```

**There is no integration suite. Unit tests and the CDK stack test are the whole of this connector's
automated coverage.**

No Databricks workspace is available to this project — the one used during design was a 14-day
trial — so the suites that ran against a real warehouse were **removed** rather than left in place to
skip. A skipped test reports green and reads as a pass, which is a worse outcome than an absence you
can see.

Behaviour against real Databricks is instead evidenced by a **one-off manual verification record**, in
the connector LLD's §8.1 ("One-off manual verification record"). Its provenance is
`src/test/resources/fixtures.sql`: every observation in that record was made against the schema that
script creates, and the script's own header says the same thing. It is kept for that reason, and
because it is the starting point for anyone who acquires a workspace and wants to build the suite §8.1
describes — read its header first, especially the note that its row *values* are illustrative while its
column names, comments, types and constraints are the part to assert on.

**What the absence costs, concretely.** Every claim this connector makes about *Databricks'* behaviour
is now unchecked by CI, because none of them has a unit-test equivalent: that `table_type` is never
`'BASE TABLE'`; that `information_schema.tables` reports internal side tables which `SHOW TABLES`
omits; that table names fold to lower case while column names do not; that `ordinal_position` counts
from a different base in two views; and, most expensively, that the ANSI constraint join pairs a
composite foreign key's columns correctly where the obvious join silently does not. That last one was
previously covered by a test that ran **both** joins and required the naive one to be wrong, so the
reason for the complexity was checkable. **It is now asserted only in a comment.** Anyone changing
discovery should build the suite from §8.1's table before doing so; nothing automated would catch a
regression there today.

### Unit tests

Fully mocked, no warehouse and no AWS. JDBC is a recorded layer (`FakeJdbc`) built from dynamic
proxies, so a test asserts the exact SQL issued and the exact parameters bound. Covered: schema
assembly, comment and tag encoding including a composite foreign key, the `information_schema` SQL
shapes, the `table_type` allowlist, configuration validation and every rejection, auth-mode selection
from the secret's shape, identifier quoting including names containing quote characters, the row
ceiling, error classification and redaction, and that a column comment lands in the Arrow **schema's**
metadata rather than on a field.

What they cannot cover is anything requiring a warehouse to answer, which is the list above.

### The build

`mvn test` prints an SLF4J *multiple providers* warning and names `log4j-slf4j2-impl`. It is
**test-classpath only** and needs no action: `athena-jdbc`'s uber-jar bundles that provider, and the
shade filter in `pom.xml` keeps it out of the shipped artifact, which has exactly one. Confirm with:

```bash
unzip -p target/databricks-connector-1.0.0.jar META-INF/services/org.slf4j.spi.SLF4JServiceProvider
# -> org.slf4j.simple.SimpleServiceProvider, and nothing else
```

The deployment package is **83 MB zipped / 212.6 MB unzipped**, against Lambda's 250 MB unzipped
limit — 85%, about 37 MB of headroom. Re-measure after any dependency bump: the failure mode is a
deploy CloudFormation rejects, not a build that fails.

## Monitoring

The stack creates **seven alarms**. Set `ALARM_TOPIC_ARN` to a topic your rota reads, or they notify
nobody — an alarm without an action changes state in the console and stops there.

Four of them watch metrics the connector emits itself, in the CloudWatch namespace **`COA/Connectors`**,
dimensioned `Connector=databricks`. They are written as Embedded Metric Format on stdout and extracted
from the log group, so they cost no `PutMetricData` call and need no IAM grant — but they share the log
group's **7-day** retention, and a metric is only as durable as CloudWatch's own 15 months.

**Why the connector emits its own at all**, given the Lambda already has metrics: each of these four
failures is caught, classified and reported, so **the invocation succeeds**. It appears in neither the
error rate nor the duration. Without these, a connector refusing every query for a rotated credential
looks perfectly healthy.

| Alarm | Fires when | First action |
| --- | --- | --- |
| `-config-resolution-failures` | Any failure in 15 min | The connector could not read its own configuration. Compare the four required variables against the cold-start log line, which names the host, catalog and schema it actually resolved |
| `-warehouse-connect-failures` | More than 5 in 5 min | Tell the three cases apart: a stopped warehouse recovers on resume; `DATABRICKS_AUTHENTICATION_FAILED` in the log means the credential expired or rotated — see [Creating the credential secret](#creating-the-credential-secret); anything else is network. Five rather than one because a warehouse scaled to zero refuses the first connections of every resume |
| `-table-ceiling-exceeded` | Any breach | A query was refused by `DATABRICKS_MAX_ROWS_PER_TABLE`. The log names the table. Narrow the predicate or set `tableExcludeFilter` on the source; raising the ceiling means raising memory and timeout with it. See [The aggregation caveat](#the-aggregation-caveat) |
| `-rows-returned-p95` | p95 rows per read is within 20% of the ceiling, twice running | The only one here that fires before anything has failed: the next slightly wider question breaches the ceiling. A push-down regression and an aggregate-heavy workload look identical from Athena's side, so check `DATABRICKS_ADVERTISE_PUSHDOWN` before concluding the workload changed |

The other three are the function's own — throttles above zero, error rate above 1%, and p99 duration
within 20% of the 120 s timeout. The throttle one matters more than it looks: **one connector serves
every COA source pointed at it**, so a throttle is not one source degrading but all of them at once, and
nothing on COA's side can see that.

Alarm names are `<function-name>-<suffix>`, so they carry `FUNCTION_NAME_PREFIX` and two deployments in
one account do not collide.

**Not covered, deliberately.** There is no metric for a credential the connector cannot *read* — an IAM
or KMS fault, reported as `CONNECTOR_CREDENTIAL_UNREADABLE`. It fails every invocation from the first
one, so the error-rate alarm catches it, and it is a deployment fault rather than a running one. There
is also no discovery-duration or constraint-count metric: both belong to COA's scan pipeline rather than
to the connector, which sees one `DESCRIBE` at a time and cannot tell a scan from a query.

## When it goes wrong

Start from the symptom. See [`connectors/README.md`](../README.md#when-it-goes-wrong) for the failures
common to every connector.

The connector's own logs are in **`/aws/lambda/<function-name>`** — `databricks-coa-connector` unless
you set `FUNCTION_NAME_PREFIX` — and are kept for **7 days**. Several rows below tell you to grep them,
so check the age of what you are investigating first: past a week there is nothing to grep.

Every error the connector raises starts with one of six stable prefixes. **Three of them do not name
Databricks, and that is deliberate** — a `CONNECTOR_` prefix means the problem is in this account or in
this connector, and looking at the warehouse will waste your time.

| Symptom | Look at |
| --- | --- |
| `DATABRICKS_WAREHOUSE_NOT_RUNNING` | Expected on the first query after an auto-stop. Retry; serverless resumes in seconds. If it persists, the warehouse is stopped rather than starting |
| `DATABRICKS_AUTHENTICATION_FAILED` | Databricks rejected a credential it *received*. The secret's value, and then the three Unity Catalog grants — the message names both because the second is the more common cause |
| `CONNECTOR_CREDENTIAL_UNREADABLE` | The connector could not *read* the secret, which is an IAM problem here rather than a Databricks one. `secretsmanager:GetSecretValue` on the secret, and for a customer-managed key `kms:Decrypt` on **both** the function's role and the key's own policy. The CDK app grants the role half from `CREDENTIAL_KMS_KEY_ARN`; the key policy is the key owner's. This is the most likely first-deploy failure |
| `CONNECTOR_INTERNAL_ERROR` | A fault that did not come from Databricks: a bug here, a missing IAM grant, a packaging problem. The connector's own log group, not the warehouse |
| `DATABRICKS_REQUEST_FAILED` | Databricks refused the request for a reason that is neither of the two above — a missing table, a syntax error, a permission on one object. The driver's own message is quoted at the end |
| `DATABRICKS_TABLE_TOO_LARGE` | Expected for a table past the row ceiling. Narrow the query's predicate, or raise `DATABRICKS_MAX_ROWS_PER_TABLE` together with memory and timeout |
| `400` on source create, naming `connectorFunctionArn` | The connector is not in COA's region, or the ARN is a partial or name-only form. Redeploy in COA's region; pass the full `arn:aws:lambda:...` form |
| Scan finds fewer tables than the schema has | `SELECT` is missing on some of them. `information_schema` is privilege-filtered, so this presents as a small schema rather than as an error |
| Scan finds views but no tables | Something is filtering on `table_type IN ('BASE TABLE', 'VIEW')`. Databricks never says `BASE TABLE` |
| Tables named `__materialization_...` or `event_log_...` in the ontology | Something enumerated `information_schema.tables` instead of `SHOW TABLES` |
| A mixed-case column returns nulls or the wrong values | A column name was lower-cased somewhere. Table names fold; column names do not |
| A composite foreign key's columns point at the wrong parents | The constraint join lost its `referential_constraints` hop. Note no test would have caught this — see [Tests](#tests) |
| A foreign key Unity Catalog declares is missing from the ontology | Its parent is outside the exposed schema, is a `table_type` this connector excludes, or is invisible to the principal. Grep the log for `Dropping the declared foreign key` |
| `has table_type MANAGED_SHALLOW_CLONE, which this connector does not expose` | Working as designed, on a table you named directly. Excluded types are refused by `DESCRIBE` and `SELECT` as well as omitted from discovery |
| Scan finds nothing at all, but the schema is not empty | Grep for `none survived the table_type allowlist`. Either the principal lacks `SELECT`, or every object really is an excluded type |
| A column description contains stray text where a tag was | A customer wrote a malformed tag in the Unity Catalog comment. The connector neutralised it rather than failing the table; grep for `Neutralised a malformed constraint tag` |
| `DESCRIBE` shows no comments at all | The comment went onto the Arrow field instead of the schema's metadata. Use the toolkit's `TableSchema`, which makes that unexpressible |
| Comments are there but COA finds no keys | The **discovery** role was not granted invoke, so nothing ever ran `DESCRIBE`. `SELECT` works throughout |
| Every column of a table is the column's own name repeated | Double-quoted identifiers with ANSI mode off. This connector uses backticks for exactly this reason |
| A `GROUP BY` is far slower and dearer than the same query in Databricks | Working as designed. See [The aggregation caveat](#the-aggregation-caveat) |
| Queries return no rows but succeed, only for large results | The connector could not write its spill. Check the bucket, the prefix and the execution role. If the connector is in COA's own account, spill is unsupported there at all — see [If you deploy this into COA's own account](#if-you-deploy-this-into-coas-own-account) |
| A warm query takes ~8–13 s and you expected ~6 s | Working as measured. Athena's federation orchestration, not the connector — see [Measured performance and cost](DESIGN.md#measured-performance-and-cost) |
