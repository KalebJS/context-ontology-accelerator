// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package dev.coa.databricks;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import dev.coa.connector.metadata.CoaMetadataHandler;
import dev.coa.connector.metadata.CoaTable;
import dev.coa.connector.metrics.ConnectorMetrics;
import dev.coa.databricks.config.ConnectionConfig;
import dev.coa.databricks.config.ConnectionConfigProvider;
import dev.coa.databricks.config.CredentialSource;
import dev.coa.databricks.config.EnvironmentConnectionConfigProvider;
import dev.coa.databricks.config.MeteredConnectionConfigProvider;
import dev.coa.databricks.jdbc.DatabricksConnectionFactory;
import dev.coa.databricks.metadata.InformationSchemaReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * The metadata half: one Athena schema, its tables, and each table's columns, comments and declared
 * keys.
 *
 * <p>This extends the COA toolkit's {@link CoaMetadataHandler} while
 * {@link DatabricksRecordHandler} extends {@code athena-jdbc}'s record handler, since a Java class
 * cannot do both. What the toolkit supplies here is the comment builder, the {@code @pk}/{@code @fk}
 * encoder, and the Arrow-schema placement that makes a comment reach Athena at all.
 *
 * <p>Unity Catalog addresses {@code catalog.schema.table}, and an Athena federated catalog has one
 * level left below the registered catalog name. This connector spends it on the Unity Catalog schema
 * and takes the catalog itself from {@code DATABRICKS_CATALOG}. Which schemas it advertises depends on
 * {@code DATABRICKS_SCHEMA}: set, exactly that one, and every other name is refused, which is a
 * containment boundary independent of the credential's Unity Catalog grants; unset, every schema in the
 * catalog that Athena can address, enumerated from {@code information_schema}, with those grants the
 * only boundary. What it advertises and what it serves are the same set either way — see
 * {@link #servable}. The record half enumerates nothing, since it reads the schema off the request, but
 * it enforces the pin as well, so the boundary does not depend on Athena having called
 * {@code GetTable} first.
 *
 * <p>{@link #doGetSplits} emits a split with no properties. The toolkit's default puts the table name
 * on it under the key {@code "table"}, and {@code athena-jdbc}'s query builder reads every split
 * property as a partition value: it drops those names from the projection, skips their constraints, and
 * feeds the split's value to the extractor instead of the result set. A Databricks table with a column
 * named {@code table} would return the literal string {@code orders} in it for every row, silently. The
 * table name is already on the request.
 */
public class DatabricksMetadataHandler extends CoaMetadataHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksMetadataHandler.class);

    /**
     * Short name for the source type; the SDK uses it in metrics and log lines, and it is the
     * {@code Connector} dimension every metric this connector emits carries.
     *
     * <p>Public because the connection factory needs it for that dimension and sits in a sub-package.
     */
    public static final String SOURCE_TYPE = "databricks";

    private final ConnectionConfigProvider configs;
    private final Map<String, java.util.List<
            com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType>>
            capabilities;

    /**
     * Opens connections for a configuration. The module's containment boundary runs through
     * {@link #readerFor}, so this is a field rather than a {@code new} expression inside it: with the
     * factory constructed inline, {@code listTables} and {@code describeTable} could only be exercised
     * against a live warehouse, and the connector has no integration suite.
     */
    private final Function<ConnectionConfig, Supplier<Connection>> connections;

    /** The advertised schema list per configuration, for an unpinned connector. */
    private final ServableSchemas servableSchemas;

    /**
     * Lambda entry point's constructor.
     *
     * @throws IllegalArgumentException if the environment is missing or invalid, naming the variable.
     *                                 Thrown during initialisation so a misconfigured connector fails
     *                                 once, loudly, rather than once per request.
     */
    public DatabricksMetadataHandler(Map<String, String> configOptions)
    {
        this(configOptions,
                new MeteredConnectionConfigProvider(
                        new EnvironmentConnectionConfigProvider(configOptions),
                        new ConnectorMetrics(SOURCE_TYPE)));
    }

    /**
     * @param configs resolves the endpoint. A parameter so a later phase can supply an SSM-backed
     *                implementation without touching this class.
     */
    public DatabricksMetadataHandler(Map<String, String> configOptions,
                                     ConnectionConfigProvider configs)
    {
        this(configOptions, configs, null, CredentialSource.DEFAULT_TTL_MILLIS);
    }

    /**
     * The constructor a test uses.
     *
     * @param connections            opens connections for a resolved configuration, or null for the real
     *                               one. Null rather than an overload taking the real value, because the
     *                               real one needs {@code this::getSecret} — the federation SDK's own
     *                               caching Secrets Manager client — which no caller can reference before
     *                               this constructor has run.
     * @param schemaCacheTtlMillis   how long an unpinned connector may reuse an enumerated schema list.
     *                               Zero disables the cache, which is what a test asserting the
     *                               enumeration happened wants.
     */
    DatabricksMetadataHandler(Map<String, String> configOptions,
                              ConnectionConfigProvider configs,
                              Function<ConnectionConfig, Supplier<Connection>> connections,
                              long schemaCacheTtlMillis)
    {
        super(SOURCE_TYPE, configOptions);
        this.configs = configs;
        this.capabilities = PushdownCapabilities.from(Settings.advertisedPushdown(configOptions));
        CredentialSource credentials = new CredentialSource(this::getSecret);
        this.connections = (connections != null)
                ? connections
                : config -> new DatabricksConnectionFactory(config, credentials)::open;
        this.servableSchemas = new ServableSchemas(schemaCacheTtlMillis);

        // A cold start happens before any catalog name exists, so this is one of the two sites that
        // passes null, and a multiplexed provider has to tolerate it.
        ConnectionConfig config = configs.configFor(null);
        // Enough to tell an operator which endpoint this container serves, and nothing that identifies
        // a credential, a warehouse or a predicate.
        LOGGER.info("Databricks connector ready: host={} catalog={} schema={} pushdown={}",
                config.workspaceHostname(), config.catalog(),
                config.isSchemaPinned() ? config.schema() : "<every schema in the catalog>",
                capabilities.isEmpty() ? "none" : capabilities.keySet());
    }

    /**
     * The pinned Unity Catalog schema alone, or, when unpinned, those of the catalog's schemas this
     * connector can also serve.
     */
    @Override
    protected List<String> listDatabases(String catalog)
    {
        ConnectionConfig config = configs.configFor(catalog);
        if (config.isSchemaPinned()) {
            // No connection needed: the pin is the answer, and a warehouse round-trip could only agree
            // with it or fail.
            return Collections.singletonList(config.schema());
        }
        return advertisedSchemas(config);
    }

    /**
     * The schema's tables, allowlisted {@code table_type} values only.
     *
     * @throws IllegalArgumentException if {@code database} is not a schema this connector exposes.
     */
    @Override
    protected List<String> listTables(String catalog, String database)
    {
        return readerFor(catalog, database).listTables();
    }

    /**
     * One table's columns, types, prose and declared keys. The toolkit turns the keys into
     * {@code @pk}/{@code @fk} tags and puts them where Athena reads them.
     */
    @Override
    protected CoaTable describeTable(String catalog, String database, String tableName)
    {
        return readerFor(catalog, database).describeTable(tableName);
    }

    /**
     * {@inheritDoc}
     *
     * <p>One split with no properties, since {@code athena-jdbc}'s query builder reads every split
     * property as a partition column.
     */
    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
    {
        Split split = Split.newBuilder(makeSpillLocation(request), makeEncryptionKey()).build();
        return new GetSplitsResponse(request.getCatalogName(), split);
    }

    /**
     * {@inheritDoc}
     *
     * <p>The SDK's default is an empty map, so this decides whether Athena pushes anything at all. It
     * ships returning empty; {@link PushdownCapabilities} says why.
     */
    @Override
    public GetDataSourceCapabilitiesResponse doGetDataSourceCapabilities(
            BlockAllocator allocator, GetDataSourceCapabilitiesRequest request)
    {
        return new GetDataSourceCapabilitiesResponse(request.getCatalogName(), capabilities);
    }

    /**
     * A reader scoped to {@code database}, after checking this connector will serve it. Athena calls
     * {@code GetTable} for a name the user typed, so without the check
     * {@code SELECT * FROM cat.made_up.t} is answered from whichever schema the config happens to name,
     * reading as though {@code made_up} existed.
     *
     * @throws IllegalArgumentException if it will not serve {@code database}. Three messages, because
     *                                 the fixes differ: a pinned connector asked for another schema is a
     *                                 deployment decision, an unpinned one asked for a name Athena
     *                                 cannot address is a schema that needs renaming or fronting with a
     *                                 view, and an unpinned one asked for a schema that does not exist
     *                                 is a typo or a missing grant.
     */
    private InformationSchemaReader readerFor(String athenaCatalog, String database)
    {
        ConnectionConfig config = configs.configFor(athenaCatalog);

        if (config.isSchemaPinned()) {
            if (!config.schema().equals(database)) {
                throw new IllegalArgumentException(
                        "Unknown schema: \"" + database + "\". This connector is pinned to \""
                                + config.schema() + "\" by " + ConnectionConfig.SCHEMA_VAR
                                + ", so it serves that schema and no other. Unset "
                                + ConnectionConfig.SCHEMA_VAR + " to serve every schema in catalog \""
                                + config.catalog() + "\", or deploy a second connector for \""
                                + database + "\".");
            }
            return schemaScopedReader(config);
        }

        // Checked before the catalog is asked, so a name this connector could never address is refused
        // by shape rather than by absence. Unity Catalog allows a quoted name containing almost
        // anything, and Athena cannot address one on both its parsers, so such a schema is neither
        // advertised nor served; saying so is the only message that names the actual obstacle.
        if (!ConnectionConfig.isServableSchemaName(database)) {
            throw new IllegalArgumentException(
                    "Unknown schema: \"" + database + "\". Its name is not a bare SQL identifier, so"
                            + " this connector neither advertises nor serves it, whether or not catalog"
                            + " \"" + config.catalog() + "\" contains it: Athena parses SHOW/DESCRIBE"
                            + " and SELECT with different quoting rules, so a name needing quotes"
                            + " cannot be addressed on both paths. Expose the tables through a schema"
                            + " whose name is a bare identifier, or pin a connector per schema.");
        }

        // Unpinned: the catalog decides what exists. Enumerated rather than probed, so the failure can
        // list the alternatives. A schema name is a coordinate, not a secret, and the principal can
        // already see every name this returns.
        List<String> available = advertisedSchemas(config);
        if (!available.contains(database)) {
            throw new IllegalArgumentException(
                    "Unknown schema: \"" + database + "\". Catalog \"" + config.catalog()
                            + "\" exposes " + describe(available) + ". Check the spelling, or that"
                            + " the connector's principal holds USE SCHEMA on it — a schema the"
                            + " credential cannot see is indistinguishable from one that does not"
                            + " exist.");
        }
        return schemaScopedReader(config.withSchema(database));
    }

    /**
     * The schemas an unpinned connector advertises for {@code config}, which are exactly the ones it
     * will serve.
     *
     * <p>Cached per container. This is read on every {@code ListTables} and {@code GetTable} as well as
     * on {@code ListSchemas}, and discovery is a per-table {@code GetTable} fan-out, so without the
     * cache a 200-table schema pays 201 metastore-wide {@code information_schema.schemata} scans and
     * 201 extra connections for one enumeration that does not change between them.
     */
    private List<String> advertisedSchemas(ConnectionConfig config)
    {
        return servableSchemas.get(config,
                () -> servable(catalogScopedReader(config).listSchemas(), config.catalog()));
    }

    /**
     * The subset of {@code discovered} this connector can address, each rejection logged with its
     * reason.
     *
     * <p>Static and taking the list rather than fetching it, so the rule that decides what is advertised
     * is testable without a warehouse. The rule has to be the one {@link ConnectionConfig#withSchema}
     * applies, or the connector advertises a schema and then refuses every request against it, blaming
     * {@link ConnectionConfig#SCHEMA_VAR} — which an unpinned deployment never set.
     *
     * @param discovered  the names {@link InformationSchemaReader#listSchemas()} returned.
     * @param unityCatalog the catalog they came from, for the log lines.
     */
    static List<String> servable(List<String> discovered, String unityCatalog)
    {
        List<String> servable = new ArrayList<>(discovered.size());
        for (String name : discovered) {
            if (ConnectionConfig.isServableSchemaName(name)) {
                servable.add(name);
                continue;
            }
            // WARN, not INFO: from the operator's side this is a schema that has gone missing from a
            // source they expected to see, and nothing else says why.
            LOGGER.warn("Not advertising schema {}.{}: its name is not a bare SQL identifier, and"
                            + " Athena cannot address it on both its SHOW/DESCRIBE and SELECT parsers,"
                            + " so a source onboarded against it could be listed and never read. Rename"
                            + " it, or expose its tables through views in a schema whose name is one.",
                    unityCatalog, name);
        }
        if (servable.isEmpty() && !discovered.isEmpty()) {
            // Every name refused. One refusal is a data-modelling choice; all of them looks from the
            // outside like an empty catalog or a broken connector.
            LOGGER.warn("Catalog {} exposes {} schema(s) to this credential and none of their names is"
                            + " a bare SQL identifier, so this connector is advertising none.",
                    unityCatalog, discovered.size());
        }
        return Collections.unmodifiableList(servable);
    }

    /** A reader for {@code config}'s own schema. {@code config} has to be pinned. */
    private InformationSchemaReader schemaScopedReader(ConnectionConfig config)
    {
        return new InformationSchemaReader(config, connections.apply(config));
    }

    /**
     * A reader for {@link InformationSchemaReader#listSchemas()} only, which is the one method on that
     * class safe to call on an unpinned config.
     */
    private InformationSchemaReader catalogScopedReader(ConnectionConfig config)
    {
        return new InformationSchemaReader(config, connections.apply(config));
    }

    /** The names quoted and comma-separated, or a phrase saying there are none. */
    private static String describe(List<String> schemas)
    {
        if (schemas.isEmpty()) {
            return "no schemas this credential can see";
        }
        return "\"" + String.join("\", \"", schemas) + "\"";
    }

    /**
     * One enumerated schema list per configuration, held for a jittered TTL.
     *
     * <p>Keyed on the {@link ConnectionConfig}, not on the Athena catalog name: a multiplexed provider
     * hands out a different configuration per catalog, and two of them must not share a list. Bounded by
     * the number of distinct configurations a container serves, which is one unless a multiplexed
     * provider is wired in.
     *
     * <p>The TTL matches {@link CredentialSource}'s, and is jittered for the same reason: a schema's
     * worth of containers filling their caches in the same second would arrive at the warehouse
     * together.
     */
    private static final class ServableSchemas
    {
        private final long ttlMillis;
        private final ConcurrentMap<ConnectionConfig, Snapshot> byConfig = new ConcurrentHashMap<>();

        private ServableSchemas(long ttlMillis)
        {
            if (ttlMillis < 0) {
                throw new IllegalArgumentException("ttlMillis must not be negative");
            }
            this.ttlMillis = ttlMillis;
        }

        /** The cached list for {@code config}, or {@code discover}'s answer, cached. */
        private List<String> get(ConnectionConfig config, Supplier<List<String>> discover)
        {
            long now = System.currentTimeMillis();
            Snapshot snapshot = byConfig.get(config);
            if (snapshot != null && now < snapshot.expiresAtMillis) {
                return snapshot.schemas;
            }
            // Not computeIfAbsent: that holds a bin lock across the warehouse round-trip, and a benign
            // race here costs one duplicate enumeration.
            List<String> schemas = discover.get();
            byConfig.put(config, new Snapshot(schemas, now + jittered(ttlMillis)));
            return schemas;
        }

        /** {@code ttl} scattered by up to ±20%. Zero stays zero, which disables the cache. */
        private static long jittered(long ttl)
        {
            if (ttl == 0) {
                return 0;
            }
            long spread = Math.max(1L, ttl / 5L);
            return ttl - spread + ThreadLocalRandom.current().nextLong(2L * spread);
        }

        private static final class Snapshot
        {
            private final List<String> schemas;
            private final long expiresAtMillis;

            private Snapshot(List<String> schemas, long expiresAtMillis)
            {
                this.schemas = schemas;
                this.expiresAtMillis = expiresAtMillis;
            }
        }
    }
}
