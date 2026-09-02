// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package dev.coa.databricks;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcRecordHandler;
import dev.coa.databricks.config.ConnectionConfig;
import dev.coa.databricks.config.ConnectionConfigProvider;
import dev.coa.databricks.config.CredentialSource;
import dev.coa.connector.metrics.ConnectorMetrics;
import dev.coa.databricks.config.EnvironmentConnectionConfigProvider;
import dev.coa.databricks.config.MeteredConnectionConfigProvider;
import dev.coa.databricks.config.SecretsManagerReader;
import dev.coa.databricks.jdbc.DatabricksConnectionFactory;
import dev.coa.databricks.jdbc.DatabricksErrors;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

/**
 * The record half: streams rows for one split, as Arrow.
 *
 * <p>{@link JdbcRecordHandler} supplies the read loop and {@code makeExtractor}, a typed extractor per
 * projected column for eleven Arrow types, with two corrections that are only obvious once they have
 * bitten: a date before 1970 is off by one if read as millis, and {@code FLOAT8} sometimes arrives as a
 * currency-formatted string. The loop honours schema projection (writing a column absent from the
 * request's schema throws inside {@code BlockUtils.setValue}, and unprojected queries pass, so the
 * mistake surfaces late) and checks for query cancellation between rows.
 * {@link DatabricksQueryBuilder} supplies the statement. This class supplies configuration, the
 * connection, the row ceiling, error classification, and the pinned-schema check that keeps
 * {@code DATABRICKS_SCHEMA} a boundary on this path too ({@link #requireServableSchema}).
 */
public class DatabricksRecordHandler extends JdbcRecordHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksRecordHandler.class);

    /** Seconds a row-reading statement may run. Below the connector's 120 s invocation timeout. */
    static final int QUERY_TIMEOUT_SECONDS = 100;

    private final ConnectionConfigProvider configs;
    private final DatabricksQueryBuilder queryBuilder;
    private final long maxRowsPerTable;
    private final ConnectorMetrics metrics;

    /**
     * Lambda entry point's constructor.
     *
     * @throws IllegalArgumentException if the environment is missing or invalid, naming the variable.
     */
    public DatabricksRecordHandler(Map<String, String> configOptions)
    {
        this(configOptions,
                new MeteredConnectionConfigProvider(
                        new EnvironmentConnectionConfigProvider(configOptions),
                        new ConnectorMetrics(DatabricksMetadataHandler.SOURCE_TYPE)));
    }

    /**
     * @param configs resolves the endpoint. A parameter so a later phase can supply an SSM-backed
     *                implementation without touching this class.
     */
    public DatabricksRecordHandler(Map<String, String> configOptions,
                                   ConnectionConfigProvider configs)
    {
        this(configOptions, configs, connectionFactoryFor(configs));
    }

    /**
     * The constructor that calls {@code super}. Split out because the factory has to exist before the
     * {@code super(...)} expression, where it appears twice (once for the JDBC URL the base class
     * records, once as the factory itself) and {@code this} is not available.
     */
    private DatabricksRecordHandler(Map<String, String> configOptions,
                                    ConnectionConfigProvider configs,
                                    DatabricksConnectionFactory factory)
    {
        // The six-argument constructor is the only one that sets the connection factory the inherited
        // read loop uses; the shorter one is for the multiplexing handler and leaves it null.
        super(S3Client.create(),
                SecretsManagerClient.create(),
                AthenaClient.create(),
                // The three-argument DatabaseConnectionConfig, with no secret name. With one, the base
                // class resolves it into a user-and-password pair the Databricks driver cannot use.
                new DatabaseConnectionConfig(
                        // No request exists at construction, so this is the other legitimate null.
                        configs.configFor(null).catalog(),
                        DatabricksMetadataHandler.SOURCE_TYPE,
                        factory.url()),
                factory,
                configOptions);
        this.configs = configs;
        this.queryBuilder = new DatabricksQueryBuilder(configs.configFor(null).catalog());
        this.maxRowsPerTable = Settings.maxRowsPerTable(configOptions);
        // Its own instance rather than one threaded down from the entry-point constructor. The emitter
        // holds no state beyond the connector id, so a second one costs nothing, and threading it would
        // mean a fourth constructor on a class that already has three.
        this.metrics = new ConnectorMetrics(DatabricksMetadataHandler.SOURCE_TYPE);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Wraps the spiller so the inherited loop is bounded, then hands off. A driver failure is
     * classified on the way out, so a stopped warehouse reads as one rather than as a generic query
     * error.
     *
     * <p>The pin is checked here, before anything opens a connection, and again in
     * {@link #buildSplitSql}.
     */
    @Override
    public void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest request,
                                   QueryStatusChecker queryStatusChecker)
            throws Exception
    {
        requireServableSchema(configs.configFor(request.getCatalogName()), request.getTableName());
        String table = request.getTableName().getTableName();
        String catalog = request.getCatalogName();
        RowCeilingSpiller bounded = new RowCeilingSpiller(spiller, table, maxRowsPerTable);
        try {
            super.readWithConstraint(bounded, request, queryStatusChecker);
        }
        // SQLException AND RuntimeException: the driver's authentication path throws
        // DatabricksDriverException, which extends RuntimeException, so a catch on SQLException alone
        // lets an OAuth failure escape both classification and redaction. asConnectorFailure passes the
        // row ceiling's own AthenaConnectorException through rather than re-wrapping it.
        catch (SQLException | RuntimeException cause) {
            // The spiller counts the row that breached before refusing it, so the comparison identifies
            // a ceiling breach exactly. Matching on the exception's message would work today and break
            // the first time that message is reworded.
            if (bounded.rowsWritten() > maxRowsPerTable) {
                metrics.count(ConnectorMetrics.TABLE_CEILING_EXCEEDED, catalog);
            }
            throw DatabricksErrors.asConnectorFailure(
                    "reading rows from " + qualified(request), cause);
        }
        metrics.emit(ConnectorMetrics.ROWS_RETURNED, bounded.rowsWritten(),
                ConnectorMetrics.UNIT_COUNT, catalog);
        // Row count only. No predicate value, no SQL, no identifier beyond the table's name.
        LOGGER.info("Read {} rows from {}", bounded.rowsWritten(), table);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Always null. The base class resolves a secret into a user-and-password pair, and a Databricks
     * credential is neither: a personal access token goes in {@code PWD} under the fixed user
     * {@code token}, and OAuth M2M has no user. {@link DatabricksConnectionFactory} resolves its own.
     */
    @Override
    protected CredentialsProvider getCredentialProvider()
    {
        return null;
    }

    /**
     * {@inheritDoc}
     *
     * @param catalogName Athena's catalog name. Ignored by the query builder, which substitutes the Unity
     *                    Catalog catalog, but it is what resolves the configuration this request is
     *                    served from, so the pin is checked against the right one.
     */
    @Override
    public PreparedStatement buildSplitSql(Connection jdbcConnection, String catalogName,
                                           TableName tableName, Schema schema,
                                           Constraints constraints, Split split)
            throws SQLException
    {
        // Also checked in readWithConstraint, which is earlier and cheaper. Repeated here because this is
        // the method that turns a request into SQL, and it is public: an inherited read loop, a
        // multiplexing handler or a later override reaches it without going through the other one.
        requireServableSchema(configs.configFor(catalogName), tableName);
        PreparedStatement statement = queryBuilder.buildSql(
                jdbcConnection,
                catalogName,
                tableName.getSchemaName(),
                tableName.getTableName(),
                schema,
                constraints,
                split);
        statement.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
        return statement;
    }

    /**
     * Refuses a read of a schema a pinned connector does not serve.
     *
     * <p>{@link ConnectionConfig#SCHEMA_VAR} is documented as a containment boundary independent of the
     * credential's Unity Catalog grants, and until this check existed it was one only on the metadata
     * path. Athena reaches a read through {@code GetTable}, which the metadata handler refuses first, but
     * a principal holding {@code lambda:InvokeFunction} can post a hand-built {@code ReadRecordsRequest}
     * naming any schema, and every statement this connector builds takes the schema from the request. So
     * the boundary held only because of the order Athena happens to call in.
     *
     * <p>Nothing to check when the connector is unpinned: there the credential's grants are the boundary,
     * and the metadata handler's own gate decides which schemas are addressable.
     *
     * <p>Static, and taking the configuration rather than reading a field, so it is testable: this class
     * builds three AWS clients in its constructor.
     *
     * @throws IllegalArgumentException if {@code config} is pinned to another schema.
     */
    static void requireServableSchema(ConnectionConfig config, TableName tableName)
    {
        if (!config.isSchemaPinned()) {
            return;
        }
        String schema = tableName.getSchemaName();
        if (!config.schema().equals(schema)) {
            throw new IllegalArgumentException(
                    "Refusing to read \"" + schema + "." + tableName.getTableName() + "\". This"
                            + " connector is pinned to schema \"" + config.schema() + "\" in catalog \""
                            + config.catalog() + "\" by " + ConnectionConfig.SCHEMA_VAR + ", and that"
                            + " pin bounds the record path as well as the metadata path. Unset "
                            + ConnectionConfig.SCHEMA_VAR + " to serve every schema in the catalog, or"
                            + " deploy a second connector for \"" + schema + "\".");
        }
    }

    private String qualified(ReadRecordsRequest request)
    {
        ConnectionConfig config = configs.configFor(request.getCatalogName());
        return config.catalog() + "." + request.getTableName().getSchemaName() + "."
                + request.getTableName().getTableName();
    }

    private static DatabricksConnectionFactory connectionFactoryFor(ConnectionConfigProvider configs)
    {
        return new DatabricksConnectionFactory(
                configs.configFor(null), new CredentialSource(new SecretsManagerReader()));
    }
}
