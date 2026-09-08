// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package dev.coa.databricks;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import dev.coa.databricks.config.ConnectionConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The pinned-schema boundary on the record path.
 *
 * <p>{@code DATABRICKS_SCHEMA} is documented as a containment boundary independent of the credential's
 * Unity Catalog grants, and it was one only on the metadata path: the record handler took the schema from
 * the request and never compared it to the pin. Athena cannot reach a read without calling
 * {@code GetTable} first, which the metadata handler refuses, but a principal holding
 * {@code lambda:InvokeFunction} can post a hand-built {@code ReadRecordsRequest}, and then the only
 * boundary left was the credential's grants — which is precisely what the pin is documented as sitting on
 * top of.
 *
 * <p>The check is static because {@link DatabricksRecordHandler}'s constructor builds three AWS clients,
 * so the class cannot be instantiated in a unit test.
 */
class DatabricksRecordHandlerTest
{
    private static ConnectionConfig.Builder valid()
    {
        return ConnectionConfig.builder()
                .workspaceHostname("dbc-a1b2345c-d6e7.cloud.databricks.com")
                .httpPath("/sql/1.0/warehouses/a1b234c567d8e9fa")
                .catalog("main")
                .credentialSecretArn(
                        "arn:aws:secretsmanager:us-east-1:111122223333:secret:dbx-AbCdEf");
    }

    @Test
    void aPinnedConnectorRefusesToReadAnotherSchema()
    {
        ConnectionConfig pinned = valid().schema("sales").build();

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> DatabricksRecordHandler.requireServableSchema(
                        pinned, new TableName("finance", "salaries")));

        assertTrue(failure.getMessage().contains("finance"), failure.getMessage());
        assertTrue(failure.getMessage().contains("sales"), failure.getMessage());
        // Set, so naming it is the actionable part: unsetting it is one of the two fixes.
        assertTrue(failure.getMessage().contains(ConnectionConfig.SCHEMA_VAR), failure.getMessage());
    }

    @Test
    void aPinnedConnectorReadsItsOwnSchema()
    {
        ConnectionConfig pinned = valid().schema("sales").build();

        assertDoesNotThrow(() -> DatabricksRecordHandler.requireServableSchema(
                pinned, new TableName("sales", "orders")));
    }

    @Test
    void anUnpinnedConnectorLeavesTheCredentialsGrantsAsTheBoundary()
    {
        // Nothing to compare against: the unpinned mode's contract is that the credential's Unity Catalog
        // grants decide, and the metadata handler's own gate decides which names are addressable at all.
        ConnectionConfig unpinned = valid().build();

        assertDoesNotThrow(() -> DatabricksRecordHandler.requireServableSchema(
                unpinned, new TableName("anything", "orders")));
    }

    @Test
    void theComparisonIsExactRatherThanCaseFolded()
    {
        // The same comparison the metadata path makes, so a name that got past one cannot fail only at the
        // other. config.schema() is already folded; a request naming "Sales" was never advertised.
        ConnectionConfig pinned = valid().schema("sales").build();

        assertThrows(IllegalArgumentException.class,
                () -> DatabricksRecordHandler.requireServableSchema(
                        pinned, new TableName("Sales", "orders")));
    }
}
