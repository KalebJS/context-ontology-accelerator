// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package dev.coa.databricks.config;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Reading the five coordinates out of the Lambda's environment. */
class EnvironmentConnectionConfigProviderTest
{
    private static Map<String, String> environment()
    {
        Map<String, String> environment = new HashMap<>();
        environment.put(ConnectionConfig.WORKSPACE_HOSTNAME_VAR,
                "dbc-a1b2345c-d6e7.cloud.databricks.com");
        environment.put(ConnectionConfig.HTTP_PATH_VAR, "/sql/1.0/warehouses/a1b234c567d8e9fa");
        environment.put(ConnectionConfig.CATALOG_VAR, "main");
        environment.put(ConnectionConfig.SCHEMA_VAR, "sales");
        environment.put(ConnectionConfig.CREDENTIAL_SECRET_ARN_VAR,
                "arn:aws:secretsmanager:eu-central-1:111122223333:secret:dbx-AbCdEf");
        return environment;
    }

    @Test
    void readsAllFiveCoordinates()
    {
        ConnectionConfig config =
                new EnvironmentConnectionConfigProvider(environment()).configFor("anything");
        assertEquals("dbc-a1b2345c-d6e7.cloud.databricks.com", config.workspaceHostname());
        assertEquals("/sql/1.0/warehouses/a1b234c567d8e9fa", config.httpPath());
        assertEquals("main", config.catalog());
        assertEquals("sales", config.schema());
    }

    @Test
    void anAbsentSchemaVariableLeavesTheConnectorUnpinned()
    {
        Map<String, String> unpinned = environment();
        unpinned.remove(ConnectionConfig.SCHEMA_VAR);

        ConnectionConfig config =
                new EnvironmentConnectionConfigProvider(unpinned).configFor(null);

        assertFalse(config.isSchemaPinned());
        assertEquals("main", config.catalog(), "the catalog is still required");
    }

    @Test
    void anEmptySchemaVariableLeavesTheConnectorUnpinned()
    {
        // CDK omits the variable; a console edit or a shell `export DATABRICKS_SCHEMA=` leaves it present
        // and empty. Both mean "unpinned" to an operator, so both mean it here.
        for (String blank : new String[] {"", "   "}) {
            Map<String, String> unpinned = environment();
            unpinned.put(ConnectionConfig.SCHEMA_VAR, blank);

            assertFalse(new EnvironmentConnectionConfigProvider(unpinned).configFor(null)
                            .isSchemaPinned(),
                    "DATABRICKS_SCHEMA=\"" + blank + "\" should leave it unpinned");
        }
    }

    @Test
    void aMalformedSchemaVariableStillFailsAtInitialisation()
    {
        // Optional must not mean unvalidated. Failing during Lambda init, naming the variable, is the
        // difference between one loud cold-start error and a per-query "Unknown schema" that looks like the
        // unpinned mode working as designed.
        Map<String, String> malformed = environment();
        malformed.put(ConnectionConfig.SCHEMA_VAR, "not a schema");

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> new EnvironmentConnectionConfigProvider(malformed));
        assertTrue(failure.getMessage().contains(ConnectionConfig.SCHEMA_VAR),
                failure.getMessage());
    }

    @Test
    void anAbsentCatalogVariableStillFails()
    {
        // An Athena federated catalog has one namespace level below the registered name and this connector
        // spends it on the schema, so the UC catalog cannot travel in a request.
        Map<String, String> noCatalog = environment();
        noCatalog.remove(ConnectionConfig.CATALOG_VAR);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> new EnvironmentConnectionConfigProvider(noCatalog));
        assertTrue(failure.getMessage().contains(ConnectionConfig.CATALOG_VAR),
                failure.getMessage());
    }

    @Test
    void servesTheSameConfigurationForEveryCatalogName()
    {
        // One endpoint, fixed at deploy time. The argument is there because Athena passes the catalog name
        // on every call path, which is what a multiplexed phase resolves on.
        EnvironmentConnectionConfigProvider provider =
                new EnvironmentConnectionConfigProvider(environment());
        assertSame(provider.configFor("scldevds_144a95d84d98c87d"), provider.configFor(null));
    }

    @Test
    void matchesVariableNamesCaseInsensitively()
    {
        Map<String, String> lowerCased = new HashMap<>();
        for (Map.Entry<String, String> entry : environment().entrySet()) {
            lowerCased.put(entry.getKey().toLowerCase(Locale.ROOT), entry.getValue());
        }
        assertEquals("sales",
                new EnvironmentConnectionConfigProvider(lowerCased).configFor(null).schema());
    }

    @Test
    void matchesAGenuinelyMixedCaseVariableName()
    {
        // An exact/lower/upper triple resolves databricks_schema and DATABRICKS_SCHEMA but not
        // Databricks_Schema, which is why the lookup scans.
        Map<String, String> mixedCase = new HashMap<>();
        for (Map.Entry<String, String> entry : environment().entrySet()) {
            String name = entry.getKey();
            StringBuilder camel = new StringBuilder();
            for (String word : name.split("_")) {
                camel.append(camel.length() == 0 ? "" : "_")
                        .append(word.charAt(0))
                        .append(word.substring(1).toLowerCase(Locale.ROOT));
            }
            mixedCase.put(camel.toString(), entry.getValue());
        }
        // The keys really are neither all-lower nor all-upper.
        for (String key : mixedCase.keySet()) {
            assertNotEquals(key, key.toLowerCase(Locale.ROOT), key);
            assertNotEquals(key, key.toUpperCase(Locale.ROOT), key);
        }
        assertEquals("sales",
                new EnvironmentConnectionConfigProvider(mixedCase).configFor(null).schema());
    }

    @Test
    void failsAtConstructionNamingTheMissingVariable()
    {
        Map<String, String> incomplete = environment();
        incomplete.remove(ConnectionConfig.HTTP_PATH_VAR);
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> new EnvironmentConnectionConfigProvider(incomplete));
        assertTrue(failure.getMessage().contains(ConnectionConfig.HTTP_PATH_VAR),
                failure.getMessage());
        // The hint has to say what a correct value looks like: "not set" alone sends the operator to the
        // Databricks console with nothing to search for.
        assertTrue(failure.getMessage().contains("/sql/1.0/warehouses/"), failure.getMessage());
    }

    @Test
    void failsOnAnEmptyEnvironment()
    {
        assertThrows(IllegalArgumentException.class,
                () -> new EnvironmentConnectionConfigProvider(new HashMap<>()));
        assertThrows(IllegalArgumentException.class,
                () -> new EnvironmentConnectionConfigProvider(null));
    }
}
