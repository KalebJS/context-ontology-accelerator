// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package dev.coa.databricks.config;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Auth-mode selection from the secret's shape, and the JDBC properties each mode produces. */
class DatabricksCredentialTest
{
    private static final String ARN =
            "arn:aws:secretsmanager:eu-central-1:111122223333:secret:dbx-AbCdEf";

    @Test
    void aTokenSelectsPersonalAccessToken()
    {
        DatabricksCredential credential =
                DatabricksCredential.fromSecretJson("{\"token\": \"dapi-example\"}", ARN);
        assertEquals(DatabricksCredential.Mode.PERSONAL_ACCESS_TOKEN, credential.mode());

        Properties properties = new Properties();
        credential.applyTo(properties);
        // AuthMech 3 and 11 are the only values the driver's own switch accepts, verified by disassembling
        // AuthMech.parseAuthMechValue.
        assertEquals("3", properties.getProperty("AuthMech"));
        assertEquals("token", properties.getProperty("UID"));
        assertEquals("dapi-example", properties.getProperty("PWD"));
        assertNull(properties.getProperty("OAuth2ClientId"));
        assertNull(properties.getProperty("Auth_Flow"));
    }

    @Test
    void aClientIdAndSecretSelectOauthMachineToMachine()
    {
        DatabricksCredential credential = DatabricksCredential.fromSecretJson(
                "{\"client_id\": \"sp-1234\", \"client_secret\": \"dose-example\"}", ARN);
        assertEquals(DatabricksCredential.Mode.OAUTH_M2M, credential.mode());

        Properties properties = new Properties();
        credential.applyTo(properties);
        assertEquals("11", properties.getProperty("AuthMech"));
        assertEquals("1", properties.getProperty("Auth_Flow"));
        assertEquals("sp-1234", properties.getProperty("OAuth2ClientId"));
        assertEquals("dose-example", properties.getProperty("OAuth2Secret"));
        assertNull(properties.getProperty("PWD"));
        assertNull(properties.getProperty("UID"));
    }

    @Test
    void refusesASecretCarryingBothShapes()
    {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> DatabricksCredential.fromSecretJson(
                        "{\"token\": \"dapi-example\", \"client_id\": \"sp\","
                                + " \"client_secret\": \"s\"}", ARN));
        assertTrue(failure.getMessage().contains("both"), failure.getMessage());
        assertTrue(failure.getMessage().contains(ARN), failure.getMessage());
    }

    @Test
    void refusesTokenAlongsideAClientIdAlone()
    {
        // A half-written OAuth secret plus a token is still ambiguous, so it is refused rather than resolved
        // to the token by precedence.
        assertThrows(IllegalArgumentException.class, () -> DatabricksCredential.fromSecretJson(
                "{\"token\": \"dapi\", \"client_id\": \"sp\"}", ARN));
    }

    @Test
    void refusesAShapeThatIsNeither()
    {
        for (String json : new String[] {
            "{\"username\": \"u\", \"password\": \"p\"}",
            "{}",
            "{\"accessToken\": \"x\"}"}) {
            IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                    () -> DatabricksCredential.fromSecretJson(json, ARN));
            assertTrue(failure.getMessage().contains("neither supported shape"), failure.getMessage());
        }
    }

    @Test
    void refusesAnIncompleteOauthShape()
    {
        assertThrows(IllegalArgumentException.class,
                () -> DatabricksCredential.fromSecretJson("{\"client_id\": \"sp-1234\"}", ARN));
        assertThrows(IllegalArgumentException.class,
                () -> DatabricksCredential.fromSecretJson("{\"client_secret\": \"s\"}", ARN));
        assertThrows(IllegalArgumentException.class, () -> DatabricksCredential.fromSecretJson(
                "{\"client_id\": \"sp\", \"client_secret\": \"  \"}", ARN));
    }

    @Test
    void refusesAnEmptyOrNonJsonSecret()
    {
        assertThrows(IllegalArgumentException.class,
                () -> DatabricksCredential.fromSecretJson(null, ARN));
        assertThrows(IllegalArgumentException.class,
                () -> DatabricksCredential.fromSecretJson("   ", ARN));
        assertThrows(IllegalArgumentException.class,
                () -> DatabricksCredential.fromSecretJson("dapi-a-bare-token", ARN));
        assertThrows(IllegalArgumentException.class,
                () -> DatabricksCredential.fromSecretJson("[\"token\"]", ARN));
    }

    @Test
    void aRejectionNeverEchoesTheSecretsValue()
    {
        // A parse failure's own message quotes the input, which is why it is not chained through.
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> DatabricksCredential.fromSecretJson("{\"token\": \"dapi-SUPERSECRET\",,}", ARN));
        assertFalse(failure.getMessage().contains("SUPERSECRET"), failure.getMessage());
    }

    @Test
    void ignoresUnrelatedKeys()
    {
        // Customers put comments and rotation metadata in secrets, so an extra key is not an error.
        DatabricksCredential credential = DatabricksCredential.fromSecretJson(
                "{\"token\": \"dapi-example\", \"comment\": \"rotated 2026-08-01\"}", ARN);
        assertEquals(DatabricksCredential.Mode.PERSONAL_ACCESS_TOKEN, credential.mode());
    }

    @Test
    void toStringNamesTheModeAndNothingElse()
    {
        String rendered = DatabricksCredential
                .fromSecretJson("{\"token\": \"dapi-SUPERSECRET\"}", ARN).toString();
        assertFalse(rendered.contains("SUPERSECRET"), rendered);
        assertTrue(rendered.contains("PERSONAL_ACCESS_TOKEN"), rendered);
    }

    @Test
    void applyToRefusesANullPropertiesObject()
    {
        DatabricksCredential credential =
                DatabricksCredential.fromSecretJson("{\"token\": \"t\"}", ARN);
        assertThrows(IllegalArgumentException.class, () -> credential.applyTo(null));
    }
}
