// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package dev.coa.databricks.config;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Caching, and that the cache is keyed on the secret's ARN rather than assumed constant. */
class CredentialSourceTest
{
    private static final String ARN =
            "arn:aws:secretsmanager:eu-central-1:111122223333:secret:dbx-AbCdEf";
    private static final String OTHER_ARN =
            "arn:aws:secretsmanager:eu-central-1:111122223333:secret:dbx-Other0";

    private static ConnectionConfig configFor(String secretArn)
    {
        return ConnectionConfig.builder()
                .workspaceHostname("dbc-a1b2345c-d6e7.cloud.databricks.com")
                .httpPath("/sql/1.0/warehouses/a1b234c567d8e9fa")
                .catalog("main")
                .schema("sales")
                .credentialSecretArn(secretArn)
                .build();
    }

    @Test
    void readsTheSecretOnceAndThenServesTheCachedCredential()
    {
        AtomicInteger reads = new AtomicInteger();
        CredentialSource source = new CredentialSource(arn -> {
            reads.incrementAndGet();
            return "{\"token\": \"dapi-example\"}";
        });
        ConnectionConfig config = configFor(ARN);

        DatabricksCredential first = source.credentialFor(config);
        assertSame(first, source.credentialFor(config));
        assertEquals(1, reads.get());
    }

    @Test
    void readsAgainWhenTheSecretArnChanges()
    {
        // Not reachable in this phase, with one endpoint fixed at deploy time, but the cache has to be keyed
        // rather than unconditional so a later per-catalog provider cannot serve one source's credential to
        // another.
        AtomicInteger reads = new AtomicInteger();
        CredentialSource source = new CredentialSource(arn -> {
            reads.incrementAndGet();
            return "{\"token\": \"dapi-" + arn + "\"}";
        });

        source.credentialFor(configFor(ARN));
        source.credentialFor(configFor(OTHER_ARN));
        assertEquals(2, reads.get());
    }

    @Test
    void readsEveryTimeWhenTheTtlIsZero()
    {
        AtomicInteger reads = new AtomicInteger();
        CredentialSource source = new CredentialSource(arn -> {
            reads.incrementAndGet();
            return "{\"token\": \"dapi-example\"}";
        }, 0L);
        ConnectionConfig config = configFor(ARN);

        source.credentialFor(config);
        source.credentialFor(config);
        assertEquals(2, reads.get());
    }

    @Test
    void propagatesAnUnusableSecretShape()
    {
        CredentialSource source =
                new CredentialSource(arn -> "{\"username\": \"u\", \"password\": \"p\"}");
        assertThrows(IllegalArgumentException.class, () -> source.credentialFor(configFor(ARN)));
    }

    @Test
    void refusesANullReaderOrNegativeTtl()
    {
        assertThrows(NullPointerException.class, () -> new CredentialSource(null));
        assertThrows(IllegalArgumentException.class,
                () -> new CredentialSource(arn -> "{}", -1L));
    }
}
