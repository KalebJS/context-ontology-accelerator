// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package dev.coa.databricks.config;

import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;

import java.util.function.Function;

/**
 * Reads a secret's value by ARN, building its Secrets Manager client on first use.
 *
 * <p>Lazily, because this is constructed before {@code super(...)} in
 * {@link dev.coa.databricks.DatabricksRecordHandler}, where {@code this::getSecret} is not yet
 * referenceable, so it has to be constructible without credentials or a region.
 *
 * <p>Prefer the handler's own {@code this::getSecret} where it is available, since that goes through the
 * SDK's caching client. This is for the one place it is not.
 *
 * <p>Thread-safe. A benign race builds a second client and discards it.
 */
public final class SecretsManagerReader implements Function<String, String>
{
    private volatile SecretsManagerClient client;

    /** @param secretId the secret's ARN or name. */
    @Override
    public String apply(String secretId)
    {
        return client().getSecretValue(
                        GetSecretValueRequest.builder().secretId(secretId).build())
                .secretString();
    }

    private SecretsManagerClient client()
    {
        SecretsManagerClient snapshot = client;
        if (snapshot == null) {
            snapshot = SecretsManagerClient.create();
            client = snapshot;
        }
        return snapshot;
    }
}
