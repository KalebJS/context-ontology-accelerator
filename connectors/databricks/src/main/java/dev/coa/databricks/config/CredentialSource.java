// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package dev.coa.databricks.config;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

/**
 * Reads a credential secret and caches the parsed result per container, so a rotated credential takes
 * effect without waiting for containers to recycle.
 *
 * <p>Takes a reader rather than a Secrets Manager client. The federation SDK's {@code MetadataHandler}
 * and {@code RecordHandler} already hold a caching client and expose it as {@code getSecret(name)}, so
 * passing that method in reuses one client, one set of credentials and one retry policy, and lets this
 * class be exercised with a lambda instead of an AWS mock. Constructing a
 * {@code SecretsManagerClient} here would put a network client in a unit test's constructor.
 *
 * <p>The TTL is jittered. Discovery is a per-table {@code DESCRIBE} fan-out, so a schema's worth of
 * containers can expire their caches in the same second and stampede Secrets Manager. A random ±20%
 * spread on each deadline breaks the alignment for one call to a thread-local random.
 *
 * <p>Thread-safe: the cached value is only replaced wholesale, and a benign race re-reads the secret.
 */
public final class CredentialSource
{
    /** Default cache lifetime, before jitter. */
    public static final long DEFAULT_TTL_MILLIS = 5L * 60L * 1000L;

    private final Function<String, String> secretReader;
    private final long ttlMillis;

    private volatile Cached cached;

    /** @param secretReader reads a secret's value by ARN or name, normally {@code this::getSecret}. */
    public CredentialSource(Function<String, String> secretReader)
    {
        this(secretReader, DEFAULT_TTL_MILLIS);
    }

    /**
     * @param ttlMillis cache lifetime before jitter. Zero disables caching, which is what a test
     *                  asserting the reader was called wants.
     */
    public CredentialSource(Function<String, String> secretReader, long ttlMillis)
    {
        this.secretReader = Objects.requireNonNull(secretReader, "secretReader");
        if (ttlMillis < 0) {
            throw new IllegalArgumentException("ttlMillis must not be negative");
        }
        this.ttlMillis = ttlMillis;
    }

    /**
     * The parsed credential, from cache when it is still fresh and the ARN has not changed.
     *
     * @throws IllegalArgumentException if the secret is unreadable or is not a supported shape.
     */
    public DatabricksCredential credentialFor(ConnectionConfig config)
    {
        String arn = config.credentialSecretArn();
        Cached snapshot = cached;
        if (snapshot != null && snapshot.isFreshFor(arn)) {
            return snapshot.credential;
        }
        DatabricksCredential credential =
                DatabricksCredential.fromSecretJson(secretReader.apply(arn), arn);
        cached = new Cached(arn, credential, System.currentTimeMillis() + jittered(ttlMillis));
        return credential;
    }

    /** {@code ttl} scattered by up to ±20%, so caches across containers do not align. */
    private static long jittered(long ttl)
    {
        if (ttl == 0) {
            return 0;
        }
        long spread = Math.max(1L, ttl / 5L);
        return ttl - spread + ThreadLocalRandom.current().nextLong(2L * spread);
    }

    private static final class Cached
    {
        private final String secretArn;
        private final DatabricksCredential credential;
        private final long expiresAtMillis;

        private Cached(String secretArn, DatabricksCredential credential, long expiresAtMillis)
        {
            this.secretArn = secretArn;
            this.credential = credential;
            this.expiresAtMillis = expiresAtMillis;
        }

        private boolean isFreshFor(String arn)
        {
            return secretArn.equals(arn) && System.currentTimeMillis() < expiresAtMillis;
        }
    }
}
