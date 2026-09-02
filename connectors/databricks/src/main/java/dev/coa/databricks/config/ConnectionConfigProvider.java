// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package dev.coa.databricks.config;

/**
 * Resolves the {@link ConnectionConfig} for the Athena catalog a request arrived under.
 *
 * <p>{@link EnvironmentConnectionConfigProvider} ignores the argument, since it serves one endpoint
 * fixed at deploy time. The parameter is here because Athena passes the registered catalog name
 * verbatim on every call path, which makes it the discriminator a multiplexed deployment resolves on.
 *
 * <p>An implementation is called on every request, so it should cache, and it should re-validate
 * whatever it read: a store that can be written to is not a trusted input.
 */
public interface ConnectionConfigProvider
{
    /**
     * @param athenaCatalogName the catalog name Athena invoked the connector under. <b>May be
     *                          null</b>, at exactly two sites that run before any request exists:
     *                          each handler's constructor, for its cold-start log line and its query
     *                          builder. An implementation that discriminates on the name has to
     *                          either serve a deployment-wide default for null or fail saying it was
     *                          asked to resolve configuration before Athena named a catalog.
     * @throws IllegalArgumentException if no valid configuration exists for it. The message reaches
     *                                 the user's query, so it has to name what is wrong.
     */
    ConnectionConfig configFor(String athenaCatalogName);
}
