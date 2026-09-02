// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package dev.coa.databricks;

import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;

/**
 * Lambda entry point: routes metadata and record requests to the two handlers. The Lambda handler string
 * is {@code dev.coa.databricks.DatabricksCompositeHandler}.
 *
 * <p>Both halves read the same environment and validate the same variables. The duplication is worth it:
 * a connector whose metadata half starts and whose record half does not would discover a schema and then
 * fail every query.
 */
public class DatabricksCompositeHandler extends CompositeHandler
{
    public DatabricksCompositeHandler()
    {
        super(new DatabricksMetadataHandler(System.getenv()),
                new DatabricksRecordHandler(System.getenv()));
    }
}
