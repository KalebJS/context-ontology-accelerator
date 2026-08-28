// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package dev.coa.example;

import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;

/**
 * Lambda entry point: routes metadata and record requests to the two handlers.
 * Lambda handler string: {@code dev.coa.example.ExampleCompositeHandler}
 */
public class ExampleCompositeHandler extends CompositeHandler
{
    public ExampleCompositeHandler()
    {
        super(new ExampleMetadataHandler(System.getenv()), new ExampleRecordHandler(System.getenv()));
    }
}
