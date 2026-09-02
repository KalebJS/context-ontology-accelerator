// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package dev.coa.databricks;

import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The capability map, and that silence is the default. */
class PushdownCapabilitiesTest
{
    @Test
    void advertisesNothingByDefault()
    {
        // An advertisement is a guarantee: Athena stops applying an optimisation it believes the connector
        // honours, so a limit the record path ignores returns too many rows with no error, and a predicate
        // form the driver rejects fails at query time.
        assertTrue(PushdownCapabilities.from(null).isEmpty());
        assertTrue(PushdownCapabilities.from("").isEmpty());
        assertTrue(PushdownCapabilities.from("   ").isEmpty());
        assertTrue(PushdownCapabilities.from(",,").isEmpty());
    }

    @Test
    void advertisesNothingWhenTheEnvironmentDoesNotAskForIt()
    {
        assertTrue(PushdownCapabilities.from(Settings.advertisedPushdown(new HashMap<>())).isEmpty());
    }

    @Test
    void limitAdvertisesAnIntegerConstantLimit()
    {
        Map<String, List<OptimizationSubType>> capabilities =
                PushdownCapabilities.from(PushdownCapabilities.LIMIT);
        assertEquals(1, capabilities.size());
        assertTrue(capabilities.containsKey("supports_limit_pushdown"),
                "unexpected key set: " + capabilities.keySet());
        assertEquals("integer_constant",
                capabilities.get("supports_limit_pushdown").get(0).getSubType());
    }

    @Test
    void filterAdvertisesSortedRangeSetsAndNullableComparison()
    {
        Map<String, List<OptimizationSubType>> capabilities =
                PushdownCapabilities.from(PushdownCapabilities.FILTER);
        assertEquals(1, capabilities.size());
        assertTrue(capabilities.containsKey("supports_filter_pushdown"),
                "unexpected key set: " + capabilities.keySet());
        assertEquals(2, capabilities.get("supports_filter_pushdown").size());
    }

    @Test
    void topNAdvertisesOrderBy()
    {
        Map<String, List<OptimizationSubType>> capabilities =
                PushdownCapabilities.from(PushdownCapabilities.TOP_N);
        assertTrue(capabilities.containsKey("supports_top_n_pushdown"),
                "unexpected key set: " + capabilities.keySet());
    }

    @Test
    void severalNamesCombine()
    {
        Map<String, List<OptimizationSubType>> capabilities =
                PushdownCapabilities.from("filter, limit ,topn");
        assertEquals(3, capabilities.size());
    }

    @Test
    void namesAreCaseInsensitiveAndTrimmed()
    {
        assertEquals(1, PushdownCapabilities.from("  LIMIT  ").size());
    }

    @Test
    void anUnrecognisedNameIsIgnoredRatherThanFatal()
    {
        // A tuning knob. Refusing to start over a typo in it makes the connector unavailable rather than
        // merely slower.
        Map<String, List<OptimizationSubType>> capabilities =
                PushdownCapabilities.from("limit,aggregation,typo");
        assertEquals(1, capabilities.size());
        assertTrue(capabilities.containsKey("supports_limit_pushdown"));
    }

    @Test
    void aggregationIsNotAdvertisableAtAll()
    {
        // Athena's federation protocol has no way to express aggregation, so a GROUP BY reads every
        // predicate-matching row out of the warehouse whatever is advertised.
        assertTrue(PushdownCapabilities.from("aggregation").isEmpty());
        assertTrue(!PushdownCapabilities.KNOWN.contains("aggregation"));
    }

    @Test
    void theKnownNamesAreTheThreeTheReadmeLists()
    {
        assertEquals(3, PushdownCapabilities.KNOWN.size());
        assertTrue(PushdownCapabilities.KNOWN.contains(PushdownCapabilities.FILTER));
        assertTrue(PushdownCapabilities.KNOWN.contains(PushdownCapabilities.LIMIT));
        assertTrue(PushdownCapabilities.KNOWN.contains(PushdownCapabilities.TOP_N));
    }

    @Test
    void complexExpressionPushdownIsNotOfferedAtAll()
    {
        // Offering it would be offering a switch that breaks queries. DatabricksQueryBuilder uses
        // JdbcSplitQueryBuilder's single-argument constructor, which installs
        // DefaultJdbcFederationExpressionParser, whose mapFunctionToDataSourceSyntax is an unconditional
        // throw in 2025.15.1: "Subclass does not yet support complex expressions."
        assertTrue(PushdownCapabilities.from("complex").isEmpty());
        assertTrue(!PushdownCapabilities.KNOWN.contains("complex"));
        assertTrue(PushdownCapabilities.from("filter,complex,limit").size() == 2);
    }

    @Test
    void theResultIsImmutable()
    {
        Map<String, List<OptimizationSubType>> capabilities = PushdownCapabilities.from("limit");
        try {
            capabilities.put("supports_aggregation_pushdown", null);
            assertTrue(false, "the capability map must not be mutable");
        }
        catch (UnsupportedOperationException expected) {
            assertEquals(1, capabilities.size());
        }
    }
}
