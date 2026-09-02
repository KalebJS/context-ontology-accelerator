// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package dev.coa.connector.metrics;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The document has to be valid EMF or CloudWatch silently extracts nothing — there is no error, the
 * metric simply never appears. So these tests parse the line rather than matching substrings.
 */
class ConnectorMetricsTest
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final List<String> emitted = new ArrayList<>();
    private final ConnectorMetrics metrics = new ConnectorMetrics("databricks", emitted::add);

    private JsonNode only() throws Exception
    {
        assertEquals(1, emitted.size(), "expected exactly one emitted line");
        return MAPPER.readTree(emitted.get(0));
    }

    private JsonNode metricDirective(JsonNode doc)
    {
        return doc.get("_aws").get("CloudWatchMetrics").get(0);
    }

    @Test
    void countEmitsValidEmfWithBothDimensionSets() throws Exception
    {
        metrics.count(ConnectorMetrics.ROWS_RETURNED, "acme_dbx");

        JsonNode doc = only();
        JsonNode directive = metricDirective(doc);

        assertEquals(ConnectorMetrics.NAMESPACE, directive.get("Namespace").asText());
        assertEquals(ConnectorMetrics.ROWS_RETURNED, directive.get("Metrics").get(0).get("Name").asText());
        assertEquals(ConnectorMetrics.UNIT_COUNT, directive.get("Metrics").get(0).get("Unit").asText());

        JsonNode dimensions = directive.get("Dimensions");
        assertEquals(2, dimensions.size(), "per-catalog and fleet-wide sets");
        assertEquals("[\"Connector\",\"Catalog\"]", dimensions.get(0).toString());
        assertEquals("[\"Connector\"]", dimensions.get(1).toString());

        // Every dimension named in the directive must also exist as a top-level property, or the whole
        // document is discarded.
        assertEquals("databricks", doc.get("Connector").asText());
        assertEquals("acme_dbx", doc.get("Catalog").asText());
        assertEquals(1, doc.get(ConnectorMetrics.ROWS_RETURNED).asInt());
    }

    @Test
    void nullCatalogDropsThatDimensionRatherThanInventingAValue() throws Exception
    {
        metrics.count(ConnectorMetrics.WAREHOUSE_CONNECT_FAILURES, null);

        JsonNode doc = only();
        JsonNode dimensions = metricDirective(doc).get("Dimensions");

        assertEquals(1, dimensions.size());
        assertEquals("[\"Connector\"]", dimensions.get(0).toString());
        assertFalse(doc.has("Catalog"), "an \"unknown\" bucket would merge unrelated deployments");
    }

    @Test
    void emptyCatalogIsTreatedAsAbsent() throws Exception
    {
        metrics.count(ConnectorMetrics.CONFIG_RESOLUTION_FAILURES, "");

        assertEquals(1, metricDirective(only()).get("Dimensions").size());
    }

    @Test
    void wholeNumbersAreWrittenWithoutADecimalPoint() throws Exception
    {
        metrics.emit(ConnectorMetrics.ROWS_RETURNED, 2_000_000, ConnectorMetrics.UNIT_COUNT, "c");

        assertTrue(emitted.get(0).contains("\"ConnectorRowsReturned\":2000000"),
                "a count should read as a count in the log: " + emitted.get(0));
    }

    @Test
    void fractionalValuesSurvive() throws Exception
    {
        metrics.emit("SomeLatency", 12.5, ConnectorMetrics.UNIT_MILLISECONDS, "c");

        assertEquals(12.5, only().get("SomeLatency").asDouble(), 0.0001);
    }

    @Test
    void quotesAndControlCharactersInADimensionCannotBreakTheDocument() throws Exception
    {
        // A catalog name cannot contain these today, but one malformed line breaks metric extraction for
        // the whole invocation, so the escaping is not left to the caller's good behaviour.
        new ConnectorMetrics("db\"ricks\\", emitted::add)
                .count(ConnectorMetrics.ROWS_RETURNED, "a\nb\tc");

        JsonNode doc = only();
        assertEquals("db\"ricks\\", doc.get("Connector").asText());
        assertEquals("a\nb\tc", doc.get("Catalog").asText());
    }

    @Test
    void nonFiniteValuesAreDroppedRatherThanEmittedAsInvalidJson()
    {
        metrics.emit("X", Double.NaN, ConnectorMetrics.UNIT_COUNT, "c");
        metrics.emit("X", Double.POSITIVE_INFINITY, ConnectorMetrics.UNIT_COUNT, "c");

        // NaN and Infinity are not JSON numbers; emitting them would poison the line.
        assertTrue(emitted.isEmpty(), "expected nothing emitted, got " + emitted);
    }

    @Test
    void missingMetricNameEmitsNothing()
    {
        metrics.count(null, "c");
        metrics.count("", "c");

        assertTrue(emitted.isEmpty());
    }

    @Test
    void aFailingSinkDoesNotSurfaceAsAConnectorFailure()
    {
        ConnectorMetrics broken = new ConnectorMetrics("databricks", line -> {
            throw new IllegalStateException("stdout is gone");
        });

        // The caller is usually inside an exception handler; a metric must never replace the exception
        // it was counting.
        assertDoesNotThrow(() -> broken.count(ConnectorMetrics.TABLE_CEILING_EXCEEDED, "c"));
    }

    @Test
    void aMissingConnectorIdStillProducesAUsableDocument() throws Exception
    {
        new ConnectorMetrics(null, emitted::add).count(ConnectorMetrics.ROWS_RETURNED, "c");

        assertEquals("unknown", only().get("Connector").asText());
    }

    @Test
    void unitDefaultsToCount() throws Exception
    {
        metrics.emit(ConnectorMetrics.ROWS_RETURNED, 1, null, "c");

        assertEquals(ConnectorMetrics.UNIT_COUNT,
                metricDirective(only()).get("Metrics").get(0).get("Unit").asText());
    }
}
