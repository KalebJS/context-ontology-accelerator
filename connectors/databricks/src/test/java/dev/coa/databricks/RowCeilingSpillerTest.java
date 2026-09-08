// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package dev.coa.databricks;

import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import dev.coa.databricks.jdbc.DatabricksErrors;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The table-size ceiling: it stops, and it says which table. */
class RowCeilingSpillerTest
{
    /** Counts what reaches the real spiller and implements nothing else. */
    private static final class CountingSpiller implements BlockSpiller
    {
        private final AtomicInteger rows = new AtomicInteger();
        private boolean closed;

        @Override
        public void writeRows(RowWriter rowWriter)
        {
            rows.incrementAndGet();
        }

        @Override
        public ConstraintEvaluator getConstraintEvaluator()
        {
            return null;
        }

        @Override
        public boolean spilled()
        {
            return false;
        }

        @Override
        public com.amazonaws.athena.connector.lambda.data.Block getBlock()
        {
            return null;
        }

        @Override
        public List<SpillLocation> getSpillLocations()
        {
            return Collections.emptyList();
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private static final BlockWriter.RowWriter NO_OP = (block, rowNum) -> 1;

    @Test
    void passesRowsThroughUpToTheCeiling()
    {
        CountingSpiller delegate = new CountingSpiller();
        RowCeilingSpiller bounded = new RowCeilingSpiller(delegate, "orders", 3);
        for (int i = 0; i < 3; i++) {
            bounded.writeRows(NO_OP);
        }
        assertEquals(3, delegate.rows.get());
        assertEquals(3, bounded.rowsWritten());
    }

    @Test
    void failsPastTheCeilingWithAnErrorNamingTheTable()
    {
        // The alternative is a Lambda timeout, which names no table, suggests no action, and is retried by
        // Athena, paying for the read again.
        RowCeilingSpiller bounded = new RowCeilingSpiller(new CountingSpiller(), "order_lines", 2);
        bounded.writeRows(NO_OP);
        bounded.writeRows(NO_OP);

        AthenaConnectorException failure =
                assertThrows(AthenaConnectorException.class, () -> bounded.writeRows(NO_OP));
        assertTrue(failure.getMessage().contains("order_lines"), failure.getMessage());
        assertTrue(failure.getMessage().startsWith(DatabricksErrors.TABLE_TOO_LARGE_PREFIX),
                failure.getMessage());
        assertTrue(failure.getMessage().contains(Settings.MAX_ROWS_PER_TABLE_VAR),
                "the message must say which setting to change: " + failure.getMessage());
    }

    @Test
    void theCeilingIsReportedAsPermanentRatherThanTransient()
    {
        // A ceiling breach is deterministic, so the transient code would invite the retry storm the ceiling
        // exists to prevent: Athena re-invokes a failed connector, and one oversized GROUP BY would bill N
        // full warehouse reads.
        RowCeilingSpiller bounded = new RowCeilingSpiller(new CountingSpiller(), "orders", 1);
        bounded.writeRows(NO_OP);
        AthenaConnectorException failure =
                assertThrows(AthenaConnectorException.class, () -> bounded.writeRows(NO_OP));

        assertEquals(FederationSourceErrorCode.OPERATION_NOT_SUPPORTED_EXCEPTION.toString(),
                failure.getErrorDetails().errorCode());
        assertNotEquals(FederationSourceErrorCode.OPERATION_TIMEOUT_EXCEPTION.toString(),
                failure.getErrorDetails().errorCode(),
                "the timeout code is reserved for the warehouse-starting case, where retrying is"
                        + " the right advice");
    }

    @Test
    void theRowPastTheCeilingNeverReachesTheDelegate()
    {
        CountingSpiller delegate = new CountingSpiller();
        RowCeilingSpiller bounded = new RowCeilingSpiller(delegate, "orders", 1);
        bounded.writeRows(NO_OP);
        assertThrows(AthenaConnectorException.class, () -> bounded.writeRows(NO_OP));
        assertEquals(1, delegate.rows.get());
    }

    @Test
    void explainsWhyABigTableIsAProblemHere()
    {
        // Athena's federation protocol cannot express aggregation, so a GROUP BY reads every matching row
        // out of the warehouse. An operator who does not know that reads the ceiling as an arbitrary limit.
        RowCeilingSpiller bounded = new RowCeilingSpiller(new CountingSpiller(), "orders", 1);
        bounded.writeRows(NO_OP);
        AthenaConnectorException failure =
                assertThrows(AthenaConnectorException.class, () -> bounded.writeRows(NO_OP));
        assertTrue(failure.getMessage().contains("aggregation"), failure.getMessage());
    }

    @Test
    void delegatesEverythingElse()
    {
        CountingSpiller delegate = new CountingSpiller();
        RowCeilingSpiller bounded = new RowCeilingSpiller(delegate, "orders", 10);
        assertEquals(false, bounded.spilled());
        assertEquals(Collections.emptyList(), bounded.getSpillLocations());
        assertEquals(null, bounded.getBlock());
        assertEquals(null, bounded.getConstraintEvaluator());
        bounded.close();
        assertTrue(delegate.closed);
    }

    @Test
    void refusesANonPositiveCeilingOrANullDelegate()
    {
        assertThrows(IllegalArgumentException.class,
                () -> new RowCeilingSpiller(new CountingSpiller(), "orders", 0));
        assertThrows(IllegalArgumentException.class,
                () -> new RowCeilingSpiller(new CountingSpiller(), "orders", -1));
        assertThrows(NullPointerException.class, () -> new RowCeilingSpiller(null, "orders", 1));
        assertThrows(NullPointerException.class,
                () -> new RowCeilingSpiller(new CountingSpiller(), null, 1));
    }

    @Test
    void theDefaultCeilingIsTwoMillionRows()
    {
        assertEquals(2_000_000L, Settings.DEFAULT_MAX_ROWS_PER_TABLE);
    }
}
