// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package dev.coa.example;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.List;
import java.util.Map;

/**
 * Data half of the example connector: generates rows in-process.
 *
 * <p>A real connector would open a client to the foreign system here. This one proves the
 * full read path — Athena invoking the Lambda, rows returned as Arrow, spilling to S3 when
 * the response is large — without an external dependency. Row values come from
 * {@link ExampleCatalog}, so the schema the metadata half advertises and the rows this half
 * writes cannot drift apart.
 */
public class ExampleRecordHandler extends RecordHandler
{
    private static final String SOURCE_TYPE = "example_source";

    private final Map<String, ExampleTable> tables;

    public ExampleRecordHandler(Map<String, String> configOptions)
    {
        super(SOURCE_TYPE, configOptions);
        this.tables = ExampleCatalog.tables(configOptions);
    }

    @Override
    protected void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest request,
                                      QueryStatusChecker queryStatusChecker)
    {
        ExampleTable table = tables.get(request.getTableName().getTableName());
        if (table == null) {
            throw new IllegalArgumentException("Unknown table: " + request.getTableName().getTableName());
        }

        // Athena PROJECTS: the request schema carries only the columns the query needs, and
        // writing a column that is absent from it throws NullPointerException inside
        // BlockUtils.setValue ("vector is null"). Driving the write loop off the request's
        // own fields is what makes that impossible — the alternative, writing every column
        // the table has, passes for `SELECT *` and fails for every projected query.
        final List<Field> projected = request.getSchema().getFields();

        for (int rowNumber = 1; rowNumber <= table.rowCount(); rowNumber++) {
            if (!queryStatusChecker.isQueryRunning()) {
                return;
            }
            final Map<String, Object> values = table.row(rowNumber);
            // writeRows returns 1 to count the row, 0 to skip it. setValue returns false
            // when the value fails the query's constraints, which is how predicate
            // push-down is honoured: a row that misses is simply not counted.
            spiller.writeRows((block, blockRow) -> {
                boolean matched = true;
                for (Field field : projected) {
                    // `&=`, NOT `&&`: every projected field must be written, so the call cannot be
                    // short-circuited once one value has missed. `matched = matched && setValue(...)`
                    // would stop writing the remaining columns of a row that has already failed a
                    // constraint, leaving the block half-populated.
                    matched &= block.setValue(field.getName(), blockRow, values.get(field.getName()));
                }
                return matched ? 1 : 0;
            });
        }
    }
}
