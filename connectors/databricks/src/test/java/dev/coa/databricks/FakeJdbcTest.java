// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package dev.coa.databricks;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;

import static dev.coa.databricks.FakeJdbc.row;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A test for the test double, covering the one behaviour every other test in this module depends on.
 *
 * <p>A double returning {@code null} for a column label it does not have makes every label assertion
 * circular: a fixture keyed {@code table_name} and production code asking for {@code tableName} both
 * appear to work, leaving the labels that matter ({@code tableName}, {@code full_data_type},
 * {@code position_in_unique_constraint}) load-bearing only in the integration suite, which skips without
 * a warehouse. So the strictness is pinned here: regress it and this fails, rather than twenty other
 * tests quietly going vacuous.
 */
class FakeJdbcTest
{
    private static ResultSet oneRow(FakeJdbc jdbc) throws SQLException
    {
        Connection connection = jdbc.connection();
        PreparedStatement statement = connection.prepareStatement("SELECT anything");
        ResultSet rows = statement.executeQuery();
        assertTrue(rows.next());
        return rows;
    }

    @Test
    void readsAValueByItsLabel()
    throws SQLException
    {
        FakeJdbc jdbc = new FakeJdbc(sql -> Collections.singletonList(
                row("tableName", "orders", "isTemporary", "false")));
        assertEquals("orders", oneRow(jdbc).getString("tableName"));
    }

    @Test
    void throwsOnAnUnknownLabelRatherThanReturningNull()
    {
        // A null here would make a wrong label indistinguishable from a null value.
        FakeJdbc jdbc = new FakeJdbc(sql -> Collections.singletonList(row("table_name", "orders")));
        SQLException failure = assertThrows(SQLException.class,
                () -> oneRow(jdbc).getString("tableName"));
        assertTrue(failure.getMessage().contains("tableName"), failure.getMessage());
        // The message names what the row does have, so the fix is obvious from the failure alone.
        assertTrue(failure.getMessage().contains("table_name"), failure.getMessage());
    }

    @Test
    void stillDistinguishesAnAbsentColumnFromANullValue()
    throws SQLException
    {
        // A present-but-null column has to read as null and set wasNull, which is how information_schema
        // reports a column with no comment.
        FakeJdbc jdbc = new FakeJdbc(sql -> Collections.singletonList(
                row("column_name", "email", "comment", null)));
        ResultSet rows = oneRow(jdbc);
        assertEquals("email", rows.getString("column_name"));
        assertEquals(null, rows.getString("comment"));
        assertTrue(rows.wasNull());
    }

    @Test
    void readsAValueByItsPosition()
    throws SQLException
    {
        FakeJdbc jdbc = new FakeJdbc(sql -> Collections.singletonList(
                row("first", "a", "second", "b")));
        assertEquals("a", oneRow(jdbc).getString(1));
    }

    @Test
    void failsLoudlyOnAJdbcMethodItDoesNotImplement()
    {
        // So the code under test growing a new JDBC call is a named failure rather than a mysterious null.
        FakeJdbc jdbc = new FakeJdbc(sql -> Collections.singletonList(row("a", "b")));
        SQLException failure = assertThrows(SQLException.class, () -> oneRow(jdbc).getInt("a"));
        assertTrue(failure.getMessage().contains("getInt"), failure.getMessage());
    }

    @Test
    void recordsEveryStatementAndItsBoundParameters()
    throws SQLException
    {
        FakeJdbc jdbc = new FakeJdbc(sql -> Collections.emptyList());
        Connection connection = jdbc.connection();
        PreparedStatement statement = connection.prepareStatement("SELECT ? , ?");
        statement.setString(1, "one");
        statement.setString(2, "two");
        statement.executeQuery();

        assertEquals(1, jdbc.statements().size());
        assertEquals("SELECT ? , ?", jdbc.statements().get(0).sql());
        assertEquals(java.util.Arrays.asList("one", "two"), jdbc.statements().get(0).parameters());
    }

    @Test
    void countsConnectionsOpenedAndClosed()
    throws SQLException
    {
        FakeJdbc jdbc = new FakeJdbc(sql -> Collections.emptyList());
        try (Connection connection = jdbc.connection()) {
            assertEquals(1, jdbc.connectionsOpened());
            assertEquals(0, jdbc.connectionsClosed());
        }
        assertEquals(1, jdbc.connectionsClosed());
    }
}
