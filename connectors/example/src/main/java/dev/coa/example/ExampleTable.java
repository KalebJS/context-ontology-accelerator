// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package dev.coa.example;

import java.util.Map;

/**
 * The rows one fabricated table serves: how many, and how to make the nth one.
 *
 * <p>Rows only. A table's <i>shape</i> — columns, prose, declared keys — lives in
 * {@link ExampleMetadataHandler}, which is where a connector author should be reading.
 *
 * <p>The split means shape and rows could disagree: a column declared but never written comes back
 * as nulls rather than an error. {@code ExampleCatalogTest} asserts every declared column of every
 * table gets a value, which is what keeps them honest.
 */
final class ExampleTable
{
    /** Makes the values for one row. */
    @FunctionalInterface
    interface RowFactory
    {
        /**
         * @param rowNumber 1-based row ordinal.
         * @return column name to value, with a value for every column of the table. Values
         *         must be of the Java type the column's Arrow type expects — {@code Long}
         *         for BIGINT, {@code Integer} for INT and for DATEDAY (epoch day),
         *         {@code Long} for DATEMILLI (epoch millis, UTC), {@code Double} for FLOAT8,
         *         {@code String} for VARCHAR.
         */
        Map<String, Object> row(int rowNumber);
    }

    private final String name;
    private final int rowCount;
    private final RowFactory rowFactory;

    private ExampleTable(Builder builder)
    {
        this.name = builder.name;
        this.rowCount = builder.rowCount;
        this.rowFactory = builder.rowFactory;
    }

    /**
     * @param name the table name as Athena will see it.
     * @return a builder.
     */
    static Builder named(String name)
    {
        return new Builder(name);
    }

    /** @return the table's name, as {@link ExampleMetadataHandler} declares it. */
    String name()
    {
        return name;
    }

    /** @return how many rows this table serves. */
    int rowCount()
    {
        return rowCount;
    }

    /**
     * @param rowNumber 1-based row ordinal, up to {@link #rowCount()}.
     * @return the row's values by column name.
     */
    Map<String, Object> row(int rowNumber)
    {
        return rowFactory.row(rowNumber);
    }

    /** Fluent builder for {@link ExampleTable}. */
    static final class Builder
    {
        private final String name;
        private int rowCount;
        private RowFactory rowFactory;

        private Builder(String name)
        {
            this.name = name;
        }

        /**
         * @param count how many rows the table serves.
         * @return this builder.
         */
        Builder rows(int count)
        {
            this.rowCount = count;
            return this;
        }

        /**
         * @param factory makes one row; see {@link RowFactory}.
         * @return this builder.
         */
        Builder rowsFrom(RowFactory factory)
        {
            this.rowFactory = factory;
            return this;
        }

        ExampleTable build()
        {
            if (rowFactory == null) {
                throw new IllegalStateException("Table " + name + " has no row factory");
            }
            return new ExampleTable(this);
        }
    }
}
