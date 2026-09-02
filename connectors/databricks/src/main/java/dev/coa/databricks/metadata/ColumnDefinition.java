// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package dev.coa.databricks.metadata;

import java.util.Objects;

/**
 * One row of {@code information_schema.columns}: a column's name, its declared type, and the comment a
 * data engineer wrote on it.
 *
 * <p>Raw. The comment is what Unity Catalog holds, tags and all; stripping happens in
 * {@link TableAssembler}, so a test can tell "the connector read the comment" from "the connector
 * cleaned it".
 *
 * <p>Nullability is not carried. Athena's {@code Column} type has no field for it and {@code DESCRIBE}
 * returns name, type and comment, so it cannot reach COA on this route. The comment channel is no help
 * either: COA's parser strips a tag only when it recognises one, so an unrecognised {@code @notnull}
 * would end up in the stored description as literal text.
 */
public final class ColumnDefinition
{
    private final String name;
    private final String fullDataType;
    private final String comment;

    /**
     * @param name         the column name, exactly as {@code information_schema.columns} spells it.
     *                     <b>Case-preserving:</b> Unity Catalog lower-cases a table name but keeps a
     *                     column's case, so {@code CustomerName STRING} comes back as
     *                     {@code column_name = "CustomerName"} (measured; see
     *                     {@link InformationSchemaSql}). Do not normalise it — this string has to match
     *                     the Arrow field name Athena projects, and folding it breaks every mixed-case
     *                     column with no error anywhere.
     * @param fullDataType the value of {@code full_data_type}, e.g. {@code decimal(10,2)}.
     * @param comment      the column comment, or null when it has none. Kept verbatim.
     * @throws IllegalArgumentException if the name or the type is null or blank.
     */
    public ColumnDefinition(String name, String fullDataType, String comment)
    {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Column name must not be null or blank");
        }
        if (fullDataType == null || fullDataType.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Column " + name + " has no type in information_schema.columns.full_data_type");
        }
        this.name = name;
        this.fullDataType = fullDataType;
        this.comment = comment;
    }

    public String name()
    {
        return name;
    }

    /** The Databricks type, arguments included. */
    public String fullDataType()
    {
        return fullDataType;
    }

    /** The comment as Unity Catalog holds it, or null. */
    public String comment()
    {
        return comment;
    }

    @Override
    public String toString()
    {
        return "ColumnDefinition{" + name + " " + fullDataType + "}";
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ColumnDefinition)) {
            return false;
        }
        ColumnDefinition that = (ColumnDefinition) other;
        return name.equals(that.name)
                && fullDataType.equals(that.fullDataType)
                && Objects.equals(comment, that.comment);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, fullDataType, comment);
    }
}
