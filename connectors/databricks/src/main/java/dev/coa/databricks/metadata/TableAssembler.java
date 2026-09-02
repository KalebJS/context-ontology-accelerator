// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package dev.coa.databricks.metadata;

import dev.coa.connector.metadata.CoaColumn;
import dev.coa.connector.metadata.CoaTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Turns {@code information_schema} rows into a {@link CoaTable}: where a Unity Catalog constraint
 * becomes a comment tag. Pure and static, so every rule here is assertable without a warehouse.
 *
 * <p>The toolkit does the encoding. Declared keys reach COA inside column comments, since the
 * federation protocol has no field for a key, and getting that wrong fails silently in two places: a
 * mis-spelled tag is stored as literal prose, and Athena reads comments from the Arrow <b>schema's</b>
 * metadata map rather than a field's, so a comment on a field is delivered and ignored. This class
 * expresses intent through {@link CoaColumn#primaryKey()} and
 * {@link CoaColumn#foreignKey(String, String)} and lets {@code CoaTable.toTableSchema()} spell and
 * place it.
 *
 * <p>Column order is the primary key's order. The tag grammar has no ordinal: COA reads a composite
 * primary key as the set of {@code @pk} columns in {@code DESCRIBE} order. So columns are added in
 * {@code ordinal_position} order, which {@link InformationSchemaSql#columns(String)} sorts by, and the
 * caller must not reorder them.
 *
 * <p>The customer's comment is stripped and then neutralised, in that order.
 * {@link CommentTags#strip(String)} clears the channel, since a tag a data engineer wrote in Databricks
 * is indistinguishable from a generated one by the time COA sees it.
 * {@link CommentTags#neutralise(String)} then removes what the toolkit's encoder would refuse, which is
 * a broader set, and the encoder refuses by throwing.
 */
public final class TableAssembler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TableAssembler.class);

    private TableAssembler()
    {
    }

    /**
     * @param columns its columns, in {@code ordinal_position} order.
     * @param keys    its declared keys, or {@link DeclaredKeys#none()}.
     * @throws IllegalArgumentException if {@code columns} is empty, which means the reader asked about a
     *                                 table that does not exist, or asked with the wrong catalog or
     *                                 schema.
     */
    public static CoaTable assemble(String tableName, List<ColumnDefinition> columns,
                                    DeclaredKeys keys)
    {
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException(
                    "information_schema.columns returned no columns for table \"" + tableName
                            + "\". Either the table does not exist in the catalog and schema this"
                            + " connector is configured for, or the credential's principal cannot"
                            + " see it.");
        }
        DeclaredKeys declared = (keys == null) ? DeclaredKeys.none() : keys;
        CoaTable.Builder table = CoaTable.named(tableName);
        for (ColumnDefinition definition : columns) {
            table.column(toCoaColumn(tableName, definition, declared));
        }
        return table.build();
    }

    private static CoaColumn toCoaColumn(String tableName, ColumnDefinition definition,
                                         DeclaredKeys keys)
    {
        CoaColumn column = CoaColumn.of(
                definition.name(),
                DatabricksTypes.toArrowType(definition.fullDataType()));

        // Both steps. strip() mirrors COA's parser, which leaves a malformed @fk( in place as feedback
        // to whoever wrote it. The toolkit's encoder refuses ANY @fk( in prose, closed or not, and
        // refuses by throwing, so forwarding strip()'s output directly lets one bad customer comment
        // fail this table's DESCRIBE permanently and take the schema's scan with it.
        String stripped = CommentTags.strip(definition.comment());
        String prose = CommentTags.neutralise(stripped);
        if (!prose.equals(stripped)) {
            // The comment is malformed in a way its author will never be told about, and this is the
            // only place that is observable. The text itself is not logged: it is a customer's, and it
            // is already in their catalog.
            LOGGER.warn("Neutralised a malformed constraint tag in the comment on {}.{};"
                            + " forwarding the remaining prose", tableName, definition.name());
        }
        if (!prose.isEmpty()) {
            column.describedAs(prose);
        }
        if (keys.isPrimaryKeyMember(definition.name())) {
            column.primaryKey();
        }
        // One tag per participating child column, each naming its own parent, never one tag listing two:
        // COA stores a composite key as N single-column records.
        for (DeclaredKeys.ParentReference parent : keys.foreignKeysFor(definition.name())) {
            column.foreignKey(parent.table(), parent.column());
        }
        return column;
    }
}
