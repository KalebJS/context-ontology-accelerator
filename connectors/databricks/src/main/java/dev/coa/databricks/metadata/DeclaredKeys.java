// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package dev.coa.databricks.metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * One table's declared primary key and foreign keys, as Unity Catalog holds them. Immutable.
 *
 * <p>A composite foreign key is N single-column references rather than one grouped thing, because COA's
 * model has no constraint id and the comment-tag grammar has no grouped spelling: a two-column foreign
 * key is two {@code @fk(...)} tags on two columns, each naming its own parent column. So foreign keys
 * are keyed by child column with a list per column, since a column can reference more than one parent
 * and can be both a primary-key member and a foreign key. What is lost is the constraint's identity,
 * and COA loses that too.
 *
 * <p>Databricks constraints are informational. Unity Catalog validates neither uniqueness nor
 * referential integrity, so a declared primary key may contain duplicates and a declared foreign key may
 * not resolve. Nothing here tries to verify either.
 */
public final class DeclaredKeys
{
    private static final DeclaredKeys NONE =
            new DeclaredKeys(Collections.emptySet(), Collections.emptyMap());

    private final Set<String> primaryKeyColumns;
    private final Map<String, List<ParentReference>> foreignKeysByChildColumn;

    private DeclaredKeys(Set<String> primaryKeyColumns,
                         Map<String, List<ParentReference>> foreignKeysByChildColumn)
    {
        this.primaryKeyColumns = primaryKeyColumns;
        this.foreignKeysByChildColumn = foreignKeysByChildColumn;
    }

    /** The shared empty instance, for a table that declares nothing. */
    public static DeclaredKeys none()
    {
        return NONE;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    /** Whether {@code columnName} is a member of the declared primary key. */
    public boolean isPrimaryKeyMember(String columnName)
    {
        return primaryKeyColumns.contains(columnName);
    }

    /** The primary key's columns, in key order. Empty when none is declared. */
    public Set<String> primaryKeyColumns()
    {
        return primaryKeyColumns;
    }

    /** The parents {@code columnName} references, in constraint order. Empty when it references none. */
    public List<ParentReference> foreignKeysFor(String columnName)
    {
        List<ParentReference> parents = foreignKeysByChildColumn.get(columnName);
        return (parents == null) ? Collections.emptyList() : parents;
    }

    /** How many child-column references were declared in total, across every constraint. */
    public int foreignKeyColumnCount()
    {
        int total = 0;
        for (List<ParentReference> parents : foreignKeysByChildColumn.values()) {
            total += parents.size();
        }
        return total;
    }

    @Override
    public String toString()
    {
        return "DeclaredKeys{pk=" + primaryKeyColumns + ", fk=" + foreignKeysByChildColumn + "}";
    }

    /** One {@code parent_table.parent_column} a child column references. Immutable. */
    public static final class ParentReference
    {
        private final String table;
        private final String column;

        /**
         * @param table  the parent table, unquoted, as {@code information_schema} spells it.
         * @param column the parent column, unquoted.
         */
        public ParentReference(String table, String column)
        {
            if (table == null || table.trim().isEmpty()) {
                throw new IllegalArgumentException("Foreign key parent table must not be blank");
            }
            if (column == null || column.trim().isEmpty()) {
                throw new IllegalArgumentException("Foreign key parent column must not be blank");
            }
            this.table = table;
            this.column = column;
        }

        public String table()
        {
            return table;
        }

        public String column()
        {
            return column;
        }

        @Override
        public String toString()
        {
            return table + "." + column;
        }

        @Override
        public boolean equals(Object other)
        {
            if (this == other) {
                return true;
            }
            if (!(other instanceof ParentReference)) {
                return false;
            }
            ParentReference that = (ParentReference) other;
            return table.equals(that.table) && column.equals(that.column);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(table, column);
        }
    }

    /** Fluent builder. Not thread-safe; build one per table. */
    public static final class Builder
    {
        private final Set<String> primaryKeyColumns = new LinkedHashSet<>();
        private final Map<String, List<ParentReference>> foreignKeys = new LinkedHashMap<>();

        private Builder()
        {
        }

        /** @param columnName a primary-key member, added in key order. */
        public Builder primaryKeyColumn(String columnName)
        {
            if (columnName == null || columnName.trim().isEmpty()) {
                throw new IllegalArgumentException("Primary key column must not be blank");
            }
            primaryKeyColumns.add(columnName);
            return this;
        }

        /** @param childColumn the column in this table carrying the reference. */
        public Builder foreignKey(String childColumn, String parentTable, String parentColumn)
        {
            if (childColumn == null || childColumn.trim().isEmpty()) {
                throw new IllegalArgumentException("Foreign key child column must not be blank");
            }
            ParentReference parent = new ParentReference(parentTable, parentColumn);
            List<ParentReference> parents =
                    foreignKeys.computeIfAbsent(childColumn, ignored -> new ArrayList<>());
            // Deduplicated by resolved target, matching COA, which stores the decoded pair and so
            // cannot represent the same target twice.
            if (!parents.contains(parent)) {
                parents.add(parent);
            }
            return this;
        }

        public DeclaredKeys build()
        {
            Map<String, List<ParentReference>> snapshot = new LinkedHashMap<>();
            for (Map.Entry<String, List<ParentReference>> entry : foreignKeys.entrySet()) {
                snapshot.put(entry.getKey(),
                        Collections.unmodifiableList(new ArrayList<>(entry.getValue())));
            }
            return new DeclaredKeys(
                    Collections.unmodifiableSet(new LinkedHashSet<>(primaryKeyColumns)),
                    Collections.unmodifiableMap(snapshot));
        }
    }
}
