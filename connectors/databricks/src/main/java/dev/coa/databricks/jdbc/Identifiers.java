// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package dev.coa.databricks.jdbc;

/**
 * Quotes an identifier for Databricks SQL. Values are never quoted here; every value this connector puts
 * in a statement is a bound parameter.
 *
 * <p>Backticks, not double quotes. Databricks SQL delimits an identifier with backticks, doubling an
 * embedded one, and double quotes delimit an identifier only while the session's {@code ANSI_MODE} is on.
 * The open-source driver sets it, but the workspace, the warehouse, a session configuration or a future
 * driver release can each turn it off. With it off, {@code SELECT "customer_id" FROM t} is valid SQL
 * returning the string literal {@code customer_id} once per row: the query succeeds, nothing is logged,
 * and every value in the column is wrong. A backtick has one meaning in every Databricks and Spark
 * version. Either way the quote character is doubled, so no identifier can terminate its own quoting.
 */
public final class Identifiers
{
    /** Databricks SQL's identifier delimiter. */
    public static final char QUOTE = '`';

    private Identifiers()
    {
    }

    /**
     * {@code identifier} wrapped in backticks, with every embedded backtick doubled.
     *
     * @throws IllegalArgumentException if null or empty, since {@code ``} would silently generate invalid
     *                                 SQL.
     */
    public static String quote(String identifier)
    {
        if (identifier == null || identifier.isEmpty()) {
            throw new IllegalArgumentException("Identifier must not be null or empty");
        }
        StringBuilder out = new StringBuilder(identifier.length() + 4);
        out.append(QUOTE);
        for (int i = 0; i < identifier.length(); i++) {
            char character = identifier.charAt(i);
            if (character == QUOTE) {
                out.append(QUOTE);
            }
            out.append(character);
        }
        out.append(QUOTE);
        return out.toString();
    }

    /**
     * @param segments the parts of a dotted name, outermost first, each unquoted. Quoted individually and
     *                 joined with {@code .}.
     * @throws IllegalArgumentException if there are none, or any is null or empty.
     */
    public static String qualify(String... segments)
    {
        if (segments == null || segments.length == 0) {
            throw new IllegalArgumentException("A qualified name needs at least one segment");
        }
        StringBuilder out = new StringBuilder();
        for (String segment : segments) {
            if (out.length() > 0) {
                out.append('.');
            }
            out.append(quote(segment));
        }
        return out.toString();
    }
}
