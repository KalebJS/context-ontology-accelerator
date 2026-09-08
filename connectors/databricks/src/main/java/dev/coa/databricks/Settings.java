// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package dev.coa.databricks;

import java.util.Map;

/**
 * The connector's two operational settings, and how they are read.
 *
 * <p>Separate from {@link dev.coa.databricks.config.ConnectionConfig}, which says where the warehouse
 * is. These say how the connector behaves against it, they have working defaults, and an unusable value
 * falls back rather than failing initialisation: a typo in an operational knob should not present as
 * "the connector is broken". Names are matched case-insensitively, by comparison rather than by trying
 * a few spellings.
 */
public final class Settings
{
    /**
     * Rows one table may return before the connector fails with an error naming it. A ceiling exists
     * because federation cannot express aggregation: a {@code GROUP BY} reads every predicate-matching
     * row out of the warehouse for Athena to aggregate. At 3008 MB and a 120 s timeout, a table well
     * past this ceiling times out, and a timeout says nothing about which table was too big.
     */
    public static final String MAX_ROWS_PER_TABLE_VAR = "DATABRICKS_MAX_ROWS_PER_TABLE";

    /** Default for {@link #MAX_ROWS_PER_TABLE_VAR}. */
    public static final long DEFAULT_MAX_ROWS_PER_TABLE = 2_000_000L;

    /**
     * Opt-in, comma-separated, for the push-down optimisations this connector advertises. Unset
     * advertises nothing, which is the safe default and the shipped one.
     *
     * <p>Do not set this because the code supports the optimisation. An advertisement is a guarantee:
     * Athena stops applying an optimisation it believes the connector honours, so a predicate form the
     * driver rejects becomes a query-time failure and a {@code LIMIT} the record path ignores becomes a
     * wrong row count with no error. Set it only for values whose effect you have seen in your own
     * warehouse's query history.
     */
    public static final String PUSHDOWN_VAR = "DATABRICKS_ADVERTISE_PUSHDOWN";

    private Settings()
    {
    }

    /** The row ceiling: the configured value when it is a positive long, the default otherwise. */
    public static long maxRowsPerTable(Map<String, String> environment)
    {
        String raw = lookUp(environment, MAX_ROWS_PER_TABLE_VAR);
        if (raw == null) {
            return DEFAULT_MAX_ROWS_PER_TABLE;
        }
        try {
            long value = Long.parseLong(raw.trim());
            return (value > 0) ? value : DEFAULT_MAX_ROWS_PER_TABLE;
        }
        catch (NumberFormatException ignored) {
            return DEFAULT_MAX_ROWS_PER_TABLE;
        }
    }

    /** The raw {@link #PUSHDOWN_VAR} value, or {@code ""} when unset. */
    public static String advertisedPushdown(Map<String, String> environment)
    {
        String raw = lookUp(environment, PUSHDOWN_VAR);
        return (raw == null) ? "" : raw.trim();
    }

    /**
     * The value of {@code name}, matched case-insensitively, or null when unset or blank. A scan rather
     * than a few exact lookups, because an exact/lower/upper triple misses a mixed-case name.
     */
    private static String lookUp(Map<String, String> environment, String name)
    {
        if (environment == null) {
            return null;
        }
        String value = environment.get(name);
        if (value == null) {
            for (Map.Entry<String, String> entry : environment.entrySet()) {
                if (entry.getKey() != null && entry.getKey().equalsIgnoreCase(name)) {
                    value = entry.getValue();
                    break;
                }
            }
        }
        return (value == null || value.trim().isEmpty()) ? null : value;
    }
}
