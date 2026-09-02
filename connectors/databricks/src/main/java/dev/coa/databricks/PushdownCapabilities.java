// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package dev.coa.databricks;

import com.amazonaws.athena.connector.lambda.metadata.optimizations.DataSourceOptimizations;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.FilterPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.LimitPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.TopNPushdownSubType;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Builds the capability map {@code doGetDataSourceCapabilities} returns.
 *
 * <p>This connector ships advertising nothing, and that was measured to cost nothing.
 *
 * <p>An earlier version of this comment claimed that "Athena pushes nothing into a connector that
 * advertises nothing: it requests whole tables and applies the predicate and the {@code LIMIT} itself",
 * and concluded that advertising is worth real money. <b>That is false</b>, and it was the premise the
 * whole capability map was justified on, so it is worth correcting plainly rather than quietly deleting.
 * Measured against a live SQL Warehouse with an empty capability map, the warehouse's own
 * {@code system.query.history} showed Athena had already sent the predicate, the row limit and the
 * top-N {@code ORDER BY ... LIMIT}. Advertising {@code filter,limit,topn} produced byte-identical
 * statements and identical row counts across six query shapes. {@code Constraints.getSummary()},
 * {@code getLimit()} and {@code getOrderByClause()} are part of the base {@code ReadRecords} payload,
 * not something the advertisement unlocks, and this connector's query builder reads all three
 * unconditionally.
 *
 * <p>So what an advertisement buys is not push-down but Athena's permission to stop re-applying the
 * predicate and the limit on its own side. That makes silence strictly the safer of two equal options:
 * Athena's re-application is a correctness net costing Athena CPU rather than warehouse reads, and an
 * advertisement is a guarantee it will hold this connector to. Advertise {@code LIMIT} push-down and
 * Athena may stop applying the limit, so a record path that ever dropped {@code Constraints.getLimit()}
 * would return too many rows with nothing erroring. Advertise a predicate form the query builder spells
 * in a way the driver rejects and the query fails at run time, after deploy, for one query shape.
 *
 * <p>One nuance the same pass measured: given a predicate <i>and</i> a limit together, Athena pushes the
 * predicate but not the limit, and applies the limit itself after filtering.
 *
 * <p>{@link Settings#PUSHDOWN_VAR} therefore exists to re-test the question on a future Athena or SDK
 * release, not because setting it is expected to help. Re-testing means a query run against a real SQL
 * Warehouse with the effect read out of that warehouse's query history — it cannot be settled from the
 * capability map alone, which is what the original claim assumed.
 *
 * <p>Turn one on through {@link Settings#PUSHDOWN_VAR}, comma-separated, from {@link #KNOWN}. Each name
 * maps to the optimisation and sub-types every AWS-authored JDBC connector declares for the same
 * feature, so the spelling is theirs. Unrecognised names are ignored rather than fatal: this is a tuning
 * knob, and refusing to start over a typo in one would be worse than not pushing down. The record path
 * honours {@code Constraints.getLimit()} and {@code getOrderByClause()} through the inherited query
 * builder whether or not they are advertised, so enabling one of these changes what Athena sends rather
 * than what the connector does with it.
 *
 * <p>There is no {@code complex} name, and adding one would be offering a switch that breaks queries.
 * {@link dev.coa.databricks.DatabricksQueryBuilder} uses {@code JdbcSplitQueryBuilder}'s
 * single-argument constructor, which installs {@code DefaultJdbcFederationExpressionParser}, whose
 * {@code mapFunctionToDataSourceSyntax} is an unconditional {@code throw} in 2025.15.1:
 * <i>"Subclass does not yet support complex expressions."</i> Supporting it means a Databricks-specific
 * {@code FederationExpressionParser} mapping each {@code FunctionName} Athena may send to Spark SQL's
 * spelling. Future work, not a flag.
 */
public final class PushdownCapabilities
{
    /** {@code filter} — predicates: comparison, range, {@code IN}, null checks. */
    public static final String FILTER = "filter";

    /** {@code limit} — {@code LIMIT n} with an integer constant. */
    public static final String LIMIT = "limit";

    /** {@code topn} — {@code ORDER BY ... LIMIT n}. Implies the source can sort. */
    public static final String TOP_N = "topn";

    /** Every recognised name, in the order the README lists them. */
    public static final Set<String> KNOWN = Collections.unmodifiableSet(
            new LinkedHashSet<>(java.util.Arrays.asList(FILTER, LIMIT, TOP_N)));

    private PushdownCapabilities()
    {
    }

    /**
     * @param setting the raw {@link Settings#PUSHDOWN_VAR} value: comma-separated names, or empty, which
     *                is the shipped default and yields an empty map.
     */
    public static Map<String, java.util.List<OptimizationSubType>> from(String setting)
    {
        Map<String, java.util.List<OptimizationSubType>> capabilities = new LinkedHashMap<>();
        if (setting == null || setting.trim().isEmpty()) {
            return Collections.unmodifiableMap(capabilities);
        }
        for (String token : setting.split(",")) {
            String name = token.trim().toLowerCase(Locale.ROOT);
            if (name.isEmpty()) {
                continue;
            }
            add(capabilities, name);
        }
        return Collections.unmodifiableMap(capabilities);
    }

    private static void add(Map<String, java.util.List<OptimizationSubType>> capabilities,
                            String name)
    {
        switch (name) {
            case FILTER:
                put(capabilities, DataSourceOptimizations.SUPPORTS_FILTER_PUSHDOWN
                        .withSupportedSubTypes(
                                FilterPushdownSubType.SORTED_RANGE_SET,
                                FilterPushdownSubType.NULLABLE_COMPARISON));
                return;
            case LIMIT:
                put(capabilities, DataSourceOptimizations.SUPPORTS_LIMIT_PUSHDOWN
                        .withSupportedSubTypes(LimitPushdownSubType.INTEGER_CONSTANT));
                return;
            case TOP_N:
                put(capabilities, DataSourceOptimizations.SUPPORTS_TOP_N_PUSHDOWN
                        .withSupportedSubTypes(TopNPushdownSubType.SUPPORTS_ORDER_BY));
                return;
            default:
                // An unrecognised name is a typo in a tuning knob, not a reason to refuse to start.
                return;
        }
    }

    private static void put(Map<String, java.util.List<OptimizationSubType>> capabilities,
                            Map.Entry<String, java.util.List<OptimizationSubType>> entry)
    {
        capabilities.put(entry.getKey(), entry.getValue());
    }
}
