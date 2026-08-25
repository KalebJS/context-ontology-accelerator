# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Agentic NL-to-SQL strategy — the Tier-2 adapter for the NL→SQL agent.

This is the production (AgentCore-runtime) counterpart of the local agentic
harness. Where :class:`NLtoSQLStrategy` does a fixed retrieve → generate →
(correct) pipeline, this strategy delegates to :class:`SqlAgent`, which
*iteratively* discovers the right tables (inspecting candidate schemas, following
FKs) before writing SQL, then executes and self-corrects. On the multi-source
Spider 2.0 regime that iterative schema inspection is the main lever over
single-shot retrieval (it is the doc's "agentic" column, ~50% vs single-shot ~26%).

The strategy itself holds no prompt, no tools and no agent loop. It is the seam
between the ``StructuredQueryStrategy`` protocol and the reusable pieces:

  * :class:`~coa_serve.tier2.tools.TableCatalog` — ``search_tables`` / ``get_table_schema``
  * :class:`~coa_serve.tier2.tools.SqlAuthoringTool` — ``generate_sql``
  * :class:`~coa_serve.sql_execution.SqlExecutionService` — firewall + executor
  * :class:`~coa_serve.agents.SqlAgent` — the prompt + the bounded tool-use loop

so it constructs those from the request context, runs the agent, and maps the
outcome onto trace records and a :class:`StrategyResult`. The same tools back any
other consumer (an MCP tool, an HTTP route) without going through this strategy.

Worst-case latency and token spend are bounded by the orchestrator's request
deadline and the per-statement execution timeout, not by a turn count (see
:mod:`coa_serve.agents.sql_agent`). It is an OPT-IN strategy: it runs only when
the request pins ``options.strategy = "agentic"`` (see
``StructuredQueryTier._strategies_for``), never as an implicit fallback.
"""

from __future__ import annotations

import structlog
from coa_common import ontology_vector_index_name

from ...agents import SqlAgent
from ...clients.base import QueryExecutor, VectorClient
from ...exceptions import AccessDeniedError
from ...sql_execution import SqlExecutionService
from ...step_ids import StepId
from ..sql_firewall import SQLFirewall
from ..strategy import (
    EMPTY_RESULT_CONFIDENCE_FLOOR,
    StrategyContext,
    StrategyOption,
    StrategyResult,
    capped_evidence,
    capped_max_rows,
)
from ..tools import SqlAuthoringTool, TableCatalog
from .sql_generator import SQLGenerator

logger = structlog.get_logger(__name__)

_DEFAULT_REGION = "us-east-1"


def _reported_confidence(outcome_confidence: float) -> float:
    """Confidence for a statement the agent verified by executing it.

    The score is the SQL writer's own self-rating for that statement — the same
    signal, on the same scale, that the single-shot NL→SQL strategy reports, so
    the two paths stay comparable and this one reports a measurement rather than
    a constant.

    Floored at :data:`EMPTY_RESULT_CONFIDENCE_FLOOR` because this path has one
    thing single-shot does not: the statement RAN. A verified result must not be
    discarded as a low-confidence empty (``is_low_confidence_empty``) or dropped
    by the orchestrator's confidence gate on the strength of a self-rating the
    execution already checked.

    Args:
        outcome_confidence: The writer's self-reported confidence, 0.0 if unknown.

    Returns:
        The confidence to report on the StrategyResult.
    """
    return max(outcome_confidence, EMPTY_RESULT_CONFIDENCE_FLOOR)


class AgenticStrategy:
    """StructuredQueryStrategy: bounded tool-use agent over the serve clients."""

    name: str = StrategyOption.AGENTIC

    def __init__(
        self,
        sql_generator: SQLGenerator,
        firewall: SQLFirewall,
        query_executor: QueryExecutor | None,
        vector_client: VectorClient,
        oss_ontology_index: str = "",
        prefetch_schemas: int | None = None,
    ):
        """Wire the strategy to the shared serve clients (see module docstring)."""
        self._sql_generator = sql_generator
        self._firewall = firewall
        self._query_executor = query_executor
        self._vector = vector_client
        self._oss_ontology_index = oss_ontology_index
        self._prefetch_schemas = prefetch_schemas

    async def resolve(self, query: str, namespace: str, context: StrategyContext) -> StrategyResult | None:
        """Run the bounded tool-use agent; return a StrategyResult or None on miss."""
        if not self._query_executor:
            context.trace.record(StepId.T2_SQL_EXECUTE, "skipped", 0, detail="no query_executor configured")
            return None

        trace = context.trace
        options = context.options
        index_name = (
            ontology_vector_index_name(self._oss_ontology_index, namespace) if self._oss_ontology_index else None
        )

        # Request-scoped tool set: the catalog accumulates every hit the agent
        # sees (its cache is also what pins the data source), the authoring tool
        # writes SQL over that cache, and the execution primitive is the ONLY way
        # a statement reaches an engine.
        catalog = TableCatalog(self._vector, self._sql_generator.llm, namespace=namespace, index=index_name)
        authoring = SqlAuthoringTool(self._sql_generator, catalog, model_id=context.model_id)
        agent = SqlAgent(
            catalog=catalog,
            authoring=authoring,
            execution=SqlExecutionService(self._firewall, self._query_executor),
            region=getattr(self._sql_generator.llm, "_region", None) or _DEFAULT_REGION,
            model_id=context.model_id,
            prefetch_schemas=self._prefetch_schemas,
        )

        outcome = await agent.run(
            query,
            namespace=namespace,
            profile=context.profile,
            evidence=capped_evidence(options.get("evidence", "")),
            max_rows=capped_max_rows(options),
            data_source_id=options.get("dataSourceId", ""),
        )
        t_ms = outcome.duration_ms

        # A firewall denial anywhere in the loop is a terminal 403 (mirrors NL→SQL).
        if outcome.denied_reason:
            trace.record(
                StepId.T2_SQL_FIREWALL,
                "denied",
                t_ms,
                detail={"reason": outcome.denied_reason},
                tool_used="sql-firewall",
            )
            raise AccessDeniedError(outcome.denied_reason)

        if not outcome.succeeded:
            trace.record(
                StepId.T2_SQL_GENERATE, "error", t_ms, detail="agent_produced_no_executed_sql", tool_used="bedrock"
            )
            logger.info("agentic_no_result", namespace=namespace, duration_ms=t_ms, unavailable=outcome.unavailable)
            return None

        confidence = _reported_confidence(outcome.confidence)
        trace.record(
            StepId.T2_SQL_EXECUTE,
            "success",
            t_ms,
            detail={
                "rowCount": outcome.row_count,
                "tables": outcome.tables,
                "agentic": True,
                "confidence": confidence,
            },
            tool_used="bedrock",
        )
        return StrategyResult(
            sql=outcome.executed_sql,
            rows=outcome.rows,
            columns=outcome.columns,
            confidence=confidence,
            strategy_name=StrategyOption.AGENTIC,
            trace_steps=[],
            row_count=outcome.row_count,
            truncated=False,
            retrieved_tables=outcome.tables,
            expanded_tables=outcome.tables,
            data_source_id=outcome.data_source_id,
        )
