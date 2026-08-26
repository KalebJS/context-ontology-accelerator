# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Agents — LLM tool-use loops that CONSUME the tiers' tools.

An agent here owns only its prompt, its loop bound and the translation between the
model and the capabilities it may use. The capabilities themselves live where they
belong: query-authoring tools in :mod:`coa_serve.tier2.tools`, execution in the
shared :mod:`coa_serve.sql_execution` primitive. That keeps an agent one consumer
among several (an MCP tool or an HTTP route can call the same tools) instead of
the only way to reach them.
"""

from __future__ import annotations

from .sql_agent import SqlAgent, SqlAgentOutcome, build_system_prompt

__all__ = ["SqlAgent", "SqlAgentOutcome", "build_system_prompt"]
