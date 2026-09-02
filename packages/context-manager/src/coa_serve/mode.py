# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Execution mode — the request-level policy axis, orthogonal to tiers.

Why this exists
---------------
"Tier" conflates two unrelated things. It is a **routing cascade** (try metrics,
then NL→SQL, then RAG) *and* it is used as the name of the *engine* that answers
(``tier3Mode``). That made the deep-reasoning engine look like a Tier-3
implementation detail, when it is really a different way of executing *any* request.

This module separates the axes:

* **MODE** — *how* to execute: single-shot (``standard``) or an iterative
  plan/act/assess loop (``deep-reasoning``). Caller-controllable.
* **COMPOSITION** — *what* the namespace holds (structured / unstructured /
  mixed). Derived from the sources registry, never caller-settable.
* **TIER** — retained purely as a label for the *shape* of the answer that came
  back (1 = metric, 2 = rows, 3 = prose), which is all it ever actually meant on
  the response.

Mode and composition together select a retrieval plan; the tier cascade still
decides *which* answer shape wins. A namespace with no documents cannot run a
document retriever in either mode, and a namespace with no database cannot run
NL→SQL in either mode — that is composition's job, not mode's.

Scope note: this module is deliberately inert — it defines and resolves the
contract only. Nothing here changes which engine runs; ``resolve_mode`` reproduces
the existing ``_is_deep_reasoning_engaged`` precedence exactly (request over deployment
default), so wiring it in is a provable no-op. Behavior changes land in the
follow-ups that consume :class:`ResolutionPlan`.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

import structlog

from .clients.sources_registry import SourceComposition

logger = structlog.get_logger(__name__)


class Mode(StrEnum):
    """How a request is executed. Orthogonal to which tier answers it."""

    STANDARD = "standard"
    """Single-shot: retrieve once, synthesize once. Lower latency."""

    DEEP_REASONING = "deep-reasoning"
    """Iterative: decompose, then loop plan → select tool → invoke → assess."""

    AUTO = "auto"
    """Reserved: pick per-query. Accepted by the parser but not yet dispatched;
    :func:`resolve_mode` never returns it (see ``_resolve_auto``)."""


LEGACY_MODE_ALIASES = {"agentic": Mode.DEEP_REASONING}
"""Pre-rename wire spellings still accepted on ``options.mode``.

``deep-reasoning`` was called ``agentic`` before the rebrand. ``options.mode`` is a
public request field (REST, MCP, benchmark drivers, notebooks), so dropping the old
spelling would 400 every caller pinned to it. Remove once those callers are updated.
"""


class Composition(StrEnum):
    """What kinds of sources a namespace holds, derived from the registry."""

    STRUCTURED = "structured"
    UNSTRUCTURED = "unstructured"
    MIXED = "mixed"

    @classmethod
    def from_source_composition(cls, composition: SourceComposition) -> Composition:
        """Collapse the registry's two booleans into a single label.

        ``unknown`` (registry unavailable / timeout) maps to :attr:`MIXED` — the
        fail-open choice, matching ``SourceComposition.unknown_composition``,
        which sets both booleans True so no capability is withheld on a lookup
        miss. A namespace with neither source type also maps to ``MIXED`` rather
        than inventing an "empty" case: there is nothing to withhold, and every
        downstream retriever already handles the no-data path.
        """
        if composition.unknown:
            return cls.MIXED
        if composition.has_structured_source and composition.has_unstructured_source:
            return cls.MIXED
        if composition.has_structured_source:
            return cls.STRUCTURED
        if composition.has_unstructured_source:
            return cls.UNSTRUCTURED
        return cls.MIXED


@dataclass(frozen=True)
class ResolutionPlan:
    """The resolved execution policy for one request.

    Attributes:
        mode: The effective :class:`Mode` (never :attr:`Mode.AUTO` — auto is
            resolved to a concrete mode before a plan is built).
        composition: The namespace's derived :class:`Composition`.
        mode_source: Where ``mode`` came from — ``"request"``, ``"deployment"``,
            or ``"auto"``. Recorded so a benchmark run can prove which arm it
            actually measured (a hardcoded arm label previously mislabeled a run).
    """

    mode: Mode
    composition: Composition
    mode_source: str


def resolve_mode(
    request_mode: str | None,
    *,
    deployment_default: Mode,
    deep_reasoning_available: bool,
) -> tuple[Mode, str]:
    """Resolve the effective mode, request winning over deployment default.

    Reproduces ``Orchestrator._is_deep_reasoning_engaged``'s precedence exactly:
    an explicit request value wins (either direction), absent falls through to
    the deployment default, and an unavailable deep-reasoning retriever forces
    ``standard`` regardless — the reasoning loop cannot run if it was never built.

    An unrecognized request value is treated as absent (warn + fall through)
    rather than raising, matching ``resolve_strategy``'s defensive posture; the
    model layer validates this field, so reaching here means something upstream
    changed.

    Args:
        request_mode: Per-request ``options.mode``, or ``None`` when absent.
        deployment_default: The mode this deployment runs when the request is silent.
        deep_reasoning_available: Whether a deep-reasoning retriever was actually
            constructed.

    Returns:
        ``(mode, mode_source)`` where ``mode_source`` is ``"request"``,
        ``"deployment"``, or ``"auto"``.
    """
    mode, source = _requested_or_default(request_mode, deployment_default)

    if mode is Mode.AUTO:
        mode, source = _resolve_auto()

    if mode is Mode.DEEP_REASONING and not deep_reasoning_available:
        logger.warning("deep_reasoning_mode_unavailable", requested_source=source, fallback="standard")
        return Mode.STANDARD, source

    return mode, source


def _requested_or_default(request_mode: str | None, deployment_default: Mode) -> tuple[Mode, str]:
    if request_mode is None:
        return deployment_default, "deployment"
    if request_mode in LEGACY_MODE_ALIASES:
        return LEGACY_MODE_ALIASES[request_mode], "request"
    try:
        return Mode(request_mode), "request"
    except ValueError:
        logger.warning(
            "unrecognized_request_mode",
            request_mode=request_mode,
            action="treating as absent; falling back to deployment default",
        )
        return deployment_default, "deployment"


def _resolve_auto() -> tuple[Mode, str]:
    """Collapse :attr:`Mode.AUTO` to a concrete mode.

    Until per-query classification lands, auto resolves to ``deep-reasoning``: it is
    the higher-recall path, so auto never answers worse than an explicit request would
    — it only costs latency. Kept as its own function so the classifier has exactly
    one seam to replace.
    """
    return Mode.DEEP_REASONING, "auto"


def build_plan(
    request_mode: str | None,
    composition: SourceComposition,
    *,
    deployment_default: Mode,
    deep_reasoning_available: bool,
) -> ResolutionPlan:
    """Resolve mode + composition into a :class:`ResolutionPlan`."""
    mode, mode_source = resolve_mode(
        request_mode,
        deployment_default=deployment_default,
        deep_reasoning_available=deep_reasoning_available,
    )
    return ResolutionPlan(
        mode=mode,
        composition=Composition.from_source_composition(composition),
        mode_source=mode_source,
    )
