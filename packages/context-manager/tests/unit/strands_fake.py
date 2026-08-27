# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""A fake ``strands`` module so the NL→SQL agent loop can be tested without an LLM.

:class:`~coa_serve.agents.SqlAgent` imports ``strands`` / ``strands.models``
lazily inside ``run()``. Installing a fake beforehand lets a test-supplied
"driver" callback exercise the tool closures exactly as the real agent loop would
and then finish, so the tests cover OUR orchestration (tool contracts, handle
registry, feedback threading, outcome mapping) rather than model behaviour.
"""

from __future__ import annotations

import sys
import types
from collections.abc import Awaitable, Callable

# The most recently constructed fake Agent, so a test can assert on how the agent
# was configured (system prompt, model kwargs, invocation kwargs).
last_agent: FakeAgent | None = None


class FakeAgent:
    """Stands in for ``strands.Agent``; hands its tools to the test driver."""

    def __init__(self, driver, model=None, tools=None, system_prompt=None):
        self.driver = driver
        self.model = model
        self.tools = {getattr(t, "__name__", f"t{i}"): t for i, t in enumerate(tools or [])}
        self.system_prompt = system_prompt
        self.callback_handler = None
        self.invoke_kwargs: dict = {}
        self.prompt = ""

    async def invoke_async(self, prompt, **kwargs):
        """Record the invocation, then let the driver play the agent's turns."""
        # The real Strands Agent absorbs unknown kwargs via **kwargs, so a renamed
        # loop-bound key would silently fail to bound the loop — tests assert on this.
        self.invoke_kwargs = kwargs
        self.prompt = prompt
        return await self.driver(self.tools)


class FakeBedrockModel:
    """Stands in for ``strands.models.BedrockModel``; records its kwargs."""

    def __init__(self, **kwargs):
        self.kwargs = kwargs


def install_fake_strands(driver: Callable[[dict], Awaitable[None]]) -> None:
    """Install the fake ``strands`` modules, routing the agent loop to ``driver``.

    Args:
        driver: Async callback receiving ``{tool_name: callable}``; it plays the
            turns the agent would take.
    """
    strands = types.ModuleType("strands")
    strands_models = types.ModuleType("strands.models")

    def tool(fn):  # passthrough decorator — keep the async callable
        return fn

    def agent_factory(model=None, tools=None, system_prompt=None):
        global last_agent
        last_agent = FakeAgent(driver, model=model, tools=tools, system_prompt=system_prompt)
        return last_agent

    strands.Agent = agent_factory
    strands.tool = tool
    strands_models.BedrockModel = FakeBedrockModel
    sys.modules["strands"] = strands
    sys.modules["strands.models"] = strands_models


def uninstall_fake_strands() -> None:
    """Remove the fake modules so a later import gets the real (or no) package."""
    sys.modules.pop("strands", None)
    sys.modules.pop("strands.models", None)
