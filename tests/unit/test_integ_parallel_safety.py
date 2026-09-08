# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Guard the two-phase split that makes the context-manager integ suite `-n`-safe.

`pytest -n 8 --dist loadfile` keeps one file's tests on one worker but still runs
DIFFERENT files concurrently. A test that mutates state ambient to the whole suite
— the integ user's grants — therefore sits inside other files' query windows:
`test_email_keyed_grant_enforcement_integ.py` holds a deny-all grant on the shared
knowledge namespace, and in job 9539493 it passed on gw5 while
`test_tier2_strategies.py` failed on gw4 with `AccessDeniedError` two seconds later.

The fix is a marker plus a serial second phase in CI. Both halves are load-bearing
and neither is self-evident from reading either file alone, so they are pinned here.
The failure this most guards against is not a red pipeline but a GREEN one: get the
two `-m` expressions wrong and the excluded tests simply never run.
"""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

pytestmark = pytest.mark.unit

_REPO_ROOT = Path(__file__).resolve().parents[2]
_CI_FILE = _REPO_ROOT / "ci" / "mainline.yml"
_INTEG_DIR = _REPO_ROOT / "packages" / "context-manager" / "tests" / "integ"
_MARKER = "mutates_ambient_authz"

# `ci/` is not published to the public mirror (see the allowlist in .npmignore).
if not _CI_FILE.exists():
    pytest.skip("ci/ is absent from this checkout (public mirror)", allow_module_level=True)


def _job_script(job: str) -> str:
    """The `script:` body of a GitLab job, up to the next top-level key."""
    match = re.search(rf"^{re.escape(job)}:$.*?(?=^\S)", _CI_FILE.read_text(), re.MULTILINE | re.DOTALL)
    assert match, f"job '{job}' not found in {_CI_FILE.name}"
    return match.group(0)


def _phases() -> list[str]:
    """The `-m "..."` selection expression of each pytest phase, in order."""
    return re.findall(r'-m "([^"]+)"', _job_script("integ-test-context-manager"))


def test_every_excluded_marker_has_a_phase_that_runs_it() -> None:
    """A `-m "... and not X"` phase must be matched by a phase that selects X.

    Otherwise the deselected tests are silently dropped and the job goes green
    having never run them.
    """
    phases = _phases()
    assert phases, "no -m selection found — integ-test-context-manager was restructured"

    excluded = {m for phase in phases for m in re.findall(r"not\s+(\w+)", phase)}
    assert excluded, f"expected the parallel phase to exclude a serial marker, got {phases}"

    for marker in sorted(excluded):
        assert any(re.search(rf"\band {marker}\b", p) for p in phases), (
            f"'{marker}' is excluded from a phase but no phase selects it, so those tests never run. Phases: {phases}"
        )


def test_only_the_marker_excluding_phase_runs_in_parallel() -> None:
    """The serial phase must not carry `-n`, or the split achieves nothing."""
    script = _job_script("integ-test-context-manager")
    # Split into per-invocation chunks so each `-m` is paired with its own flags.
    invocations = [c for c in re.split(r"uv run pytest", script)[1:]]
    assert len(invocations) >= 2, "expected a parallel phase and a serial phase"

    for chunk in invocations:
        selection = re.search(r'-m "([^"]+)"', chunk)
        assert selection, f"pytest invocation without a -m selection: {chunk[:120]}"
        parallel = re.search(r"-n\s+\d+", chunk) is not None
        serial_phase = "not " not in selection.group(1)
        assert not (serial_phase and parallel), (
            f"the phase selecting '{selection.group(1)}' runs with -n; it exists "
            "precisely because those tests cannot run concurrently"
        )


def test_grant_mutating_integ_tests_are_marked_serial() -> None:
    """A test that writes grants must carry the marker that keeps it out of the fan-out.

    Grants are ambient to the integ user, not scoped to the test, so an unmarked
    one reintroduces the race for every file querying the same namespace.
    """
    unmarked = []
    for path in sorted(_INTEG_DIR.glob("test_*.py")):
        source = path.read_text()
        writes_grants = "/grants" in source and ("requests.post" in source or "tableAllowlist" in source)
        if writes_grants and _MARKER not in source:
            unmarked.append(path.name)

    assert not unmarked, (
        f"{unmarked} write grants for the integ user but are not marked "
        f"`pytest.mark.{_MARKER}`, so they run inside the parallel fan-out and will "
        "deny other workers' queries at random."
    )


# ---------------------------------------------------------------------------
# Namespace-bound credential secret copies
# ---------------------------------------------------------------------------


def _fixtures_module():
    """Load tests/integ/fixtures.py by path (it is not an installed package)."""
    path = _REPO_ROOT / "tests" / "integ" / "fixtures.py"
    spec = importlib.util.spec_from_file_location("integ_fixtures_under_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _bound_secret_name(monkeypatch, worker: str | None) -> str:
    """Name `bind_secret_to_namespace` would give its copy, under *worker*."""
    fx = _fixtures_module()
    if worker is None:
        monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)
    else:
        monkeypatch.setenv("PYTEST_XDIST_WORKER", worker)

    sm = MagicMock()
    sm.get_secret_value.return_value = {"SecretString": '{"username": "u", "password": "p"}'}
    sm.create_secret.return_value = {"ARN": "arn:aws:secretsmanager:us-east-1:1:secret:copy-AbCdEf"}
    sm.exceptions = SimpleNamespace(ResourceExistsException=type("E", (Exception,), {}))
    monkeypatch.setattr(fx.boto3, "client", lambda *a, **k: sm)

    fx.bind_secret_to_namespace(
        "arn:aws:secretsmanager:us-east-1:1:secret:integ/db-XyZ123",
        "550e8400-e29b-41d4-a716-446655440000",
    )
    return sm.create_secret.call_args.kwargs["Name"]


def test_bound_secret_copies_are_scoped_per_xdist_worker(monkeypatch) -> None:
    """Two workers must not share one copy of a namespace-bound credential secret.

    Every xdist worker is its own pytest session, so each runs the session-scoped
    `ns_bound_secret` teardown independently. Several workers share the ONE fixed
    sources namespace, so a copy name keyed only on (namespace, source secret)
    hands them all the same secret — and the first worker to finish force-deletes
    it while the others are still scanning. The recreated copy gets a fresh ARN
    suffix, so an already-registered source then fails GetSecretValue as
    AccessDenied (the reader's grant is conditioned on a tag a nonexistent secret
    cannot carry), which is what broke
    test_snowflake_federated_catalog_exposes_databases in job 10838590.
    """
    gw0 = _bound_secret_name(monkeypatch, "gw0")
    gw1 = _bound_secret_name(monkeypatch, "gw1")
    assert gw0 != gw1, f"both workers would own the same secret copy: {gw0}"
    assert "gw0" in gw0 and "gw1" in gw1


def test_bound_secret_name_is_stable_for_one_worker(monkeypatch) -> None:
    """Reuse across runs is the point — the name must not carry per-run entropy.

    A fresh name every run would leave one orphaned credential copy per run in
    the account instead of refreshing the same one.
    """
    assert _bound_secret_name(monkeypatch, "gw3") == _bound_secret_name(monkeypatch, "gw3")


def test_bound_secret_name_is_defined_without_xdist(monkeypatch) -> None:
    """An interactive (non-xdist) run has no worker env var and must still work."""
    name = _bound_secret_name(monkeypatch, None)
    assert name and "None" not in name
