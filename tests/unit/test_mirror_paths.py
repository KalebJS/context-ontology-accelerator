# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Assert that paths referenced in README and Makefile actually exist.

Catches the class of bug where the mirror strips a directory but leaves its
consumers (Makefile targets, README structure listings) intact — making the
product look broken rather than intentionally trimmed.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]


def test_readme_structure_paths_exist() -> None:
    """Every top-level directory listed in README's Repository Structure must exist."""
    readme = _REPO_ROOT / "README.md"
    content = readme.read_text()

    # Extract the fenced code block under "## Repository Structure"
    match = re.search(
        r"## Repository Structure\s*```[^\n]*\n(.*?)```",
        content,
        re.DOTALL,
    )
    assert match, "README.md must contain a '## Repository Structure' fenced block"

    # Parse ONLY top-level tree lines (no leading │ indent) like:
    #   ├── models/   # comment
    # Skip nested entries (those indented with │) since they're relative to their parent.
    top_level_pattern = re.compile(r"^[├└]── ([a-zA-Z][a-zA-Z0-9_./-]+?)/\s", re.MULTILINE)
    paths = top_level_pattern.findall(match.group(1))
    assert paths, "Should find at least one top-level path in the structure block"

    # Directories that are build outputs (generated, not checked in) are
    # legitimately listed in the structure but absent until `make generate` runs.
    _GENERATED_DIRS = {"smithy-generated"}

    missing = [p for p in paths if p not in _GENERATED_DIRS and not (_REPO_ROOT / p).exists()]
    assert not missing, (
        f"README Repository Structure lists paths that do not exist: {missing}. "
        f"Either add the directories or remove them from the listing."
    )


@pytest.mark.parametrize(
    "target",
    [
        "docs",
        "test-integ",
        "load-test",
        "load-test-slow",
        "load-test-teardown",
    ],
)
def test_guarded_makefile_targets_do_not_fail_on_missing_dirs(target: str) -> None:
    """Makefile targets that reference potentially-stripped dirs must be guarded."""
    makefile = _REPO_ROOT / "Makefile"
    content = makefile.read_text()

    # Find the recipe lines for this target
    pattern = re.compile(rf"^{re.escape(target)}:.*?(?=^\S|\Z)", re.MULTILINE | re.DOTALL)
    match = pattern.search(content)
    assert match, f"Makefile target '{target}' not found"

    recipe = match.group(0)
    # The recipe must contain a guard (if/test/[) rather than unconditional execution
    assert re.search(r"\bif\b|\btest\b|\[", recipe), (
        f"Makefile target '{target}' executes unconditionally but references a "
        f"directory that may be stripped from the public mirror. Wrap it in a guard."
    )


# `tests/unit/` is mirrored publicly; every other `tests/` subdirectory is not (see
# the allowlist in .npmignore). Matches the `_REPO_ROOT / "tests" / "cdk"` idiom, plus
# the shorter `Path(__file__).parents[1] / "integ"` form that names no "tests" segment.
_UNMIRRORED_TESTS_REF = re.compile(r'"tests"\s*/\s*"(?!unit)|/\s*"(?:integ|cdk)"')


def _mirrored_unit_test_files() -> list[Path]:
    """Every `tests/unit/*.py` that ships to the public mirror.

    Not just the repo-root suite: `.npmignore` re-includes `/packages/` wholesale,
    so each package's own `tests/unit/` ships too, and each is just as capable of
    reaching into that package's stripped `tests/integ/` at import time.
    """
    files = list((_REPO_ROOT / "tests" / "unit").glob("*.py"))
    for package_dir in sorted((_REPO_ROOT / "packages").glob("*/tests/unit")):
        files.extend(package_dir.glob("*.py"))
    return files


def test_unit_tests_reaching_outside_tests_unit_skip_when_the_path_is_absent() -> None:
    """A mirrored unit test must not hard-require an unmirrored fixture.

    The public mirror ships `tests/unit/` without `tests/cdk/` (or, per-package,
    without that package's `tests/integ/`). A test that reads from an unmirrored
    path at import time turns into a pytest *collection* error there, which aborts
    the entire suite (exit 2, zero tests run) rather than failing just itself. The
    fix is always a module-level skip, so require one.
    """
    offenders = []
    for path in sorted(_mirrored_unit_test_files()):
        if path.name == Path(__file__).name:
            continue  # this file only names the idiom in a comment
        source = path.read_text()
        if _UNMIRRORED_TESTS_REF.search(source) and "allow_module_level=True" not in source:
            offenders.append(str(path.relative_to(_REPO_ROOT)))

    assert not offenders, (
        f"{offenders} reference a tests/ subdirectory that is stripped from the public "
        "mirror without a module-level skip. Add: "
        "if not _SCRIPT.exists(): pytest.skip(..., allow_module_level=True)"
    )
