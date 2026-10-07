"""Verify tool artifacts are ignored by the repository's own rules."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path


def test_tool_caches_and_coverage_artifacts_are_ignored(tmp_path: Path) -> None:
    gitignore = Path(__file__).resolve().parents[1] / ".gitignore"
    (tmp_path / ".gitignore").write_bytes(gitignore.read_bytes())
    subprocess.run(["git", "init", "--quiet", str(tmp_path)], check=True, capture_output=True)
    paths = [
        ".mypy_cache/probe",
        ".pytest_cache/probe",
        ".ruff_cache/probe",
        "coverage.xml",
        ".coverage",
    ]
    result = subprocess.run(
        [
            "git",
            "-C",
            str(tmp_path),
            "-c",
            f"core.excludesFile={os.devnull}",
            "check-ignore",
            "--no-index",
            "--",
            *paths,
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    # Git exits zero when ANY path is ignored; require every requested path.
    assert result.stdout.splitlines() == paths
