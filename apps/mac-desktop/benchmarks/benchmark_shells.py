#!/usr/bin/env python3
"""Generate a benchmark comparison report for Tauri vs Electron shell metrics."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ShellMetrics:
    startup_ms: float
    idle_memory_mb: float
    render_fps: float


def _load_metrics(path: Path) -> ShellMetrics:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return ShellMetrics(
        startup_ms=float(payload["startup_ms"]),
        idle_memory_mb=float(payload["idle_memory_mb"]),
        render_fps=float(payload["render_fps"]),
    )


def _pct_delta(reference: float, comparison: float) -> float:
    if reference == 0:
        return 0.0
    return ((comparison - reference) / reference) * 100.0


def _build_report(tauri: ShellMetrics, electron: ShellMetrics) -> str:
    startup_delta = _pct_delta(tauri.startup_ms, electron.startup_ms)
    memory_delta = _pct_delta(tauri.idle_memory_mb, electron.idle_memory_mb)
    fps_delta = _pct_delta(tauri.render_fps, electron.render_fps)

    return "\n".join(
        [
            "# Shell Benchmark Report",
            "",
            "| Metric | Tauri | Electron | Delta (Electron vs Tauri) |",
            "| --- | ---: | ---: | ---: |",
            f"| Startup (ms) | {tauri.startup_ms:.1f} | {electron.startup_ms:.1f} | {startup_delta:+.1f}% |",
            f"| Idle memory (MB) | {tauri.idle_memory_mb:.1f} | {electron.idle_memory_mb:.1f} | {memory_delta:+.1f}% |",
            f"| Render FPS | {tauri.render_fps:.1f} | {electron.render_fps:.1f} | {fps_delta:+.1f}% |",
            "",
            "## Interpretation",
            "",
            "- Positive startup/memory delta means Electron is heavier than Tauri.",
            "- Positive FPS delta means Electron renders faster in the sampled scenario.",
            "- Keep Tauri as default unless Electron shows a requirement-level advantage.",
            "",
        ]
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tauri", type=Path, required=True, help="Path to Tauri metrics JSON")
    parser.add_argument(
        "--electron", type=Path, required=True, help="Path to Electron metrics JSON"
    )
    parser.add_argument("--out", type=Path, required=True, help="Path to markdown output report")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    tauri = _load_metrics(args.tauri)
    electron = _load_metrics(args.electron)
    report = _build_report(tauri, electron)
    args.out.write_text(report, encoding="utf-8")
    print(f"Wrote benchmark report to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
