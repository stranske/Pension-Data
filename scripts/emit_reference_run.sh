#!/usr/bin/env bash
set -euo pipefail

out_dir="${1:-artifacts/reference}"
python scripts/emit_backplane_reference_run.py --out "$out_dir"
