#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WEB_DIR="$ROOT_DIR/../web"
TARGET_DIR="$ROOT_DIR/src-ui"

rm -rf "$TARGET_DIR"
mkdir -p "$TARGET_DIR"
cp -R "$WEB_DIR"/. "$TARGET_DIR"/

echo "Synced web workspace assets to $TARGET_DIR"
