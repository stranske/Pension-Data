"""#640: the web config's advertised API port must match the real server default.

Deliberate-break: point apps/web/config/default.json apiBaseUrl at a different port
and this test fails; restore it and it passes.
"""

from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import urlsplit

from pension_data.api.app import DEFAULT_PORT

CONFIG_PATH = Path(__file__).resolve().parents[2] / "apps" / "web" / "config" / "default.json"


def test_config_api_ports_match_server_default() -> None:
    config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    for key in ("apiBaseUrl", "artifactBaseUrl"):
        port = urlsplit(str(config[key])).port
        assert port == DEFAULT_PORT, (
            f"{key} port {port} != api/app.py DEFAULT_PORT {DEFAULT_PORT}; the displayed "
            "endpoint would mislead operators about where the server actually listens"
        )
