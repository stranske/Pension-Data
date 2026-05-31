"""Tests for the in-perimeter real-data web workspace path."""

from __future__ import annotations

import importlib.util
import json
import threading
from pathlib import Path
from urllib.request import urlopen

import pytest

ROOT = Path(__file__).resolve().parents[2]
SERVE_LOCAL_PATH = ROOT / "scripts" / "web" / "serve_local.py"

spec = importlib.util.spec_from_file_location("serve_local", SERVE_LOCAL_PATH)
assert spec is not None and spec.loader is not None
serve_local = importlib.util.module_from_spec(spec)
spec.loader.exec_module(serve_local)


def _generated_bundle(tmp_path: Path) -> Path:
    bundle = {
        "contractVersion": "1.0.0",
        "data_origin": "generated",
        "datasets": [
            {
                "domain": "pension",
                "freshness": "generated",
                "id": "one-pdf-pilot-review",
                "kind": "core_metrics",
                "lastUpdated": "2026-05-30",
                "name": "Generated review bundle",
                "rows": [
                    {
                        "confidence": 0.95,
                        "entity": "CA-PERS",
                        "metric": "funded_ratio",
                        "metric_family": "funded_status",
                        "plan_period": "FY2024",
                        "provenance": {
                            "evidence_refs": ["page=52"],
                            "source_document": "calpers-fy2024",
                        },
                        "value": 0.81,
                    }
                ],
            }
        ],
    }
    path = tmp_path / "workspace.json"
    path.write_text(json.dumps(bundle), encoding="utf-8")
    return path


def _fetch_json(url: str) -> dict[str, object]:
    with urlopen(url, timeout=5) as response:  # noqa: S310 - local test server
        payload = json.loads(response.read().decode("utf-8"))
    assert isinstance(payload, dict)
    return payload


def test_local_server_serves_generated_bundle_and_non_external_config(tmp_path: Path) -> None:
    bundle = serve_local.load_workspace_bundle(_generated_bundle(tmp_path))
    config = serve_local.build_runtime_config(artifact_base_url="/artifacts")
    handler = serve_local.make_handler(
        web_root=ROOT / "apps" / "web",
        workspace_bundle=bundle,
        runtime_config=config,
    )
    server = serve_local.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base_url = f"http://127.0.0.1:{server.server_port}"
        workspace = _fetch_json(f"{base_url}/data/workspace.json")
        served_config = _fetch_json(f"{base_url}/config/default.json")
    finally:
        server.shutdown()
        thread.join(timeout=5)

    assert workspace["data_origin"] == "generated"
    assert workspace["datasets"]
    assert served_config["apiBaseUrl"] == ""
    assert served_config["artifactBaseUrl"] == "/artifacts"
    assert served_config["enableQueryOverrides"] is False
    assert not serve_local.is_external_url(str(served_config["apiBaseUrl"]))
    assert not serve_local.is_external_url(str(served_config["artifactBaseUrl"]))
    assert serve_local.DISALLOWED_LLM_CONFIG_KEYS.isdisjoint(served_config)


def test_fixture_bundle_is_rejected_for_real_data_path(tmp_path: Path) -> None:
    path = _generated_bundle(tmp_path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["data_origin"] = "fixture"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="data_origin of generated, live"):
        serve_local.load_workspace_bundle(path)


def test_external_artifact_url_is_rejected() -> None:
    with pytest.raises(ValueError, match="artifactBaseUrl"):
        serve_local.build_runtime_config(artifact_base_url="https://example.test/artifacts")


def test_runtime_config_has_no_llm_endpoint_keys() -> None:
    config = serve_local.build_runtime_config(artifact_base_url="/artifacts")
    assert serve_local.DISALLOWED_LLM_CONFIG_KEYS.isdisjoint(config)
