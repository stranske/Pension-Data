# In-Perimeter Real-Data Browser Review

Use this path when a reviewer needs to open a generated or live Pension-Data workspace bundle without sending proprietary data to public SaaS.

## Decision

Chosen delivery option: **internal host** (including localhost loopback) serving the existing static SPA at `apps/web/`.

The SPA already uses client-side bundle loading (`loadJson(WORKSPACE_DATA_PATH)` in `apps/web/app.js`) and does not require an external API transport for this review path. Public Cloudflare Pages and GitHub Pages remain fixture/synthetic demo surfaces only.

## Zero-Egress Guarantee

- The server binds to `127.0.0.1` by default and uses only Python stdlib HTTP serving.
- `/data/workspace.json` is served from the local generated/live bundle provided with `--bundle`.
- `/config/default.json` and `/config/runtime.json` expose `apiBaseUrl: ""`, so the SPA does not call an external API host for this bundle-only review path.
- The local server emits a restrictive `Content-Security-Policy` with `script-src 'self'` and `connect-src 'self'`, blocking the public Plotly CDN tag in the static SPA during real-data review.
- `artifactBaseUrl` must be relative, localhost, or loopback. External hosts are rejected before serving.
- LLM-backed routes are absent from this static path. For `pension-data-serve`, `PENSION_DATA_DATA_ZONE=proprietary` keeps LLM routes disabled unless `OPENAI_BASE_URL` or `ANTHROPIC_BASE_URL` points to an approved no-train endpoint.

## Build The Bundle

Create a generated workspace bundle from a one-PDF pilot run:

```bash
python scripts/web/build_workspace_bundle.py \
  --pilot-run-dir outputs/one_pdf_pilot/<run-id> \
  --out outputs/web/workspace.generated.json
```

The generated bundle must contain `data_origin: "generated"` and at least one dataset. Fixture bundles are rejected by the real-data serving command.

## Open The Review Path

```bash
python scripts/web/serve_local.py \
  --bundle outputs/web/workspace.generated.json \
  --host 127.0.0.1 \
  --port 8766
```

Then open `http://127.0.0.1:8766/`. The browser loads the SPA assets from `apps/web/`, loads the generated bundle from `/data/workspace.json`, and uses only relative or loopback artifact links.

## Reviewer Checks

- Confirm the data-origin badge reads generated or live, not fixture/demo.
- Confirm `GET /data/workspace.json` returns the expected generated/live bundle.
- Confirm `GET /config/default.json` has `apiBaseUrl` empty and `artifactBaseUrl` relative or loopback.
- Confirm the browser network panel shows no requests to public SaaS domains while loading the workspace.
