# PC Zero-Install Mode: IT/Security Review Guide

This guide describes the browser-only mode for Pension-Data in locked-down PC environments.

## Overview

- Delivery model: static web app assets served from Cloudflare Pages for synthetic demo data only.
- Internal/on-prem serving model: `pension-data-serve` binds to `127.0.0.1` by default and serves the same browser assets plus deterministic API routes for real in-perimeter data.
- Optional install: Progressive Web App (PWA) install where enterprise policy allows it.
- No required local binary installs for browser mode.
- Real extracted pension data is served only from the in-perimeter host, not from Cloudflare Pages.

## Data Flow

1. Browser downloads static assets (`index.html`, `app.js`, `styles.css`, manifest, service worker).
2. Browser loads the checked-in workspace data bundle from `apps/web/data/workspace.json`. The Cloudflare Pages bundle must be labeled `data_origin: fixture` and is synthetic demo data only.
3. Analysis/filtering/chart rendering run client-side in the browser session.
4. Optional exports (CSV, JSON, PNG, SVG, HTML) are generated client-side and downloaded by the user.
5. Real or generated pension data is loaded only through a separate in-perimeter host or a user-selected local JSON file; Pages must not carry proprietary data outside the organization boundary.

## Internal API Host

Use the internal host for `data_origin: live` bundles and any shared real-data browser session. It serves the same `apps/web/` assets inside the organization perimeter and keeps deterministic saved-view / metric-history analysis in-process instead of moving pension data to public Pages.

Start the internal host from an approved workstation or server:

```bash
PENSION_DATA_API_KEY="$(openssl rand -hex 24)" PENSION_DATA_DATA_ZONE=proprietary pension-data-serve
curl http://127.0.0.1:8765/health
```

`GET /health` returns service readiness, `GET /config` returns the browser config
keys (`environment`, `apiBaseUrl`, `artifactBaseUrl`), and deterministic routes
such as `/api/saved-views/funding-trend` and `/api/metric-history/{entity_id}`
run without LLM egress. LLM-backed NL/findings routes remain disabled in
`proprietary` mode unless the provider base URL actually consumed by the
LangChain runtime (`OPENAI_BASE_URL` or `ANTHROPIC_BASE_URL`) points at an
authorized no-train proxy.

## Auth and Access

- Recommended deployment posture: Cloudflare Access in front of Pages deployment.
- Access control is managed in Cloudflare Zero Trust policies, not in browser local scripts.
- Cloudflare Access protects the synthetic demo surface; it is not approval to upload real or generated pension data to Pages.
- No additional privileged local service is required.

## Local Storage and Retention

Browser mode stores only local browser data:

- `pension-data.saved-views.v1` (saved filter presets)
- `pension-data.offline-workspace.v1` (last loaded workspace bundle for offline fallback)
- `pension-data.offline-workspace-source.v1` (source label of cached bundle)

Retention behavior:

- Data persists until browser storage is cleared by user/policy.
- No automatic upload from local storage back to repo/runtime.

## Offline Behavior

- Service worker caches core static assets for offline read access.
- If network load of workspace bundle fails, app attempts to load cached workspace bundle from local storage.
- If no cached bundle exists, app fails closed with initialization error.

## Local File Interaction

- User can load a local JSON bundle via file picker in browser session.
- Loaded local bundle is parsed and validated in browser before use.
- Every bundle must declare `data_origin` as `fixture`, `generated`, or `live`; the UI surfaces fixture bundles as `Demo data - not live`.
- Bundle is not uploaded by this feature; it remains local to browser context unless user separately exports/shares it.

## Security Notes

- App renders dataset content with DOM-safe APIs to reduce XSS exposure from untrusted bundle content.
- Chart export HTML escapes `<` when embedding JSON to avoid script-tag breakout injection.
- Query-string config overrides are limited to local contexts or explicit opt-in config flag.

## IT Approval Checklist

- [ ] Confirm Cloudflare Access policy and identity provider requirements.
- [ ] Confirm allowed browser storage policy for local cache keys listed above.
- [ ] Confirm handling expectations for downloaded exports (CSV/JSON/PNG/SVG/HTML).
- [ ] Confirm whether PWA install is allowed or blocked by policy.
- [ ] Confirm offline usage expectations and local cache clearing policy.
- [ ] Confirm reviewers understand the Pages deployment is fixture-only / demo data only; real artifacts require the in-perimeter host.
