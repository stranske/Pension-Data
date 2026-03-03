# PC Zero-Install Mode: IT/Security Review Guide

This guide describes the browser-only mode for Pension-Data in locked-down PC environments.

## Overview

- Delivery model: static web app assets served from Cloudflare Pages.
- Optional install: Progressive Web App (PWA) install where enterprise policy allows it.
- No required local binary installs for browser mode.

## Data Flow

1. Browser downloads static assets (`index.html`, `app.js`, `styles.css`, manifest, service worker).
2. Browser loads workspace data bundle from `apps/web/data/workspace.json` (or user-selected local JSON file).
3. Analysis/filtering/chart rendering run client-side in the browser session.
4. Optional exports (CSV, JSON, PNG, SVG, HTML) are generated client-side and downloaded by the user.

## Auth and Access

- Recommended deployment posture: Cloudflare Access in front of Pages deployment.
- Access control is managed in Cloudflare Zero Trust policies, not in browser local scripts.
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
