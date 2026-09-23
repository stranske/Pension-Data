# Renderer shell

This directory is the canonical, dependency-free static renderer shell. It is
designed to be copied into a consumer's static web root; it does not own runtime
configuration, workspace data, icons, the web manifest, or the service worker.

Pension-Data keeps deployable copies under `apps/web` because browsers and
Cloudflare Pages cannot import files outside that static root. Update the
canonical files here, then materialize and verify them with:

```bash
python scripts/web/sync_renderer_shell.py
python scripts/web/sync_renderer_shell.py --check
```

`apps/web/app.js` remains a host-owned one-line module wrapper. It imports the
materialized implementation at `apps/web/renderer-shell/app.js`, while all
document-relative config, data, vendor, and service-worker paths continue to
resolve against the web root.
