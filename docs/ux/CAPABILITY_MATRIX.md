# Capability Matrix: Browser-Only vs Desktop Shell

| Capability | Browser-Only (Cloudflare + PWA) | Desktop Shell (Tauri/Electron) |
| --- | --- | --- |
| Zero-install on locked-down PCs | Yes (open URL) | No (installer/signing required) |
| Works behind enterprise browser policy | Yes | Depends on desktop app policy |
| Cloudflare Access protected login | Yes | Yes (via embedded browser flow) |
| Offline read of previously loaded bundle | Yes (service worker + local storage) | Yes (local app cache) |
| Load local JSON workspace bundle | Yes | Yes |
| Filtering/provenance/chart interactions | Yes | Yes |
| Export CSV/JSON/PNG/SVG/HTML | Yes | Yes |
| Deep local filesystem automation | Limited | Stronger |
| Native OS integrations | Limited | Stronger |
| Upgrade and patch management | Web deploy | App release lifecycle |

## Recommended Usage Split

- Default for most users: Browser-only mode (fast rollout, no install friction).
- Power-user path: Desktop shell for advanced local integrations and packaged distribution.
