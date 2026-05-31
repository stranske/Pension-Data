# Synthetic-Data Demo Hosting (Cloudflare Pages)

> For fixture/synthetic data only. Do not publish bundles whose `data_origin` is `live`.

This guide configures the `apps/web` scaffold for a Cloudflare Pages synthetic/demo deployment with private access controls. Cloudflare Pages is not the real-data hosting path.

## 1. Create Cloudflare Pages Project

1. In Cloudflare dashboard, create a Pages project (for example `pension-data-web`).
2. Configure build settings:
   - Framework preset: `None`
   - Build command: _(empty)_
   - Build output directory: `apps/web`

## 2. GitHub Repository Configuration

Set repository secrets:

- `CF_API_TOKEN`: Cloudflare API token with Pages edit permissions.
- `CF_ACCOUNT_ID`: Cloudflare account identifier.
- `CF_ACCESS_CLIENT_ID` and `CF_ACCESS_CLIENT_SECRET` (optional but recommended): service token pair for post-deploy smoke checks when Cloudflare Access is enabled.

Set repository variables:

- `CF_PAGES_PROJECT_NAME`: Pages project name.
- `PENSION_DATA_API_BASE_URL`: Base URL for API calls.
- `PENSION_DATA_ARTIFACT_BASE_URL`: Base URL for artifacts.
- `PENSION_DATA_ENVIRONMENT_LABEL`: Environment label shown in UI (for example `prod-private`).

## 3. Data Classification

Cloudflare Pages is an external SaaS target and may serve only the checked-in fixture
bundle (`data_origin: fixture`). Treat this deployment as a synthetic demo surface.
Do not upload generated or live pension data to Pages; real extracted data belongs on
the companion in-perimeter host.

## 4. Cloudflare Access Policy (Private Usage)

1. In Cloudflare Zero Trust, add an Access application for the Pages domain.
2. Define allow policy by identity provider, email domain, or group.
3. Add explicit deny rules for anonymous access.
4. Test with one allowed and one blocked account before production cutover.

## 5. Deployment Workflow

Workflow: `.github/workflows/web-cloudflare-pages.yml`

- Pull requests run local smoke checks only.
- Pushes to `main` run smoke checks and deploy to Cloudflare Pages.
- Deploy job writes `apps/web/config/runtime.json` from repository variables.
- Deploy job fails before `cloudflare/pages-action` if `apps/web/data/workspace.json` is not `data_origin: fixture`.
- The deploy-time smoke check validates the fixture bundle and runtime config without `--require-runtime`, because this public target is synthetic/demo data only.

## 6. Rollback Procedure

1. Open Cloudflare Pages and select last known good deployment.
2. Promote that deployment as active.
3. If code rollback is required, revert the merge commit and push to `main`.
4. Confirm runtime config values still match expected environment URLs.

## 7. Verification

- Check workflow summary for successful smoke and deploy jobs.
- Open deployed URL and confirm:
  - Environment badge is populated.
  - API and artifact endpoints show expected values.
  - Data origin badge shows the fixture/demo classification for the Pages deployment.
  - Access policy blocks unauthorized users.

## 8. Reviewer Gate

Run this check during review and confirm no public-hosting guidance points to publishing live bundles:

```bash
grep -rniE "data_origin.*live" docs apps/web
```

Expected review outcome:
- `docs/deploy/CLOUDFLARE_PAGES_SETUP.md` includes the fixture-only warning banner.
- Any `data_origin.*live` hits in docs are prohibition language or internal-hosting guidance, not public-hosting instructions.
- `apps/web/data/workspace.json` remains `data_origin: fixture` for the public demo bundle.
