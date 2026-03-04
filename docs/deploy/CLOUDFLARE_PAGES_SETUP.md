# Cloudflare Pages Setup (Private Access)

This guide configures the `apps/web` scaffold for Cloudflare Pages deployment with private access controls.

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

## 3. Cloudflare Access Policy (Private Usage)

1. In Cloudflare Zero Trust, add an Access application for the Pages domain.
2. Define allow policy by identity provider, email domain, or group.
3. Add explicit deny rules for anonymous access.
4. Test with one allowed and one blocked account before production cutover.

## 4. Deployment Workflow

Workflow: `.github/workflows/web-cloudflare-pages.yml`

- Pull requests run local smoke checks only.
- Pushes to `main` run smoke checks and deploy to Cloudflare Pages.
- Deploy job writes `apps/web/config/runtime.json` from repository variables.

## 5. Rollback Procedure

1. Open Cloudflare Pages and select last known good deployment.
2. Promote that deployment as active.
3. If code rollback is required, revert the merge commit and push to `main`.
4. Confirm runtime config values still match expected environment URLs.

## 6. Verification

- Check workflow summary for successful smoke and deploy jobs.
- Open deployed URL and confirm:
  - Environment badge is populated.
  - API and artifact endpoints show expected values.
  - Access policy blocks unauthorized users.
