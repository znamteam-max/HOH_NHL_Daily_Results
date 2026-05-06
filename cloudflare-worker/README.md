# Cloudflare Worker

This Worker replaces the Vercel Telegram webhook/menu layer.

It handles:

- `GET /` health check
- `POST|GET /api/setup-webhook`
- `POST|GET /api/menu`
- `POST /api/telegram`
- `GET /api/cron`
- Cloudflare Cron Trigger every 15 minutes, disabled by default with `CLOUDFLARE_CRON_ENABLED=0`

The heavy NHL result formatter still runs in GitHub Actions. The Worker triggers it through GitHub `repository_dispatch`.

## Required Cloudflare Secrets

Set these with Wrangler:

```powershell
npx wrangler secret put TELEGRAM_BOT_TOKEN
npx wrangler secret put GITHUB_DISPATCH_TOKEN
```

`GITHUB_DISPATCH_TOKEN` can be a fine-grained GitHub token for `znamteam-max/HOH_NHL_Daily_Results` with `Contents: Read and write`, because the Worker uses the repository dispatch endpoint.

## Deploy

```powershell
npm install
npx wrangler login
npx wrangler deploy
```

After deploy, configure Telegram:

```powershell
$worker = "https://hoh-nhl-daily-results.<your-subdomain>.workers.dev"
Invoke-WebRequest -Method Post -Uri "$worker/api/setup-webhook?secret=hook-123"
```

To send the menu again:

```powershell
Invoke-WebRequest -Method Post -Uri "$worker/api/menu?secret=hook-123&chat=-1003167239288"
```

## Optional: Move Polling To Cloudflare Cron

After confirming the Worker is deployed and `GITHUB_DISPATCH_TOKEN` works, set `CLOUDFLARE_CRON_ENABLED=1` in Cloudflare. Then Cloudflare Cron can trigger GitHub Actions every 15 minutes.

Keep the GitHub Actions schedule enabled until Cloudflare Cron is confirmed, so the bot does not miss games during migration.
