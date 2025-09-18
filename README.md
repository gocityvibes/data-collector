# ES/NQ Collector — Zero-Drama Render Deploy

Root-level Node service that ingests 1m/5m bars from yahoo-finance2 `chart()` with:
- Auto migration (`AUTO_MIGRATE=1`)
- One-click wipe (`RESET_DB=1`)
- Live collection cron every 2 minutes
- Simple health & status endpoints

## Files (root-level)
- `package.json`
- `server.js`
- `schema.sql`
- `.env.example` (optional)
- `render.yaml` (optional; blueprint deploy)

## Render (Web Service, Node)
- **Root Directory:** (leave blank)
- **Build Command:** `npm install`
- **Start Command:** `node server.js`

### Env Vars
- `DATABASE_URL=postgresql://USER:PASS@HOST:5432/DB?sslmode=require`
- `NODE_ENV=production`
- `SYMBOLS=ES=F` (or `ES=F,NQ=F`)
- `AUTO_MIGRATE=1`

### One-Time Wipe (fresh start)
1. Set `RESET_DB=1` and `AUTO_MIGRATE=0`
2. Deploy once (drops `public` and runs `schema.sql`)
3. Remove `RESET_DB`, set `AUTO_MIGRATE=1`, redeploy

### Verify
- `/healthz`
- `/api/status`
- `/api/data-sample`
- `POST /api/start-backfill`

### Notes
- Uses `chart()` for intraday to avoid deprecated `historical()`.
- If you see any "package.json ENOENT" on Render, **the fix is always**: keep files in repo root and keep Render **Root Directory empty**. If using `render.yaml`, make sure it does **not** set `rootDir` to a subfolder.