# ES Collector (root-level)
- Uses yahoo-finance2 chart() for 1m/5m (no interval error)
- Auto-creates tables if AUTO_MIGRATE=1

## Render
Root Directory: (leave blank)
Build Command: npm install
Start Command: node server.js

Env:
DATABASE_URL=postgresql://user:pass@host:5432/reversal_db?sslmode=require
NODE_ENV=production
SYMBOLS=ES=F
AUTO_MIGRATE=1


## One-time DB Reset
Set `RESET_DB=1` and `AUTO_MIGRATE=0` in Render env, redeploy once, then remove `RESET_DB` and set `AUTO_MIGRATE=1`.
