# Phase 2 Option B (Web API + Worker + Cron)

## Components
- `package.json` → includes express, pg, axios, etc.
- `collector.js` → background worker polling Yahoo Finance into Postgres.
- `render.yaml` → defines three Render services:
  - Web API (trading-api)
  - Worker (yahoo-collector)
  - Cron (phase2-refresh)
- `db/schema_phase2.sql` → creates candles_raw + materialized view training_fingerprints.

## Usage
1. Push to GitHub.
2. Deploy on Render with Blueprint.
3. Web API = your Express app (`server.js`).
4. Worker = runs collector.js forever.
5. Cron = applies/refreshes Phase 2 MV every 5 min.

## Notes
- Ensure `server.js` exists with your Express API code.
- Netlify frontend calls the Web API (Render web service).