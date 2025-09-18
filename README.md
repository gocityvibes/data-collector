# Phase 2 Option B (Web API + Worker + Cron)

## Components
- `server.js` → Express API with `/health`, `/candles` (no node-cron).
- `package.json` → includes express, pg, axios, etc.
- `collector.js` → background worker polling Yahoo Finance into Postgres.
- `render.yaml` → defines three Render services:
  - Web API (trading-api)
  - Worker (yahoo-collector)
  - Cron (phase2-refresh)
- `db/schema_phase2.sql` → creates candles_raw + materialized view training_fingerprints, and refreshes it.

## Deploy
1) Push to GitHub.
2) Render → New Blueprint → select this repo.
3) Render provisions the DB + services. Logs will show:
   - trading-api listening
   - yahoo-collector inserting candles
   - phase2-refresh running every 5 min

## Notes
- If `candles_raw` isn't ready yet, `/candles` returns 204 instead of 500.
- Frontend (Netlify) should call the Render web service URL for data.