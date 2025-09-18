# Phase 2 Deploy

This repo sets up Phase 2 (fingerprints, neighbors, gold/hard negatives) on Render.

- `db/schema_phase2.sql`: creates materialized view, indexes, neighbor function
- `package.json`: safe start script
- `render.yaml`: sets up backend, database, and cron job to apply/refresh MV every 5 minutes

## Deploy
1. Push this repo to GitHub
2. Connect repo to Render (Blueprint deploy)
3. Render provisions Postgres, backend, and cron job automatically
