# Phase 1 + Phase 2 Deploy

This repo now includes:
- Phase 1 collector (existing backend)
- Phase 2 schema (fingerprints + neighbors + labels)

## How it works
- `db/schema_phase2.sql` contains the Phase 2 view, indexes, neighbor function
- `render.yaml` runs your Phase 1 collector service as before
- Added a cron job that:
  1. Applies Phase 2 schema safely (view + function only)
  2. Refreshes `training_fingerprints` every 5 minutes

## Deploy
1. Push to GitHub
2. Render redeploys your service
3. Phase 1 collector runs as normal
4. Cron job keeps Phase 2 fingerprints updated in the same DB (`reversal-data-db`)
