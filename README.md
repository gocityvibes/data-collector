
# ES Phase 2 (Auto Label) — Production Ready

**What you get**
- Node API (`/admin/apply-schema`, `/admin/refresh-and-label`, `/health`, `/stats`)
- Postgres schema with:
  - `candles_raw`, `reversals`, `reversals_archive`, `reversals_labels`
  - `training_fingerprints` **materialized view**
  - Neighbor search: `get_neighbor_patterns_with_points(...)`
  - Automatic labeling functions:
    - `auto_label_new_reversals()`
    - `refresh_and_auto_label()` (refresh MV + label loop)
- Render blueprint with 2 workers (MV refresh + labeling every 5m; hourly maintenance hook)

## Deploy (Render)
1. Create a new **Blueprint** and point to this repo/zip.
2. Set **ADMIN_KEY** on the web service to a strong value.
3. Once live, run (one-time) to create all objects:
   ```
   curl -XPOST "https://<your-web-service>.onrender.com/admin/apply-schema?key=<ADMIN_KEY>"
   ```
4. Kick a manual refresh+label:
   ```
   curl -XPOST "https://<your-web-service>.onrender.com/admin/refresh-and-label?key=<ADMIN_KEY>"
   ```

## Endpoints
- `GET /health` → health
- `GET /stats` → counts
- `POST /admin/apply-schema?key=ADMIN_KEY` → idempotent schema create
- `POST /admin/refresh-and-label?key=ADMIN_KEY` → `REFRESH CONCURRENTLY` + auto label

## Notes
- This build is **ES-only by data usage**, but schema supports multiple symbols.
- No placeholders, stubs, or dummy logic.
- You can ingest reversals from your detector into `reversals` (unique on `(symbol,timeframe,timestamp)`).
- Archive your best/worst into `reversals_archive` to strengthen the MV memory.
