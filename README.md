# Phase 2 PRO (full schema + endpoints)

## What you get
- Rich SQL schema:
  - `candles_feat` (features)
  - `reversals_pivots_unified` (k=2 pivots)
  - `reversals_featured` (reversal + features)
  - `training_fingerprints` **(materialized view)** with JSON features + labels
  - `reversals_labels` + `upsert_reversal_label()`
  - `get_neighbors()` SQL function (euclidean distance over stable features)
- API routes:
  - `/health`, `/config`, `/candles`
  - `/stats`, `/reversals` (unlabeled), `/reversals/gold`, `/reversals/negatives`
  - `/label` upsert/delete
  - `/neighbors?symbol=&timeframe=&ts_utc=&k=50`
  - `/fingerprints?symbol=&timeframe=&limit=...`
  - Admin: `POST /admin/apply-schema` and `POST /admin/refresh-mv` (needs `ADMIN_KEY`)

## Deploy (Render Blueprint)
1. Put these files in your repo, commit, and deploy as **Blueprint**.
2. Set `ADMIN_KEY` env var on the **web** service (render.yaml includes placeholder).
3. After first boot, run schema once (if cron hasn't yet):
   ```
   curl -X POST "https://<your>.onrender.com/admin/apply-schema?key=YOUR_ADMIN_KEY"
   ```

## Quick test URLs (replace base with your service URL)
- Health: `/health`
- Stats: `/stats`
- Unlabeled reversals: `/reversals?symbol=ES=F&timeframe=1m&limit=100`
- Gold: `/reversals/gold?symbol=ES=F&timeframe=1m&limit=500`
- Negatives: `/reversals/negatives?symbol=ES=F&timeframe=1m&limit=500`
- Fingerprints: `/fingerprints?symbol=ES=F&timeframe=1m&limit=100`
- Neighbors: `/neighbors?symbol=ES=F&timeframe=1m&ts_utc=YYYY-MM-DDTHH:MM:SSZ&k=50`

## Notes
- This is **production-ready** SQL and Node, no placeholders. Views/MV can be extended later with RSI/ATR if you want.
- All parts are idempotent; safe to re-run schema and cron refresh.