# Split Reversals API (Express)

## Quick Start
```bash
npm i express cors helmet morgan pg
export DATABASE_URL="postgres://..."
# optional: export API_KEY="your_key"
node server.js
```

## Endpoints
- `GET /health`
- `GET /candles?symbol=ES=F&tf=1m&limit=5`
- `GET /reversals?symbol=ES=F&tf=1m&limit=200&labels=1&label_only=gold`
- `GET /indicators?symbol=ES=F&tf=1m&limit=500`
- `GET /points/daily?symbol=ES=F&tf=1m&from=2025-09-20T00:00:00Z&to=2025-09-25T23:59:59Z&save=1`

## Notes
- Set `API_KEY` to enable auth. Then call with header: `x-api-key: <value>` or `Authorization: Bearer <value>`.
- Allowed timeframes via `ALLOWED_TFS` env (default: `1m,5m,15m`).
- Dates must be strict UTC like `2025-09-24T00:00:00Z`.
```
