# Candle Collector (Postgres, Render Ready)

Collects 1m, 5m, 15m candles for ES=F from Yahoo every minute, stores them in Postgres (Render), and keeps 60 days of data.

## API Endpoints
- `/db/ping`
- `/candles/recent?tf=1m&limit=100`
- `/candles/range?tf=5m&from=2025-08-01T00:00:00Z&to=2025-08-02T00:00:00Z`
- `/candles/last60days?tf=15m`

## Render
- **Build Command:** `npm install`
- **Start Command:** `node src/server.js`
