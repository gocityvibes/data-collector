// Minimal Express API (no node-cron) for Option B
const express = require('express');
const cors = require('cors');
const pino = require('pino-http')();
const { Pool } = require('pg');

const app = express();
app.use(cors());
app.use(express.json({ limit: '1mb' }));
app.use(pino);

const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) {
  console.warn('Warning: DATABASE_URL not set. /health will report unhealthy.');
}
const pool = DATABASE_URL ? new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false }
}) : null;

// Health check
app.get(['/','/health','/healthz'], async (req, res) => {
  if (!pool) return res.status(503).json({ ok:false, status:'unhealthy', reason:'no DATABASE_URL' });
  try {
    const r = await pool.query('SELECT NOW() as now');
    res.json({ ok:true, status:'healthy', now:r.rows[0].now });
  } catch (e) {
    res.status(503).json({ ok:false, status:'unhealthy', error: String(e) });
  }
});

// Optional: quick config echo for frontends
app.get('/config', (req, res) => {
  res.json({
    ok: true,
    service: 'trading-api',
    ts: new Date().toISOString(),
    config: {
      timeframes: ['1m','5m','15m'],
      symbols: ['ES=F','NQ=F']
    }
  });
});

// Read a few recent candles from candles_raw if present
app.get('/candles', async (req, res) => {
  const limit = Math.min(parseInt(req.query.limit || '50', 10), 500);
  if (!pool) return res.status(503).json({ ok:false, error:'no DATABASE_URL' });
  try {
    const q = `
      SELECT symbol, timeframe, ts_utc, open, high, low, close, volume, source, ingest_ts
      FROM candles_raw
      ORDER BY ts_utc DESC
      LIMIT $1
    `;
    const r = await pool.query(q, [limit]);
    res.json({ ok:true, rows:r.rows });
  } catch (e) {
    // If table not ready yet, return 204 with hint
    if (String(e).includes('relation "candles_raw" does not exist')) {
      return res.status(204).end();
    }
    res.status(500).json({ ok:false, error:String(e) });
  }
});

// Fallback 404
app.use((req, res) => res.status(404).json({ ok:false, error:'Not Found'}));

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
  console.log(`Trading API listening on :${PORT}`);
});