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
// ------- /stats: candles + label totals -------
app.get('/stats', async (req, res) => {
  try {
    const candles = await pool.query(`
      SELECT symbol, timeframe, COUNT(*) AS rows, MAX(ts_utc) AS last_ts
      FROM candles_raw
      GROUP BY symbol, timeframe
      ORDER BY symbol, timeframe
    `);
    let labels = { rows: [] };
    try {
      labels = await pool.query(`
        SELECT label, COUNT(*) AS rows
        FROM reversals_labels
        GROUP BY label
      `);
    } catch (_) { /* table may not exist yet */ }
    res.json({ ok: true, candles: candles.rows, labels: labels.rows });
  } catch (e) {
    res.status(500).json({ ok:false, error:String(e) });
  }
});

// ------- /reversals (unlabeled pivots) -------
app.get('/reversals', async (req, res) => {
  const symbol = req.query.symbol || 'ES=F';
  const timeframe = req.query.timeframe || '1m';
  const limit = Math.min(parseInt(req.query.limit || '50', 10), 200);
  try {
    const r = await pool.query(`
      SELECT r.symbol, r.timeframe, r.ts_utc, r.open, r.high, r.low, r.close, r.reversal_type
      FROM reversals_pivots_unified r
      LEFT JOIN reversals_labels rl
        ON r.symbol=rl.symbol AND r.timeframe=rl.timeframe AND r.ts_utc=rl.ts_utc
      WHERE rl.symbol IS NULL AND r.symbol=$1 AND r.timeframe=$2
      ORDER BY r.ts_utc DESC
      LIMIT $3
    `, [symbol, timeframe, limit]);
    if (!r.rows.length) return res.status(204).end();
    res.json({ ok:true, rows:r.rows });
  } catch (e) {
    if (String(e).includes('reversals_pivots_unified')) return res.status(204).end();
    res.status(500).json({ ok:false, error:String(e) });
  }
});

// ------- /reversals/gold -------
app.get('/reversals/gold', async (req, res) => {
  const symbol = req.query.symbol || 'ES=F';
  const timeframe = req.query.timeframe || '1m';
  const limit = Math.min(parseInt(req.query.limit || '50', 10), 500);
  try {
    const r = await pool.query(`
      SELECT * FROM reversals_gold
      WHERE symbol=$1 AND timeframe=$2
      ORDER BY ts_utc DESC LIMIT $3
    `, [symbol, timeframe, limit]);
    if (!r.rows.length) return res.status(204).end();
    res.json({ ok:true, rows:r.rows });
  } catch (e) { res.status(500).json({ ok:false, error:String(e) }); }
});

// ------- /reversals/negatives -------
app.get('/reversals/negatives', async (req, res) => {
  const symbol = req.query.symbol || 'ES=F';
  const timeframe = req.query.timeframe || '1m';
  const limit = Math.min(parseInt(req.query.limit || '50', 10), 500);
  try {
    const r = await pool.query(`
      SELECT * FROM reversals_negative
      WHERE symbol=$1 AND timeframe=$2
      ORDER BY ts_utc DESC LIMIT $3
    `, [symbol, timeframe, limit]);
    if (!r.rows.length) return res.status(204).end();
    res.json({ ok:true, rows:r.rows });
  } catch (e) { res.status(500).json({ ok:false, error:String(e) }); }
});

// ------- POST /reversals/label  (gold|negative) -------
app.post('/reversals/label', async (req, res) => {
  const { symbol, timeframe, ts_utc, label, notes } = req.body || {};
  if (!symbol || !timeframe || !ts_utc || !label)
    return res.status(400).json({ ok:false, error:'symbol,timeframe,ts_utc,label required' });
  if (!['gold','negative'].includes(label))
    return res.status(400).json({ ok:false, error:'label must be gold or negative' });
  try {
    await pool.query('SELECT upsert_reversal_label($1,$2,$3,$4,$5)', [symbol, timeframe, ts_utc, label, notes || null]);
    res.json({ ok:true });
  } catch (e) { res.status(500).json({ ok:false, error:String(e) }); }
});

// ------- DELETE /reversals/label -------
app.delete('/reversals/label', async (req, res) => {
  const { symbol, timeframe, ts_utc } = req.query;
  if (!symbol || !timeframe || !ts_utc)
    return res.status(400).json({ ok:false, error:'symbol,timeframe,ts_utc required' });
  try {
    await pool.query('DELETE FROM reversals_labels WHERE symbol=$1 AND timeframe=$2 AND ts_utc=$3', [symbol, timeframe, ts_utc]);
    res.json({ ok:true });
  } catch (e) { res.status(500).json({ ok:false, error:String(e) }); }
});

// Fallback 404
app.use((req, res) => res.status(404).json({ ok:false, error:'Not Found'}));

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
  console.log(`Trading API listening on :${PORT}`);
});
