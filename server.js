// Phase 2 Pro server: health, candles, stats, reversals, labels, neighbors, admin
const express = require('express');
const cors = require('cors');
const pino = require('pino-http')();
const fs = require('fs');
const { Pool } = require('pg');

const app = express();
app.use(cors());
app.use(express.json({ limit: '1mb' }));
app.use(pino);

const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });

// Health
app.get(['/','/health','/healthz'], async (req, res) => {
  try {
    const r = await pool.query('SELECT NOW() as now');
    res.json({ ok:true, status:'healthy', now:r.rows[0].now });
  } catch (e) {
    res.status(503).json({ ok:false, status:'unhealthy', error:String(e) });
  }
});

// Config echo
app.get('/config', (req, res) => {
  res.json({ ok:true, service:'trading-api', ts:new Date().toISOString(),
    config: { timeframes:['1m','5m','15m'], symbols:['ES=F','NQ=F'] } });
});

// Recent candles
app.get('/candles', async (req, res) => {
  const limit = Math.min(parseInt(req.query.limit || '50', 10), 500);
  const symbol = req.query.symbol || 'ES=F';
  const timeframe = req.query.timeframe || '1m';
  try {
    const r = await pool.query(`
      SELECT symbol, timeframe, ts_utc, open, high, low, close, volume, source, ingest_ts
      FROM candles_raw
      WHERE symbol=$1 AND timeframe=$2
      ORDER BY ts_utc DESC LIMIT $3
    `, [symbol, timeframe, limit]);
    if (!r.rows.length) return res.status(204).end();
    res.json({ ok:true, rows:r.rows });
  } catch (e) {
    if (String(e).includes('candles_raw')) return res.status(204).end();
    res.status(500).json({ ok:false, error:String(e) });
  }
});

// Stats + label totals
app.get('/stats', async (req, res) => {
  try {
    const candles = await pool.query(`
      SELECT symbol, timeframe, COUNT(*) AS rows, MAX(ts_utc) AS last_ts
      FROM candles_raw GROUP BY symbol, timeframe ORDER BY symbol, timeframe
    `);
    let labels = { rows: [] };
    try {
      labels = await pool.query(`SELECT label, COUNT(*) AS rows FROM reversals_labels GROUP BY label`);
    } catch (_) {}
    res.json({ ok:true, candles:candles.rows, labels:labels.rows });
  } catch (e) { res.status(500).json({ ok:false, error:String(e) }); }
});

// Unlabeled reversals (candidates)
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

// GOLD / NEGATIVE lists
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

// Label upsert / delete
app.post('/reversals/label', async (req, res) => {
  const { symbol, timeframe, ts_utc, label, notes } = req.body || {};
  if (!symbol || !timeframe || !ts_utc || !label) return res.status(400).json({ ok:false, error:'symbol,timeframe,ts_utc,label required' });
  if (!['gold','negative'].includes(label)) return res.status(400).json({ ok:false, error:'label must be gold or negative' });
  try {
    await pool.query('SELECT upsert_reversal_label($1,$2,$3,$4,$5)', [symbol, timeframe, ts_utc, label, notes || null]);
    res.json({ ok:true });
  } catch (e) { res.status(500).json({ ok:false, error:String(e) }); }
});
app.delete('/reversals/label', async (req, res) => {
  const { symbol, timeframe, ts_utc } = req.query;
  if (!symbol || !timeframe || !ts_utc) return res.status(400).json({ ok:false, error:'symbol,timeframe,ts_utc required' });
  try {
    await pool.query('DELETE FROM reversals_labels WHERE symbol=$1 AND timeframe=$2 AND ts_utc=$3', [symbol, timeframe, ts_utc]);
    res.json({ ok:true });
  } catch (e) { res.status(500).json({ ok:false, error:String(e) }); }
});

// Neighbors for a given reversal
app.get('/neighbors', async (req, res) => {
  const symbol = req.query.symbol || 'ES=F';
  const timeframe = req.query.timeframe || '1m';
  const ts_utc = req.query.ts_utc;
  const k = Math.min(parseInt(req.query.k || '50', 10), 200);
  if (!ts_utc) return res.status(400).json({ ok:false, error:'ts_utc required' });
  try {
    const r = await pool.query('SELECT * FROM get_neighbors($1,$2,$3,$4)', [symbol, timeframe, ts_utc, k]);
    if (!r.rows.length) return res.status(204).end();
    res.json({ ok:true, rows:r.rows });
  } catch (e) { res.status(500).json({ ok:false, error:String(e) }); }
});

// Fingerprints (recent reversals + features)
app.get('/fingerprints', async (req, res) => {
  const symbol = req.query.symbol || 'ES=F';
  const timeframe = req.query.timeframe || '1m';
  const limit = Math.min(parseInt(req.query.limit || '100', 10), 1000);
  try {
    const r = await pool.query(`
      SELECT symbol, timeframe, ts_utc, reversal_type, label, features
      FROM training_fingerprints
      WHERE symbol=$1 AND timeframe=$2
      ORDER BY ts_utc DESC LIMIT $3
    `, [symbol, timeframe, limit]);
    if (!r.rows.length) return res.status(204).end();
    res.json({ ok:true, rows:r.rows });
  } catch (e) { res.status(500).json({ ok:false, error:String(e) }); }
});

// Admin: apply schema file (auth via ADMIN_KEY)
app.post('/admin/apply-schema', async (req, res) => {
  try {
    const key = req.query.key || req.headers['x-admin-key'];
    if (!process.env.ADMIN_KEY || key !== process.env.ADMIN_KEY) {
      return res.status(401).json({ ok:false, error:'unauthorized' });
    }
    const sql = fs.readFileSync('db/schema_phase2.sql', 'utf8');
    await pool.query(sql);
    res.json({ ok:true, message:'schema applied' });
  } catch (e) { res.status(500).json({ ok:false, error:String(e) }); }
});

// Admin: refresh MV (auth)
app.post('/admin/refresh-mv', async (req, res) => {
  try {
    const key = req.query.key || req.headers['x-admin-key'];
    if (!process.env.ADMIN_KEY || key !== process.env.ADMIN_KEY) {
      return res.status(401).json({ ok:false, error:'unauthorized' });
    }
    await pool.query('REFRESH MATERIALIZED VIEW CONCURRENTLY training_fingerprints');
    res.json({ ok:true, message:'training_fingerprints refreshed' });
  } catch (e) { res.status(500).json({ ok:false, error:String(e) }); }
});

// 404
app.use((req, res) => res.status(404).json({ ok:false, error:'Not Found'}));

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => console.log(`Trading API (Phase 2 Pro) listening on :${PORT}`));