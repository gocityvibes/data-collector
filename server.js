
/* server.js with startup backfill check fixed: 1m=30d, 5m=60d */
const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const cron = require('node-cron');
const yahooFinance = require('yahoo-finance2').default;
require('dotenv').config();

yahooFinance.suppressNotices?.(['ripHistorical']); 

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3000;
const SYMBOLS = (process.env.SYMBOLS || 'ES=F').split(',').map(s => s.trim()).filter(Boolean);
const AUTO_MIGRATE = process.env.AUTO_MIGRATE === '1';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

async function runSql(sql, params=[]) {
  const client = await pool.connect();
  try { return await client.query(sql, params); }
  finally { client.release(); }
}

async function resetDatabaseOnce() {
  console.warn('[DB] RESET_DB=1 — dropping and recreating public schema');
  await runSql("DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public; GRANT ALL ON SCHEMA public TO public;");
  const fs = require('fs');
  const schema = fs.readFileSync('schema.sql', 'utf8');
  await runSql(schema);
}

const MIGRATION_SQL = require('fs').readFileSync('schema.sql', 'utf8');

async function migrateIfNeeded() {
  await runSql(MIGRATION_SQL);
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function addHours(d, h) { return new Date(d.getTime() + h * 3600 * 1000); }
function clampEnd(start, end, sliceHours) {
  const sliceEnd = addHours(start, sliceHours);
  return sliceEnd > end ? end : sliceEnd;
}

async function getWatermark(symbol, timeframe) {
  const r = await runSql('SELECT last_ts_utc FROM ingest_watermarks WHERE symbol=$1 AND timeframe=$2', [symbol, timeframe]);
  return r.rows[0]?.last_ts_utc ? new Date(r.rows[0].last_ts_utc) : null;
}
async function setWatermark(symbol, timeframe, ts) {
  await runSql(
    `INSERT INTO ingest_watermarks (symbol, timeframe, last_ts_utc, updated_at)
     VALUES ($1,$2,$3,NOW())
     ON CONFLICT (symbol,timeframe) DO UPDATE SET last_ts_utc=$3, updated_at=NOW()`,
    [symbol, timeframe, ts]
  );
}

async function updateCollectionStatus(processName, status, errMsg=null) {
  await runSql(
    `INSERT INTO collection_status (process_name, status, last_run, error_count, last_error, updated_at, created_at)
     VALUES ($1,$2,NOW(),$3,$4,NOW(),NOW())
     ON CONFLICT (process_name) DO UPDATE SET status=$2, last_run=NOW(),
       error_count = CASE WHEN $2='error' THEN collection_status.error_count + 1 ELSE collection_status.error_count END,
       last_error = $4, updated_at = NOW()`,
    [processName, status, errMsg ? 1 : 0, errMsg]
  );
}

async function fetchYahooData(symbol, timeframe, startDate, endDate, attempt=1) {
  try {
    if (timeframe === '1m' || timeframe === '5m') {
      const res = await yahooFinance.chart(symbol, {
        period1: startDate, period2: endDate, interval: timeframe
      });
      const bars = (res?.quotes || []).map(b => ({
        date: new Date(b.date),
        open: b.open, high: b.high, low: b.low, close: b.close, volume: b.volume ?? null
      })).filter(b => b.open != null && b.high != null && b.low != null && b.close != null);
      return bars;
    } else {
      const res = await yahooFinance.historical(symbol, { period1: startDate, period2: endDate, interval: '1d' });
      return (res || []).map(b => ({
        date: new Date(b.date),
        open: b.open, high: b.high, low: b.low, close: b.close, volume: b.volume ?? null
      })).filter(b => b.open != null && b.high != null && b.low != null && b.close != null);
    }
  } catch (e) {
    if (attempt < 3) { await sleep(400 * attempt); return fetchYahooData(symbol, timeframe, startDate, endDate, attempt + 1); }
    throw e;
  }
}

function maxBarTs(bars) {
  let m = null;
  for (const b of bars) if (!m || b.date > m) m = b.date;
  return m;
}

async function upsertCandles(bars, symbol, timeframe) {
  if (!bars?.length) return { rows: 0, maxTs: null };
  const params = [];
  const values = [];
  for (let i = 0; i < bars.length; i++) {
    const b = bars[i];
    const base = i * 8;
    params.push(symbol, timeframe, b.date, b.open, b.high, b.low, b.close, b.volume);
    values.push(`($${base+1},$${base+2},$${base+3},$${base+4},$${base+5},$${base+6},$${base+7},$${base+8})`);
  }
  const sql = `
    INSERT INTO candles_raw (symbol, timeframe, ts_utc, open, high, low, close, volume)
    VALUES ${values.join(',')}
    ON CONFLICT (symbol,timeframe,ts_utc) DO NOTHING
  `;
  const r = await runSql(sql, params);
  return { rows: r.rowCount || 0, maxTs: maxBarTs(bars) };
}

let isBackfilling = false;
let isCollecting = false;
const collectionStats = { status: 'idle', lastUpdate: null, errors: 0, totalInserted: 0 };

async function backfillRange(symbol, timeframe, start, end, sliceHours = 6) {
  let cursor = new Date(start);
  while (cursor < end) {
    const actualEnd = clampEnd(cursor, end, sliceHours);
    const bars = await fetchYahooData(symbol, timeframe, cursor, actualEnd);
    const { rows, maxTs } = await upsertCandles(bars, symbol, timeframe);
    collectionStats.totalInserted += rows;
    if (maxTs) await setWatermark(symbol, timeframe, maxTs);
    collectionStats.lastUpdate = new Date().toISOString();
    cursor = actualEnd;
    await sleep(150);
  }
}

async function backfillSymbol(symbol) {
  if (isBackfilling) return;
  isBackfilling = true;
  collectionStats.status = 'backfilling';
  await updateCollectionStatus(`backfill_${symbol}`, 'running');
  try {
    const now = new Date();
    const d30 = new Date(now.getTime() - 30 * 24 * 3600 * 1000);
    const d60 = new Date(now.getTime() - 60 * 24 * 3600 * 1000);
    await backfillRange(symbol, '5m', d60, d30, 6);
    await backfillRange(symbol, '1m', d30, now, 6);
    collectionStats.status = 'live';
    await updateCollectionStatus(`backfill_${symbol}`, 'completed');
  } catch (e) {
    collectionStats.errors++;
    await updateCollectionStatus(`backfill_${symbol}`, 'error', e.message);
  } finally {
    isBackfilling = false;
  }
}

async function liveCollect() {
  if (isCollecting) return;
  isCollecting = true;
  try {
    const now = new Date();
    for (const sym of SYMBOLS) {
      for (const tf of ['1m', '5m']) {
        const wm = await getWatermark(sym, tf);
        const defaultStart = new Date(now.getTime() - 30 * 60 * 1000);
        const start = wm ? new Date(Math.min(wm.getTime(), now.getTime() - 60 * 1000)) : defaultStart;
        const bars = await fetchYahooData(sym, tf, start, now);
        const { rows, maxTs } = await upsertCandles(bars, sym, tf);
        if (maxTs) await setWatermark(sym, tf, maxTs);
        collectionStats.totalInserted += rows;
        collectionStats.lastUpdate = new Date().toISOString();
      }
    }
    await updateCollectionStatus('live_collection', 'ok');
  } catch (e) {
    collectionStats.errors++;
    await updateCollectionStatus('live_collection', 'error', e.message);
  } finally {
    isCollecting = false;
  }
}

app.get('/healthz', async (req, res) => {
  try { await runSql('SELECT 1'); res.json({ ok: true, using: 'chart() for 1m/5m' }); }
  catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.get('/api/status', async (req, res) => {
  try {
    const coverage = [];
    for (const sym of SYMBOLS) {
      for (const tf of ['1m','5m']) {
        const days = tf === '1m' ? 30 : 60;
        const r = await runSql(
          `SELECT symbol, timeframe, COUNT(*)::int AS total_bars,
                  MIN(ts_utc) AS earliest, MAX(ts_utc) AS latest
           FROM candles_raw WHERE symbol=$1 AND ts_utc >= NOW() - INTERVAL '${days} days'
           GROUP BY symbol, timeframe ORDER BY timeframe`,
          [sym]
        );
        coverage.push(...r.rows);
      }
    }
    const proc = await runSql('SELECT * FROM collection_status ORDER BY updated_at DESC LIMIT 50');
    res.json({ symbols: SYMBOLS, collection: collectionStats, coverage, processes: proc.rows });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/start-backfill', async (req, res) => {
  for (const sym of SYMBOLS) backfillSymbol(sym).catch(() => {});
  res.json({ ok: true, started: SYMBOLS });
});

app.get('/api/data-sample', async (req, res) => {
  try {
    const r = await runSql('SELECT * FROM candles_raw ORDER BY ts_utc DESC LIMIT 25');
    res.json(r.rows);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

(async () => {
  try {
    if (process.env.RESET_DB === '1') {
      await resetDatabaseOnce();
    } else if (AUTO_MIGRATE) {
      await migrateIfNeeded();
    }

    for (const s of SYMBOLS) {
      for (const tf of ['1m','5m']) {
        const days = tf === '1m' ? 30 : 60;
        const r = await runSql(
          `SELECT COUNT(*)::int AS cnt
           FROM candles_raw
           WHERE symbol=$1 AND timeframe=$2 AND ts_utc >= NOW() - INTERVAL '${days} days'`,
          [s, tf]
        );
        const cnt = r.rows[0]?.cnt ?? 0;
        if (cnt < (tf === '1m' ? 30000 : 5000)) {
          console.log(`[${s}:${tf}] starting automatic backfill (rows last ${days}d = ${cnt})`);
          backfillSymbol(s).catch(() => {});
        } else {
          console.log(`[${s}:${tf}] sufficient data detected (rows last ${days}d = ${cnt}); entering live mode`);
          collectionStats.status = 'live';
        }
      }
    }

    cron.schedule('*/2 * * * *', () => liveCollect().catch(() => {}));

    app.listen(PORT, () => console.log(`Collector listening on :${PORT}`));
  } catch (e) {
    console.error('Startup error:', e);
    process.exit(1);
  }
})();
