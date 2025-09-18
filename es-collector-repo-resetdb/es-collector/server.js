
// Root-level server.js (no subfolder)
const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const cron = require('node-cron');
const yahooFinance = require('yahoo-finance2').default;
require('dotenv').config();

yahooFinance.suppressNotices?.(['ripHistorical']);

const app = express();
const port = process.env.PORT || 3000;
const SYMBOLS = (process.env.SYMBOLS || 'ES=F').split(',').map(s=>s.trim()).filter(Boolean);

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});


async function resetDatabaseOnce() {
  console.warn("[DB] RESET_DB=1 — dropping and recreating public schema!");
  await pool.query(`
    DROP SCHEMA IF EXISTS public CASCADE;
    CREATE SCHEMA public;
    GRANT ALL ON SCHEMA public TO public;
  `);
  const fs = require('fs');
  const schema = fs.readFileSync('schema.sql', 'utf8');
  await pool.query(schema);
}
app.use(cors());
app.use(express.json());

const AUTO_MIGRATE = process.env.AUTO_MIGRATE === '1';
const schemaSql = `
CREATE TABLE IF NOT EXISTS candles_raw (
  id SERIAL PRIMARY KEY,
  symbol VARCHAR(20) NOT NULL,
  timeframe VARCHAR(10) NOT NULL,
  ts_utc TIMESTAMPTZ NOT NULL,
  open DECIMAL(12,4) NOT NULL,
  high DECIMAL(12,4) NOT NULL,
  low DECIMAL(12,4) NOT NULL,
  close DECIMAL(12,4) NOT NULL,
  volume BIGINT,
  source VARCHAR(50) DEFAULT 'yahoo',
  ingest_ts TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(symbol, timeframe, ts_utc)
);
CREATE TABLE IF NOT EXISTS ingest_watermarks (
  symbol VARCHAR(20) NOT NULL,
  timeframe VARCHAR(10) NOT NULL,
  last_ts_utc TIMESTAMPTZ NOT NULL,
  status VARCHAR(20) DEFAULT 'active',
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY(symbol, timeframe)
);
CREATE TABLE IF NOT EXISTS collection_status (
  id SERIAL PRIMARY KEY,
  process_name VARCHAR(50) NOT NULL,
  status VARCHAR(20) NOT NULL,
  last_run TIMESTAMPTZ,
  next_run TIMESTAMPTZ,
  error_count INTEGER DEFAULT 0,
  last_error TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(process_name)
);
CREATE INDEX IF NOT EXISTS idx_candles_symbol_tf_ts ON candles_raw(symbol, timeframe, ts_utc);
CREATE INDEX IF NOT EXISTS idx_candles_ts ON candles_raw(ts_utc);
`;

async function migrateIfNeeded() { if (AUTO_MIGRATE) await pool.query(schemaSql); }
const sleep = ms => new Promise(r=>setTimeout(r,ms));
const addHours = (d,h)=>new Date(d.getTime()+h*3600*1000);
const maxBarTs = arr => arr.reduce((m,b)=>(!m||b.date>m?b.date:m), null);

async function updateCollectionStatus(pname, status, err=null){
  await pool.query(`
    INSERT INTO collection_status (process_name, status, last_run, error_count, last_error, updated_at)
    VALUES ($1,$2,NOW(),$3,$4,NOW())
    ON CONFLICT (process_name) DO UPDATE SET status=$2,last_run=NOW(),
      error_count=CASE WHEN $2='error' THEN collection_status.error_count+1 ELSE 0 END,
      last_error=$4, updated_at=NOW()
  `,[pname, status, err?1:0, err]);
}
async function getWatermark(sym, tf){
  const r=await pool.query('SELECT last_ts_utc FROM ingest_watermarks WHERE symbol=$1 AND timeframe=$2',[sym,tf]);
  return r.rows[0]?.last_ts_utc ? new Date(r.rows[0].last_ts_utc) : null;
}
async function setWatermark(sym, tf, ts){
  await pool.query(`
    INSERT INTO ingest_watermarks (symbol,timeframe,last_ts_utc,updated_at)
    VALUES ($1,$2,$3,NOW())
    ON CONFLICT (symbol,timeframe) DO UPDATE SET last_ts_utc=$3, updated_at=NOW()
  `,[sym,tf,ts]);
}

async function fetchYahooData(symbol, timeframe, startDate, endDate, attempt=1){
  try{
    if (timeframe==='1m' || timeframe==='5m'){
      const res = await yahooFinance.chart(symbol, {
        period1: startDate,
        period2: endDate,
        interval: timeframe,
        includePrePost: true
      });
      const ts = res?.timestamp || [];
      const q  = res?.indicators?.quote?.[0] || {};
      return ts.map((t,i)=>({
        date:new Date(t*1000),
        open:q.open?.[i],
        high:q.high?.[i],
        low: q.low?.[i],
        close:q.close?.[i],
        volume:q.volume?.[i] ?? 0
      })).filter(b=>b.open!=null&&b.high!=null&&b.low!=null&&b.close!=null);
    } else {
      const res = await yahooFinance.historical(symbol, {
        period1: startDate, period2: endDate, interval: '1d'
      });
      return (res||[]).map(b=>({date:b.date,open:b.open,high:b.high,low:b.low,close:b.close,volume:b.volume}));
    }
  }catch(e){
    if (attempt<3){ await sleep(400*attempt); return fetchYahooData(symbol,timeframe,startDate,endDate,attempt+1); }
    throw e;
  }
}
async function upsertCandles(bars, symbol, timeframe){
  if (!bars?.length) return {rows:0, maxTs:null};
  const params=[]; const values=bars.map((b,i)=>{const base=i*8; params.push(symbol,timeframe,b.date,b.open,b.high,b.low,b.close,b.volume||0); return `($${base+1},$${base+2},$${base+3},$${base+4},$${base+5},$${base+6},$${base+7},$${base+8})`;});
  const r=await pool.query(`INSERT INTO candles_raw (symbol,timeframe,ts_utc,open,high,low,close,volume) VALUES ${values.join(',')} ON CONFLICT (symbol,timeframe,ts_utc) DO NOTHING`, params);
  return {rows:r.rowCount, maxTs:maxBarTs(bars)};
}

let isBackfilling=false, isCollecting=false;
const collectionStats = { status:'idle', lastUpdate:null, errors:0, totalInserted:0 };

async function backfillRange(sym, tf, start, end, sliceHours=6){
  let current = new Date(start);
  while (current < end){
    const sliceEnd = addHours(current, sliceHours);
    const actualEnd = sliceEnd > end ? end : sliceEnd;
    const bars = await fetchYahooData(sym, tf, current, actualEnd);
    const {rows, maxTs} = await upsertCandles(bars, sym, tf);
    collectionStats.totalInserted += rows;
    collectionStats.lastUpdate = new Date();
    if (maxTs) await setWatermark(sym, tf, maxTs);
    await sleep(400);
    current = actualEnd;
  }
}
async function backfillSymbol(sym){
  if (isBackfilling) return;
  isBackfilling = true;
  collectionStats.status='backfilling';
  await updateCollectionStatus(`backfill_${sym}`, 'running');
  try{
    const now = new Date();
    const d30 = new Date(now.getTime() - 30*24*3600*1000);
    const d60 = new Date(now.getTime() - 60*24*3600*1000);
    await backfillRange(sym,'5m', d60, d30, 6);
    await backfillRange(sym,'1m', d30, now, 6);
    collectionStats.status='live';
    await updateCollectionStatus(`backfill_${sym}`, 'completed');
  }catch(e){
    collectionStats.errors++;
    await updateCollectionStatus(`backfill_${sym}`, 'error', e.message);
  }finally{ isBackfilling=false; }
}
async function liveCollect(){
  if (isCollecting) return;
  isCollecting = true;
  try{
    const now = new Date();
    for (const sym of SYMBOLS){
      for (const tf of ['1m','5m']){
        const wm = await getWatermark(sym, tf);
        const defaultStart = new Date(now.getTime() - 30*60*1000);
        const start = wm ? new Date(Math.min(wm.getTime(), now.getTime()-60*1000)) : defaultStart;
        if (start >= now) continue;
        const bars = await fetchYahooData(sym, tf, start, now);
        const {rows, maxTs} = await upsertCandles(bars, sym, tf);
        collectionStats.totalInserted += rows;
        collectionStats.lastUpdate = new Date();
        if (maxTs) await setWatermark(sym, tf, maxTs);
      }
    }
    await updateCollectionStatus('live_collection','running');
  }catch(e){
    collectionStats.errors++;
    await updateCollectionStatus('live_collection','error', e.message);
  }finally{ isCollecting=false; }
}

app.get('/healthz', async (req,res)=>{
  try { await pool.query('SELECT 1'); res.json({ ok:true, using:'chart() for 1m/5m' }); }
  catch(e){ res.status(500).json({ ok:false, error:e.message }); }
});
app.get('/api/status', async (req,res)=>{
  try{
    const coverage=[];
    for (const sym of SYMBOLS){
      const r = await pool.query(`
        SELECT symbol,timeframe,COUNT(*)::int AS total_bars,
               MIN(ts_utc) AS earliest, MAX(ts_utc) AS latest
        FROM candles_raw WHERE symbol=$1
        GROUP BY symbol,timeframe ORDER BY timeframe
      `,[sym]);
      coverage.push(...r.rows);
    }
    const proc = await pool.query('SELECT * FROM collection_status ORDER BY updated_at DESC LIMIT 50');
    res.json({ symbols:SYMBOLS, collection:collectionStats, coverage, processes:proc.rows });
  }catch(e){ res.status(500).json({error:e.message}); }
});
app.post('/api/start-backfill', async (req,res)=>{
  for (const sym of SYMBOLS) backfillSymbol(sym).catch(()=>{});
  res.json({ ok:true, started:SYMBOLS });
});
app.get('/api/data-sample', async (req,res)=>{
  try{
    const s = SYMBOLS[0];
    const r = await pool.query('SELECT * FROM candles_raw WHERE symbol=$1 ORDER BY ts_utc DESC LIMIT 10',[s]);
    res.json(r.rows);
  }catch(e){ res.status(500).json({error:e.message}); }
});

cron.schedule('*/2 * * * *', ()=>{
  if (['live','collecting','idle'].includes(collectionStats.status)) liveCollect().catch(()=>{});
});

app.listen(port, async ()=>{
  console.log(`Collector listening on :${port}`);
  try{
    if (process.env.RESET_DB === '1') { await resetDatabaseOnce(); } else { await migrateIfNeeded(); }
    for (const s of SYMBOLS){
      const r = await pool.query(`SELECT COUNT(*)::int AS cnt FROM candles_raw WHERE symbol=$1 AND ts_utc >= NOW() - INTERVAL '60 days'`,[s]);
      const cnt = r.rows[0].cnt;
      if (cnt < 5000){
        console.log(`[${s}] starting automatic backfill (rows last 60d = ${cnt})`);
        backfillSymbol(s).catch(()=>{});
      } else {
        console.log(`[${s}] sufficient data detected (rows last 60d = ${cnt}); entering live mode`);
        collectionStats.status='live';
      }
    }
  }catch(e){ console.error('Startup error:', e); }
});
