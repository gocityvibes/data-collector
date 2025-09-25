import * as yahooFinance from "yahoo-finance2";
import pool from "./db.js";
import logger from "./logger.js";

const TF_TO_INTERVAL = { "1m": "1m", "5m": "5m", "15m": "15m" };
const LOOKBACK = {
  "1m": parseInt(process.env.LOOKBACK_MIN_1m || "180"),
  "5m": parseInt(process.env.LOOKBACK_MIN_5m || "720"),
  "15m": parseInt(process.env.LOOKBACK_MIN_15m || "1440")
};

function msAgo(mins){ return Date.now() - mins*60*1000; }

async function fetchChart(symbol, interval, lookbackMins){
  const period1 = new Date(msAgo(lookbackMins));
  const period2 = new Date();
  if (typeof yahooFinance.chart === "function") {
    return await yahooFinance.chart(symbol, { interval, period1, period2 });
  }
  // Fallback: direct HTTP call to Yahoo chart API
  const u = new URL(`https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}`);
  u.searchParams.set("interval", interval);
  u.searchParams.set("period1", Math.floor(period1.getTime()/1000).toString());
  u.searchParams.set("period2", Math.floor(period2.getTime()/1000).toString());
  const resp = await fetch(u, { headers: { "accept": "application/json" }});
  if(!resp.ok) throw new Error(`Yahoo HTTP ${resp.status}`);
  const data = await resp.json();
  // Normalize to yahoo-finance2-like response
  const timestamps = data?.chart?.result?.[0]?.timestamp || [];
  const quotes = data?.chart?.result?.[0]?.indicators?.quote?.[0] || {};
  const out = [];
  for(let i=0;i<timestamps.length;i++){
    out.push({
      date: new Date(timestamps[i]*1000).toISOString(),
      open: quotes.open?.[i] ?? null,
      high: quotes.high?.[i] ?? null,
      low: quotes.low?.[i] ?? null,
      close: quotes.close?.[i] ?? null,
      volume: quotes.volume?.[i] ?? 0
    });
  }
  return { quotes: out };
}

export async function collectCandles() {
  const symbols = (process.env.SYMBOLS || "ES=F").split(",").map(s=>s.trim()).filter(Boolean);
  const tfs = (process.env.TIMEFRAMES || "1m,5m,15m").split(",").map(s=>s.trim()).filter(Boolean);

  for (const symbol of symbols) {
    for (const tf of tfs) {
      try {
        const interval = TF_TO_INTERVAL[tf];
        if (!interval) continue;
        const lookbackMins = LOOKBACK[tf];
        const result = await fetchChart(symbol, interval, lookbackMins);
        const rows = (result?.quotes || []).map(q => ({
          ts: q.date,
          open: q.open,
          high: q.high,
          low: q.low,
          close: q.close,
          volume: q.volume
        })).filter(r => r.open!=null && r.high!=null && r.low!=null && r.close!=null);

        for (const r of rows) {
          await pool.query(
            `INSERT INTO candles_raw (symbol, timeframe, ts_utc, open, high, low, close, volume)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
             ON CONFLICT (symbol, timeframe, ts_utc) DO NOTHING`,
            [symbol, tf, r.ts, r.open, r.high, r.low, r.close, r.volume || 0]
          );
        }

        // purge >60 days
        await pool.query(
          `DELETE FROM candles_raw WHERE symbol=$1 AND timeframe=$2 AND ts_utc < NOW() - interval '60 days'`,
          [symbol, tf]
        );

        logger.info({symbol, tf, count: rows.length}, "✅ Collected candles");
      } catch (err) {
        logger.error({ err: err.message, symbol, tf }, "❌ Collector error");
      }
    }
  }
}
