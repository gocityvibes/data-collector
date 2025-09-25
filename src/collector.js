import yahooFinance from "yahoo-finance2";
import pool from "./db.js";
import logger from "./logger.js";

const TF_TO_INTERVAL = { "1m": "1m", "5m": "5m", "15m": "15m" };
const LOOKBACK = {
  "1m": parseInt(process.env.LOOKBACK_MIN_1m || "180"),
  "5m": parseInt(process.env.LOOKBACK_MIN_5m || "720"),
  "15m": parseInt(process.env.LOOKBACK_MIN_15m || "1440")
};

export async function collectCandles() {
  const symbols = (process.env.SYMBOLS || "ES=F").split(",");
  const tfs = (process.env.TIMEFRAMES || "1m,5m,15m").split(",");
  for (const symbol of symbols) {
    for (const tf of tfs) {
      try {
        const interval = TF_TO_INTERVAL[tf];
        if (!interval) continue;
        const lookbackMins = LOOKBACK[tf];
        const period = `${lookbackMins}m`;
        const result = await yahooFinance.chart(symbol, { interval, range: period });
        const rows = result.quotes.map(q => ({
          ts: q.date,
          open: q.open,
          high: q.high,
          low: q.low,
          close: q.close,
          volume: q.volume
        }));

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

        logger.info(`✅ Collected ${rows.length} ${tf} candles for ${symbol}`);
      } catch (err) {
        logger.error({ err }, `❌ Error collecting ${tf} candles for ${symbol}`);
      }
    }
  }
}
