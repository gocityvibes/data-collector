import pkg from "pg";
import logger from "./logger.js";

const { Pool } = pkg;
const pool = new Pool({ connectionString: process.env.DATABASE_URL });

export async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS candles_raw (
      id BIGSERIAL PRIMARY KEY,
      symbol TEXT NOT NULL,
      timeframe TEXT NOT NULL,
      ts_utc TIMESTAMPTZ NOT NULL,
      open NUMERIC NOT NULL,
      high NUMERIC NOT NULL,
      low NUMERIC NOT NULL,
      close NUMERIC NOT NULL,
      volume NUMERIC NOT NULL,
      UNIQUE(symbol, timeframe, ts_utc)
    );
    CREATE INDEX IF NOT EXISTS idx_candles_raw_tf_ts ON candles_raw(timeframe, ts_utc);
  `);
  logger.info("✅ Database ready");
}

export default pool;
