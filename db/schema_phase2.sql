-- Phase 2 Schema for reversal-data-db
CREATE TABLE IF NOT EXISTS candles_raw (
  id SERIAL PRIMARY KEY,
  symbol TEXT,
  timeframe TEXT,
  ts_utc TIMESTAMPTZ,
  open NUMERIC,
  high NUMERIC,
  low NUMERIC,
  close NUMERIC,
  volume NUMERIC,
  source TEXT,
  ingest_ts TIMESTAMPTZ DEFAULT NOW()
);

-- Example materialized view
CREATE MATERIALIZED VIEW IF NOT EXISTS training_fingerprints AS
SELECT symbol, timeframe, COUNT(*) AS bars, MAX(ts_utc) AS last_bar
FROM candles_raw
GROUP BY symbol, timeframe;

CREATE UNIQUE INDEX IF NOT EXISTS training_fingerprints_idx ON training_fingerprints(symbol, timeframe);