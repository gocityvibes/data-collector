-- Full schema for ES/NQ intraday collection
CREATE TABLE IF NOT EXISTS candles_raw (
  id SERIAL PRIMARY KEY,
  symbol VARCHAR(20) NOT NULL,
  timeframe VARCHAR(10) NOT NULL,
  ts_utc TIMESTAMPTZ NOT NULL,
  open NUMERIC(12,4) NOT NULL,
  high NUMERIC(12,4) NOT NULL,
  low  NUMERIC(12,4) NOT NULL,
  close NUMERIC(12,4) NOT NULL,
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
  process_name VARCHAR(50) PRIMARY KEY,
  status VARCHAR(20) NOT NULL,
  last_run TIMESTAMPTZ,
  next_run TIMESTAMPTZ,
  error_count INTEGER DEFAULT 0,
  last_error TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);