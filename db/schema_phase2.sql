-- =========================
-- Phase 2 (Pro) – Full Schema
-- =========================
-- Safe to re-run. Creates base tables, labels, feature views, MVs, and helper functions.

-- 1) Raw candles
CREATE TABLE IF NOT EXISTS candles_raw (
  id BIGSERIAL PRIMARY KEY,
  symbol TEXT NOT NULL,
  timeframe TEXT NOT NULL,
  ts_utc TIMESTAMPTZ NOT NULL,
  open NUMERIC,
  high NUMERIC,
  low NUMERIC,
  close NUMERIC,
  volume NUMERIC,
  source TEXT,
  ingest_ts TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS candles_raw_sym_tf_ts_idx ON candles_raw(symbol, timeframe, ts_utc);
CREATE INDEX IF NOT EXISTS candles_raw_ts_idx ON candles_raw(ts_utc);

-- 2) Simple engineered features per bar (no extensions)
--    We compute body/wicks/range and simple returns using window functions.
CREATE OR REPLACE VIEW candles_feat AS
SELECT
  symbol, timeframe, ts_utc, open, high, low, close, volume, source, ingest_ts,
  (high - low)                         AS range,
  ABS(close - open)                    AS body,
  CASE WHEN close>=open THEN high - close ELSE high - open END AS wick_top,
  CASE WHEN close>=open THEN open - low ELSE close - low END  AS wick_bot,
  NULLIF(high - low,0)                  AS _rng,  -- helper to avoid div by zero
  (close - LAG(close) OVER (PARTITION BY symbol,timeframe ORDER BY ts_utc)) AS ret_1,
  (close - LAG(close,3) OVER (PARTITION BY symbol,timeframe ORDER BY ts_utc)) AS ret_3,
  (close - LAG(close,5) OVER (PARTITION BY symbol,timeframe ORDER BY ts_utc)) AS ret_5,
  AVG(close)  OVER (PARTITION BY symbol,timeframe ORDER BY ts_utc ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS sma_10,
  AVG(close)  OVER (PARTITION BY symbol,timeframe ORDER BY ts_utc ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS sma_20,
  STDDEV_POP(close) OVER (PARTITION BY symbol,timeframe ORDER BY ts_utc ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS vol_20,
  CASE WHEN (high - low)<>0 THEN ABS(close - open)/(high - low) END AS body_pct_of_range,
  CASE WHEN (high - low)<>0 THEN (CASE WHEN close>=open THEN high - close ELSE high - open END)/(high - low) END AS wick_top_pct,
  CASE WHEN (high - low)<>0 THEN (CASE WHEN close>=open THEN open - low ELSE close - low END)/(high - low) END AS wick_bot_pct
FROM candles_raw;

-- 3) Reversal pivot detection (k=2) and unified type
CREATE OR REPLACE VIEW reversals_pivots AS
WITH base AS (
  SELECT
    symbol, timeframe, ts_utc, open, high, low, close, volume, source, ingest_ts,
    LAG(high,1) OVER (PARTITION BY symbol,timeframe ORDER BY ts_utc) AS ph1,
    LAG(high,2) OVER (PARTITION BY symbol,timeframe ORDER BY ts_utc) AS ph2,
    LEAD(high,1) OVER (PARTITION BY symbol,timeframe ORDER BY ts_utc) AS nh1,
    LEAD(high,2) OVER (PARTITION BY symbol,timeframe ORDER BY ts_utc) AS nh2,
    LAG(low,1)  OVER (PARTITION BY symbol,timeframe ORDER BY ts_utc) AS pl1,
    LAG(low,2)  OVER (PARTITION BY symbol,timeframe ORDER BY ts_utc) AS pl2,
    LEAD(low,1) OVER (PARTITION BY symbol,timeframe ORDER BY ts_utc) AS nl1,
    LEAD(low,2) OVER (PARTITION BY symbol,timeframe ORDER BY ts_utc) AS nl2
  FROM candles_raw
)
SELECT
  symbol, timeframe, ts_utc, open, high, low, close, volume, source, ingest_ts,
  CASE WHEN high IS NOT NULL AND ph1 IS NOT NULL AND ph2 IS NOT NULL AND nh1 IS NOT NULL AND nh2 IS NOT NULL
        AND high > ph1 AND high > ph2 AND high > nh1 AND high > nh2
       THEN 'pivot_high' END AS reversal_type_high,
  CASE WHEN low IS NOT NULL AND pl1 IS NOT NULL AND pl2 IS NOT NULL AND nl1 IS NOT NULL AND nl2 IS NOT NULL
        AND low < pl1 AND low < pl2 AND low < nl1 AND low < nl2
       THEN 'pivot_low' END AS reversal_type_low
FROM base
WHERE
  (high IS NOT NULL AND ph1 IS NOT NULL AND ph2 IS NOT NULL AND nh1 IS NOT NULL AND nh2 IS NOT NULL AND high > GREATEST(ph1,ph2,nh1,nh2))
  OR
  (low  IS NOT NULL AND pl1 IS NOT NULL AND pl2 IS NOT NULL AND nl1 IS NOT NULL AND nl2 IS NOT NULL AND low  < LEAST(pl1,pl2,nl1,nl2));

CREATE OR REPLACE VIEW reversals_pivots_unified AS
SELECT
  r.symbol, r.timeframe, r.ts_utc, r.open, r.high, r.low, r.close, r.volume, r.source, r.ingest_ts,
  COALESCE(r.reversal_type_high, r.reversal_type_low) AS reversal_type
FROM reversals_pivots r;

-- 4) Labels table + helper
CREATE TABLE IF NOT EXISTS reversals_labels (
  symbol TEXT NOT NULL,
  timeframe TEXT NOT NULL,
  ts_utc TIMESTAMPTZ NOT NULL,
  label TEXT NOT NULL CHECK (label IN ('gold','negative')),
  notes TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (symbol, timeframe, ts_utc)
);
CREATE INDEX IF NOT EXISTS reversals_labels_label_idx ON reversals_labels(label);

CREATE OR REPLACE FUNCTION upsert_reversal_label(p_symbol TEXT, p_timeframe TEXT, p_ts TIMESTAMPTZ, p_label TEXT, p_notes TEXT)
RETURNS VOID AS $$
BEGIN
  INSERT INTO reversals_labels(symbol, timeframe, ts_utc, label, notes)
  VALUES(p_symbol, p_timeframe, p_ts, p_label, p_notes)
  ON CONFLICT (symbol, timeframe, ts_utc)
  DO UPDATE SET label = EXCLUDED.label,
                notes = COALESCE(EXCLUDED.notes, reversals_labels.notes),
                updated_at = NOW();
END;
$$ LANGUAGE plpgsql;

-- 5) Feature snapshot for reversals
CREATE OR REPLACE VIEW reversals_featured AS
SELECT
  u.symbol, u.timeframe, u.ts_utc, u.reversal_type,
  f.open, f.high, f.low, f.close, f.volume,
  f.range, f.body, f.wick_top, f.wick_bot,
  f.body_pct_of_range, f.wick_top_pct, f.wick_bot_pct,
  f.ret_1, f.ret_3, f.ret_5, f.sma_10, f.sma_20, f.vol_20
FROM reversals_pivots_unified u
JOIN candles_feat f USING (symbol, timeframe, ts_utc);

-- 6) Materialized view for training fingerprints (rich features + label)
CREATE MATERIALIZED VIEW IF NOT EXISTS training_fingerprints AS
SELECT
  rf.symbol, rf.timeframe, rf.ts_utc, rf.reversal_type,
  rf.open, rf.high, rf.low, rf.close, rf.volume,
  rf.range, rf.body, rf.wick_top, rf.wick_bot,
  rf.body_pct_of_range, rf.wick_top_pct, rf.wick_bot_pct,
  rf.ret_1, rf.ret_3, rf.ret_5, rf.sma_10, rf.sma_20, rf.vol_20,
  rl.label,
  jsonb_build_object(
    'range', rf.range, 'body', rf.body,
    'wt', rf.wick_top, 'wb', rf.wick_bot,
    'bpct', rf.body_pct_of_range,
    'wtpct', rf.wick_top_pct,
    'wbpct', rf.wick_bot_pct,
    'r1', rf.ret_1, 'r3', rf.ret_3, 'r5', rf.ret_5,
    's10', rf.sma_10, 's20', rf.sma_20, 'v20', rf.vol_20
  ) AS features
FROM reversals_featured rf
LEFT JOIN reversals_labels rl
  ON (rf.symbol, rf.timeframe, rf.ts_utc) = (rl.symbol, rl.timeframe, rl.ts_utc);

CREATE UNIQUE INDEX IF NOT EXISTS training_fingerprints_pk ON training_fingerprints(symbol, timeframe, ts_utc);
CREATE INDEX IF NOT EXISTS training_fingerprints_label_idx ON training_fingerprints(label);

-- 7) Neighbor search (euclidean distance over selected normalized features)
--    Returns the K most similar **prior** reversals for a given (symbol, timeframe, ts_utc).
CREATE OR REPLACE FUNCTION get_neighbors(
  p_symbol TEXT, p_timeframe TEXT, p_ts TIMESTAMPTZ, p_k INT DEFAULT 50
)
RETURNS TABLE(
  n_symbol TEXT, n_timeframe TEXT, n_ts TIMESTAMPTZ, n_label TEXT,
  dist NUMERIC, reversal_type TEXT,
  open NUMERIC, high NUMERIC, low NUMERIC, close NUMERIC,
  body_pct_of_range NUMERIC, wick_top_pct NUMERIC, wick_bot_pct NUMERIC,
  ret_1 NUMERIC, ret_3 NUMERIC, ret_5 NUMERIC, vol_20 NUMERIC, sma_10 NUMERIC, sma_20 NUMERIC
) AS $$
BEGIN
  RETURN QUERY
  WITH q AS (
    SELECT * FROM training_fingerprints
    WHERE symbol = p_symbol AND timeframe = p_timeframe AND ts_utc = p_ts
  ),
  prior AS (
    SELECT * FROM training_fingerprints
    WHERE symbol = p_symbol AND timeframe = p_timeframe AND ts_utc < p_ts
  )
  SELECT
    p.symbol, p.timeframe, p.ts_utc, p.label,
    /* distance over a few scale-stable features */
    (
      POWER( (COALESCE(p.body_pct_of_range,0) - COALESCE(q.body_pct_of_range,0)), 2 ) +
      POWER( (COALESCE(p.wick_top_pct,0)      - COALESCE(q.wick_top_pct,0)), 2 ) +
      POWER( (COALESCE(p.wick_bot_pct,0)      - COALESCE(q.wick_bot_pct,0)), 2 ) +
      POWER( (COALESCE(p.ret_1,0)             - COALESCE(q.ret_1,0)), 2 ) +
      POWER( (COALESCE(p.ret_3,0)             - COALESCE(q.ret_3,0)), 2 ) +
      POWER( (COALESCE(p.ret_5,0)             - COALESCE(q.ret_5,0)), 2 ) +
      POWER( (COALESCE(p.vol_20,0)            - COALESCE(q.vol_20,0)), 2 )
    ) AS dist,
    p.reversal_type,
    p.open, p.high, p.low, p.close,
    p.body_pct_of_range, p.wick_top_pct, p.wick_bot_pct,
    p.ret_1, p.ret_3, p.ret_5, p.vol_20, p.sma_10, p.sma_20
  FROM prior p, q
  ORDER BY dist ASC NULLS LAST
  LIMIT p_k;
END;
$$ LANGUAGE plpgsql;

-- 8) Convenience labeled views
CREATE OR REPLACE VIEW reversals_labeled AS
SELECT t.* FROM training_fingerprints t WHERE t.label IS NOT NULL;
CREATE OR REPLACE VIEW reversals_gold AS
SELECT * FROM training_fingerprints WHERE label = 'gold';
CREATE OR REPLACE VIEW reversals_negative AS
SELECT * FROM training_fingerprints WHERE label = 'negative';

-- 9) Initial refresh (idempotent on first run)
REFRESH MATERIALIZED VIEW CONCURRENTLY training_fingerprints;