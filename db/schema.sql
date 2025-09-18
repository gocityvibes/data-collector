-- === Phase 2 ES-only schema (production, no placeholders) ===

-- Extensions (safe if preinstalled)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- === Core tables ===

-- Raw candles (ES-only by usage; not enforced here)
CREATE TABLE IF NOT EXISTS candles_raw (
  id BIGSERIAL PRIMARY KEY,
  symbol VARCHAR(20) NOT NULL,
  timeframe VARCHAR(10) NOT NULL, -- '1m','5m','15m'
  ts_utc TIMESTAMPTZ NOT NULL,
  open NUMERIC(18,6) NOT NULL,
  high NUMERIC(18,6) NOT NULL,
  low  NUMERIC(18,6) NOT NULL,
  close NUMERIC(18,6) NOT NULL,
  volume BIGINT DEFAULT 0,
  source VARCHAR(20) NOT NULL DEFAULT 'yahoo',
  ingest_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Prevent duplicates per bar
CREATE UNIQUE INDEX IF NOT EXISTS candles_raw_unique
  ON candles_raw(symbol, timeframe, ts_utc);

CREATE INDEX IF NOT EXISTS candles_raw_idx_ts
  ON candles_raw(ts_utc);

-- Reversals (detected patterns). This schema matches the MV columns used.
CREATE TABLE IF NOT EXISTS reversals (
  id BIGSERIAL PRIMARY KEY,
  symbol VARCHAR(20) NOT NULL,
  timeframe VARCHAR(10) NOT NULL,
  "timestamp" TIMESTAMPTZ NOT NULL,
  session VARCHAR(20),
  swing_trend_incoming VARCHAR(20),
  broke_last_higher_low BOOLEAN,
  broke_last_lower_high BOOLEAN,
  bars_since_peak_trough INT,
  rsi NUMERIC(10,4),
  rsi_lookback_max NUMERIC(10,4),
  rsi_lookback_min NUMERIC(10,4),
  move_vs_atr NUMERIC(10,4),
  price_to_vwap_pct NUMERIC(10,4),
  price_to_ema21_pct NUMERIC(10,4),
  rel_vol_20 NUMERIC(10,4),
  vol_climax_flag BOOLEAN,
  candle_pattern VARCHAR(30),
  ema8_cross_ema21 VARCHAR(10),
  rsi_cross_50 VARCHAR(10),
  vwap_retest_result VARCHAR(20),
  follow_through_atr NUMERIC(10,4),
  follow_through_bars_10 INT,
  tf5_bias VARCHAR(20),
  daily_bias VARCHAR(20),
  outcome VARCHAR(20) NOT NULL DEFAULT 'pending', -- 'gold','hard_negative','neutral','pending'
  quality_score INT,
  mfe_atr NUMERIC(10,4),
  mae_atr NUMERIC(10,4),
  rr_achieved NUMERIC(10,4),
  time_to_target_bars INT,
  time_to_stop_bars INT,
  structure_invalidated BOOLEAN,
  trend_points INT,
  exhaustion_points INT,
  structure_points INT,
  confirmation_points INT,
  follow_points INT,
  base_points INT,
  bonus_points INT,
  penalty_points INT,
  total_points INT,
  point_tier VARCHAR(20),
  run_id UUID DEFAULT uuid_generate_v4(),
  data_quality VARCHAR(20),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS reversals_unique_key
  ON reversals(symbol, timeframe, "timestamp");

-- Archive table for gold/hard_negative exemplars
CREATE TABLE IF NOT EXISTS reversals_archive (LIKE reversals INCLUDING ALL);

-- Labels table (write-once per (symbol,timeframe,timestamp) typically)
CREATE TABLE IF NOT EXISTS reversals_labels (
  id BIGSERIAL PRIMARY KEY,
  symbol VARCHAR(20) NOT NULL,
  timeframe VARCHAR(10) NOT NULL,
  "timestamp" TIMESTAMPTZ NOT NULL,
  label VARCHAR(20) NOT NULL, -- 'gold','hard_negative','neutral'
  confidence NUMERIC(5,4) NOT NULL DEFAULT 0.0,
  labeled_by VARCHAR(20) NOT NULL, -- 'auto' or 'manual'
  notes TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(symbol, timeframe, "timestamp")
);

-- === Utility function: Upsert a label ===
CREATE OR REPLACE FUNCTION upsert_reversal_label(
  p_symbol VARCHAR, p_timeframe VARCHAR, p_ts TIMESTAMPTZ,
  p_label VARCHAR, p_confidence NUMERIC, p_by VARCHAR, p_notes TEXT DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
  INSERT INTO reversals_labels(symbol,timeframe,"timestamp",label,confidence,labeled_by,notes)
  VALUES (p_symbol,p_timeframe,p_ts,p_label,p_confidence,p_by,p_notes)
  ON CONFLICT (symbol,timeframe,"timestamp")
  DO UPDATE SET label=EXCLUDED.label, confidence=EXCLUDED.confidence, labeled_by=EXCLUDED.labeled_by, notes=COALESCE(EXCLUDED.notes, reversals_labels.notes);
END;
$$ LANGUAGE plpgsql;

-- === MATERIALIZED VIEW: training_fingerprints ===
DROP MATERIALIZED VIEW IF EXISTS training_fingerprints CASCADE;

CREATE MATERIALIZED VIEW training_fingerprints AS
WITH live_patterns AS (
  SELECT 
    'live' AS source_table,
    id, symbol, timeframe, "timestamp", session, swing_trend_incoming,
    broke_last_higher_low, broke_last_lower_high, bars_since_peak_trough,
    rsi, rsi_lookback_max, rsi_lookback_min, move_vs_atr, price_to_vwap_pct, price_to_ema21_pct,
    rel_vol_20, vol_climax_flag, candle_pattern, ema8_cross_ema21, rsi_cross_50, vwap_retest_result,
    follow_through_atr, follow_through_bars_10, tf5_bias, daily_bias, outcome, quality_score,
    mfe_atr, mae_atr, rr_achieved, time_to_target_bars, time_to_stop_bars, structure_invalidated,
    trend_points, exhaustion_points, structure_points, confirmation_points, follow_points,
    base_points, bonus_points, penalty_points, total_points, point_tier, run_id, data_quality,
    created_at, updated_at
  FROM reversals
  WHERE "timestamp" > NOW() - INTERVAL '60 days' AND outcome <> 'pending'
),
archived_patterns AS (
  SELECT 
    'archive' AS source_table,
    id, symbol, timeframe, "timestamp", session, swing_trend_incoming,
    broke_last_higher_low, broke_last_lower_high, bars_since_peak_trough,
    rsi, rsi_lookback_max, rsi_lookback_min, move_vs_atr, price_to_vwap_pct, price_to_ema21_pct,
    rel_vol_20, vol_climax_flag, candle_pattern, ema8_cross_ema21, rsi_cross_50, vwap_retest_result,
    follow_through_atr, follow_through_bars_10, tf5_bias, daily_bias, outcome, quality_score,
    mfe_atr, mae_atr, rr_achieved, time_to_target_bars, time_to_stop_bars, structure_invalidated,
    trend_points, exhaustion_points, structure_points, confirmation_points, follow_points,
    base_points, bonus_points, penalty_points, total_points, point_tier, run_id, data_quality,
    created_at, updated_at
  FROM reversals_archive
  WHERE outcome IN ('gold','hard_negative')
),
combined_patterns AS (
  SELECT * FROM live_patterns
  UNION ALL
  SELECT * FROM archived_patterns
)
SELECT
  source_table, id, symbol, timeframe, "timestamp", session, swing_trend_incoming,
  broke_last_higher_low, broke_last_lower_high, bars_since_peak_trough,
  rsi, rsi_lookback_max, rsi_lookback_min, move_vs_atr, price_to_vwap_pct, price_to_ema21_pct,
  rel_vol_20, vol_climax_flag, candle_pattern, ema8_cross_ema21, rsi_cross_50, vwap_retest_result,
  follow_through_atr, follow_through_bars_10, tf5_bias, daily_bias, outcome, quality_score,
  mfe_atr, mae_atr, rr_achieved, time_to_target_bars, time_to_stop_bars, structure_invalidated,
  trend_points, exhaustion_points, structure_points, confirmation_points, follow_points,
  base_points, bonus_points, penalty_points, total_points, point_tier, run_id, data_quality,
  created_at, updated_at,
  CASE 
    WHEN source_table='archive' THEN 1.0
    WHEN outcome='gold' THEN 0.9
    WHEN outcome='hard_negative' THEN 0.8
    ELSE 0.6
  END AS pattern_weight,
  CASE 
    WHEN "timestamp" > NOW() - INTERVAL '7 days' THEN 1.0
    WHEN "timestamp" > NOW() - INTERVAL '30 days' THEN 0.8
    WHEN "timestamp" > NOW() - INTERVAL '90 days' THEN 0.6
    ELSE 0.4
  END AS recency_score,
  CASE 
    WHEN total_points >= 80 AND vol_climax_flag AND ABS(move_vs_atr) >= 1.2 THEN 'high'
    WHEN total_points >= 70 AND (vol_climax_flag OR ABS(move_vs_atr) >= 1.0) THEN 'medium'
    WHEN total_points >= 60 THEN 'medium'
    ELSE 'low'
  END AS conviction_level,
  CASE
    WHEN total_points >= 80 THEN 'elite'
    WHEN total_points >= 70 THEN 'high'
    WHEN total_points >= 60 THEN 'standard'
    WHEN total_points >= 50 THEN 'light'
    ELSE 'skip'
  END AS quality_tier,
  EXTRACT(HOUR FROM "timestamp") AS hour_of_day,
  EXTRACT(DOW FROM "timestamp") AS day_of_week,
  ROUND((trend_points::DECIMAL / 10) * 100) AS trend_pct,
  ROUND((exhaustion_points::DECIMAL / 15) * 100) AS exhaustion_pct,
  ROUND((structure_points::DECIMAL / 8) * 100) AS structure_pct,
  ROUND((confirmation_points::DECIMAL / 10) * 100) AS confirmation_pct,
  ROUND((follow_points::DECIMAL / 7) * 100) AS follow_pct,
  MD5(symbol || timeframe || swing_trend_incoming ||
      ROUND(COALESCE(rsi,0)::numeric,0)::text ||
      ROUND(COALESCE(move_vs_atr,0)::numeric,1)::text ||
      COALESCE(point_tier,'')) AS similarity_hash
FROM combined_patterns;

-- Indexes for MV (needed for concurrent refresh + query perf)
CREATE UNIQUE INDEX IF NOT EXISTS idx_training_fingerprints_id
  ON training_fingerprints(source_table, id);

CREATE INDEX IF NOT EXISTS idx_training_fingerprints_gpt_search
  ON training_fingerprints(symbol, timeframe, swing_trend_incoming, outcome, total_points, quality_tier);

CREATE INDEX IF NOT EXISTS idx_training_fingerprints_points
  ON training_fingerprints(total_points, point_tier, quality_tier);

CREATE INDEX IF NOT EXISTS idx_training_fingerprints_similarity
  ON training_fingerprints(symbol, timeframe, rsi, move_vs_atr, price_to_vwap_pct, total_points)
  WHERE outcome IN ('gold','hard_negative','neutral');

CREATE INDEX IF NOT EXISTS idx_training_fingerprints_conviction
  ON training_fingerprints(conviction_level, total_points, pattern_weight);

-- Neighbor search using MV
CREATE OR REPLACE FUNCTION get_neighbor_patterns_with_points(
  p_symbol VARCHAR, p_timeframe VARCHAR, p_trend VARCHAR,
  p_rsi NUMERIC, p_move_atr NUMERIC, p_vwap_pct NUMERIC,
  p_min_points INT DEFAULT 50, p_limit INT DEFAULT 20
) RETURNS TABLE (
  pattern_id BIGINT, source_table TEXT, outcome VARCHAR, quality_score INT,
  total_points INT, point_tier VARCHAR, similarity_score NUMERIC,
  rsi NUMERIC, move_vs_atr NUMERIC, price_to_vwap_pct NUMERIC,
  pattern_weight NUMERIC, conviction_level TEXT,
  trend_points INT, exhaustion_points INT, structure_points INT,
  confirmation_points INT, follow_points INT
) AS $$
BEGIN
  RETURN QUERY
  SELECT
    tf.id, tf.source_table, tf.outcome, tf.quality_score, tf.total_points, tf.point_tier,
    (1.0 / (1.0 +
      SQRT(
        POWER(COALESCE(tf.rsi,0) - p_rsi, 2) * 0.01 +
        POWER(COALESCE(tf.move_vs_atr,0) - p_move_atr, 2) * 1.0 +
        POWER(COALESCE(tf.price_to_vwap_pct,0) - p_vwap_pct, 2) * 0.1 +
        POWER((tf.total_points - p_min_points)::NUMERIC / 100, 2) * 0.5
      )
    ))::NUMERIC(6,4) AS similarity_score,
    tf.rsi, tf.move_vs_atr, tf.price_to_vwap_pct, tf.pattern_weight, tf.conviction_level,
    tf.trend_points, tf.exhaustion_points, tf.structure_points, tf.confirmation_points, tf.follow_points
  FROM training_fingerprints tf
  WHERE tf.symbol = p_symbol
    AND tf.timeframe = p_timeframe
    AND tf.swing_trend_incoming = p_trend
    AND tf.total_points >= p_min_points
    AND tf.outcome <> 'pending'
  ORDER BY tf.pattern_weight DESC, similarity_score DESC, tf.total_points DESC,
           tf.quality_score DESC NULLS LAST
  LIMIT p_limit;
END;
$$ LANGUAGE plpgsql;

-- === AUTO LABELING ===

-- Auto-label rules:
-- 1) gold if points >=80 and (vol climax OR |move_vs_atr| >=1.2) and recency >=0.8
-- 2) hard_negative if points <=50 OR quality_tier='skip'
-- Otherwise skip.

CREATE OR REPLACE FUNCTION auto_label_new_reversals()
RETURNS TABLE(gold_labeled INT, hard_negative_labeled INT, skipped INT) AS $$
DECLARE
  v_gold INT := 0;
  v_neg INT := 0;
  v_skip INT := 0;
BEGIN
  -- Ensure MV is built once before using (if empty, just skip)
  PERFORM 1 FROM training_fingerprints LIMIT 1;

  -- Candidate live rows not yet labeled (by timestamp key)
  WITH candidates AS (
    SELECT tf.*
    FROM training_fingerprints tf
    LEFT JOIN reversals_labels rl
      ON rl.symbol=tf.symbol AND rl.timeframe=tf.timeframe AND rl."timestamp"=tf."timestamp"
    WHERE tf.source_table='live' AND rl.id IS NULL
  ), decided AS (
    SELECT
      c.*,
      CASE
        WHEN c.total_points >= 80 AND (c.vol_climax_flag IS TRUE OR ABS(c.move_vs_atr) >= 1.2) AND
             (CASE WHEN c."timestamp" > NOW() - INTERVAL '30 days' THEN 0.8
                   WHEN c."timestamp" > NOW() - INTERVAL '90 days' THEN 0.6 ELSE 0.4 END) >= 0.8
          THEN 'gold'
        WHEN c.total_points <= 50 OR c.quality_tier='skip'
          THEN 'hard_negative'
        ELSE 'skip'
      END AS auto_label,
      CASE
        WHEN c.total_points >= 80 AND (c.vol_climax_flag IS TRUE OR ABS(c.move_vs_atr) >= 1.2) THEN 0.95
        WHEN c.total_points <= 50 OR c.quality_tier='skip' THEN 0.90
        ELSE 0.0
      END AS conf
    FROM candidates c
  )
  SELECT 0,0,0 INTO v_gold, v_neg, v_skip;

  -- Insert labels
  INSERT INTO reversals_labels(symbol,timeframe,"timestamp",label,confidence,labeled_by,notes)
  SELECT d.symbol, d.timeframe, d."timestamp", d.auto_label, d.conf, 'auto', 'phase2 rules'
  FROM decided d
  WHERE d.auto_label IN ('gold','hard_negative')
  ON CONFLICT (symbol,timeframe,"timestamp") DO NOTHING;

  GET DIAGNOSTICS v_gold = ROW_COUNT; -- counts both gold and hard_negative inserts

  -- Count by type for return
  SELECT 
    COALESCE(SUM(CASE WHEN auto_label='gold' THEN 1 ELSE 0 END),0),
    COALESCE(SUM(CASE WHEN auto_label='hard_negative' THEN 1 ELSE 0 END),0),
    COALESCE(SUM(CASE WHEN auto_label='skip' THEN 1 ELSE 0 END),0)
  INTO v_gold, v_neg, v_skip
  FROM decided;

  RETURN QUERY SELECT v_gold, v_neg, v_skip;
END;
$$ LANGUAGE plpgsql;

-- Wrapper to refresh MV and then auto-label
CREATE OR REPLACE FUNCTION refresh_and_auto_label()
RETURNS TABLE(gold_labeled INT, hard_negative_labeled INT, skipped INT) AS $$
DECLARE
  v_gold INT; v_neg INT; v_skip INT;
BEGIN
  -- Try concurrent refresh; if MV empty initially, normal refresh is fine.
  BEGIN
    PERFORM 1 FROM training_fingerprints LIMIT 1;
    EXECUTE 'REFRESH MATERIALIZED VIEW CONCURRENTLY training_fingerprints';
  EXCEPTION WHEN undefined_table THEN
    EXECUTE 'REFRESH MATERIALIZED VIEW training_fingerprints';
  WHEN feature_not_supported THEN
    EXECUTE 'REFRESH MATERIALIZED VIEW training_fingerprints';
  END;

  SELECT gold_labeled, hard_negative_labeled, skipped
  INTO v_gold, v_neg, v_skip
  FROM auto_label_new_reversals();

  RETURN QUERY SELECT v_gold, v_neg, v_skip;
END;
$$ LANGUAGE plpgsql;
