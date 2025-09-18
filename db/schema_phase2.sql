-- Phase 2 schema: training_fingerprints MV, indexes, neighbor function
-- SAFE: only touches view + function, not raw tables

DROP MATERIALIZED VIEW IF EXISTS training_fingerprints CASCADE;

CREATE MATERIALIZED VIEW training_fingerprints AS
WITH live_patterns AS (
  SELECT *, 'live'::TEXT AS source_table
  FROM reversals
  WHERE "timestamp" > NOW() - INTERVAL '60 days'
    AND outcome <> 'pending'
),
archived_patterns AS (
  SELECT *, 'archive'::TEXT AS source_table
  FROM reversals_archive
  WHERE outcome IN ('gold','hard_negative')
)
SELECT 
  id, symbol, timeframe, "timestamp", session, swing_trend_incoming,
  broke_last_higher_low, broke_last_lower_high, bars_since_peak_trough,
  rsi, rsi_lookback_max, rsi_lookback_min, move_vs_atr,
  price_to_vwap_pct, price_to_ema21_pct, rel_vol_20, vol_climax_flag,
  candle_pattern, ema8_cross_ema21, rsi_cross_50, vwap_retest_result,
  follow_through_atr, follow_through_bars_10, tf5_bias, daily_bias,
  outcome, quality_score, mfe_atr, mae_atr, rr_achieved,
  time_to_target_bars, time_to_stop_bars, structure_invalidated,
  trend_points, exhaustion_points, structure_points, confirmation_points,
  follow_points, base_points, bonus_points, penalty_points,
  total_points, point_tier, run_id, data_quality, created_at, updated_at,
  source_table,
  CASE WHEN source_table='archive' THEN 1.0
       WHEN outcome='gold' THEN 0.9
       WHEN outcome='hard_negative' THEN 0.8
       ELSE 0.6 END::NUMERIC AS pattern_weight,
  CASE WHEN "timestamp" > NOW() - INTERVAL '7 days' THEN 1.0
       WHEN "timestamp" > NOW() - INTERVAL '30 days' THEN 0.8
       WHEN "timestamp" > NOW() - INTERVAL '90 days' THEN 0.6
       ELSE 0.4 END::NUMERIC AS recency_score,
  CASE WHEN total_points >= 80 AND vol_climax_flag IS TRUE AND ABS(move_vs_atr) >= 1.2 THEN 'high'
       WHEN total_points >= 70 AND (vol_climax_flag IS TRUE OR ABS(move_vs_atr) >= 1.0) THEN 'medium'
       WHEN total_points >= 60 THEN 'medium'
       ELSE 'low' END AS conviction_level,
  CASE WHEN total_points >= 80 THEN 'elite'
       WHEN total_points >= 70 THEN 'high'
       WHEN total_points >= 60 THEN 'standard'
       WHEN total_points >= 50 THEN 'light'
       ELSE 'skip' END AS quality_tier,
  EXTRACT(HOUR FROM "timestamp")::INT AS hour_of_day,
  EXTRACT(DOW FROM "timestamp")::INT AS day_of_week,
  ROUND((COALESCE(trend_points,0)::DECIMAL / 10) * 100)::INT AS trend_pct,
  ROUND((COALESCE(exhaustion_points,0)::DECIMAL / 15) * 100)::INT AS exhaustion_pct,
  ROUND((COALESCE(structure_points,0)::DECIMAL / 8) * 100)::INT AS structure_pct,
  ROUND((COALESCE(confirmation_points,0)::DECIMAL / 10) * 100)::INT AS confirmation_pct,
  ROUND((COALESCE(follow_points,0)::DECIMAL / 7) * 100)::INT AS follow_pct,
  MD5(
    COALESCE(symbol,'') ||
    COALESCE(timeframe,'') ||
    COALESCE(swing_trend_incoming,'') ||
    COALESCE(ROUND(rsi::NUMERIC,0)::TEXT,'') ||
    COALESCE(ROUND(move_vs_atr::NUMERIC,1)::TEXT,'') ||
    COALESCE(point_tier,'')
  ) AS similarity_hash
FROM (
  SELECT * FROM live_patterns
  UNION ALL
  SELECT * FROM archived_patterns
) combined;

-- Indexes
CREATE UNIQUE INDEX IF NOT EXISTS idx_training_fingerprints_source_id 
  ON training_fingerprints (source_table, id);
CREATE INDEX IF NOT EXISTS idx_training_fingerprints_symbol_tf_ts 
  ON training_fingerprints (symbol, timeframe, "timestamp" DESC);
CREATE INDEX IF NOT EXISTS idx_training_fingerprints_symbol_tf_trend_outcome 
  ON training_fingerprints (symbol, timeframe, swing_trend_incoming, outcome);
CREATE INDEX IF NOT EXISTS idx_training_fingerprints_total_points 
  ON training_fingerprints (total_points);
CREATE INDEX IF NOT EXISTS idx_training_fingerprints_similarity_prefilter 
  ON training_fingerprints (symbol, timeframe, rsi, move_vs_atr, price_to_vwap_pct, total_points)
  WHERE outcome IN ('gold','hard_negative','neutral');
CREATE INDEX IF NOT EXISTS idx_training_fingerprints_weight_recency
  ON training_fingerprints ( (pattern_weight * recency_score) );

-- Neighbor function
DROP FUNCTION IF EXISTS get_neighbor_patterns_with_points(VARCHAR, VARCHAR, VARCHAR, NUMERIC, NUMERIC, NUMERIC, INTEGER, INTEGER) CASCADE;
CREATE OR REPLACE FUNCTION get_neighbor_patterns_with_points(
  p_symbol VARCHAR,
  p_timeframe VARCHAR,
  p_trend VARCHAR,
  p_rsi NUMERIC,
  p_move_atr NUMERIC,
  p_vwap_pct NUMERIC,
  p_min_points INTEGER DEFAULT 50,
  p_limit INTEGER DEFAULT 20
)
RETURNS TABLE (
  pattern_id BIGINT,
  source_table TEXT,
  outcome TEXT,
  quality_score INT,
  total_points INT,
  point_tier TEXT,
  similarity_score NUMERIC(6,4),
  rsi NUMERIC,
  move_vs_atr NUMERIC,
  price_to_vwap_pct NUMERIC,
  pattern_weight NUMERIC,
  conviction_level TEXT,
  trend_points INT,
  exhaustion_points INT,
  structure_points INT,
  confirmation_points INT,
  follow_points INT
)
LANGUAGE SQL STABLE
AS $$
  SELECT 
    tf.id AS pattern_id,
    tf.source_table,
    tf.outcome,
    tf.quality_score,
    tf.total_points,
    tf.point_tier,
    (1.0 / (1.0 + SQRT(
      POWER(tf.rsi - p_rsi, 2) * 0.01 +
      POWER(tf.move_vs_atr - p_move_atr, 2) * 1.0 +
      POWER(COALESCE(tf.price_to_vwap_pct,0) - p_vwap_pct, 2) * 0.1 +
      POWER((tf.total_points - p_min_points)::NUMERIC / 100, 2) * 0.5
    )))::NUMERIC(6,4) AS similarity_score,
    tf.rsi,
    tf.move_vs_atr,
    tf.price_to_vwap_pct,
    tf.pattern_weight,
    tf.conviction_level,
    tf.trend_points,
    tf.exhaustion_points,
    tf.structure_points,
    tf.confirmation_points,
    tf.follow_points
  FROM training_fingerprints tf
  WHERE tf.symbol = p_symbol
    AND tf.timeframe = p_timeframe
    AND COALESCE(tf.swing_trend_incoming,'?') = COALESCE(p_trend,'?')
    AND tf.total_points >= p_min_points
    AND tf.outcome <> 'pending'
  ORDER BY
    (tf.pattern_weight * tf.recency_score) DESC,
    similarity_score DESC,
    tf.total_points DESC,
    tf.quality_score DESC NULLS LAST
  LIMIT p_limit
$$;
