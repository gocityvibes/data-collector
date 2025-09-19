
-- db/schema.sql  (Minimal, safe, single-file schema for Phase 2)
-- This file is required by /admin/apply-schema. It includes:
--   * IF NOT EXISTS table stubs (no data loss)
--   * Fixed refresh_and_auto_label() implementation (unambiguous)
-- You can keep ONLY this file if you want; the endpoint reads ./db/schema.sql.

SET search_path TO public;

-- ---------- TABLE STUBS (safe if they already exist) ----------
CREATE TABLE IF NOT EXISTS candles_raw (
  id BIGSERIAL PRIMARY KEY,
  symbol TEXT NOT NULL,
  timeframe TEXT NOT NULL,
  ts_utc TIMESTAMPTZ NOT NULL,
  open NUMERIC NOT NULL,
  high NUMERIC NOT NULL,
  low NUMERIC NOT NULL,
  close NUMERIC NOT NULL,
  volume NUMERIC,
  source TEXT DEFAULT 'yahoo',
  ingest_ts TIMESTAMPTZ DEFAULT NOW(),
  CONSTRAINT uq_candles_raw UNIQUE (symbol, timeframe, ts_utc)
);

CREATE TABLE IF NOT EXISTS reversals (
  id BIGSERIAL PRIMARY KEY,
  symbol TEXT NOT NULL,
  timeframe TEXT NOT NULL,
  "timestamp" TIMESTAMPTZ NOT NULL,
  direction TEXT,
  total_points INT DEFAULT 0,
  move_vs_atr NUMERIC,
  vol_climax_flag BOOLEAN DEFAULT FALSE,
  quality_tier TEXT,
  outcome TEXT,
  similarity NUMERIC,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  CONSTRAINT uq_reversals UNIQUE (symbol, timeframe, "timestamp")
);

CREATE TABLE IF NOT EXISTS reversals_archive (
  id BIGSERIAL PRIMARY KEY,
  source_id BIGINT,
  symbol TEXT NOT NULL,
  timeframe TEXT NOT NULL,
  "timestamp" TIMESTAMPTZ NOT NULL,
  direction TEXT,
  total_points INT DEFAULT 0,
  move_vs_atr NUMERIC,
  vol_climax_flag BOOLEAN DEFAULT FALSE,
  quality_tier TEXT,
  outcome TEXT,
  similarity NUMERIC,
  label TEXT,
  archived_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS reversals_labels (
  id BIGSERIAL PRIMARY KEY,
  symbol TEXT NOT NULL,
  timeframe TEXT NOT NULL,
  "timestamp" TIMESTAMPTZ NOT NULL,
  label TEXT NOT NULL CHECK (label IN ('gold','hard_negative','neutral')),
  labeled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_reversals_labels UNIQUE (symbol, timeframe, "timestamp")
);

-- ---------- AUTO-LABEL POLICY (idempotent) ----------
CREATE OR REPLACE FUNCTION public.auto_label_new_reversals()
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
  -- GOLD
  INSERT INTO public.reversals_labels(symbol, timeframe, "timestamp", label)
  SELECT r.symbol, r.timeframe, r."timestamp", 'gold'
  FROM public.reversals r
  WHERE r."timestamp" >= NOW() - INTERVAL '30 days'
    AND r.total_points >= 80
    AND (r.vol_climax_flag = TRUE OR COALESCE(ABS(r.move_vs_atr),0) >= 1.2)
    AND NOT EXISTS (
      SELECT 1 FROM public.reversals_labels l
      WHERE l.symbol=r.symbol AND l.timeframe=r.timeframe AND l."timestamp"=r."timestamp"
    );

  -- HARD NEGATIVE
  INSERT INTO public.reversals_labels(symbol, timeframe, "timestamp", label)
  SELECT r.symbol, r.timeframe, r."timestamp", 'hard_negative'
  FROM public.reversals r
  WHERE r."timestamp" >= NOW() - INTERVAL '30 days'
    AND (r.total_points <= 50 OR COALESCE(r.quality_tier,'')='skip')
    AND NOT EXISTS (
      SELECT 1 FROM public.reversals_labels l
      WHERE l.symbol=r.symbol AND l.timeframe=r.timeframe AND l."timestamp"=r."timestamp"
    );
END;
$$;

-- ---------- FIXED REFRESH & REPORT (no ambiguous columns) ----------
DROP FUNCTION IF EXISTS public.refresh_and_auto_label();

CREATE OR REPLACE FUNCTION public.refresh_and_auto_label()
RETURNS TABLE (
  gold_labeled INT,
  hard_negative_labeled INT,
  skipped INT
)
LANGUAGE plpgsql
AS $$
BEGIN
  -- Label pass
  PERFORM public.auto_label_new_reversals();

  -- MV refresh (skip if mv missing)
  BEGIN
    PERFORM 1 FROM pg_matviews WHERE schemaname='public' AND matviewname='training_fingerprints';
    IF FOUND THEN
      BEGIN
        EXECUTE 'REFRESH MATERIALIZED VIEW CONCURRENTLY public.training_fingerprints';
      EXCEPTION WHEN feature_not_supported THEN
        EXECUTE 'REFRESH MATERIALIZED VIEW public.training_fingerprints';
      END;
    END IF;
  END;

  -- Recent counts, explicit aliases
  RETURN QUERY
  WITH c AS (
    SELECT
      COUNT(*) FILTER (WHERE l.label='gold')          AS c_gold,
      COUNT(*) FILTER (WHERE l.label='hard_negative') AS c_hard_negative,
      COUNT(*) FILTER (WHERE l.label NOT IN ('gold','hard_negative')) AS c_skipped
    FROM public.reversals_labels l
    WHERE l.labeled_at >= NOW() - INTERVAL '90 minutes'
  )
  SELECT
    COALESCE(c.c_gold,0)          AS gold_labeled,
    COALESCE(c.c_hard_negative,0) AS hard_negative_labeled,
    COALESCE(c.c_skipped,0)       AS skipped
  FROM c;
END;
$$;
