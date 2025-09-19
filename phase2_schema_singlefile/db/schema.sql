
-- =====================================================================
-- Phase 2 "Perfect" — Single-File Database Schema
-- File: db/schema.sql
-- Purpose: Replace all existing DB files with this single authoritative schema.
-- Includes:
--   - Tables: candles_raw, reversals, reversals_archive, reversals_labels
--   - Constraints & indexes (dup-prevention + perf)
--   - Materialized View: training_fingerprints  (with UNIQUE(source_table,id))
--   - Functions:
--       * auto_label_new_reversals()
--       * refresh_and_auto_label()  (FIXED: unambiguous result columns)
-- =====================================================================

-- ---------- SAFETY: create extensions if needed (harmless if present) ----------
-- (Intentionally minimal; standard Render Postgres doesn't require extras.)

-- ---------- TABLES ----------

CREATE TABLE IF NOT EXISTS candles_raw (
  id           BIGSERIAL PRIMARY KEY,
  symbol       TEXT        NOT NULL,
  timeframe    TEXT        NOT NULL,
  ts_utc       TIMESTAMPTZ NOT NULL,
  open         NUMERIC     NOT NULL,
  high         NUMERIC     NOT NULL,
  low          NUMERIC     NOT NULL,
  close        NUMERIC     NOT NULL,
  volume       NUMERIC,
  source       TEXT        DEFAULT 'yahoo',
  ingest_ts    TIMESTAMPTZ DEFAULT NOW(),
  CONSTRAINT uq_candles_raw UNIQUE (symbol, timeframe, ts_utc)
);

-- Reversal candidates (live, last ~60–120d depending on your retention process)
CREATE TABLE IF NOT EXISTS reversals (
  id               BIGSERIAL PRIMARY KEY,
  symbol           TEXT        NOT NULL,
  timeframe        TEXT        NOT NULL,
  "timestamp"      TIMESTAMPTZ NOT NULL,          -- detection timestamp (UTC)
  direction        TEXT        CHECK (direction IN ('long','short')),
  total_points     INT         NOT NULL DEFAULT 0, -- scoring total used by labeling
  move_vs_atr      NUMERIC,                        -- |move| vs ATR ratio
  vol_climax_flag  BOOLEAN     NOT NULL DEFAULT FALSE,
  quality_tier     TEXT,                            -- e.g., 'skip','ok','good'
  outcome          TEXT,                            -- optional: 'win','loss','neutral'
  similarity       NUMERIC,                         -- optional: neighbor sim score
  created_at       TIMESTAMPTZ DEFAULT NOW(),
  CONSTRAINT uq_reversals UNIQUE (symbol, timeframe, "timestamp")
);

-- Archive of reversals (older or finalized, optional label snapshot)
CREATE TABLE IF NOT EXISTS reversals_archive (
  id               BIGSERIAL PRIMARY KEY,
  source_id        BIGINT,                          -- optional pointer to reversals.id
  symbol           TEXT        NOT NULL,
  timeframe        TEXT        NOT NULL,
  "timestamp"      TIMESTAMPTZ NOT NULL,
  direction        TEXT        CHECK (direction IN ('long','short')),
  total_points     INT         NOT NULL DEFAULT 0,
  move_vs_atr      NUMERIC,
  vol_climax_flag  BOOLEAN     NOT NULL DEFAULT FALSE,
  quality_tier     TEXT,
  outcome          TEXT,
  similarity       NUMERIC,
  label            TEXT,                            -- optional snapshot of label at archive time
  archived_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Label table (only GOLD / HARD_NEGATIVE writes per policy; neutral/others skipped)
CREATE TABLE IF NOT EXISTS reversals_labels (
  id          BIGSERIAL PRIMARY KEY,
  symbol      TEXT        NOT NULL,
  timeframe   TEXT        NOT NULL,
  "timestamp" TIMESTAMPTZ NOT NULL,
  label       TEXT        NOT NULL CHECK (label IN ('gold','hard_negative','neutral')),
  labeled_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_reversals_labels UNIQUE (symbol, timeframe, "timestamp")
);

-- ---------- INDEXES (Performance & Search) ----------

-- Candles: ensure dup-prevention+fast point lookups
CREATE UNIQUE INDEX IF NOT EXISTS idx_candles_raw_symbol_tf_ts
  ON candles_raw(symbol, timeframe, ts_utc);

-- Reversals: search/perf helpers
CREATE UNIQUE INDEX IF NOT EXISTS idx_reversals_symbol_tf_ts
  ON reversals(symbol, timeframe, "timestamp");

CREATE INDEX IF NOT EXISTS idx_reversals_points
  ON reversals(total_points);

CREATE INDEX IF NOT EXISTS idx_reversals_outcome
  ON reversals(outcome);

CREATE INDEX IF NOT EXISTS idx_reversals_similarity
  ON reversals(similarity);

-- Labels: timestamp & label filters
CREATE UNIQUE INDEX IF NOT EXISTS idx_reversals_labels_symbol_tf_ts
  ON reversals_labels(symbol, timeframe, "timestamp");

CREATE INDEX IF NOT EXISTS idx_reversals_labels_labeled_at
  ON reversals_labels(labeled_at);

CREATE INDEX IF NOT EXISTS idx_reversals_labels_label
  ON reversals_labels(label);

-- Archive: helpful filters
CREATE INDEX IF NOT EXISTS idx_reversals_archive_ts
  ON reversals_archive("timestamp");

CREATE INDEX IF NOT EXISTS idx_reversals_archive_label
  ON reversals_archive(label);

-- ---------- MATERIALIZED VIEW ----------
-- MV consolidates live + archive for training queries.
-- Must provide UNIQUE (source_table, id) to allow CONCURRENT refresh.

DROP MATERIALIZED VIEW IF EXISTS training_fingerprints;

CREATE MATERIALIZED VIEW training_fingerprints AS
SELECT
  'reversals'        ::TEXT AS source_table,
  r.id               ::BIGINT AS id,
  r.symbol,
  r.timeframe,
  r."timestamp",
  r.total_points,
  r.outcome,
  r.similarity,
  r.quality_tier
FROM reversals r
UNION ALL
SELECT
  'reversals_archive'::TEXT AS source_table,
  a.id               ::BIGINT AS id,
  a.symbol,
  a.timeframe,
  a."timestamp",
  a.total_points,
  a.outcome,
  a.similarity,
  a.quality_tier
FROM reversals_archive a;

-- Unique index enabling REFRESH ... CONCURRENTLY
CREATE UNIQUE INDEX IF NOT EXISTS uq_training_fingerprints_source_id
  ON training_fingerprints(source_table, id);

-- Helpful filters
CREATE INDEX IF NOT EXISTS idx_training_fingerprints_points
  ON training_fingerprints(total_points);

CREATE INDEX IF NOT EXISTS idx_training_fingerprints_outcome
  ON training_fingerprints(outcome);

CREATE INDEX IF NOT EXISTS idx_training_fingerprints_similarity
  ON training_fingerprints(similarity);

-- ---------- FUNCTIONS ----------

-- Auto-label policy:
-- gold: total_points ≥ 80 AND (vol_climax_flag OR |move_vs_atr| ≥ 1.2) AND recent (≤30d)
-- hard_negative: total_points ≤ 50 OR quality_tier='skip'
-- otherwise: skipped (no write)
CREATE OR REPLACE FUNCTION auto_label_new_reversals()
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
  -- GOLD
  INSERT INTO reversals_labels(symbol, timeframe, "timestamp", label)
  SELECT r.symbol, r.timeframe, r."timestamp", 'gold'::TEXT
  FROM reversals r
  WHERE r."timestamp" >= NOW() - INTERVAL '30 days'
    AND r.total_points >= 80
    AND (r.vol_climax_flag = TRUE OR COALESCE(ABS(r.move_vs_atr), 0) >= 1.2)
    AND NOT EXISTS (
      SELECT 1 FROM reversals_labels l
      WHERE l.symbol = r.symbol
        AND l.timeframe = r.timeframe
        AND l."timestamp" = r."timestamp"
    );

  -- HARD NEGATIVE
  INSERT INTO reversals_labels(symbol, timeframe, "timestamp", label)
  SELECT r.symbol, r.timeframe, r."timestamp", 'hard_negative'::TEXT
  FROM reversals r
  WHERE r."timestamp" >= NOW() - INTERVAL '30 days'
    AND (
      r.total_points <= 50
      OR COALESCE(r.quality_tier,'') = 'skip'
    )
    AND NOT EXISTS (
      SELECT 1 FROM reversals_labels l
      WHERE l.symbol = r.symbol
        AND l.timeframe = r.timeframe
        AND l."timestamp" = r."timestamp"
    );

  -- Neutral / other cases are intentionally skipped (no write)
END;
$$;

-- Fixed, unambiguous result columns + safe MV refresh
CREATE OR REPLACE FUNCTION refresh_and_auto_label()
RETURNS TABLE (
  gold_labeled INT,
  hard_negative_labeled INT,
  skipped INT
)
LANGUAGE plpgsql
AS $$
BEGIN
  -- 1) Run the labeling pass
  PERFORM auto_label_new_reversals();

  -- 2) Refresh the MV (concurrent when possible)
  BEGIN
    PERFORM 1
    FROM pg_matviews
    WHERE schemaname = 'public' AND matviewname = 'training_fingerprints';

    IF FOUND THEN
      BEGIN
        EXECUTE 'REFRESH MATERIALIZED VIEW CONCURRENTLY training_fingerprints';
      EXCEPTION WHEN feature_not_supported THEN
        EXECUTE 'REFRESH MATERIALIZED VIEW training_fingerprints';
      END;
    END IF;
  END;

  -- 3) Report recent label counts (unambiguous aliases)
  RETURN QUERY
  WITH c AS (
    SELECT
      COUNT(*) FILTER (WHERE label = 'gold')          AS c_gold,
      COUNT(*) FILTER (WHERE label = 'hard_negative') AS c_hard_negative,
      COUNT(*) FILTER (WHERE label NOT IN ('gold','hard_negative')) AS c_skipped
    FROM reversals_labels
    WHERE labeled_at >= NOW() - INTERVAL '90 minutes'
  )
  SELECT
    COALESCE(c.c_gold, 0)          AS gold_labeled,
    COALESCE(c.c_hard_negative, 0) AS hard_negative_labeled,
    COALESCE(c.c_skipped, 0)       AS skipped
  FROM c;
END;
$$;

-- =====================================================================
-- End of schema.sql
-- =====================================================================
