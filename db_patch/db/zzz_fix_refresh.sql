-- Fix ambiguous columns in refresh-and-auto-label pipeline.
-- This overrides the old function with explicit, uniquely-aliased output columns.

CREATE OR REPLACE FUNCTION refresh_and_auto_label()
RETURNS TABLE (
  gold_labeled INT,
  hard_negative_labeled INT,
  skipped INT
)
LANGUAGE plpgsql
AS $$
BEGIN
  -- 1) Run the labeling pass (uses your auto_label_new_reversals() policy)
  PERFORM auto_label_new_reversals();

  -- 2) Refresh MV safely (CONCURRENTLY if supported)
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

  -- 3) Return unambiguous counts (explicit aliases, no name clashes)
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