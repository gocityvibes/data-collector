-- Fix for refresh_and_auto_label ambiguity

CREATE OR REPLACE FUNCTION refresh_and_auto_label()
RETURNS TABLE(gold_labeled INT, hard_negative_labeled INT, skipped INT) AS $$
DECLARE
  v_gold INT; 
  v_neg INT; 
  v_skip INT;
BEGIN
  -- Refresh the materialized view
  BEGIN
    PERFORM 1 FROM training_fingerprints LIMIT 1;
    EXECUTE 'REFRESH MATERIALIZED VIEW CONCURRENTLY training_fingerprints';
  EXCEPTION WHEN undefined_table THEN
    EXECUTE 'REFRESH MATERIALIZED VIEW training_fingerprints';
  WHEN feature_not_supported THEN
    EXECUTE 'REFRESH MATERIALIZED VIEW training_fingerprints';
  END;

  -- Explicit alias to avoid ambiguity
  SELECT r.gold_labeled, r.hard_negative_labeled, r.skipped
  INTO v_gold, v_neg, v_skip
  FROM auto_label_new_reversals() r;

  RETURN QUERY SELECT v_gold, v_neg, v_skip;
END;
$$ LANGUAGE plpgsql;
