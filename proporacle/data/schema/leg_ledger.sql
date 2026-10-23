-- PropORACLE analysis ledger (DuckDB).
-- Physical store: data/ledger/<table>/slate_date=YYYY-MM-DD/sha=<16>.parquet
-- Append-only: a new source hash writes a new part; identical hash is a no-op.
--
-- leg_id_v1 = 'v1:' + sha1(version|slate_date_et|sport|player_fold|prop_canon|line_norm|direction|pick)[:16]
-- Do NOT hash pp_projection_id (often blank). Normalized components are stored as columns.

CREATE VIEW IF NOT EXISTS v_fill_by_date AS
SELECT
  slate_date,
  sport,
  COUNT(*) AS n,
  AVG(CASE WHEN pp_projection_id IS NULL OR CAST(pp_projection_id AS VARCHAR) = '' THEN 0.0 ELSE 1.0 END) AS proj_id_fill,
  AVG(CASE WHEN league IS NULL OR league = '' THEN 0.0 ELSE 1.0 END) AS league_fill
FROM legs
GROUP BY 1, 2;

CREATE VIEW IF NOT EXISTS v_ticket_join_rate AS
SELECT
  tl.slate_date,
  COUNT(*) AS ticket_leg_rows,
  SUM(CASE WHEN l.leg_id IS NOT NULL THEN 1 ELSE 0 END) AS matched,
  COUNT(*) - SUM(CASE WHEN l.leg_id IS NOT NULL THEN 1 ELSE 0 END) AS unmatched,
  AVG(CASE WHEN l.leg_id IS NOT NULL THEN 1.0 ELSE 0.0 END) AS match_rate
FROM ticket_legs tl
LEFT JOIN legs l ON tl.leg_id = l.leg_id AND CAST(tl.slate_date AS VARCHAR) = CAST(l.slate_date AS VARCHAR)
GROUP BY 1
ORDER BY 1;

CREATE VIEW IF NOT EXISTS v_ticket_legs_unmatched AS
SELECT
  tl.slate_date,
  tl.ticket_id,
  tl.sport,
  tl.player,
  tl.prop,
  tl.line,
  tl.direction,
  tl.pick_type,
  tl.player_fold,
  tl.prop_canon,
  tl.line_norm,
  tl.source_filename
FROM ticket_legs tl
LEFT JOIN legs l ON tl.leg_id = l.leg_id AND CAST(tl.slate_date AS VARCHAR) = CAST(l.slate_date AS VARCHAR)
WHERE l.leg_id IS NULL;

-- Split unmatched ticket legs: no_actual (player/prop/line missing from that
-- day's graded slate) vs fingerprint (same player+prop+line, different
-- direction/pick-type labels in leg_id_v1). Sibling Goblin/Standard/Demon
-- lines on the same player are no_actual, not a hash bug.
CREATE VIEW IF NOT EXISTS v_ticket_legs_unmatched_reason AS
SELECT
  u.*,
  CASE
    WHEN EXISTS (
      SELECT 1 FROM legs g
      WHERE CAST(g.slate_date AS VARCHAR) = CAST(u.slate_date AS VARCHAR)
        AND g.sport = u.sport
        AND g.player_fold = u.player_fold
        AND g.prop_canon = u.prop_canon
        AND g.line_norm = u.line_norm
    ) THEN 'fingerprint'
    ELSE 'no_actual'
  END AS unmatched_reason
FROM v_ticket_legs_unmatched u;

-- Ticket dates with no matching graded face (join < 90% is a warn in ledger_checks).
-- Missing ticket_runs for a date that has tickets is checked in Python
-- (dates_with_tickets_missing_runs) because live JSON lives outside this schema.
