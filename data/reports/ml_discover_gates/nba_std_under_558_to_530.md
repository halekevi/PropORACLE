# Standard UNDER 55.8% → 53.0% (n=249)

Same window (2026-02-19 → 2026-10-20, 89 slates). Same sleeve size.
Oct 24 report: 139/249 = 55.8%. Oct 25 re-run: 132/249 = 53.0%. Seven hits left the daily top-3.

## Not contamination

Ungated Standard UNDER is identical: **16,477 / 8,781 / 53.29%**. Face verification, attach status, and DNP-zero cleanup did not change this book. If they had, ungated n or hits would have moved.

GATE hits also held (19). GATE n went 41 → 40 — one leg fell off L5=5+L10≥8+D after the box refresh, not a grade flip.

## Not role_fit / wGap / slate_minutes

Those were never ranking inputs on this scout. Rank is still `proj_edge` from `attach_nba_projections`. `slate_minutes` joined 0 historical rows (only tonight’s pre-tip snapshot exists).

## What moved

`scout top-3/day` is `nlargest(3, proj_edge)` on unheld legs. When the box DB refreshed (this morning, `proporacle_ref.db` 2026-10-25 09:54, boxes through 2026-10-24) the walk-forward `proj_mu` / `proj_edge` order changed and a different 249 names filled the same slots. Seven of the replacements missed.

That is a ranking snapshot, not a new book. Last week’s **ungated** numbers can be trusted. Last week’s **top-3/day 55.8%** should not be — it was the same unstable sleeve, one box ingest later.

## Freeze

Do not re-run `ml.nba_scout_quotes` before **2026-11-20**. Let pre-tip snapshots accumulate, then one walk-forward with `slate_minutes` joined only where `pre_tip` is true.
