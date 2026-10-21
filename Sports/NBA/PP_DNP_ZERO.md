# PP chart DNP-as-zero holds

PrizePicks player charts can show a **0** for a game the athlete did not play
(no boxscore row, or 0 with no minutes). That makes Unders look cleaner than
the box-score last-five. Example: Jaren Jackson Rebounds 6.0 card `0,4,5,4,6`
with Apr 10 vs MEM = 0, while ESPN/boxscore last-five played is `4,5,4,6,9`.

## Rule

When chart values exist (scrape columns or manual fixture):

| Condition | Action |
|---|---|
| Chart date with **no** boxscore row | `pp_shows_dnp_zero` **hold** |
| Chart value `0` and minutes missing/0 | **hold** |
| No chart on the row | skip (do not flag) |

Hold uses `void_reason=pp_shows_dnp_zero` (same class as attach / PP reconcile
mismatches — not flag-only).

## Code

- Detector: `utils/pp_dnp_zero.py`
- NBA step8 calls `apply_pp_dnp_zero_holds` when the ref DB is available
- Fixture until scrape: `Sports/NBA/data/fixtures/pp_chart_manual.json`
- Future scrape should fill `pp_chart_values` (+ optional `pp_chart_dates`)

Until the chart scrape lands, verify Unders on injured/returning players by
eye against boxscore L5 (as with Garland / Isaiah Jackson). Keep the manual
fixture `legs` array **empty** in normal runs; tests cover the Jaren shape.
