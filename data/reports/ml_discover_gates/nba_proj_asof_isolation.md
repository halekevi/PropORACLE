# February as-of isolation (2026-02-19)

Question: did adding boxes through Oct 24 leak into February `proj_edge`?

**No.** Strip every box with `game_date >= 2026-02-19` (13,759 rows) and
re-score the 1,225 NBA graded legs on that date against the same walk-forward
cell priors. Every projection column is identical.

| col | n_diff | max_abs | compared |
|---|---|---|---|
| proj_mu | 0 | 0 | 554 |
| proj_edge | 0 | 0 | 554 |
| proj_minutes | 0 | 0 | 500 |
| proj_hold | 0 | 0 | 554 |
| base_rate | 0 | 0 | 1225 |
| proj_p_play | 0 | 0 | 554 |
| _gate | 0 flips | — | 1225 |

`k` is a frozen file (`nba_proj_k.json`, `fit_until=2026-01-01`). The Oct 25
box ingest did not refit it. Positional baselines and last-season seeds both
filter `game_date < as_of`. The first two lookahead candidates are closed.

So the Standard UNDER top-3/day 55.8% → 53.0% move is the third candidate:
the morning ingest **added or corrected older boxes** (or early graded rows
that feed cell priors). That is a legitimate as-of change, not mild lookahead.
Ungated 16,477/8,781 held, so Standard UNDER grades themselves did not flip.

History is now frozen: `data/cache/nba_proj_edge/YYYY-MM-DD.jsonl`. The
2026-11-20 scout reads those rankings and will not drift when new boxes land.

```powershell
py -3.14 scripts/check_nba_proj_asof.py --date 2026-02-19
py -3.14 scripts/freeze_nba_proj_edge.py --from 2026-02-19 --to 2026-10-20
```
