# NFL / CFB stat-attach invalid for analysis (2026-10-25)

Sunday attach audit: ~1,630 `UNSUPPORTED_PROP` + 466 `MAPPING_FAILURE` on
the NFL board (~40% not measurable). Those mapping errors are the same ones
that filled step5 history and likely grading actuals all season (Sept 13
`NO_ACTUAL` anomalies sit in this set).

`analysis_legs()` and `ml.discover_gates` drop these prop types unless
`include_invalid_attach=True` is passed. Same first-class shape as
`face_status` (`data/ledger/stat_attach_status/stat_attach_status.parquet`).

| Family | Examples | Why invalid |
|---|---|---|
| YPC | `ypc`, `yards_per_carry` | L5 used rushing yards (82 vs a 3.7 line) |
| Period / first-N | `yards_on_first_reception`, `rush_yards_in_first_5_attempts`, `quarters_with_25_pass_yards` | No own box stat; sibling yards |
| Longest reception | `longest_reception` | L5 used catch counts (avg 2–6 vs a yards line) |
| Completion | `completion_percentage`, `pass_completions`, `longest_completion` | Missing / all-zero attach |

Sacks are **not** in this table. They stay on the 2026-10-24 pre-registration
as a dated amendment: excluded from the shadow until starter sack history is
non-zero and `stat_status` is OK. Re-admit then; do not leave them out after
attach is fixed.

Code: `utils/stat_attach_invalid.py`.
