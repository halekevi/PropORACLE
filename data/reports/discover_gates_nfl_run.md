# NFL discover_gates run

Unique-game graded NFL (period/split excluded). Window 2026-09-09 → 2026-10-05.

- unique n=1769 (raw 3735, dropped 1966 ladder dupes)
- skipped period props: 69
- OOS dates: 2026-09-27, 2026-09-28, 2026-10-04, 2026-10-05
- Ticket bar used: HR≥70% and n≥25 (softer than MLB 40 due to early season)
- Split-imbalance cells (late-arriving props vs global split): 60

**Do not auto-wire.** Re-check any clear on OOS + unique n after more weeks.
`oos_ticket_clear` is cleared when `oos_split_reliable` is false.

Exports: `discover_gates_nfl_export.csv`, `discover_gates_nfl_ranked.csv`, `discover_gates_nfl_sanity.json`.
