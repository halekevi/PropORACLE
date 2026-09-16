# Investigation closeout — May→June WR bleed → Goblin floor + Standard shrinkage

Started as: does leg correlation explain the bleed? **No.**

## Shipped / ready

| Item | Status |
|--|--|
| Lever A (same-player block) | Shipped earlier this thread |
| Thin-competition / WC gate | Shipped earlier this thread |
| **Goblin floor `0.50` → `0.35`** | **Shipped** in `data/pipeline_read_checklist.json` — real step location on `hit_prob_actionable`, not a guess. See `goblin_floor_decile_read.md`, `goblin_floor_plateau_and_june2_volume.md`. |
| **Standard player-history shrinkage** | **Shipped live** — `utils/player_prob_shrinkage.py` wired into `_resolve_leg_prob` + Standard sort keys in `combined_slate_tickets.py`. Backtest mode remains `rank_by=player_shrunk` in `build_leg_correlation_matrix.py`. See `construction_picture_live.md`. |

## Two different curve shapes → two different fixes

- **Goblin:** step ~0.30, then plateau — model tells bad from not-bad, not good from great. Hard floor near the step is correct.
- **Standard:** smooth tail inversion (thin `player_prior_n`) — needs shrinkage toward a cell baseline, not a floor.

## June 2 reconciliation (closed)

Floor + same-day `hit_prob_actionable` path stacked. Floor alone never explained 238→3; retuning 0.50→0.35 restores ~375 legs / ~57→~140 predicted tickets that day. Residual to 3 is the second same-day filter + assembly — consistent, not a new mystery.

## Validated backlog

- Cross-sport player-shrunk Standard 3–4 leg clears breakeven (real second lever) — **now in live packing**.
- MLB-only Standard still fails even shrunk (~22 prior props/player) — likely needs more season, not new logic.
- 5–6 leg player-shrunk: untested.
- 2–4 leg *calibrated* Standard (pre-shrinkage): unresolved until window ≫ 61 days.
- Mixed-tier / Demon: unexamined (<3% volume).

## Still open (ops, not blocking)

Why June packing flipped toward Standard (demand-side confirmed; `31b51ab6b` + win-rate-main). Less urgent now that floor + shrinkage are live.

## Outcome

Wrong starting hypothesis → two construction rules shipped (Goblin floor + Standard shrink), Goblin threshold diagnosed and retuned with an exact number, Standard rescue mechanism with a known boundary, live regression caught to commit. Better return than if correlation had worked.
