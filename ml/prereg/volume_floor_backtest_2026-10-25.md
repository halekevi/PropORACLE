# Pre-registration: volume-floor backtests A and B

**Registered 2026-10-25.** Machine copy:
`volume_floor_backtest_2026-10-25.json`. Runner:
`py -3.14 -m ml.volume_floor_backtest`.

This file is written **before** either backtest is scored. A later notebook
that beats a softer bar, or that edits a frozen track to match a winning
slice, does not count.

## What this is not

Do **not** edit these frozen forward tracks:

1. Tackles Standard UNDER (`ml/prereg/nfl_standard_under_2026-10-24.md`).
   Sacks stay held until the stat attaches.
2. Demon volume, leaders (`ml/prereg/nfl_demon_volume_2026-10-25.md`) —
   lead-back rush attempts and target-leader receptions, spread ≤ +3.5.
3. Demon receptions floor (`ml/prereg/nfl_demon_rec_floor_2026-10-25.md`) —
   top-2 target share, share ≥ 15 points, line strictly below the L5 min.

If a result here suggests a better rule, register a **new** dated track.
Do not retune 1–3.

Nothing here is a keep gate. Nothing is placed. Historical Demon payouts
were not scraped, so neither backtest produces honest EV. Only the
forward logs, with real scraped Demon N-correct quotes, can promote.

## Shared definitions (both backtests)

Volume props only — attempts, receptions, targets. Not yards, not longest,
not made-shot efficiency.

| Sport | Volume canons in | Volume out |
|---|---|---|
| NBA, WNBA, NBA1H, WNBA1H, CBB | `fga`, `fg2a`, `threes_att`, `fta` | points, rebounds, assists, combos, made shots |
| MLB | `pitches_thrown` | hits, TB, Ks, ERA, walks_allowed |
| NFL, CFB | `receptions` / `rec`, `rush_att` / `rush_attempts`, `rec_tgt` | yards, longest, TDs, tackles |
| Soccer | `shots` | SOT, goals, G+A, saves, fouls |

Role = top-2 share **on the team**, as-of (games strictly before this one).
Floor = the next game meets or beats the L5 **minimum** (five prior decided
values). On a PrizePicks OVER face, `min(L5) > line` is the same event as
directional L5 = 5.

## A. Line-based (ledger)

Verified PrizePicks history. Main mass is NBA (Feb–Oct 2026) and MLB.
Smaller pieces of the other sports ride along when the sport-date is
face-verified.

| Item | Value |
|---|---|
| Source | `graded_props_*.json` (main_cp first), features as-of from that row |
| Dates | face-verified only (`utils.face_ladder.face_status_by_sport_date` status `verified`) |
| Books | Goblin OVER, Standard OVER, Demon OVER — separate cells |
| Floor flag | directional L5 = 5 (≡ line strictly below the L5 min on OVER) |
| Role flag | usage / share rank ≤ 2 on that team-date when the share exists; missing share skips the role slice, not the volume slice |
| Current gate | directional L5 = 5 + L10 ≥ 8 + D (OVER Weak\|Below Avg) |
| Walk-forward | last 30% of dates held out; never a random split |
| Cell floor | n ≥ 50 or the cell is printed as thin, not tested |
| Multiple testing | Benjamini–Hochberg, α = 0.10, one-sided vs that rung’s approximate breakeven, OOS legs only |
| Breakeven | Goblin 1.65x → 84.6%/leg; Standard 3x → 57.7%/leg. Demon has **no** historical quote — HR and Wilson only; Goblin 1.65x may be shown as a labeled reference, not a price |

Report per sport × book: hit rate, n, Wilson lower bound, HR vs that
rung’s breakeven, overlap of the floor+role slice with the current gate,
and volume vs non-volume **inside the same gate**.

## B. Line-free floor persistence (nflverse)

NFL verified PrizePicks history is a few weeks. This question does not
need a line: for a top-2 volume player, how often does the **next** game
meet or beat his L5 minimum? That is the same as asking how often a Demon
line set at his floor would clear.

| Item | Value |
|---|---|
| Source | nflverse `stats_player_week_{season}.csv.gz`, regular season |
| Seasons | 2021–2025 (headline). 2026 through the last completed week is tagged extra, not the headline |
| Families | rush attempts (RB/FB `carries`); receptions (WR/TE/RB `receptions`) |
| As-of | L5 min and L3 share use only games strictly before this week, same season |
| Role | team rank ≤ 2 on L3 carry share (rush) or L3 target share (receptions) |
| Receptions extra | share ≥ 15 percentage points (same cut as the rec-floor track) |
| Persistence | `actual ≥ L5 min`, five prior games required |
| Cell floor | n ≥ 50 |
| Price | none. Probability only |

## How to read the results (locked)

1. **Volume beats non-volume inside the same gate (A)** → volume is a
   filter or tiebreaker across rungs. Still not a promotion.
2. **Floor persistence high for leaders (B), say 80%+** → the Demon
   tracks have a strong prior. Watch the forward logs.
3. **Floor persistence near 60% (B)** → Skattebo and Tucker were mostly a
   good day.
4. **Neither shows a difference** → the two Demons were variance. Nothing
   was promoted, so nothing was lost.

A suggested tighter (or looser) cut is a new track, not an edit to 1–3.
