# Pre-registration: NFL Demon receptions L5-floor observe

**Registered 2026-10-25.** Separate from the frozen lead-back / target-leader
track (`nfl_demon_volume_2026-10-25`). Do not edit that file. Logger:
`utils/nfl_demon_rec_floor_shadow.py` (track `nfl_demon_rec_floor_shadow`).
Nothing here is placed or shown on `/tickets`.

The frozen gate would not have logged Tucker: it only takes each team’s
target leader, and Las Vegas’s is Brock Bowers. This track tests the
broader idea — a pass-catcher with a real target share whose Demon
receptions line sits under his L5 **minimum**.

## Motivating example (not in the log)

Tre Tucker Demon OVER receptions 3.5 on 2026-10-25: 5 catches, 103 yards.
Game date is at or before this registration, so it does not enter the
forward log. One hit is not evidence.

## What is being tested

Whether NFL **Demon OVER receptions** clear a real scraped Demon N-correct
quote when:

1. The player is a **top-2 target** on his team (target share L3, else
   rec-target share) **and** that share is at least **15** percentage
   points. Rank 3+ is out even if the line is easy. Missing share skips.
2. The posted Demon line is **strictly below the L5 floor** — the
   minimum of the five most recent decided game values (`current_stat_g1..g5`,
   else `stat_g1..g5`). He would have cleared this line in every one of
   those games. Fewer than 5 numeric games, or min ≤ line, skips.

Volume only. Receptions, not yards, not longest. Attempts stay on the
frozen rush-attempt arm. No spread / favorite gate here — that is the
other track’s job.

## Spec

| Item | Value |
|---|---|
| Sport | NFL, regular season only |
| Face | literal `Demon`; board must store pick_type |
| Side | OVER only. Demon UNDER is invalid |
| Props in | `receptions` |
| Props out | yards, longest, rush attempts, everything else |
| Role | team target-share rank ≤ 2 **and** share ≥ 15pp |
| Floor | `min(L5 games) > line`, n=5 numeric games |
| Timing | before kickoff; `game_date` after 2026-10-25 |
| Shape | observe legs only. `placed` false |
| Bar | ≥ 100 decided HIT/MISS; Wilson low vs a real scraped Demon N-correct. Never 1st-place |

## What happens if it clears

Report only. User decides. No keep gate, no Goblin-70 / mixer, no `/tickets`.

## Why this is not the frozen track

| | Frozen volume | This track |
|---|---|---|
| Receptions role | target **leader** only | top-2 + share ≥ 15pp |
| Rush attempts | lead back | out |
| Line check | `avg_l5 − line ≥ 1` | `min(L5) > line` |
| Script | spread ≤ +3.5 | none |
