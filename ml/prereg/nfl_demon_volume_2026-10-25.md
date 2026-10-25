# Pre-registration: NFL Demon volume observe

**FROZEN 2026-10-25.** Amendments closed. Machine copy:
`nfl_demon_volume_2026-10-25.json`. Logger:
`utils/nfl_demon_volume_shadow.py` (track `nfl_demon_volume_shadow`),
called from `scripts/build_goblin70_tickets.py` after the tackles Standard
UNDER shadow. Nothing here is placed or shown on `/tickets`.

Do not edit the conditions, thresholds, or prop list after the first
forward leg logs. A later tweak that “fits last week” voids the 100-leg
test. If a rule is wrong, register a new dated file
(`nfl_demon_volume_YYYY-MM-DD`) and a new track name. Leave this one
untouched.

## Motivating example (not in the log)

Cam Skattebo Demon OVER rush attempts 9.5 on 2026-10-25: 16 attempts,
Giants favored and leading. Volume × workhorse × favorite script. One hit
is not evidence. That game date is at or before this freeze, so it does
not enter the forward log.

## What is being tested

Whether NFL **Demon OVER** rush-attempt legs for the **lead back** and
**reception** legs for the **target leader** clear a real scraped Demon
N-correct quote often enough to become a ticket candidate — **when the
same three conditions that made Skattebo work are true again that week**.

Attempts / receptions are counting volume. They depend on role and game
script, not on breaking a long run — the same reason NBA attempt props
were the most stable observe set. One 16-carry afternoon is not a lock.

## Amendment 2026-10-25 — weekly re-check (before first forward slate)

Same-day amendment, no forward legs logged yet. A logged row must clear
all three. Missing any one skips.

1. **Lead back / target leader still holds.** No committee (runner-up
   within 15 share points) and no injury on the candidate
   (Questionable / Doubtful / Out / IR / Suspended). A new injury that
   takes carries, or a split backfield, is not this setup.
2. **Favored or close.** Team spread `<= +3.5` (favorite, pick'em, or
   one-score dog). Bigger underdogs fail — if they trail, carries drop
   fast. Unknown spread still fails.
3. **Line still low vs recent volume.** `avg_l5 − line >= 1`. After a
   spike game PrizePicks may raise the Demon; compare the live number to
   the L5 body, not last Sunday alone. Missing L5 skips.

This is not the L5=5 ticket stack. It is a “is this still a low bar for
this week's usage” check.

**Script width (frozen).** The first draft was favored only (`spread < 0`).
The amendment is `spread <= +3.5` (pick'em and one-score dogs included).
That is a wider gate. It is now the definition, not a draft.

**Odds API.** Condition 2 needs a team `spread` on the row. Until the
Odds API account is topped up and step6b writes it, candidates fail
`spread_unknown` and this log stays empty. Do not drop or loosen the
spread check to get rows. ESPN scoreboard lines are step6b's 401
fallback when they exist; they are not a reason to change this spec.

## Spec

| Item | Value |
|---|---|
| Sport | NFL, regular season only (`season_class == regular`) |
| Face | literal `Demon` in step1 `pick_type`; board must have ≥1 Goblin/Demon row (`step1_has_pick_type`) |
| Side | OVER only. Demon UNDER is invalid and is not logged |
| Props in | `rush_attempts`, `receptions` |
| Props out | everything else (including `fg_made`, yards, TDs) |
| Role | lead back = team-max rush-attempt share, else team-max Snap L3 among players who have a rush-attempt line (a WR's snaps do not beat the RB). Target leader = team-max target share (Tgt Share L3), else team-max rec-target share. Rank, not a share-floor gate. Committee (runner-up within 15 points) or candidate injury → skip |
| Script | team spread `<= +3.5` (favored or one-score). Bigger dogs and unknown spread fail |
| Line vs recent | `avg_l5 − line >= 1`. No L5=5 / L10 stack. Missing L5 skips |
| Timing | logged before kickoff only; `game_date` (ET) must be after 2026-10-25 |
| Dedupe | one row per player-prop-line |
| Shape | observe legs only. No paper 2-leg / 6-leg slips. House rules still apply if this ever tickets: 3 legs max, one player once, no unsupported props |
| Stake | paper; `placed` false. Until the bar clears, a matching live face is a small-stake candidate, not a lock |

## Bar (the 10-11 bar)

- ≥ 100 decided legs (HIT / MISS) with `game_date` after 2026-10-25.
- Wilson 95% lower bound ≥ breakeven of a **real scraped Demon N-correct**
  quote (`utils.scraped_quote.is_real_scraped_quote`). Never 1st-place.
- Composition is not locked until a scrape exists. A guessed Demon multiple
  does not meet the bar.
- Legs whose graded face is not Demon, or whose line never appears, are
  `NOT_ON_BOARD` and do not count either way.
- The bar is evaluated per arm (`rush_attempts`, `receptions`) and on the
  combined pool. Meeting one arm does not promote the other.

## What happens if it clears

Nothing automatic. Report in `scripts/report_track_comparison.py`. The
user decides whether Demon volume becomes a placed track. No keep gate,
no Goblin-70 / mixer wiring, no `/tickets` change from this file. Demon
OVER stays off the general best-props and ticket paths until that call.

## Why the slip that prompted this is not the test

The 6-leg Power that included Skattebo was already a house-rule miss
(3-leg max, one ticket per player, Szmyt FG made unsupported). This
registration logs the pick type, not that slip shape.
