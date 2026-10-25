# WNBA prop assessment

Working note next to `prop_skeleton.md` and `nba_prop_assessment.md`.
Offseason until May. Earlier gate scan found only **+1–7 point** lifts.
Usage fields were empty in graded data.

## 1. Where WNBA stands

In-season the list and Flex already require directional **L5=5**
(Standard OVER, Standard UNDER, Goblin OVER). Extra ticket rules that
stay when the season returns:

- Goblin PRA needs Off (HIGH/STAR or minutes HIGH) on top of L5=5+L10≥8+D
- Goblin Rebounds 3.5+ skips D; ≤2.5 still needs D
- Goblin Assists 2.5 off tickets (1.5 stays)
- Assists with entering-game poss/g ≥ 18 off tickets
- Points / Rebounds / Pts+Reb / Pts+Ast need season avg ≥ line+cover
- Fantasy off. Postseason allowed at cell minus 3, one slip/day

Those are keep-file rules, not a projection. The scan’s small lift and
empty usage mean the winter job is to **port the NBA layer**, not to
hunt another 3-point L5 cut.

## 2. Factors behind every WNBA prop

Same skeleton as NBA, smaller rotation, larger star-out swings.

| Layer | Factors | Why |
|---|---|---|
| Opportunity | Minutes, starter, B2B, star out | Usage vacuum is larger than NBA |
| Rate | Per-minute pts / reb / ast | What `proj_mu` should model |
| Matchup | Opp D by stat, pace | One Weak/Elite label is still coarse |
| Data quality | Empty usage in graded history | Last season’s scan could not see role |

## 3. Gaps — offseason project

1. Port `utils/nba_player_projection.py` (shrinkage, positional
   baseline, games-before) and `utils/nba_slate_minutes.py` (projected
   vs T−30 confirmed, pre-tip snapshot).
2. Backfill usage / minutes into graded history so last season can
   train. Empty usage is why the scan was +1–7.
3. Write the calibration test **before** the run: train through 2024,
   test 2025, Brier / decile / ECE per prop (points, reb, ast, PRA,
   3PM, FGA). Pass vs base-rate Brier and ECE ≤ 0.05.
4. Ready on opening day. Do not build this in May.

## 4. How the ML fits

Identical to NBA:

```
minutes (projected → confirmed)
        ×  shrunk per-minute rate
        ×  matchup / pace / star-out
        →  distribution
        →  P(over) on each rung
        →  EV at the scraped N-correct quote
```

WNBA/WNBA1H/WNBA1Q Standards stay 2–3 pick. Period boards wait on
`period_prop_assessment.md`.
