# Shared prop skeleton

Every sport in this folder uses the same stack. NBA and NFL already have
the first adapters. The rest of the notes name what is missing in each
sport; they do not invent a second architecture.

```
opportunity (minutes / snaps / TOI / routes / confirmed start)
        ×  shrunk rate (per-opportunity production)
        ×  context (script, matchup, environment)
        →  distribution
        →  P(over) on every Goblin / Standard / Demon rung
        →  EV at the scraped N-correct quote
```

Calibrate the distribution on past seasons **before** any PrizePicks
line. Edge vs PP is a later forward test on real faces only.

## Why it is one library

Face checks, DNP voiding, freshness gates, and pre-tip snapshots are
the same bugs in every pipeline. A thin adapter per sport (column map,
opportunity unit, rate family) means a soccer DNP fix or an NHL
goalie-name fill is not a one-off. That is also the shape the tree
collapse wants: one `utils/prop_model/` with sport modules, not eight
copies of step8.

Nothing in this file is a keep gate. Adapters land when the sport note
says the inputs exist.

## Adapter contract

| Piece | What the sport must supply |
|---|---|
| Opportunity | Minutes, snaps, TOI, routes, or confirmed start. Snapshot before tip / kick |
| Rate | Per-opportunity history, games strictly before *G* |
| Context | Spread, total, opponent, weather / surface / park when it moves the prop |
| Distribution | One `proj_mu` → P(over) on every rung |
| Calibration | Pre-registered split + Brier / decile / ECE **written before the run** |
| Quote | Scraped N-correct only. Wilson on dates the search has not seen |

## Shared guards (apply everywhere)

- DNP-zero → VOID only with positive playing-time evidence. Unknown PT stays graded.
- Literal `pick_type`. Empty / dash defaults are not Standard.
- Face swaps re-gate the offered face or skip. No silent prop change.
- Cache freshness is a failing assert, not a Tuesday reminder.
- Pre-tip / pre-kick snapshots lock per game. Do not train on the post-game rebuild.
- Demon UNDER is invalid. Demons stay off Goblin-70 / mixer except the NBA attempt observe list.
- Period props need period history. Full-game L5 on a 1Q line is a hold.

## Build order across sports

| Priority | Sport | First step | Note |
|---|---|---|---|
| 1 | CBB / WCBB | Dry run this week: `pick_type`, import smoke, faces, em-dash prop names | Opens ~2026-11-01 |
| 2 | Soccer | DNP voiding in grading and L5 history | Live daily; keep cells unverified until this is honest |
| 3 | NHL | Daily Faceoff goalies, game totals, ESPN vs NHL API conflicts | Live; no keep gate; mixer held |
| 4 | Tennis | Replace the stale Sackmann feed; moneyline → Total Games | Live; Total Games held |
| 5 | CFB | Set `CFBD_API_KEY` | Weekly; Player TDs need red-zone share |
| 6 | MLB, WNBA | Winter projection + calibration | Offseason after the Series / until May |
| 7 | Golf, periods | Later | Low volume or blocked |

Sport notes sit next to this file: `nba_`, `nfl_`, `nhl_`, `soccer_`,
`tennis_`, `cfb_`, `cbb_`, `mlb_`, `wnba_`, `golf_`, `period_`.
