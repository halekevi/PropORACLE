# Pre-registration: injury effects (Phase 1 measure)

**Registered 2026-10-27.** Runner: `py -3.14 scripts/measure_injury_effects.py`.
Observe only. Nothing auto-promotes onto lists, Goblin-70, mixer, or slip_check
gates beyond the Phase 1 OUT hard-drop.

Frozen NFL tracks are not edited.

## Phase 1 ticket rule (live)

A player listed **OUT** (also IR / PUP / NFI / suspended / inactive) cannot be
on a ticket. Questionable / GTD / Doubtful / Probable are flag-only.
`slip_check` warns on soft flags and on injury snapshots older than 6 hours.

## Spotting backups / minutes (flag only)

When a team's top-2 board-usage player is OUT, board props for remaining
teammates get `minutes_opp=1` + `minutes_opp_from` (who sat). The daily report
lists these as **Backup / minutes spots** — possible more minutes and stats.
This is a hand-check list only. It does **not** clear a ticket gate, change
badge, or move L5.

## What is measured (not ticketed)

Using as-of injury snapshots under `data/injuries/<date>/` with `captured_at`
strictly before tip:

1. **Teammate** — starter / top-usage player OUT → teammates' minutes, usage /
   targets / carries, and hit rate vs posted line (graded legs).
2. **Opponent** — key defender / starter OUT → do opponent props hit more?

No final box scores in the inputs. Report:
`data/reports/injury_effects_<date>.md` with n, hit rates, Wilson bounds.

## Promotion bar

Do not wire teammate or opponent effects into ticket gates, badges, or
ranker drops until a **new dated prereg** clears with:

- n≥100 decided legs per sport×prop×role cell (or an explicit lower bar),
- Wilson-low above the relevant breakeven,
- as-of leakage check pass (pre-tip status only).

## Collect cadence

5AM daily, refresh 10:30 / 4:30, and T-60 before the slate's first tip
(`scripts/run_injuries.ps1`). Snapshots never overwrite.
