# Pre-registration: floor-persistence table (all categories)

**Registered 2026-10-25.** Machine copy: `floor_persistence_2026-10-25.json`.
Runner: `py -3.14 -m ml.floor_persistence`.

Written before the table is scored. This is Step 2 of the gift scan.
It does not edit the frozen NFL tracks or the NBA/NHL floor observe
tracks. A later cut that drops a prop family is a new dated file.

## Question

For each sport × prop × role, how often does the **next** game meet or
beat the L5 minimum minus a cushion k, using only games strictly before
that one.

```
P(actual >= L5_min − k)  for k in {0, 1, 2}
```

No PrizePicks line. Same as asking how often a posted line at that
floor (or 1–2 below it) would clear.

## Sources (as-of)

| Sport | Source |
|---|---|
| NBA, CBB | `proporacle_ref.db` player boxes |
| NHL | same DB, `nhl` table |
| Soccer | same DB, `soccer` table |
| NFL | nflverse `stats_player_week` already cached |
| Others | included when a box table exists |

Unsupported / mapping-fail props (`utils.stat_attach_invalid`) stay out.

## Roles

As-of L3 mean of that stat (else minutes / TOI / snaps), ranked on the
team among players who recorded it. Games strictly before this one.

| Role | Definition |
|---|---|
| lead | rank 1 |
| top2 | rank ≤ 2 |
| all | every player-game with a 5-game history |

## Binary / threshold props

TD, double-double, and similar 0/1 outcomes do not have a useful L5
min. Report P(actual ≥ threshold) by role instead. Thresholds:

| Prop | Threshold |
|---|---|
| anytime TD / player TDs | 1 |
| double-double | 1 (the event) |
| goals / G+A soccer | 1 |

Quarters-with-X stay out until period boxes exist.

## Combos

PRA, PR, PA, RA, rush+rec yards: use the combined column’s own L5 min.

## Guards

- n ≥ 50 per cell or the cell is thin, not tested
- Walk-forward by date only
- Benjamini–Hochberg α = 0.10 on one-sided tests of P(k=0) > 0.70
- Write `data/reports/floor_persistence/`

## How the gift scan uses this

P(clear) for a live line is the cell whose k matches
`round(L5_min − line)` clipped to 0–2, at that sport × prop × role.
Goblins at k=0 are fairly priced unless the cell is well above 84.6%.
Standard / Demon gifts need that gap plus a real scraped quote.
