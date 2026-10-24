# MLB pitcher L10-primary keep gates (wired 2026-09-17)

Catalog finding: for Goblin OVER pitcher volume props, **L10≥8 alone** at
real n beats L5∩L10 at near-zero joint n; L5 often flat or hurts.

## Wired into `utils/mlb_keep_gates.py` (list + Goblin-70)

| Prop | Was | Now |
|---|---|---|
| Walks Allowed | L5≥4 + L10≥8 + own leaky | **L10≥8 + own leaky** |
| Pitches Thrown | L10≥8 + own leaky | unchanged (already L10-primary) |
| Pitcher Ks | L5=5 + L10≥8 + own stingy | **L10≥8 + own stingy** |
| Hits Allowed | L5≥4 + L10≥8 + opp bats Strong | **L10≥8 + opp bats Strong** |
| Pitching Outs | L5=5 + own stingy (no L10) | **L10≥8 + own stingy** |

ERA stays Goblin 0.5 + L5=5 + L10≥8 (line-bound; not in the L10-only set).
Hitter-side keeps (H+R+RBI / Hits / TB / Hitter Ks) unchanged.

## Corrected production-gate reference (check-in bar)

The ~90% cells from tighter L5-gated **backtest slices** are **not** the live
bar. Production dropped L5 on pitcher volume props, so expect lower live HR
at higher volume. Use this table when reading graded outcomes (incl.
`wind_out_hot` boost + L10≥8 keep grades) over the next 2–3 weeks:

| Market | Production gate | Expected live HR |
|---|---|---|
| Pitcher Ks | L10≥8 + OwnStingy | **~80%** (not 91%) |
| Hits Allowed | L10≥8 + OppStrong | **~83%** (not 89%) |
| ERA | 0.5 line + L5=5 + L10≥8 | **~88%** (L5 still in gate) |
| Walks Allowed | L10≥8 + OwnLeaky | **~81%** (not the L5=5 cell) |
| Hitter Ks | L10≥8 + K%≥28 + Elite/Above | **~77%** |
| H+R+RBI / Hits / TB | BA≥.275 + L5=5 + Weak/Below | **~65–70%** (L5 still in gate) |

ERA and H+R+RBI / Hits / TB should track closer to backtest because L5 remains
in those gates. Pitcher Ks / Hits Allowed / Walks Allowed at ~80% live is the
expected cost of the looser L10-primary gate — not a regression vs the old
~90% L5∩L10 backtest cells.

Catalog ungated L10≥8 cells (context only): Ks 76.2% n=975, HA 76.9% n=823,
BB 80.0% n=596, Outs 80.1% n=498, Pitches 78.2% n=261.

Live OOS stamp: `data/reports/mlb_l10_primary_live_watch_latest.json`.
Tests: `tests/test_mlb_keep_gates.py`.
