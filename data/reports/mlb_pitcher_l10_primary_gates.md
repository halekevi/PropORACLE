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

Catalog HR cited (ungated L10≥8 cells): Ks 76.2% n=975, HA 76.9% n=823,
BB 80.0% n=596, Outs 80.1% n=498, Pitches 78.2% n=261.

Tests: `tests/test_mlb_keep_gates.py`.
