# Parked rechecks (2026-09-24)

## Soccer Saves (own-D gate)

Leave `utils/soccer_keep_gates.py` alone until forward own-leaky OVER is
sub-50% at n≥30–40.

```powershell
py -3.14 scripts\_soccer_best_gate_od_alignment.py --forward-from 2026-09-15 --own-d-only
```

Forward pool was empty through graded ~2026-09-05; waits on post-GF/OWN_DEF
graded dates. Live gate has no opp-D fallback.

## MLB L10-primary / wind_out_hot

Grade live outcomes against `mlb_pitcher_l10_primary_gates.md` production bars:

| Market | Expected live HR |
|---|---|
| Pitcher Ks | ~80% |
| Hits Allowed | ~83% |
| ERA | ~88% (L5 still in gate) |
| Walks Allowed | ~81% |
| Hitter Ks | ~77% |
| H+R+RBI / Hits / TB | ~65–70% (L5 still in gate) |

Do **not** compare to the original ~90% L5-gated backtest cells. Include
`wind_out_hot` soft boost and L10≥8 keep grades in the same read.
