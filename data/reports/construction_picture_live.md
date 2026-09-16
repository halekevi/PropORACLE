# Construction picture — live packing (post May→June WR-bleed thread)

## Live now

| Tier | Construction rule | Where |
|--|--|--|
| **Goblin** | Hard floor on `hit_prob_actionable` **0.35** (retuned from 0.50). Curve is a step ~0.30 then plateau — floor is the correct lever, not shrinkage. Selectivity by raw `model_prob` still helps (+~10pp vs population). | `data/pipeline_read_checklist.json` → `filter_eligible` Goblin-only |
| **Standard** | **Player-history shrinkage** on ranking + leg prob: `w=n/(n+k)`, `shrunk = w·p + (1−w)·baseline`, default `k=150`. Demotes thin-history overconfident tails. | `utils/player_prob_shrinkage.py` → `_resolve_leg_prob` + `_attach_ticket_pick_order` (Standard only) |
| **Sports** | Everything allowed except thin-history competitions (`utils/competition_history.py`, e.g. WORLDCUP*). Cross-sport Standard is a validated strategy, not a fallback. | Soccer packing gate + general competition helper |

Env knobs (Standard shrink):

- `PROPORACLE_STANDARD_PLAYER_SHRINK=1` (default on; `0`/`off` to disable)
- `PROPORACLE_STANDARD_PLAYER_SHRINK_PRIOR=150`

## Proven but scoped

| Finding | Scope |
|--|--|
| Cross-sport 3–4 leg Standard, player-shrunk | Clears breakeven in May–June backtest |
| MLB-only Standard, player-shrunk | Does **not** clear BE yet (~22 prior props/player) — more season data, not more logic |
| 5–6 leg player-shrunk | **Untested** — do not assume closed |
| 2–4 leg calibrated-only (pre-shrink) | Unresolved; needs ≫61 days |

## Blind spots (low volume, unexamined)

- Mixed-tier tickets (~179 / 10,589)
- Demon tickets (~138 / 10,589)
- June ops flip toward Standard demand (confirmed demand-side; not blocking now that floor + shrink are live)

## Intentionally unchanged

- Goblin / Demon: no player-history shrink (Goblin uses floor; Demon unexamined)
- Goblin-70 ticket sleeve: still L5=5+L10≥8+D (and sport keep-gates); bypasses mixer Goblin floor
- Correlation packing: still rejected as the bleed explanation

## Smoke

```powershell
py -3.14 -c "from utils.player_prob_shrinkage import shrink_prob_toward_baseline, apply_standard_player_shrinkage; print(shrink_prob_toward_baseline(0.9, 10, 0.55, 150))"
```
