# Construction picture — live packing (post May→June WR-bleed thread)

## Live now

| Tier | Construction rule | Where |
|--|--|--|
| **Goblin** | Hard floor on `hit_prob_actionable` **0.35** (retuned from 0.50). Curve is a step ~0.30 then plateau — floor is the correct lever, not shrinkage. Selectivity by raw `model_prob` still helps (+~10pp vs population). | `data/pipeline_read_checklist.json` → `filter_eligible` Goblin-only |
| **Standard ranking** | Player-history shrinkage on **sort keys only**, gated by `PROPORACLE_STANDARD_PLAYER_SHRINK` (**default OFF** until shadow validates on live slates). | `utils/player_prob_shrinkage.py` → `_attach_ticket_pick_order` |
| **Standard EV/display** | `est_win_prob` / `leg_prob_used` / Kelly / `ev_power` stay **raw** unless `PROPORACLE_STANDARD_PLAYER_SHRINK_EV=1` (default OFF). Shrinking those is a separate signed-off change. | `_resolve_leg_prob` (`mode=ev`) |
| **Standard shadow** | Each `--write-web` emit compares top-40 Standard raw vs forced-shrunk sort (Jaccard, sport mix, MLB share, mean pri). Does not inject into MAIN. | `ui_runner/data/standard_player_shrink_shadow_latest.json` |
| **Sports** | Everything allowed except thin-history competitions. Cross-sport Standard is a validated strategy. Mixer MAIN/FINAL already **hard-bans MLB Standard** (`_leg_mlb_keep_banned`) — shrink is not what keeps MLB Std off that track. | Soccer packing gate + competition helper |

Env knobs:

| Var | Default | Effect |
|--|--|--|
| `PROPORACLE_STANDARD_PLAYER_SHRINK` | `0` | Ranking shrink on Standard `__ts_pri` |
| `PROPORACLE_STANDARD_PLAYER_SHRINK_EV` | `0` | Shrink `_resolve_leg_prob` → est_win_prob / leg_prob_used / EV |
| `PROPORACLE_STANDARD_PLAYER_SHRINK_SHADOW` | `1` | Emit raw-vs-shrunk shadow compare |
| `PROPORACLE_STANDARD_PLAYER_SHRINK_PRIOR` | `150` | `k` in `n/(n+k)` |

## Pre-main checklist

1. **Shadow before default-on** — leave rank/EV off; read `standard_player_shrink_shadow_*.json` for a few days (composition + mean shrunk pri vs raw). Flip `PROPORACLE_STANDARD_PLAYER_SHRINK=1` only after that looks healthy.
2. **est_win_prob scope** — not ranking-only if EV flag is on. Default keeps pricing/display honest-raw; turn EV on only as a coordinated change.
3. **MLB Standard** — MAIN/FINAL already hard-bans all MLB Standard via `_leg_mlb_keep_banned` (**2026-09-08**, after May–June). That ban is **newer than** the tickets this thread analyzed: May–June MLB Standard volume lived on the MAIN mixer (`MLB Standard N-Leg` / `MLB Mixed N-Leg` in `combined_slate_tickets_*.json`), not Goblin-70. An OVER-only ban on 2026-07-19 already collapsed volume before the full ban. See `mlb_standard_ban_reconciliation.md`. Shrinkage is **not** the MLB-Standard MAIN fix; its live value is cross-sport / non-MLB Standard ranking.

## Proven but scoped

| Finding | Scope |
|--|--|
| Cross-sport 3–4 leg Standard, player-shrunk | Clears breakeven in May–June backtest |
| MLB-only Standard, player-shrunk | Does **not** clear BE yet (~22 prior props/player) |
| 5–6 leg player-shrunk | Untested |
| 2–4 leg calibrated-only (pre-shrink) | Unresolved; needs ≫61 days |

## Blind spots

- Mixed-tier / Demon (<3% volume)
- June ops flip toward Standard demand

## Intentionally unchanged

- Goblin / Demon: no player-history shrink
- Goblin-70 sleeve: L5=5+L10≥8+D keep-gates; bypasses mixer Goblin floor
- Correlation packing: rejected as bleed explanation
