# MLB Goblin share collapse — supply (board) vs demand (packer)

Board proxy: all graded MLB props that day (pre-packer availability).
Ticket mix: MLB-only ticket_eval slips (`all_goblin` / `all_standard`).
Shares are Goblin / (Goblin + Standard), Demons excluded.

## Verdict

**DEMAND-SIDE (packer/selection) — not a platform Goblin shortage.**

| | May | June | Δ |
|--|--|--|--|
| Board Goblin share of (G+S) | 79.4% | 74.1% | **−5.3 pp** |
| Ticket Goblin share of (G+S) | 93.2% | 29.5% | **−63.7 pp** |
| Mean daily selection gap (ticket − board) | +13.7 pp | **−58.9 pp** |

Board absolute counts *rose* in June (mean daily Goblin ≈678→1193; Standard ≈175→416).
The platform kept offering Goblins; the packer stopped selecting them.

### Cliff timing (daily MLB tickets)

Not a slow drift — a **June 2 cliff**:

| Date | MLB all_goblin | MLB all_standard | Notes |
|--|--|--|--|
| 2026-06-01 | 238 | 23 | last high-Goblin day (~91% G) |
| 2026-06-02 | 3 | 4 | volume + mix collapse |
| 2026-06-03…17 | ~0–5 | mostly Standard | MLB nearly absent; other sports still packing |
| 2026-06-18+ | ~0 | 29–65/day | Standard volume returns; Goblin does not |

### Git-log suspects (May→June packer boundary)

| Date | Commit | Why it matters |
|--|--|--|
| **2026-06-02** | `31b51ab6b` enforce `pick_type_eligible` P(actionable) floors | Goblin floor 0.50; `filter_eligible` drops ineligible before build — **same calendar day as the cliff** |
| 2026-06-02 | `beb9d8def` / `5cb040cc8` read-model enrichment + tighter eligible gate | Same-day pool plumbing change |
| 2026-06-09 | `d22c4d7a3` / `b8b023e09` wire **win-rate builder as main** graded track | High `p_win` / short-leg main KPI — Standard-biased; replaces prior Goblin-heavy card |
| 2026-06-18 | `ea8d779b0` lower main win-rate leg gate | Matches Standard volume return with still ~0 Goblin |
| (later) | Goblin-70 script first appears **2026-08-27** (`ded211845`) | No Goblin-first production track existed in June |

Most actionable next check: did `pick_type_eligible` / 0.50 Goblin floor empty the MLB Goblin pool on 2026-06-02+ while Standard stayed eligible? That plus win-rate-main Standard preference would explain both the cliff and the lasting mix flip — and would still be costing WR if current dual-card Goblin-70 share is thin vs board.

## Board availability (graded MLB props)

```
  month  days    mean_gob   mean_std    mean_dem  mean_gob_share_gs  median_gob_share_gs  total_gob  total_std  goblin_share_gs_pooled
2026-05    31  677.548387 175.322581 2495.064516           0.804278             0.811343      21004       5435                0.794432
2026-06    30 1193.200000 416.466667 4691.866667           0.746267             0.753205      35796      12494                0.741271
```

## Packed tickets

```
  month  days  mean_ticket_gob_share  total_gob  total_std  ticket_goblin_share_gs_pooled
2026-05    18               0.939634       4257        310                       0.932122
2026-06    24               0.153022        262        626                       0.295045
```

## Board vs ticket (same days)

```
  month  mean_board_gob_gs  mean_ticket_gob_gs  mean_selection_gap
2026-05           0.802460            0.939634            0.137174
2026-06           0.742561            0.153022           -0.588905
```
