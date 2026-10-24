# Model & calibration workflow

## Production model (do not replace casually)

| Artifact | Holdout AUC | Role |
|----------|-------------|------|
| `models/edge_model_unified.pkl` | **0.7567** (trained 2026-06-13) | **Live production** — loaded by `edge_predict_utils.load_unified_edge_model` |
| `models/edge_model_unified_pre_enrichment.pkl` | 0.7546 | Archive / fallback reference |

**Live health ≠ holdout.** Rolling graded AUC (Std+Gob, Jul 2026 window) is much weaker on MLB (~0.52–0.61) and WNBA (~0.54). Treat holdout AUC as a training check, not ticket trust.

Promotion gate: new model overall holdout AUC **> 0.7567** on temporal holdout with enrichment columns **>10% fill** in `data/retrain_dataset.csv`, **and** rolling 30d graded AUC / calibration gap must not regress vs current live metrics in `data/model_performance_log.jsonl`.

### 2026-07-18 scalar refresh (ticket-eligible)

Re-fit linear scalars from graded props `min_date>=2026-06-20` (Demons excluded):

- **MLB** Goblin OVER / Std OVER / Std UNDER — cut overconfidence (`mean ml_prob` was ~0.76 vs ~0.59 HR on Goblin OVER)
- **WNBA** Goblin OVER / Std OVER / Std UNDER — mild recalibration
- **Skipped Tennis Goblin** — already calibrated (~0.33 vs 0.32); policy target 0.50 would *inflate* probs
- **Ticket cash models** remain shadow/`model_allowed: false` (overconfident cash bins)

### 2026-07-18 ticket generation alignment

| Knob | New default | Why |
|------|-------------|-----|
| `PROPORACLE_STRONG_MAX_LEGS` | **3** (hard cap) | Jul 17 STRONG ≤3 → ~73% ticket WR |
| STRONG `min_p_win` 2/3 | **0.45 / 0.38** | Toward 70% ticket WR target |
| `PROPORACLE_MAIN_MAX_LEGS` | **3** | Mixed MAIN short book stays 2–3 |
| `PROPORACLE_GOBLIN_MAX_LEGS` | **6** | Goblin-only / Long Goblin may emit 4–6 |
| `PROPORACLE_LONG_PARLAY` | **1** (on) | 5–6 long-parlay sidecar enabled |
| MLB Goblin OVER MAIN floor | **0.68** (stress props **0.72**) | Miss concentration + overconfidence |
| STRONG MLB props | Drop Hits / Total Bases | Same miss anatomy |
| STRONG min leg_prob | **0.65** (MLB **0.70**) | Match calibrated reality |

## Calibration stack (inference order)

1. **XGBoost** → raw score  
2. **Platt** (`LogisticRegression` on holdout) → `p_platt`  
3. **Slice isotonic** (`models/edge_slice_calibrators.pkl`) — per `(sport, pick_type, direction)` when `n >= 200`  
4. **Linear scalars** (`ML_PROB_CALIBRATION_SCALARS` in `scripts/edge_predict_utils.py`)

## When stats.nba.com is up (enrichment retrain)

```powershell
py -3.14 scripts/verify_enrichment_ready.py --smoke-test
pwsh -File scripts\run_enrichment_retrain_sequence.ps1 -Date (Get-Date -Format yyyy-MM-dd)
```

Compare `models/edge_model_metadata.json` to `edge_model_metadata_pre_enrichment.json` before swapping production pickle.

## Without full retrain (graded archive only)

### 1. Refresh linear scalars (WNBA now has 200+ graded rows)

```powershell
py -3.14 scripts\recalibrate_ml_prob_scalars.py --sport WNBA --min-n 50
py -3.14 scripts\recalibrate_ml_prob_scalars.py --sport WNBA --apply
```

All sports report:

```powershell
py -3.14 scripts\recalibrate_ml_prob_scalars.py --min-n 100
```

Output: `outputs/calibration/ml_prob_scalar_recommendations.csv`

### 2. Refresh slice isotonic (no XGBoost retrain)

Uses **pre-enrichment** model + full graded history:

```powershell
py -3.14 scripts\refresh_slice_isotonic.py
```

Writes `models/edge_slice_calibrators.pkl` and `models/edge_slice_isotonic_refresh.json`.

## WNBA

- Graded archive: **200+** rows required for isotonic slice (`WNBA_SLICE_ISOTONIC_MIN_N = 200` in `train_edge_model.py`).
- Scalars were `1.0` placeholders; run `recalibrate_ml_prob_scalars.py --sport WNBA --apply` after each major model swap.

## NBA scalars

Comment in `edge_predict_utils.py`: recalibrate after enrichment retrain when `usage_pct` / `team_pace` columns survive the 60% fill filter in training.

### 2026-08-27 feature-gap + calibration pass

Did **not** promote a new XGBoost pickle (still Jun-13). Live inference still uses `edge_model_features.json`.

Shipped:

- **Per-stat D** preferred for `def_rank` / `def_tier_encoded` when `stat_def_rank` is on the row (WNBA/NBA category D; soccer shots via `SHOTS_DEF_RANK` on shot props).
- **Season avg vs line**, **prop_encoded**, tennis surface features added to `FEATURE_COLUMNS` for the next retrain (ignored by the live pickle).
- Step8 keep-lists now pass `min_last5_avg` / `min_per_game`, `stat_def_*`, WNBA pace, tennis surface, soccer xG / shots D, MLB park/wind.
- **Tail caps** after scalars: MLB Goblin OVER **0.72**, Soccer Goblin OVER **0.52** (August Std+Gob sat at ~0.99 vs ~40% HR). Soccer Goblin OVER scalar **1.81 → 0.85**. Tennis uncapped.
- Recalibrate script reads `ui_runner/templates` graded JSON and compounds `current_scalar * (target / mean_p)` so `--apply` does not double-count.

Next retrain still required for Tennis-in-tree, prop category splits, and Goblin corr > 0.15.

### 2026-08-27 candidate (not promoted)

Trained `models/edge_model_candidate.pkl` (+ `_features.json`, `_metadata.json`, `_slice_calibrators.pkl`). Production `edge_model_unified.pkl` / features / slice calibrators were **not** overwritten.

| Metric | Production Jun-13 | Candidate (temporal test Jul 4–Aug 25) |
|--------|-------------------|----------------------------------------|
| Overall AUC | 0.7567 | **0.7825** (clears numeric gate) |
| MLB | 0.713 | **0.762** |
| Soccer | 0.725 | 0.724 |
| WNBA | 0.689 | **0.599** (regression) |
| Tennis | excluded | 0.887 |
| Goblin AUC / corr | (live ~0.49 MLB) | **0.626 / 0.277** (corr target >0.15) |
| Goblin mean_p vs HR | overconfident live | 0.558 vs 0.578 |

Held back because (1) step8 join left usage / `stat_def_*` / tennis surface **100% null** on the rebuilt CSV (keep-lists only help future slates), (2) WNBA holdout dropped, (3) NBA/NHL have **zero** rows in the Jul–Aug test window. Keep live tail caps until a fill-ready retrain.

Cache backfill (`scripts/enrich_retrain_from_caches.py`) then a second candidate: WNBA usage **86.7%**, stat_def **85.5%**, tennis surface **100%**. Holdout overall **0.7826**, Goblin corr **0.296**, WNBA **0.618** (was 0.599). Still not promoted — WNBA remains below Jun-13 0.689.

## Still blocked

| Item | Blocker |
|------|---------|
| Edge retrain / promote | Overall 0.7826 clears 0.7567; WNBA 0.618 still below prod 0.689 |
| Goblin `ml_prob` ranking | Candidate corr 0.296 on holdout; live pickle still Jun-13 + tail caps |
