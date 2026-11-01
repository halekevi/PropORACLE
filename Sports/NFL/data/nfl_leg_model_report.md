# NFL leg probability model - walk-forward report

Model: logistic regression. Train weeks 1..N, score week N+1 only.
Ticket threshold: p >= 0.75. Demons excluded from training.

## Decision rules (fixed before results)

- Replace gate only if OOS hit rate >= gate **and** volume >= 70% of gate.
- 70-80% calibration bin must be within 5 points (predicted vs actual).
- Bins with n < 30 flagged thin.
- Calibration fail => model is not trusted to replace the gate.

## Calibration

| Bin | n | Mean predicted | Actual HR | gap pp | Flag |
|---|---:|---:|---:|---:|---|
| 50-60 | 625 | 0.5476 | 0.5728 | 2.52 | ok |
| 60-70 | 667 | 0.6514 | 0.6402 | 1.12 | ok |
| 70-80 | 798 | 0.7459 | 0.6955 | 5.04 | FAIL_CALIBRATION_>5pp |
| 80+ | 460 | 0.8734 | 0.7239 | 14.94 | ok |

Calibration trust (70-80 within 5pp, n>=30): **FAIL**

## Model vs gate (same OOS weeks)

| Rule | hits | n | HR |
|---|---:|---:|---:|
| Gate (L5=5 + L10>=8) | 696 | 961 | 72.4% |
| Model p>=0.75 | 609 | 835 | 72.9% |

Volume ratio (model/gate): **86.9%**
HR+volume bar: **PASS**
Replace gate?: **NO - keep gate; model is packing tiebreaker only** (blocked by calibration)

Note: `min_m_pct` was all-null in this baseline (Stat G1-G5 rarely joined); cover_pct often falls back to projection-line. Odds totals still missing.

## By prop family

| Family | Gate hits/n (HR) | Model p>=0.75 hits/n (HR) |
|---|---|---|
| pass | 276/403 (68.5%) | 225/308 (73.1%) |
| rush | 94/135 (69.6%) | 61/85 (71.8%) |
| receiving | 217/288 (75.3%) | 216/296 (73.0%) |
| kicking | 37/45 (82.2%) | 38/51 (74.5%) |
| tackles | 72/90 (80.0%) | 69/95 (72.6%) |

## Direction accuracy

Higher-p side matched the unique hitting side on **116/153** = **75.8%**.

## Fold sizes

| Test week | n |
|---:|---:|
| 2 | 746 |
| 3 | 2615 |
| 4 | 269 |
| 5 | 187 |
| 6 | 454 |
| 7 | 515 |
| 8 | 1025 |

## Known gaps

- No implied totals / game_total features (Odds API).
- Injury blank — not used as a feature.
- Gate proxy here is directional L5=5 + L10>=8 (NFL Goblin ticket flag still off).

