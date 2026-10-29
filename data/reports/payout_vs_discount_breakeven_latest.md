# Payout vs Goblin line-discount → break-even

generated_at=2026-10-29T12:55:54.681002+00:00
card=`H:\PropORACLE_card_window_pr\ui_runner\data\sg_delta_payout_rate_card_latest.json`

N-correct = `power_min_x` only (never 1st-place).
All-Goblin Power cells only (`n_g == n_legs`, `n_s == 0`).

**Price before you open the slip.** Use **p25** (or the live
`N correct pays` line) for deep-cushion Goblins — not the median.
Tonight-style deep stacks have printed **1.6x–2.1x**.

Hit-rate bands are **illustrative** (catalog / check-in). They are not
a claim that any Goblin cell is +EV until a cell clears its own
scraped-quote break-even with a Wilson band.

Illustrative p: mid=75%, optimistic=77%, live_checkin=70%.

## Pre-slip lookup (use p25 BE)

| n | mean Δ bin | p25 Nx | BE@p25 | median Nx | BE@med | skip if live < |
|---:|---|---:|---:|---:|---:|---:|
| 2 | mean_Δ_1–2 | 1.30x | 87.7% | 1.40x | 84.5% | ~1.3x |
| 2 | mean_Δ_2–3 | 1.30x | 87.7% | 1.30x | 87.7% | ~1.3x |
| 2 | mean_Δ≥3 | 1.40x | 84.5% | 1.45x | 83.0% | ~1.4x |
| 3 | mean_Δ_1–2 | 1.45x | 88.4% | 1.75x | 83.0% | ~1.4x |
| 3 | mean_Δ_2–3 | 1.92x | 80.4% | 2.40x | 74.7% | ~1.9x |
| 3 | mean_Δ≥3 | 1.70x | 83.8% | 1.80x | 82.2% | ~1.7x |
| 4 | mean_Δ_1–2 | 5.69x | 64.8% | 5.88x | 64.2% | ~5.7x |
| 4 | mean_Δ_2–3 | 4.75x | 67.7% | 4.75x | 67.7% | ~4.8x |
| 4 | mean_Δ≥3 | 3.38x | 73.8% | 3.50x | 73.1% | ~3.4x |
| 5 | mean_Δ_1–2 | 5.75x | 70.5% | 5.75x | 70.5% | ~5.8x |
| 5 | mean_Δ_2–3 | 7.62x | 66.6% | 8.00x | 66.0% | ~7.6x |
| 5 | mean_Δ≥3 | 5.25x | 71.8% | 5.25x | 71.8% | ~5.2x |
| 6 | mean_Δ_1–2 | 10.50x | 67.6% | 10.50x | 67.6% | ~10.5x |
| 6 | mean_Δ_2–3 | 9.25x | 69.0% | 9.25x | 69.0% | ~9.2x |

## Observed deep-cushion prints (not rate-card medians)

| when | n | N-correct | BE/leg | note |
|---|---:|---:|---:|---|
| 2026-10-28 | 6 | 2.1x | 88.4% | deep Goblin Points stack |
| 2026-10-28 | 6 | 1.8x | 90.7% | after reboot |
| 2026-10-29 | 4 | 1.6x | 88.9% | LaVine/Camara/Mitchell/Johnson deep Δ |

## By leg count

| n | cells | p25 Nx | BE@p25 | median Nx | BE@med | min–max | EV@75% med | EV@70% p25 |
|---:|---:|---:|---:|---:|---:|---|---:|---:|
| 2 | 13 | 1.30x | 87.7% | 1.40x | 84.5% | 1.30–1.70 | -0.213 | -0.363 |
| 3 | 12 | 1.56x | 86.2% | 1.85x | 81.5% | 1.40–3.00 | -0.220 | -0.464 |
| 4 | 10 | 3.88x | 71.3% | 4.75x | 67.7% | 3.25–9.00 | +0.503 | -0.070 |
| 5 | 4 | 5.62x | 70.8% | 6.50x | 68.8% | 5.25–8.75 | +0.542 | -0.055 |
| 6 | 2 | 9.56x | 68.6% | 9.88x | 68.3% | 9.25–10.50 | +0.758 | +0.125 |

## Deeper discount → lower payout (same n)

Mean Δ = average Goblin line distance vs Standard face on the slip.

### 2-leg all-Goblin

| mean Δ bin | n | p25 Nx | BE@p25 | median Nx | BE@med | EV@75% med | EV@70% p25 |
|---|---:|---:|---:|---:|---:|---:|---:|
| mean_Δ_1–2 | 5 | 1.30x | 87.7% | 1.40x | 84.5% | -0.213 | -0.363 |
| mean_Δ_2–3 | 2 | 1.30x | 87.7% | 1.30x | 87.7% | -0.269 | -0.363 |
| mean_Δ≥3 | 6 | 1.40x | 84.5% | 1.45x | 83.0% | -0.184 | -0.314 |

### 3-leg all-Goblin

| mean Δ bin | n | p25 Nx | BE@p25 | median Nx | BE@med | EV@75% med | EV@70% p25 |
|---|---:|---:|---:|---:|---:|---:|---:|
| mean_Δ_1–2 | 6 | 1.45x | 88.4% | 1.75x | 83.0% | -0.262 | -0.503 |
| mean_Δ_2–3 | 3 | 1.92x | 80.4% | 2.40x | 74.7% | +0.012 | -0.340 |
| mean_Δ≥3 | 3 | 1.70x | 83.8% | 1.80x | 82.2% | -0.241 | -0.417 |

### 4-leg all-Goblin

| mean Δ bin | n | p25 Nx | BE@p25 | median Nx | BE@med | EV@75% med | EV@70% p25 |
|---|---:|---:|---:|---:|---:|---:|---:|
| mean_Δ_1–2 | 2 | 5.69x | 64.8% | 5.88x | 64.2% | +0.859 | +0.366 |
| mean_Δ_2–3 | 5 | 4.75x | 67.7% | 4.75x | 67.7% | +0.503 | +0.140 |
| mean_Δ≥3 | 3 | 3.38x | 73.8% | 3.50x | 73.1% | +0.107 | -0.190 |

### 5-leg all-Goblin

| mean Δ bin | n | p25 Nx | BE@p25 | median Nx | BE@med | EV@75% med | EV@70% p25 |
|---|---:|---:|---:|---:|---:|---:|---:|
| mean_Δ_1–2 | 1 | 5.75x | 70.5% | 5.75x | 70.5% | +0.365 | -0.034 |
| mean_Δ_2–3 | 2 | 7.62x | 66.6% | 8.00x | 66.0% | +0.898 | +0.282 |
| mean_Δ≥3 | 1 | 5.25x | 71.8% | 5.25x | 71.8% | +0.246 | -0.118 |

### 6-leg all-Goblin

| mean Δ bin | n | p25 Nx | BE@p25 | median Nx | BE@med | EV@75% med | EV@70% p25 |
|---|---:|---:|---:|---:|---:|---:|---:|
| mean_Δ_1–2 | 1 | 10.50x | 67.6% | 10.50x | 67.6% | +0.869 | +0.235 |
| mean_Δ_2–3 | 1 | 9.25x | 69.0% | 9.25x | 69.0% | +0.646 | +0.088 |

## Verdict

- Hit rate ≠ return. A 70–77% Goblin catalog band is **not** +EV at the
  quotes deep-cushion slips actually get.
- Deeper mean-Δ bins often pay less and need a **higher** per-leg hit rate —
  the cushion that lifts HR is the same discount that raises break-even.
- **Pre-slip rule:** if the live `N correct pays` is under the bin’s p25
  (or under ~4x on a 4-Goblin), **pass** — do not submit on median hope.
- Production floor for all-Goblin 3 is the scrape **p25 (~1.65x → BE ~84.6%)**,
  not the rate-card median.
- Do **not** claim any Goblin keep cell clears break-even until that cell’s
  forward HR Wilson-low ≥ BE at the **actual** N-correct for its Δ mix.

### High break-even bins (BE/leg ≥ 78%)

- **2-leg mean_Δ_1–2 (p25)**: 1.30x → BE/leg **87.7%**
- **2-leg mean_Δ_1–2 (median)**: 1.40x → BE/leg **84.5%**
- **2-leg mean_Δ_2–3 (p25)**: 1.30x → BE/leg **87.7%**
- **2-leg mean_Δ_2–3 (median)**: 1.30x → BE/leg **87.7%**
- **2-leg mean_Δ≥3 (p25)**: 1.40x → BE/leg **84.5%**
- **2-leg mean_Δ≥3 (median)**: 1.45x → BE/leg **83.0%**
- **3-leg mean_Δ_1–2 (p25)**: 1.45x → BE/leg **88.4%**
- **3-leg mean_Δ_1–2 (median)**: 1.75x → BE/leg **83.0%**
- **3-leg mean_Δ_2–3 (p25)**: 1.92x → BE/leg **80.4%**
- **3-leg mean_Δ≥3 (p25)**: 1.70x → BE/leg **83.8%**
- **3-leg mean_Δ≥3 (median)**: 1.80x → BE/leg **82.2%**

n_all_goblin_cells=41
any_median_ev_positive_at_75pct=True
