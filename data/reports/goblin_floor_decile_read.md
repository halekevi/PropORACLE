# Goblin `model_prob` deciles vs 0.50 floor (May–June, leg-level)

Source: full graded slate (not mixer-selected). n=86,998 Goblin legs.

## Decile table (`goblin_deciles.csv`)

9 bins (qcut merged ties). Monotonic enough early; top overconfident but not inverted:

| decile | n | mean_pred | mean_hit | gap |
|--|--|--|--|--|
| 1 | 8704 | 0.286 | **0.511** | +0.225 |
| 2 | 8722 | 0.505 | **0.613** | +0.109 |
| 3–6 | … | 0.54–0.61 | **0.59–0.63** | small |
| 9 | 17339 | 0.967 | **0.619** | −0.348 |

## Hard split at the live floor

| | n | mean_pred | **leg hit** |
|--|--|--|--|
| `< 0.50` | 10914 | 0.322 | **52.7%** |
| `≥ 0.50` | 76084 | 0.693 | **62.1%** |
| Δ | | | **+9.4 pp** |

Legs the floor cuts are worse — floor *concept* is valid.

## Is 0.50 a step, or arbitrary on a smooth curve?

Fine bins (leg hit):

| bin | n | hit |
|--|--|--|
| [0.0, 0.3) | 3717 | **39.6%** |
| [0.3, 0.35) | 210 | **62.9%** (+23 pp) |
| [0.35, 0.5) | ~7k | **~59–60%** |
| [0.5, 0.55) | 13147 | **62.5%** (+2.8 pp vs prior bin) |
| [0.55, 0.9) | … | **~61–66%** plateau |
| [0.9, 1.01) | 14290 | 61.1% (overconfident) |

**Real cliff is ~0.30, not 0.50.** Crossing 0.50 is only +2.8 pp. From ~0.35 up the curve is flat — a hard 0.50 cutoff is crudely chopping a plateau, not sitting on a step.

## p25 / retune

Goblin `model_prob` quantiles: p10=**0.41**, p25=**0.53**, p50=0.57.
A “retune to p25≈0.40” was a guess — empirical p25 is **above** the live floor. If you want a cutoff at the actual step-change, it’s nearer **0.30–0.35**, not 0.40 or 0.50.

## Read for the action item

- Don’t remove the idea of a Goblin quality bar — sub-0.50 legs are ~9 pp worse, and sub-0.30 legs are genuinely bad (~40%).
- **0.50 specifically is poorly placed** — it gates a large band of ~60% legs that sit on the same plateau as the keepers.
- Defensible options: move floor to ~**0.35** (at the step), or drop the hard floor and rely on ranking / Goblin-70 keep gates.
- **Not Standard’s problem:** Goblin’s curve doesn’t invert — a retuned hard threshold is sufficient. Shrinkage is for Standard’s thin-player tail, not Goblin.
- June 2 was brutal because the floor sat **above** the real cliff; retuning to 0.35 on `hit_prob_actionable` would have restored ~375 legs that day (~57→~140 predicted tickets). See `goblin_floor_plateau_and_june2_volume.md`.
