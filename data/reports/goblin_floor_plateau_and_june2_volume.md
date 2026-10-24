# Goblin floor retune — plateau check + June 2 volume

## Plateau reliability (finer quantile bins)

See `goblin_deciles_20bin.csv` / `goblin_deciles_30bin.csv` (Wilson lo/hi per bin).

- **20-bin** plateau (mean_pred 0.35–0.90): hit 56.0–68.3% (span 12.2 pp); no single common Wilson overlap across all bins — mild structure, not a second cliff.
- **30-bin** plateau: hit 55.2–70.0% (span 14.9 pp); same story. Pairwise Wilson overlap ~56–63% of bin pairs.
- Read: the “flat 60–65%” coarse read holds as *no step-change*; there is mild within-plateau wiggle (n is healthy in most bins), not a hidden World-Cup-style slice.

## June 2 volume at 0.35 vs 0.50

**Gate field is `hit_prob_actionable`, not `model_prob`.** Graded `model_prob` on June 2 MLB was already ≥0.50 for ~98% of Goblins — that proxy does not explain the cliff. Enriched actionable scores do:

| Floor | MLB Goblin keep (June 2 enriched) | Mix-weighted pred tickets from 238 |
|--|--|--|
| **0.50** (live) | **1049 / 1677 (62.6%)** | **~57** |
| **0.35** (retune) | **1424 / 1677 (84.9%)** | **~140** |
| Restored | **+375 legs (+36% vs 0.50 pool)** | +~83 tickets |

So retuning 0.50→0.35 would have **materially softened** June 2 (57→140 predicted) but not restored the full pre-cliff card — same-day assembly factors still account for 140→3. Closing the original incident with a number: the misplaced floor alone cost on the order of **~80 predicted tickets** that day; the rest was the other June 2 path changes.

## Structural distinction vs Standard

- **Goblin:** real step ~0.30, then roughly flat — model separates bad from not-bad, not good from great. A **retuned hard floor near the step (~0.35)** is the right fix shape, not shrinkage.
- **Standard:** smoothly inverting tail — needs **player-history shrinkage**, not a floor.
- Do not apply Standard’s shrinkage treatment to Goblin on this evidence.

## June 2 retro context

The cliff wasn’t only “a floor was added” — it was added **well above** where Goblin’s signal actually starts (~0.30), which is why it was so brutal.
