# Golf windows, PP reconcile, course-baseline projection

Handoff spec for the next Golf attach/model pass. Depends on scorecard-derived
cache (`utils/golf_scorecard.py`, step4) — already landed. Does **not** wait on
SG API, wind-wave, or keep-gate scan.

Order: shared windows → #1 PP reconcile → #2 course-baseline projection
(observe-only). Push-aware counts live in the shared window helper.

**Status**

| Slice | Code | State |
|---|---|---|
| Shared windows + push/decided | `utils/golf_windows.py`; step5/step8 columns | **landed** |
| Push-aware L5 (all sports) | `utils/l5_push_aware.py` via `fill_l5_from_stat_games` | **landed** |
| #1 PP reconcile | `utils/golf_pp_reconcile.py` + manual fixture (scrape has no chart yet) | **landed** |
| #2 Course-baseline projection + walk-forward | — | next |

---

## Shared: round rows and windows

Each cached round row carries:

| Field | Notes |
|---|---|
| `round_date` | ET date of the round |
| `event_id` | ESPN / board event id |
| `course_id` | **Scorecard venue ID**, not event name |
| `round_num` | 1–4 |
| `strokes` | Raw score |
| `to_par` | Strokes − course par |
| `vs_field` | Strokes − that event-round’s field mean |
| `birdies` | Exact birdies (−1) |
| `pars` | Exact pars |
| `bogeys_plus` | Bogeys or worse (align with PP when defined) |

Key course identity on the scorecard venue ID.

### Windows (one helper)

All ordered by date, computed in **one** helper so step4, reconcile, and
projection cannot drift:

| Window | Definition |
|---|---|
| `l5_all` | Last 5 rounds at any course |
| `l5_course` | Last N rounds at this week’s `course_id`, plus `l5_course_n` |
| `skill_window` | Last 20 rounds at any course (projection skill) |

### Push-aware counts

For each window against the line: `over` / `under` / `push` / `decided`, and

```text
hit_rate_decided = hits / decided
```

- On **integer** lines (Pars 11.0): the L5=5 gate needs **5 decided hits** in
  the chosen direction. Pushes never count as hits.
- If `decided < 3`, flag `thin_decided` only (flags-not-drops).
- Half-point lines cannot push; behavior unchanged.

PUSH grades to VOID in the grader — display/list L5 must not treat pushes as
hits or inflate a 3/3 decided record into a printed 5/5.

---

## #1 PP reconcile

**Input:** PP chart values per leg (last N + whatever filter the chart shows),
from the board scrape or a manual fixture for now.

**Match the filter first**

- Chart venue-filtered (e.g. “at Port Royal”) → compare `l5_course`
- Otherwise → compare `l5_all`

**Compare by date.** Integer stats must match exactly (no tolerance).

**Birdies definition:** If PP counts “birdies or better,” roll eagles into our
birdie count for the compare. Otherwise every eagle round looks like a mismatch.

**Output**

| Field | Values |
|---|---|
| `golf_pp_reconcile` | `match` \| `mismatch` \| `no_pp_chart` |
| `pp_mismatch_rounds` | Which rounds disagreed |

- `mismatch` → **hold** (`void_reason=golf_pp_mismatch`), same class as
  `golf_attach_suspect` (data error).
- `no_pp_chart` → flag only.

---

## #2 Course-baseline projection (observe-only)

Labels only. No gate changes. Same promotion path as NBA projection.

### Strokes

1. **Course baseline `B`:** par + historical field mean to-par at this course,
   computed per event-round from the cache. Shrink toward tour-wide mean to-par;
   weight by how many rounds of course history exist. No history → tour mean +
   flag `no_course_history`. Wind adjustment plugs in later (#4).
2. **Skill `S`:** mean `vs_field` over `skill_window`, shrunk toward 0 with
   empirical-Bayes `k` (NBA-style). All-courses window is fine — `vs_field` is
   already course-normalized.
3. Optional lightly weighted course-history term from `l5_course`.
4. **Projected mean:** `µ = B + S`
5. **Spread `σ`:** player residual SD of `vs_field`, shrunk toward pooled SD.
   Fit both from cache (no hardcoded σ).

**Probabilities**

- `.5` line: `P(over) = 1 − Φ((line − µ) / σ)`
- Integer line: also `P(push)` = mass at that integer

### Birdies

- Course-ease = course historical mean birdies/round ÷ tour mean
- Expected birdies = player’s shrunk birdies/round (all courses) × ease
- Price with Poisson, or NB if overdispersion shows

### Pars

- Same ease × shrunk rate shape
- Normal (or binomial out of 18), with explicit push mass on integer lines

### Columns

`golf_course_baseline`, `golf_skill_vs_field`, `golf_proj_mu`, `golf_proj_sd`,
`golf_proj_p_over`, `golf_proj_p_push`, `golf_proj_edge`

### Holds and promotion

- `golf_proj_hold` when skill window has fewer than 8 rounds (label only)
- Promotion bar (NBA): top-decile calibration on the diagonal, and `proj_edge`
  beats the L5 gate out of sample

### Backtest

Do not wait for graded props. Walk forward over the cached rounds (~2,588):
project each round using **only** rounds before it; check calibration of `µ/σ`
and count models. When archived PP golf lines exist, score against those too.

---

## Tests

1. **Window helper:** course filter, date order, push/decided counts on an
   integer line
2. **Reconcile:** match, mismatch, missing-chart; eagle round under
   birdies-or-better
3. **Projection:** Crowe-like fixture (`to_par −5.4` / `vs_field −2.2`) at a
   near-par course lands ≈ 68.8 with `P(over 68.5)` slightly above 0.5
4. **Walk-forward guard:** no round may use data from its own date or later

---

## Deferred (not this pass)

| # | Item |
|---|---|
| 3 | Birdies + Pars same-player block |
| 4 | Wind-wave flags (Open-Meteo) |
| 5 | Golf keep-gate scan (`scan_gate_thresholds_oos`) — needs graded normalized rows |

SG / Data Golf skill source can replace or augment `vs_field` later; projection
uses `vs_field` as the skill proxy for now.
