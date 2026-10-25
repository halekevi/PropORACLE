# Golf prop assessment

Working note next to `prop_skeleton.md`. Keep file:
`utils/golf_keep_gates.py`. Lowest priority. In season, light volume.

## 1. Where golf stands

No opponent, so D is not part of the gate. Ticket rule is L5=5 and
L10≥8. Standards stay off the live card. Scorecard attach was rebuilt.
Strokes projections are **course-blind** — the same number on a
bomber’s track as on a short, wet major setup.

That is why golf is hard to assess here. Course-adjusted strokes
gained is the opportunity × rate term, and we do not have it.

## 2. Factors behind every golf prop

| Layer | Factors | Why |
|---|---|---|
| Course fit | Length, firmness, rough, greens | The “matchup” |
| Strokes gained | OTT / APP / ARG / PUTT by course type | Rate |
| Environment | Wind, rain, tee-time wave | Morning vs afternoon scoring |
| Cut | Make-cut line, projected cut | Thursday–Friday props die at the cut |
| Data quality | Scorecard attach | Rebuilt; still no SG |

## 3. Gaps

**DataGolf** (paid) is the source that actually has course-adjusted
SG. Without it, leave golf as L5=5+L10≥8 observe and do not spend
winter on a blind strokes model.

If a key appears later: snapshot SG + weather + wave before Thursday
tee-off; calibrate on past seasons the same way as NFL volume.

## 4. How the ML fits

```
course-adjusted SG
        ×  weather / wave
        ×  cut line
        →  round / hole distribution
        →  P(over) on each rung
        →  EV at the scraped N-correct quote
```

Do this after CBB, soccer, NHL, tennis, and CFB.
