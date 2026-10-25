# MLB prop assessment

Working note next to `prop_skeleton.md`. Keep file:
`utils/mlb_keep_gates.py`. Most mature sport in the repo. World Series
now, then offseason. MLB Standard stays **off** Goblin-70.

## 1. Where MLB stands

Bespoke keep gates (props 1–10) show **+10–40 point** lift versus
ungated. Faces are verified. Most ungated Goblin hit rate is 0.5-line
counting props — the gates exist because the raw book does not.

Pitcher volume is L10-primary (L5 often hurts). Expected live rates,
not the ~90% L5-gated backtest slices: Ks ~80%, HA ~83%, Walks ~81%,
Outs L10-primary; ERA still ~88%; H+R+RBI / Hits / TB ~65–70%.

Postseason is allowed on league tickets, priced at the cell minus 3,
tagged `postseason=1`, graded on `gated_goblin_postseason`, kept out
of the 71/127 book, **one slip a day until 30 are graded**. Preseason
stays blocked.

Do not retune keep gates on the Series sample. Offseason is for the
projection layer.

## 2. Factors behind every MLB prop

| Layer | Factors | Why |
|---|---|---|
| Opportunity | Lineup spot, pitch-count leash, platoon | PA and BF are the minutes |
| Opponent | Starter hand, K%, pitch mix; opp bats for pitcher HA | D is inverted for hitter Ks |
| Park / weather | Wind, Coors, dome | TB / HR / ERA move together |
| Umpire | Extra-strike / extra-ball crews | Ks and walks |
| Bullpen / total | Implied runs, closer usage | Late counting stats |
| Data quality | Face labels (clean), stub boxes | Mature vs NFL / NHL |

Logical order on a slate:

1. Keep 1–10 only. Fade 11–21 (Runs, Singles, batter Walks, PA, RBI,
   1st-inning, HR/2B/3B/SB) and all Standard / Demon.
2. Hitter Hits / TB / H+R+RBI: BA≥.275 (min 80 AB) + L5=5 + opp pitch
   Weak|Below.
3. Hitter Ks: K%≥28 + L10≥8 + opp pitch Elite|Above (invert).
4. Pitcher volume: L10≥8 + own-pitch / opp-bats as in the keep file.
5. Price pitcher props from the **parent L10 cell**, not the L5 slice.

## 3. What moves each prop

| Prop | Drivers |
|---|---|
| Pitcher Ks / walks / pitches / outs | Leash, K% vs lineup, umpire, own-pitch tier |
| Hits allowed / ERA | Opp bats, park, pitch mix |
| H+R+RBI / Hits / TB | Lineup spot, BA, opp pitch, wind |
| Hitter Ks | K%, stingy vs leaky staff (invert) |
| HR / SB / 1st-inning | Fade. Variance or tiny n |

## 4. What is already attached

| Piece | Where | Status |
|---|---|---|
| Keep file | `utils/mlb_keep_gates.py` | Live. Faces verified |
| Matchup / BA / K% | step8 + `mlb_prop_matchup` | Live |
| Weather / wind | MLB weather attach | Live; `wind_out_hot` observe |
| Postseason tag | league ticket path | −3 pts, one slip/day |
| Standard | policy | Off Goblin-70 |

## 5. Gaps — offseason project

After the Series:

1. **pybaseball / Statcast** — exit velocity, barrel rate, pitch mix,
   platoon splits. The rate layer behind BA / K%.
2. **Umpire assignments** — extra-strike crews for pitcher Ks / walks.
3. Build the projection (PA / BF × rate × park × umpire) and
   **write the calibration test before the run**: train 2021–2024,
   test 2025, Brier / decile / ECE per keep prop. Same discipline as
   `ml/prereg/nfl_volume_projection_2026-10-25.md`.
4. Snapshot lineup + starter + umpire pre-first-pitch.

Do not invent a winter keep-gate retune. The cells already lift.

## 6. How the ML fits

```
PA or batters-faced
        ×  shrunk Statcast rate
        ×  starter / park / wind / umpire
        →  distribution
        →  P(over) on each Goblin rung
        →  EV at the scraped N-correct quote
```

You have all winter. Ready on the first spring board, not built in
April.
