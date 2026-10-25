# NHL prop assessment

Working note next to `prop_skeleton.md`. Nothing here is a keep gate.
NHL has **no keep file**. Goblin-70 stays off. The mixer is **held**
(`PROPORACLE_NHL_MIXER_TICKETS`).

## 1. Where NHL stands

No edge found. The generic L5=5+L10≥8+D stack does not lift Goblin
OVER (61.9% n=21 vs 61.3% ungated) and **hurts** Standard UNDER
(51.2% n=285 vs 58.6% ungated, below the 57.7% 3x breakeven). The
mixer was still seeding NHL Goblin OVER; that path is pulled.

The L10≥8 Standard UNDER cell (70.9% n=1266) is not a replacement
gate. Face audit 2026-10-24: graded rows defaulted to Standard on 45
dates 2026-02-25..06-15. Those “Standard UNDER” rows were Demon rungs.
Forward read is 53.4%. `NHL_STANDARD_UNDER_CELL_FACES_VERIFIED` is
false. Do not re-price from the Feb–Jun sample.

On the live board the opportunity layer is empty where it matters:

- `goalie_name` 0% filled (Daily Faceoff / confirmed starter not wired)
- `game_total` 0% filled (Odds API / step5b)
- NST cache missing TB
- ESPN vs NHL API box disagreements (hundreds of rows; see
  `data/reports/nhl_prop_agreement.json` and the SOG regrade that
  flipped 1,475 matched shots)

Work is **inputs and conflict resolution**, not a new stack.

## 2. Factors behind every NHL prop

| Layer | Factors | Why |
|---|---|---|
| Opportunity | TOI, PP1 vs PP2, line combinations | NHL minutes. A PP1 bump is the usage vacuum |
| Shot share | Individual Corsi / xG, teammate shot quality | SOG is a volume prop |
| Opponent | Shots allowed, confirmed starting goalie | Backup goalie changes every skater line |
| Script | Game total, back-to-backs, score effects | Tired third period, trailing shot volume |
| Data quality | ESPN vs NHL API, NST holes, face labels | Fake L5 and wrong grades |

Logical order on a slate:

1. Require a real `pick_type`. Default-label dates stay out of any cell.
2. Drop goals / assists / points as ticket seeds. Variance is the product.
3. Prefer SOG only when TOI + line + goalie are filled.
4. Price anything at the N-correct quote. NHL Standards stay 2–3 pick.
5. Do not re-enable the mixer until a keep file exists **and** faces verify.

## 3. What moves each prop

| Prop | Drivers |
|---|---|
| Shots on goal | TOI, shot share, PP unit, opp shots allowed, trailing script |
| Goals / points / assists | High variance. xG helps ranking, not a 70% cell |
| Blocks / hits / PIM | Noisy, low lines |
| Saves / goalie props | Confirmed start first. Backup last-minute swaps void a lot |
| Faceoffs | ESPN stored FO% as a count (`nhl_prop_agreement.json`). Do not grade until the column is the win count |

## 4. What is already attached

| Piece | Where | Status |
|---|---|---|
| Mixer hold | `utils/ticket_sport_holds.py` | Default on. Override `PROPORACLE_NHL_MIXER_TICKETS=1` |
| Face window | `NHL_UNVERIFIED_FACE_WINDOW` | 2026-02-25..06-15 unverified |
| NST attach | `step4b_attach_nst_context_nhl.py` | Live when the cache has the team |
| Goalie attach | `step3b_attach_goalie_nhl.py` | Column exists; fill is empty without Daily Faceoff |
| Game totals | step5 `enrich_with_game_totals` | Blank when Odds API is out |
| Box agreement | `data/reports/nhl_prop_agreement.json` | Conflicts logged, not all resolved |

## 5. Gaps, in build order

1. **Daily Faceoff** — confirmed starting goalies and line combinations.
   `goalie_name` has to be a hard input, same shape as NBA T−30 starters.
2. **Game totals** — Odds API top-up so script exists.
3. **Resolve ESPN vs NHL API** before trusting SOG / hits grades or L5.
4. **MoneyPuck** — free xG, shot attempts, ice-time splits. The rate layer.
5. **NST holes** — TB and any other missing tricodes fail loud, not silent.
6. Keep file only after (1)–(3). SOG first. No Standard UNDER from the
   contaminated cell.

## 6. How the ML fits

```
TOI × line / PP unit
        ×  shrunk shot rate (MoneyPuck / NST)
        ×  opponent + confirmed goalie + total
        →  SOG distribution
        →  P(over) on each rung
        →  EV at the scraped N-correct quote
```

Calibrate on past NHL seasons with pre-puck TOI / line / goalie
snapshots. Do not train on post-game ice time.
