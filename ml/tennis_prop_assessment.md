# Tennis prop assessment

Working note next to `prop_skeleton.md`. Keep file:
`utils/tennis_keep_gates.py`. Goblin faces **verified**. Standard
faces **not**. Total Games is **held** from tickets
(`PROPORACLE_TENNIS_TOTAL_GAMES_TICKETS`).

## 1. Where tennis stands

Sackmann / Tennis Abstract serve history is **151 days stale**. Aces
and double-fault L5 are empty on the live board. Only about 60 of 107
rows had an L5 on the last counted slate — the feed, not the players.

Goblin gates stand on corrected board-opponent attaches:

- Games Won discount (Standard − Goblin ≥ 4): 72/99 (72.7%, band
  64–82%). Ungated 819/1224 (66.9%). Price cap 0.62 until a new week
  moves the band. Old 83% / 76.3% were wrong-match inflated.
- Total Games L5≥4+L10≥8: **146/208 (70.2%) on the keep-file join**,
  but on face-verified dates the same cell reads **35/62 = 56.5%**
  against that 70.2% price. A 2-leg at 1.6x needs ~79%/leg. Held until
  re-measured on a face-verified step8 join. Games Won is unaffected.

Standard UNDER Aces / DF faded after serve-actual backfill (missing→0
had printed ~90%). Post-backfill ~57% / ~29%. Standard OVER Games Won
is the thin L10≥8 + gap / 2nd-won% keep only.

## 2. Factors behind every tennis prop

| Layer | Factors | Why |
|---|---|---|
| Format | Best-of-3 vs best-of-5 | BO5 Total Games must not list from BO3 last-5 |
| Serve / return | Ace rate, hold %, opponent return | Aces/DF need a live feed |
| Surface | Hard / clay / grass | Serve stats do not transfer |
| Closeness | Match moneyline, rank gap | Even matches go long; 6-2 6-2 is the lopsided default |
| Data quality | Stale Sackmann, wrong-opponent attach, face labels | 151-day hole; Apr 11–Jul 8 unverified for Standard |

Key insight for **Total Games**: the match moneyline is probably the
best single predictor. tennis-data.co.uk has years of results with
closing odds. Test that **before** building a serve model.

Logical order on a slate:

1. Fix the serve feed. Empty L5 is not “no signal.”
2. Games Won Goblin only on the verified keep (discount or L10 branch).
3. Total Games stays off tickets until the 56.5% vs 70.2% gap is closed.
4. Fade Aces / DF until the feed is current and the post-backfill rates
   are re-cut.
5. BO5 line > 31.5 does not list from a BO3 window.

## 3. What moves each prop

| Prop | Drivers |
|---|---|
| Games Won | Serve hold, opponent return, discount vs Standard, rank-gap fade (≥50 spots better → sit) |
| Total Games | Moneyline / closeness, format, surface. Hold until re-measured |
| Aces / DF | Live serve stats. Stale feed → empty L5. Standard UNDER faded |

## 4. What is already attached

| Piece | Where | Status |
|---|---|---|
| Keep file | `utils/tennis_keep_gates.py` | Goblin verified; Standard not |
| Total Games hold | `utils/ticket_sport_holds.py` | Default on |
| Face window | `TENNIS_UNVERIFIED_FACE_WINDOW` | 2026-04-11..07-08 |
| Serve backfill | `scripts/backfill_tennis_serve_actuals.py` | Historical zeros fixed; live feed still stale |
| Rank / lefty / 2nd-won | tennis step6 | Lefty / 2nd-won often blank on live legs |

## 5. Gaps, in build order

1. **Replace Sackmann.** tennis-data.co.uk (results + closing odds)
   and Tennis Abstract or Sofascore for current serve stats.
2. **Moneyline → Total Games** — pre-register the test (train through
   2024, test 2025, Brier / decile on P(total games > T) from closing
   ML). Write the spec before the run. Do not use the 70.2% price as
   the prior.
3. Re-measure Total Games L5≥4+L10≥8 on face-verified step8 after (1).
4. Snapshot pre-match serve / ML the same way NBA minutes lock.

## 6. How the ML fits

```
format × surface
        ×  serve / return (live feed)
        ×  match closeness (closing ML)
        →  Games Won / Total Games distribution
        →  P(over) on each rung
        →  EV at the scraped N-correct quote
```

Total Games is a match-level prop. Player L5 is the wrong first
feature when the moneyline already says 6-2 6-2 or 7-6 6-4.
