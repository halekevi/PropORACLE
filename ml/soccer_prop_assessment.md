# Soccer prop assessment

Working note next to `prop_skeleton.md`. Keep file:
`utils/soccer_keep_gates.py`. **Faces unverified**
(`SOCCER_KEEP_GATE_FACES_VERIFIED = False`). Saves are list-only.
Nothing soccer can be re-priced until DNP voiding is honest.

## 1. Where soccer stands

Keep cells (Shots 16/21, SOT 46/68, Saves 205/343) were taken from
Goblin-labeled rows. Default-label dates 2026-05-09..06-16 and
laddered Standard 03-26..04-11 could not inflate those Goblin numbers
by inclusion, but:

- Any soccer **Standard** cell from Mar 26–Jun 16 is label-only
  (Standard UNDER Shots 69.7% there vs 53.2% on real faces).
- Shots / SOT L5 cells **cannot be re-measured from the ledger**.
  Soccer stores hits in `last5_over`; ledger `l5_over` is blank on
  most rows. Verified L5=5 Shots Goblin is 7/11.
- Goalie Saves stay off tickets. Goblin 1.5 is 205/343 (59.8%, band
  55–65%), under the ~77% a 3-leg at 2.2x needs. The L5≥4 cell 21/26
  is not a price.

DNP-zero grading and L5 history are still the standing leftover.
Unknown minutes treated as zero invents Under hits and fake L5=5.
**Fix that first.** Do not quote the three keep cells as prices again
until they are re-measured on a face-verified step8 join after DNP
is correct.

## 2. Factors behind every soccer prop

| Layer | Factors | Why |
|---|---|---|
| Opportunity | Confirmed start (~T−60), expected minutes, sub risk | Biggest spring finding. Bench/cameo shots are a different bet |
| Rate | Shots / xG / SOT per 90 | Stable only for starters with a role |
| Opponent | Shots allowed, implied team goals | Weak keepers lift SOT more than raw “Weak D” |
| Role | Penalty, set-piece, cup rotation, international break | Competition type is not a footnote |
| Data quality | DNP zeros, face labels, thin-history cups | World Cup tags already block via `competition_history` |

Logical order on a slate:

1. Void DNP only with positive minutes / lineup evidence. Rebuild L5
   from those voids.
2. Require a confirmed starter for SOT / Shots tickets.
3. Keep props are Shots and SOT only. Goals / G+A / assists / fouls fade.
4. Saves may list at L5≥4 + settlement; they do not ticket.
5. Price at the N-correct quote after the cells are re-measured.

## 3. What moves each prop

| Prop | Drivers |
|---|---|
| Shots | Starter minutes, shot share, opponent allowed, implied goals |
| Shots on target | Same, plus finishing / xG. Ticket still needs L5=5 |
| Saves | Posted keeper must start; settlement on the competition. Off tickets |
| Goals / G+A / assists | Binary / thin. Fade |

## 4. What is already attached

| Piece | Where | Status |
|---|---|---|
| Keep file | `utils/soccer_keep_gates.py` | Live list/ticket rules; prices flagged unverified |
| Face windows | `SOCCER_UNVERIFIED_FACE_WINDOWS` | 03-26..04-11 and 05-09..06-16 |
| Competition block | `utils/competition_history.py` | Thin cups out |
| DNP void helper | `utils/dnp_zero_void.py` | Generic; soccer grading / L5 history still pending |
| Starter / Off | step8 usage | Off = STARTER or HIGH_VOL or top-3 for SOT list |

## 5. Gaps, in build order

1. **DNP voiding in grading and L5 history.** First. No new cell until
   this is done.
2. Re-measure Shots / SOT / Saves on face-verified step8 after (1).
3. **FBref / Understat** — xG and shots per 90 for the rate layer.
4. **Sofascore or official lineups** — confirmed starters as a hard
   input, snapshotted before kickoff (same as NBA minutes / NFL volume).
5. Own-defense and opponent-attack stay retired. Do not revive them
   on the unverified book.

## 6. How the ML fits

```
confirmed start × expected minutes
        ×  shots/xG per 90
        ×  opponent + implied goals
        →  Shots / SOT distribution
        →  P(over) on each rung
        →  EV at the scraped N-correct quote
```

Starter status is not a badge. It is the opportunity term.
