# Soccer prop assessment

Working note next to `prop_skeleton.md`. Keep file:
`utils/soccer_keep_gates.py`. **Faces unverified**
(`SOCCER_KEEP_GATE_FACES_VERIFIED = False`). Saves are list-only.
DNP voiding is in code (`312608837`). The leftover is a **re-measure**:
soccer step4 has not been rerun, so L5 history can still carry the old
zeros, and the Shots / SOT keep cells are still unverified.

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

DNP-zero voiding and L5 skip of `minutes_played == 0` landed in
`utils/dnp_zero_void.py` and step4. **That does not re-measure the
cells.** Until soccer step4 is rerun on a live slate and Shots / SOT
are counted on face-verified dates, do not quote the three keep cells
as prices.

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
| DNP void helper | `utils/dnp_zero_void.py` | In grading and L5. Step4 rerun + re-measure still pending |
| Starter / Off | step8 usage | Off = STARTER or HIGH_VOL or top-3 for SOT list |

## 5. Gaps, in build order

1. **Re-measure (next).** Rerun soccer step4 so L5 is rebuilt without
   DNP zeros, then recount Shots and SOT on face-verified dates. Saves
   stay off tickets. No new price until that join exists.
2. DNP voiding in grading and L5 history — **done in code**. Does not
   replace (1).
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
