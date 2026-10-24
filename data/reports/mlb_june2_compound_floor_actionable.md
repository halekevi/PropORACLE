# June 2 cliff — compounded floor + hit_prob_actionable≥0.65

## 1) Ticket-count reconciliation

- MLB Goblin board: **1677**
- Floor (elig) survivors: **1049** (62.6%)
- Floor + `hit_prob_actionable≥0.65`: **531** (31.7% of board, 50.6% of floor)
- Mix-weighted indep survival from 238 June-1 Goblin tickets:
  - floor only → **57.1** predicted
  - both/board single-shot → **10.5** predicted
  - chain (floor then 0.65 among survivors) → **7.8** predicted
- Actual June 2 MLB all_goblin tickets: **3**
- Unexplained after both filters: **~7** tickets (still not closed)

## 2) Concentration (doubly-filtered)

- **floor-only**: n=1049, games=18, top-3 share=22.2%, HHI=0.067
- **doubly-filtered**: n=531, games=16, top-3 share=25.4%, HHI=0.068

## 3) Goblin vs Standard clear @ 0.65

- Goblin all-board clear≥0.65: **31.7%**
- Standard all-board clear≥0.65: **35.0%**
- Delta (G−S): **-3.3 pp**
- Goblin among elig clear≥0.65: **50.6%**
- Standard among elig clear≥0.65: **35.0%**

## Read

`f68496216` looks **symmetric** on the 0.65 cut — not a second Goblin-only tax. Compounding with the floor still fails to explain 238→3; second mechanism remains downstream of both filters.