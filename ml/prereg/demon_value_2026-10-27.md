# Pre-registration: Demon value scanner (alert only)

**Registered 2026-10-27.** Runner: `py -3.14 scripts/scan_demon_value.py --date YYYY-MM-DD`

Alert only. **`placed` is always false.** Never writes `/tickets`, Goblin-70,
or the mixer. Demons stay off those paths. A later dated prereg is required
before any Demon is ticketed.

## Why

PrizePicks prices each Demon on its own. There is no one-step Demon table
(the 2026-10-27 CDP 3.5/40/90/215/540 scrape was Balance-as-To-Win).
A Demon is only interesting when its measured hit rate is clearly above
what that slip's Power N-correct implies.

## Implied rate (what PrizePicks thinks)

Two Standards pay **3x** on a Power 2-pick, so PrizePicks prices each
Standard at `1/√3 = 57.7%`. Any Standard is a reference with a known price.
`p_ref = 0.577`.

1. Sanity first: scrape two Standards from different teams. Power
   **"2 correct pays"** must be 3x (To Win / entry, never 1st-place,
   never Balance). That recovers 57.7%. If it does not, the parser is
   still off and the scan aborts.
2. For each Demon, scrape a 2-leg Power slip: that Demon plus one
   Standard from a **different team** (PrizePicks requires two teams).
3. `implied_p = 1 / (quote_2leg × 0.577)`.
4. No clean scrape → skip that Demon (fail closed).

Cheap formula check (does not ticket):

1. Same Demon + two different-game Standards: quotes must be identical.
2. The reference Standard is from a **different game** than the Demon
   (Towns NYK + Robinson DET is same-game even though they are two teams).
   Every implied price uses that unconnected Standard, not only the
   two-Demon product check.
3. Two Demons from different games, scraped in one pass: each Demon + a
   different-game Standard, then the two Demons together. All four legs
   in different games. Quote ≈ `1 / (p_a × p_b)`. Accumulate **3–5
   unconnected pairs** over nights. One matching pair is good; a handful
   is proof. Reed + Ausar is same-game DET and does not count. Incomplete
   or same-game-Standard rows are retried, not counted.

Scanner price stays `p_ref = 0.577` from Demon + Standard. `formula_ok`
stays true. A low two-Demon quote only means stacking Demons pays less.

The scraper adds picks, reads the payout, and clears the slip. It
**never presses Submit.**

## Measured rate (what the data says), as-of tonight

- Hit rate on this exact line. Current season weight 2, last season
  weight 1. Regular season + playoffs only (no preseason).
- Shrunk toward minutes × per-minute rate (Poisson/Normal prior, k=8).
- Wilson 95% lower bound on unweighted hits/n. Need ≥ 15 games.
- Confirmed role or minutes tonight. No injury tag. Not a team change
  in the last 5 games.

## Alert bar

Wilson low ≥ `implied_p × 1.15`. Same-game or same-team vs the
reference is **flagged, not blocked**.

When one appears: play it as a **2-pick with small stakes**, one
clear-value Demon plus the best other leg. Do not stack Demons.

## Proof

Log every alert to `data/reports/demon_value_forward.jsonl` with
`placed=false` and `result=pending`, filled HIT/MISS after the game.

After **50 graded alerts**, compare hit rate (Wilson low) to the
average `implied_p`. If alerts do not beat their price, the scanner
turns off.
