# NBA prop assessment

Working note next to this folder’s `README.md`. How to judge every PrizePicks
NBA face, what is already attached, and what is worth building. Nothing here
is a keep gate. NBA Goblin-70 stays shadow / hold.

Three observe-only volume-floor tracks (2026-10-25), each frozen and
separate: rebounds, assists, and attempts (FGA/2PA/3PA/FTA). Standard
or Demon OVER, top-2, line under the L5 min. Not pooled with NFL.
Not on `/tickets`.

Sunday 2026-10-25 slate: Odds API 401 (quota), so spreads / totals / blowout
risk were blank. LAL sat Saturday — the earlier B2B flag on Reaves was wrong.

## The number that frames the work

Every face is **opportunity × rate × matchup ± variance**. The line is the
market’s guess at all four.

| Book / shape | Breakeven / leg | Best current sleeve | Gap |
|---|---|---|---|
| Goblin Power-3 @ 1.65x | 84.6% | Scout `proj_ev_edge` 78.6% n=509, Wilson 0.748 | **~6 points**. Filtering last-season boxes will not close it. |
| Goblin Power-3 observe @ 2.00x | 79.4% | Rebounds scout 80.7% n=166, Wilson 0.741 | Point estimate close; Wilson does not clear. |
| Standard Power-2 @ 3x | 57.7% | Scout top-N UNDER 57.5% n=40, Wilson 0.422; top-3/day 53.0% n=249 | **~5–10 points** on Wilson. Point estimate on the thin top-N sleeve is at 3x; n=40 is not a book. |
| Standard Power-3 @ 6x | 55.0% | Same thin UNDER top-N, EV +0.14 on the point estimate | Not proven. Wilson 0.422. Top-3/day UNDER is now 53.0% (EV −0.11 at 6x). |

Goblin needs new information (minutes, lineup, market). Standard 6x is the
first place a minutes model could matter. Re-run scout with `oct_n`, `wGap`,
and `role_fit` on **both** books. Promote only if Wilson clears on dates the
search has not seen. NBA/WNBA full-game Standards stay **3-pick Power at 6x
or 2-pick at 3x — no 4–6 leg slips**.

## 1. Factors behind every prop

| Layer | Factors | Why |
|---|---|---|
| Opportunity (minutes) | Starter vs bench, recent minutes, rest / B2B, foul trouble, blowout (spread), injury | Largest lever. A 4-minute swing is ~3 points. |
| Usage / role | Usage %, shot / assist / rebound share, teammates out, new team or role | Why last-season L5 misleads (Porter, Jerome). |
| Rate | Per-minute production, shot profile, FT rate | More stable than totals. What `proj_mu` models. |
| Matchup | Opp D by **stat and position**, pace, reb / 3PA allowed, rim protection | One Weak/Elite label is coarse. |
| Game environment | Total, spread, home/away, travel, referee | High totals lift Overs. Big spreads sit starters. |
| Variance | Count vs continuous, 0.5-line “zero props” | Steals / blocks / TO are noisy. Points are smoother. |
| Market | PP vs sharp, line movement, Goblin / Standard / Demon rung | Often the strongest outside signal. |
| Data quality | L5 mixing seasons, DNP zeros, preseason, stale cache, combo-player attach | Fake 5/5s (Boston 4/5, Gill DNP, Dëmin+Toppin O4.0). |

Logical order on a slate (no new vendor):

1. Drop two-player combos and `l5_zero_or_dnp`.
2. Prefer **2 October RS games** over `ROOOO` Finals pads.
3. Prefer **primary OVER / non-primary UNDER** (`role_fit=aligned`).
4. Rank by October-weighted gap (`wGap`), then raw L5avg − line.
5. Veto OVER into Elite D unless the prop is opponent-proof.
6. Recheck B2B minutes before treating Platinum as tonight.
7. Price any keep idea at the **N-correct** quote, not at last spring’s 5/5.

## 2. What moves each board category

| PrizePicks category | Drivers beyond minutes |
|---|---|
| Points, FGA, FGM, 2PA / 2PM | Usage, shot share, opp D vs position, pace, teammates out |
| 3PM / 3PA | 3PA volume is stabler than makes; opp 3PA allowed. Prefer attempts as signal. |
| Rebounds (OReb / DReb) | Rebound %, opp FG% (misses), opp rebound rate, lineup size |
| Assists | Assist %, potential assists, teammates’ shooting, ball-handler role |
| Steals, blocks, stocks | High variance, low lines. Opp TO rate, rim attempts allowed |
| Turnovers | Usage, pressure / steal rate. Thin edges |
| FTM / FTA | FT rate, opp foul rate, referee crew |
| PRA / Pts+Reb / Pts+Ast / Reb+Ast | Sum of correlated parts. Smoother Overs; more ways to lose Unders |
| Double-double / triple-double | Threshold. Needs the second stat to 10 |
| Quarters with 3+ / 5+ points | Quarter history only — same leak risk as NBA1Q |
| Fantasy, combo-player | Off tickets |

Demon OVER stays off Goblin-70 / mixer. Attempt / volume Demons (FGA, 2PA,
3PA, FTA, and related made) may appear on the card-window L5=5 observe list
until n≥100. A Demon UNDER row is invalid.

## 3. What is already attached

| Piece | Where | Status |
|---|---|---|
| Per-minute × minutes projection, shrinkage, Poisson/NB/normal | `utils/nba_player_projection.py` | Prices every rung from one `proj_mu` |
| Usage + injury vacuum | step4b / 4d | Flags, not a list gate |
| Role fit (primary OVER / non-primary UNDER) | `utils/nba_role_fit.py` | Observe until n≥100 |
| Card-window L5 (RS+playoffs, no preseason) | `utils/nba_early_l5.py` | Gate window = PP card |
| October-weighted rank + raw gap | `outputs/<date>/nba_l5eq5_oct_gap_rank.txt` | Oct RS 3×, playoffs 1×, last RS 0.75× |
| D tier, pace, game lines | step6 / 6b | Blank when Odds API is out |
| Period history | NBA1Q / NBA1H | Separate slates |
| Ledger face + attach status | `proporacle/data/leg_ledger.py` | Analysis drops invalid attach |
| Quote-priced scout | `ml.nba_scout_quotes` | Goblin +10 pts vs gate; still −EV at 1.65x. Standard 3x/6x do not clear Wilson. `slate_minutes` join is 0 until tonight grades. |
| Gate search | `ml.discover_gates` + pysubgroup / imodels / optbinning / SHAP | Local only; not wired to keep files |
| `cross_edge` vs PP | enrichment / tickets render | Code exists; needs live sharp lines |

## 4. Gaps, in build order

New **information**, not another booster on May boxes.

1. **Top up Odds API** — account action. Restores totals / spreads / blowout
   and feeds `cross_edge`. Do this before Pinnacle player props or tracking.
2. **Confirmed / projected starters + slate minutes (in)** —
   `utils/nba_slate_minutes.py` on step8. ESPN depth chart (projected).
   Official starters post ~T−30 (`scripts/refresh_nba_starters_tminus.py`)
   so `tonight_starter=confirmed` arrives while a slip can still change.
   After tip the boxscore starter is grading-only. Injuries include GTD.
   Transparent minutes: role MPG ± starter floor, B2B, questionable,
   OUT=0, star-out bump. Each run appends
   `nba_slate_minutes_snapshots.jsonl` with `as_of` + `pre_tip`. Train
   later only on the pre-tip snapshot — never on post-tip confirmed.
   Scout joins `slate_minutes` only where that snapshot exists.
3. **Next scout: 2026-11-20, not before.** Oct 25 closed the NBA
   candidate. Standard UNDER top-3/day 55.8% → 53.0% was a `proj_edge`
   re-rank after the morning box ingest, not face/attach/DNP cleanup
   (ungated 16,477/8,781 held) and not `role_fit` / `wGap` (never
   wired). February as-of isolation is **CLEAN** — dropping 13,759 later
   boxes does not change 2026-02-19 `proj_edge`. The ingest added or
   corrected *older* rows, not lookahead.    History through Oct 20 is frozen under `data/cache/nba_proj_edge/`.
   New slates freeze at build time, per game, next to the minutes snapshot
   (step8 / T−30). A row locks at that game's tip; later refreshes only
   update games that have not started. Each row keeps its own `frozen_at`.
   See
   `nba_std_under_558_to_530.md` and `nba_proj_asof_isolation.md`.
   The next pass joins `slate_minutes` only on pre-tip snapshots.
4. **`nba_api` tracking** (free) — potential assists, rebound chances,
   touches, shot zones. Sharpens AST / REB.
5. **On/off from pbpstats** (free) — turns usage vacuum from a flag into a
   number when a named teammate sits.
6. **Referee assignments** (morning slate) — FTA and foul-prone bigs’ minutes.
7. **Fix `line_history` / `_mkt_key`** — movement as a feature.
8. **SHAP factor card** on each listed leg: `minutes +3 (teammate out), pace
   +1, opp reb −2`. Assessment aid, not a ticket stamp.

Do **not** bother yet: Dunks / triple-double models, two-player combo L5,
generic chat-over-the-board, or pasting a slice into a keep file.

## 5. How the ML pieces fit

```
minutes model  →  minutes ± range
rate model     →  per-stat, shrunk, matchup / pace adj
distribution   →  P(over) on every Goblin / Standard / Demon rung
isotonic cal   →  honest probs on face-verified, attach-OK legs
vs market      →  cross_edge + EV at the scraped N-correct quote
discover_gates →  residuals: where the model is systematically wrong
```

Guardrails stay the same as `README.md`: walk-forward by date, Wilson,
Benjamini–Hochberg, real quotes on unseen dates. SHAP explains a leg; it
does not approve it.

## Next build

Scout is frozen until **2026-11-20**. Do not re-run the three books before
then — the Oct 25 pass closed the NBA candidate.

```powershell
# not before 2026-11-20
py -3.14 -m ml.nba_scout_quotes
py -3.14 -m ml.nba_scout_quotes --book "Standard UNDER"
py -3.14 -m ml.nba_scout_quotes --book "Standard OVER"
```

No sleeve beats the scraped quote out of sample. Live play stays small or
shadow-only until an experiment clears the 10-11 bar.

Forward experiments (do not peek-rerun):

- NFL Standard UNDER shadow (tackles, pre-registered 2026-10-24)
- NBA pre-tip minutes snapshots, starting tonight. Next scout
  **2026-11-20**.

Waiting-time infrastructure (makes the November test honest): Odds API
top-up, tree collapse, the failing test pile, `line_history` / `_mkt_key`.

T−30: `scripts/refresh_nba_starters_tminus.py --date 2026-10-25` before
LAL@UTA so confirmed starters land in the snapshot. After tip is
grading-only.

If Standard 6x later clears Wilson on dates the search has not seen, that is
the first NBA candidate for the forward shadow — observe only,
**3-pick Power at 6x (or 2-pick at 3x); no 4–6 leg slips**.
