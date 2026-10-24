# Game lines as environment

Game spread, total, and moneyline win probability are environment on a prop. They are not ticket gates. NBA spread buckets from the March–June 2026 window failed a regular-season check, so they are not model knots either.

This is the same **soft-signal / hard-gate** split as THIN_CLEAR_OVER (visible, not ticketed) and the ticket exposure anchor-cap (structural teeth). Game lines are a third instance of that rule — not a new principle.

## Freshness (before any live attach)

- Slate calendar is **US/Eastern**, not UTC. A 9PM ET job on Sep 24 must fetch `2026-09-24`.
- Default fetch sports: **MLB, NBA, CBB, NHL, Soccer** (MLB was missing from the old default list).
- Daily STEP C0 runs `fetch_game_lines.py --date $Today --refresh`, then `assert_game_lines_fresh`. Expected sports whose newest `game_date` lags the slate by more than **2 days** fail the gate.
- `attach_market_lines_to_grades` **refuses** to join when that gate fails. Stale attach would look identical to fresh attach with no visible sign — so refuse, do not warn-and-continue.

## Presentation (locked)

Columns may land on a prop as an **unsorted reference join**. They must not drive a ranked view (explorer sort, browse tool, “best script” table). A prose caption is not enough — that is the gap-sort lesson.

## Closed leads (do not reopen)

These are finished searches. Quoting any of them as a future lead reopens a finished search.

1. **Prop-type slices.** The pass on 24 Sep 2026 cut dozens of prop × spread × total cells at 60–150 rows. Those rows are a handful of games, and the search had enough cells that some were going to look high. Do not reopen any of those cells, including ones that printed near 70%.

2. **Unders on favorites by 10+.** Pooled lift −3.9% on 99 games, 95% interval −7.6% to −0.1%. That is one of several pooled contrasts and is only just clear of zero. Regular season: −1.9% on 89 games, interval through zero. Starters: −0.8% on 94 games, interval through zero. **Not confirmed.** Not a blowout-rest story and not a lead.

## What is stored

One row per game in `data/cache/game_lines.db`:

| Field | Meaning |
|---|---|
| `spread` | Home spread. Negative means the home team is favored. This is not the prop player's number. |
| `total` | Game total from the core odds `overUnder` field. |
| `home_win_prob`, `away_win_prob` | Moneyline, vig removed. This is the favorite-strength field for every sport. MLB run line and NHL puck line stay near 1.5 and are not the feature. |
| `provider` | Book name on the core odds object. |
| `is_closing` | From the feed/event, not store time. Prefer a close/open marker on the core payload. Else final event → close; pre-tip / in-progress → not close. Spot-check: compare a near-tip store to the post-final core number on the same game; they should match if final = close. |
| `captured_at` | When this row was written. Not the sportsbook's own timestamp. Does not redefine close. |
| `home_team_id`, `away_team_id` | ESPN team ids. |
| `event_id` | ESPN event id. Cluster on this. |

The prop join writes the player's view: `game_spread` (negative means that player's team is favored), `game_total`, `team_win_prob`, `line_provider`, `line_is_closing`, `line_captured_at`.

`team_crosswalk` maps every known abbreviation to an ESPN team id. `SAS` and `SA` are both San Antonio (id 24). The same pattern covers `NYK`/`NY`, `NOP`/`NO`, `GSW`/`GS`, and Utah Hockey `UTAH`/`ARZ`. A grade abbreviation is used only when it maps to exactly one id. The join itself is the id.

### Closing-line provenance

`is_closing` follows **what the feed returned**, not when we wrote the row.

- Prefer a close/open marker or book timestamp on the core odds payload when present.
- Otherwise: if the ESPN event is already **final** at fetch time, the core odds number is treated as the **close** (`is_closing = 1`). Post-game fetches are often the best close we have — the scoreboard drops live odds, and the core feed keeps the closing spread/total.
- If the event has **not** started (or is in progress), the capture is **not** close (`is_closing = 0`), unless tip is within a few minutes and the payload is otherwise unmarked.
- `captured_at` is only our write time. It must not redefine close.

Rows fetched under this schema fill `provider` and `is_closing` that way. Rows already in the database from before this schema have team ids stamped on, but `provider`, `is_closing`, and `captured_at` stay empty until that date is fetched again.

**Any test that assumes a closing line must either exclude rows where `provider` is blank or `is_closing` is null, or flag them as a separate pool.** Mixing them with labeled closes blurs the numbers.

Store-time vs tip (diagnostic only, not the close rule). On a sample of 49 NBA games with tip times from ESPN (March–June 2026):

| When `updated_at` sits relative to tip | Games |
|---|---:|
| 0–24h before tip | 35 |
| More than 48h after tip | 14 |

The 14 post-tip rows are the ones most likely to already be closes (final event → core odds close). The 35 pre-tip rows are the ones that could still have moved. Median store lag is about 11 hours before tip. Across all 363 NBA game rows by calendar day: 246 written the same day as `game_date`, 16 within 1–2 days, 101 three or more days later. The **store-time** pool is mixed; that noise supports leaving the unlabeled NBA findings closed. A closing-line retest starts only after refetch fills `provider` / `is_closing` from the feed rule above, and the join keeps unknown-meta rows out.

## Soccer

Spread and total are read from the core odds object only. The `details` string is not parsed. It mixes moneyline and spread text (`MAN -270` is a price, not a goal line). A spread is not invented from win probability.

137 soccer rows whose absolute spread was greater than 4 were deleted. They were moneylines stored as spreads. They were not repaired. 31 soccer rows remain, all with a goal-line-sized spread or a null spread. Most of those have team ids.

Graded soccer props are stored as club names (`JUVENTUS`), not abbreviations. A one-time name → ESPN id crosswalk per league, built from scoreboard team names, would attach them without a grader change. That is a few hundred rows of review. Soccer is second in the backfill order after that crosswalk (see eligibility). Do not fuzzy-match names onto the deleted moneyline rows.

**200-game floor for Soccer:** the primary three contrasts are scored on **pooled Soccer** (all leagues in the closing-line pool together). Pooled clears the floor once lines attach. A **per-league** cell is not reportable until that league alone has 200 games in the treatment bucket and 200 in the comparison. Do not promote an EPL-only (or any single-league) look into the pooled pass.

## NBA check (confirmatory)

Window: 6 Mar 2026 through 13 Jun 2026. Decided OVER/UNDER rows for NBA, NBA1Q, and NBA1H. One row per player, prop, line, side, team, and date. 113,667 of 122,294 rows matched a team id and a spread (92.9%). None of those rows have close meta (see above).

Regular season ends 12 Apr 2026. Play-in starts 14 Apr 2026. Postseason here is play-in plus playoffs. Role uses minutes entering the game (2025–26 logs before that date), not minutes in the game being graded. Starter: mean at least 28 minutes and at least 15 prior games. Bench: mean under 16 and at least 10 prior games.

The lift is the cluster-robust difference versus the other rows on the same side. The cluster is the game. A 95% interval is the lift ± 1.96 cluster standard errors.

A role or garbage-time claim needs a bench cell with real games. Three games is not a contrast.

### Overs, favorite by 7 to 9.5

8,128 rows, **47 games**, hit rate 60.3%, lift +4.8%, cluster SE 1.5%, interval +1.8% to +7.8%.

Leave-one-date-out stays positive on all 76 dates (min +4.1%, max +5.4%). That only says one slate is not the whole pool. It does not add games.

| Slice | Rows | Games | Lift | 95% interval |
|---|---:|---:|---:|---|
| Starters | 4,136 | 47 | +7.8% | +2.9% to +12.7% |
| Bench | 41 | 3 | — | unusable (untested) |
| Regular season, through 12 Apr | 4,783 | 33 | +3.2% | −0.5% to +6.8% |
| Postseason, from 14 Apr | 3,345 | 14 | +7.2% | +2.4% to +12.0% |
| March–April | 6,522 | 40 | +3.4% | +0.3% to +6.5% |
| May–June | 1,606 | 7 | +9.2% | +1.5% to +16.9% |

The starter row is essentially the whole bucket (same 47 games). That means the bucket is starters. It does **not** mean bench players failed to show the effect. The bench cell is untested.

The regular-season interval includes zero. The postseason interval that excludes zero is 14 games. This is not a feature until a regular-season window (and a closing-line pool) says otherwise.

### Is the postseason cell just playoff minutes?

Absolute over hit rates, regular season vs postseason, by player's-team spread band:

| Band | Regular | Games | Postseason | Games | Delta |
|---|---:|---:|---:|---:|---:|
| dog 10+ | 59.4% | 84 | 60.1% | 10 | +0.7% |
| dog 3–6.5 | 54.5% | 68 | 53.1% | 35 | −1.4% |
| dog 7–9.5 | 54.9% | 33 | 52.6% | 14 | −2.4% |
| fav 10+ | 59.0% | 94 | 56.5% | 10 | −2.5% |
| fav 3–6.5 | 57.3% | 70 | 53.5% | 35 | −3.9% |
| fav 7–9.5 | 59.8% | 33 | 61.0% | 14 | +1.2% |
| pk | 54.0% | 43 | 52.5% | 16 | −1.5% |
| **ALL OVERS** | **56.9%** | **248** | **54.4%** | **75** | **−2.5%** |

Postseason overs are **down** overall, not up. Most bands fall. Only fav 7–9.5 and dog 10+ hold or rise slightly. So the +7.2% postseason *lift* for fav 7–9.5 is not “every over hits more in the playoffs.” It is this band holding while other bands drop, which widens the contrast inside a 14-game sample.

That still is not a mid-size-favorite spread effect for October. A general playoff-minutes story would have lifted every band; it did not. A specific-band playoff story needs more than 14 games and a regular-season confirmation that this window does not have. Either way, the cell does not travel.

### Overs, dog by 10 or more

10,509 rows, 94 games, hit rate 59.6%, lift +4.2%, interval +0.8% to +7.5%. Leave-one-date-out stays positive (min +3.6%).

Starters: +4.2% on 53 games, interval −1.9% to +10.4%. Bench: −3.1% on 21 games, interval −21.5% to +15.3%. Twenty-one games is too thin to treat as evidence against garbage time. Write the bench / garbage-time story as **untested**, not as “does not explain.”

Regular season: +2.9% on 84 games, interval −0.6% to +6.4%. Postseason: +6.1% on 10 games, interval through zero. The pooled interval is not a result that survives the season split.

## Locked rules for scoring

These rules were written before any of the sports below have game-line hit rates computed under this schema. Do not add buckets after seeing those rates.

### Interval and what counts as a pass

- **Exactly three contrasts per sport.** That is the multiple-comparison budget. Do not add a fourth after looking.
- Report each contrast with a **Bonferroni-corrected** cluster-robust interval: family-wise α = 0.05 over the three contrasts, so each interval uses α = 0.05/3 ≈ 0.0167 (~98.3%). That is lift ± z_{1−α/2} × cluster SE with z ≈ 2.39, not 1.96.
- **A contrast passes only if both hold:**
  1. The **regular-season** (or pre-median split for MLB / Soccer) corrected interval **excludes zero**, and
  2. The **pooled** sample has the **same sign** as that regular-season effect.
- Failing either condition means the contrast does not pass. A borderline pooled cell that only clears an uncorrected 95% cut does not become a gate. Do not argue it in after the fact.

### Reportable cell

- Closing-line pool only: drop or segregate rows with blank `provider` or null `is_closing`. `is_closing` follows the feed/event rule above, not store time.
- At least **200 games in the treatment bucket** and **200 games in the comparison**, for that sport. The floor is **per bucket (and per comparison), per sport** — not 200 games across sports pooled. Anything smaller is not written down.
- **Pre-score gate:** before computing any hit rate, count unique closing-line games in each locked bucket. If either side is below 200, mark the contrast **ineligible (below floor)** and refuse to score it. Do not report a lift that can be misread as a failed test. Counting total slate games (e.g. ~1,867 MLB) is not enough — that mistake made MLB look fully scorable when the WP ≥ 0.70 band had only dozens of games.
- Favorite strength is the player's team win probability, not the run line, puck line, or point spread.
- No prop-type cut is part of this test. A prop that hits 70% inside a bucket is not a gate and not a lead.
- NBA, WNBA, NFL, CFB, NHL, CBB, and WCBB also have to be split regular season versus postseason before a contrast is kept. For MLB and Soccer, split the window at the median game date, and choose that date before the join.

### Named contrasts (MLB first; same shape for other team sports)

The run line barely varies, so MLB does **not** use spread buckets. All three contrasts use moneyline win probability and/or the game total. Names locked before any MLB hit rate exists:

1. **Overs, heavy favorite vs pick'em (win prob).** Overs with team win probability ≥ 0.70 versus overs with team win probability in [0.42, 0.58]. Includes hitter and pitcher props.
2. **Overs, high total vs low total.** Overs in the high total band versus the low total band. **Hitter props only.** Pitcher props (Ks, walks allowed, pitches thrown, ERA, hits allowed, outs) are **excluded** from this contrast — the total is largely a pitching-matchup number, and including pitchers turns the cell into “K props in low totals.” Pitcher × total is not a fourth contrast; it is not scored in this battery.
3. **Unders, heavy favorites.** Unders with team win probability ≥ 0.70 versus unders with team win probability in [0.42, 0.58]. Includes hitter and pitcher props.

**Win probability is always the prop player's own team**, vig-removed from the moneyline. For hitters that is the batting club. For pitchers it is also the pitcher's club, not the opponent — a starter on a 0.72 favorite sits in the ≥ 0.70 band. Do not flip the sign on the pitcher path.

**Soccer three-way markets:** when `drawOdds` is present, normalize by the sum of home + draw + away implied probabilities. WP is the player's team **winning outright**, not “not losing.” Two-way sports (no draw) still use home + away only. A prior Soccer pull that used two-way normalization is corrected and the WP contrasts re-scored; that is a fix to the locked rule, not a new contrast.

**Total bands (MLB):** cut points are set from the **per-game** total distribution (one row per game in `game_lines`, not per prop). MLB totals bunch at 7.5 / 8 / 8.5 / 9, so quantile thirds land on ties and give uneven buckets. Write the cuts as **fixed values** after looking at the game distribution and before joining hits (e.g. low ≤ 7.5, high ≥ 9.0, mid excluded). Accept that the bands are not exact thirds. Do not recompute cuts from graded props — that shifts with whichever props were graded. Locked MLB cuts after the first closing-line fetch: **low ≤ 7.5**, **high ≥ 9.0** (mid 8.0–8.5 excluded). See “MLB locked total cuts” below for histogram counts.

Other team sports (WNBA, NFL, CFB, NHL, Soccer pooled, CBB, WCBB, NBA closing-line retest) use the same three named contrasts when eligible. Tennis stays out unless a separate pre-registration is added.

### MLB stacking (locked before hit rates)

MLB slates stack many props on one player in one game (hits, total bases, runs, RBIs, and so on). Those outcomes move together.

- **Primary hit rate:** one row **per decided prop** (player × prop × line × side × date). Do not collapse to one row per player-game for the main number.
- **Inference:** cluster-robust SE by **game** (`event_id`). That is what the pass rule uses.
- **Diagnostic only (not a pass gate):** collapse to one row per player-game (mean of that player's decided props that day) and print the same three contrasts. A pass or fail is never decided on the collapsed table alone. **If the collapsed table disagrees in sign with the primary rate on any contrast, record that in the report.** A sign flip usually means a few heavy-volume players are driving the pooled number.

### What 200 games implies

A wide win-prob band (≈25–35% of games) needs on the order of **600–800+ unique games** in the closing-line pool before both the treatment and the comparison can clear 200. Sports below that total cannot pass by rule, even if a thin cell looks strong.

**Soccer:** primary floor is **pooled** across leagues (see Soccer section). Per-league reportable only when that league alone clears 200+200.

### Eligibility now (counts before hit rates)

Unique-game counts from graded props through 14 Sep 2026 (opponent pairs when present; otherwise team-dates / 2). These are grades available to join once lines exist — not closing-line counts yet.

| Sport | ~Unique games | Lines in `game_lines.db` | Eligible to score now? |
|---|---:|---:|---|
| **MLB** | ~1,867 | ~1,509 closing | **Partial.** Total high/low clears the per-bucket floor. WP ≥ 0.70 does not (~dozens of heavy-fav games). Two of three contrasts are ineligible at the locked threshold. |
| **Soccer** | ~1,709 | ~1,578 closing (pooled; three-way WP) | **Scored.** After three-way WP fix: WP ≥ 0.70 is **ineligible** (127/715). Total high/low eligible and **FAIL**. Not a gate. |
| **Tennis** | ~1,400 matches | 0 | **Not these three contrasts.** No team game. Match moneyline / match total only if a separate pre-registered test is written later. Idle for this note. |
| **NHL** | ~431 | 388 | **Not yet.** Total games are under the ~600–800 needed for two WP bands at 200 each. |
| **CBB** | ~442 | 125 | **Not yet.** Same floor; graded window is also tournament-heavy. |
| **NBA** | ~428 | 363 | **Not yet for a closing-line retest.** Pool exists but 0% have close meta; refetch first. Period slates (NBA1Q / NBA1H) use the parent full-game line. |
| **WNBA** | ~201 | 0 | **Not yet.** Entire season is ~200 games; no bucket can hold 200 while the comparison also holds 200. Band edges for totals stay unset until the sport is eligible. |
| **CFB** | ~136 | 0 | **Not yet.** Three weeks of games; spread/WP bands will leave buckets short of 200. |
| **NFL** | ~8 | 0 | **Deferred by rule.** Through 14 Sep only a handful of games. No bucket reaches 200 this season. Not a choice — the floor fails before any hit rate. |
| **WCBB** | 0 graded | 0 | **No graded export.** Deferred until grades exist. |
| **NBA1Q / NBA1H** | (parent NBA) | (parent) | Same eligibility as NBA; environment is the full-game line. |

### Backfill order

1. **MLB** — partial: total high/low eligible and PASS (review-only); WP ≥ 0.70 ineligible at the locked threshold. Held-out `game_total` model test decides whether the continuous term enters the model.
2. **Soccer** — multi-league core odds + name crosswalk; three-way WP fix; per-bucket eligibility; score under the locked contrasts.
3. **WNBA and CFB** — when unique games (and closing-line attaches) clear the 200+200 floor. Not before.
4. **NHL and CBB** — same: wait until the closing-line pool is large enough for both bands.
5. **NBA** — refetch for close meta (feed rule), then a regular-season closing-line retest if the floor still holds.
6. **NFL** — later, when season volume clears 200+200. Not this year under current counts.
7. **WCBB** — after grades exist and counts clear.
8. **Tennis** — out of this battery unless a separate pre-registration is added.

Near-tip close check: next MLB slate (store within ~15 minutes of first pitch, compare to post-final).

Writing eligibility before hit rates keeps that order honest. Do not score a short sport early because a thin cell looks like 70%.

### Sample size to expect

The postseason finding — one band holding while the others fell over 14 games, while the whole over pool dropped — is exactly what one band looks like by chance when the pool moves. Closing it was right. Expect the same when the first MLB (or Soccer) result comes in: most early cells will look like that one. The 200-game floor, the corrected interval, and the regular-season pass rule are there so those cells do not become gates.

## Model, not a gate

Continuous inputs are not free. Implied win probability is close to a monotone transform of the spread, and the bucket pattern in this window was **not** monotone: mid-size favorites lifted in the pooled sample, while 10+ favorite unders moved the other way (and that under cell is not confirmed). A linear win-probability term cannot represent that shape. It will learn roughly nothing, or a slope driven by the playoffs.

Both `team_win_prob` and `game_total` have to earn a place the same way:

1. Train through a cutoff. Evaluate on held-out **regular-season** games only.
2. Compare log loss or Brier score with and without the term.
3. Put a game-clustered interval on the difference.
4. If that interval includes zero, leave the term out.

Game total maps more directly to scoring volume and is the more likely of the two to clear this bar. Neither term becomes a knot, badge, or ticket rule from the bucket work in this note. The spread buckets failed the regular-season check; they stay out.

## MLB locked total cuts

Set from 1496 closing (or available) per-game totals before joining hits.
Histogram (rounded): `{"6.0": 5, "6.5": 17, "7.0": 113, "7.5": 250, "8.0": 282, "8.5": 321, "9.0": 236, "9.5": 128, "10.0": 57, "10.5": 33, "11.0": 22, "11.5": 9, "12.0": 11, "12.5": 4, "13.0": 1, "13.5": 2, "14.0": 1, "14.5": 2, "15.0": 1, "16.0": 1}`.
**Low <= 7.5** (n=385 games). **High >= 9.0** (n=508 games). Mid excluded (n=603). Not exact thirds.

## MLB pre-score bucket eligibility

Floor is **200 games per treatment bucket and 200 per comparison**, counted from `game_lines` before hits. The earlier ~1,867 graded-game estimate counted the whole sport and wrongly treated all three contrasts as scorable.

| Contrast | Treat games | Ctrl games | Status |
|---|---:|---:|---|
| over_heavy_fav_vs_pickem | 29 | 1040 | ineligible (below floor) |
| over_high_vs_low_total_hitters | 508 | 385 | eligible |
| under_heavy_fav_vs_pickem | 29 | 1040 | ineligible (below floor) |

Counts are unique closing-line games with a team (WP) or game (total) in the locked band. Floor is 200 per bucket. Do not substitute a new WP threshold after seeing these counts; any replacement band is only testable on future games.

At WP >= 0.70, MLB cannot use two of its three locked contrasts. The 0.70 threshold stays fixed. Do not substitute a lower band on this data; any replacement band is only testable on future games.

## MLB close spot-check

Post-final core refetch vs stored closing row on the same `event_id`:

| Game | Stored O/U | Refetch O/U | Match | Stored spread | Refetch spread | Match |
|---|---:|---:|:---:|---:|---:|:---:|
| 2026-09-05 TB@TEX | 7.0 | 7.0 | Y | 1.5 | 1.5 | Y |
| 2026-09-05 OAK@SEA | 7.5 | 7.5 | Y | -1.5 | -1.5 | Y |
| 2026-09-05 NYY@SD | 7.5 | 7.5 | Y | -1.5 | -1.5 | Y |
| 2026-09-05 LAA@PIT | 7.5 | 7.5 | Y | -1.5 | -1.5 | Y |
| 2026-09-05 ATL@PHI | 8.0 | 8.0 | Y | -1.5 | -1.5 | Y |

5/5 matched on both spread and total. This pull's MLB rows were all post-final fetches (`is_closing=1`); there was no second near-tip store on the same game. The check is refetch stability after final, not equality with the first-pitch line.

### Near-tip close test (next slate)

On the next MLB slate, store a row within about 15 minutes of first pitch for a few games, then compare that line to the post-final core number on the same `event_id`. That closes the gap between feed stability after final and the true close.

## MLB battery results

Closing-line join. Median split date (from game dates before hits): **2026-05-25**.
Matched decided props: 466811. Unique games in rows: 1433.

### Primary (one row per prop; cluster by game)

#### over_heavy_fav_vs_pickem
- pre-score bucket games: treat 29 / ctrl 1040 -> ineligible (below floor)
- **decision: INELIGIBLE (BELOW FLOOR)** (ineligible (below floor)) — not scored; no lift reported.

#### over_high_vs_low_total_hitters
- pre-score bucket games: treat 508 / ctrl 385 -> eligible
- pooled: treat 46373/160527 = 28.9% (489g) vs ctrl 26614/97839 = 27.2% (368g); lift +1.7% SE 0.6% Bonf CI [+0.3%, +3.0%]; excludes0=True
- pre_median: treat 11236/37865 = 29.7% (205g) vs ctrl 10292/38005 = 27.1% (220g); lift +2.6% SE 0.8% Bonf CI [+0.8%, +4.4%]; excludes0=True
- post_median: treat 35137/122662 = 28.6% (284g) vs ctrl 16322/59834 = 27.3% (148g); lift +1.4% SE 0.8% Bonf CI [-0.4%, +3.2%]; excludes0=False
- **decision: PASS** (ok)
- **Both halves:** pre-median lift +2.6% (Bonf CI [+0.8%, +4.4%]); post-median lift +1.4% (Bonf CI [-0.4%, +3.2%]). Post-median near zero or with a wide interval means the effect may be decaying across the season; that does not reverse a pass but belongs here.

#### under_heavy_fav_vs_pickem
- pre-score bucket games: treat 29 / ctrl 1040 -> ineligible (below floor)
- **decision: INELIGIBLE (BELOW FLOOR)** (ineligible (below floor)) — not scored; no lift reported.

### Within-prop mix check (total PASS only)

Hitter overs sit near 27-29% because the pool is heavy on low-probability props (HR, runs, RBIs, etc.). High-total games may also carry a different prop menu. This reweights within-prop high-vs-low lifts to one common mix — still the same contrast, not a new one.

**Mix-weighted lift: +1.5%** (weight covered 100% of the high+low hitter-over pool).

| Prop | High rate | Low rate | Lift | Mix weight | n high / n low |
|---|---:|---:|---:|---:|---|
| total_bases | 29.3% | 26.4% | +2.9% | 25.0% | 38834/25699 |
| hits+runs+rbis | 33.4% | 30.8% | +2.5% | 14.3% | 24503/12328 |
| hits | 35.5% | 34.2% | +1.3% | 12.4% | 19535/12399 |
| hitter_ks | 35.0% | 37.9% | -2.9% | 9.6% | 14770/10145 |
| runs | 26.2% | 24.4% | +1.8% | 8.7% | 13661/8725 |
| rbis | 19.7% | 17.6% | +2.1% | 5.9% | 10223/4940 |
| walks | 29.0% | 26.8% | +2.2% | 5.4% | 8253/5795 |
| home_runs | 13.6% | 10.4% | +3.2% | 5.1% | 7929/5145 |
| singles | 29.8% | 31.7% | -1.9% | 4.2% | 6935/3904 |
| stolen_bases | 8.4% | 10.6% | -2.2% | 3.4% | 5221/3523 |
| doubles | 16.1% | 13.1% | +3.0% | 3.3% | 5289/3247 |
| hitter_fantasy_score | 48.6% | 47.1% | +1.4% | 1.9% | 3663/1254 |
| plate_appearances | 39.8% | 33.5% | +6.2% | 0.7% | 1205/647 |
| triples | 4.0% | 1.1% | +2.8% | 0.2% | 506/88 |

If weighted_lift stays near the pooled +1.7pp and positive, the effect is not just a change in prop mix. If it shrinks toward zero, high-total boards are carrying a different menu.

### Player-game collapse (diagnostic)

#### over_heavy_fav_vs_pickem
- pre-score bucket games: treat 29 / ctrl 1040 -> ineligible (below floor)
- **decision: INELIGIBLE (BELOW FLOOR)** (ineligible (below floor)) — not scored; no lift reported.

#### over_high_vs_low_total_hitters
- pre-score bucket games: treat 508 / ctrl 385 -> eligible
- pooled: treat 2532/8766 = 28.9% (489g) vs ctrl 1677/6215 = 27.0% (368g); lift +1.9% SE 0.5% Bonf CI [+0.6%, +3.2%]; excludes0=True
- pre_median: treat 976/3327 = 29.3% (205g) vs ctrl 904/3397 = 26.6% (220g); lift +2.7% SE 0.8% Bonf CI [+0.9%, +4.6%]; excludes0=True
- post_median: treat 1556/5439 = 28.6% (284g) vs ctrl 772/2818 = 27.4% (148g); lift +1.2% SE 0.8% Bonf CI [-0.6%, +3.0%]; excludes0=False
- **decision: PASS** (ok)

#### under_heavy_fav_vs_pickem
- pre-score bucket games: treat 29 / ctrl 1040 -> ineligible (below floor)
- **decision: INELIGIBLE (BELOW FLOOR)** (ineligible (below floor)) — not scored; no lift reported.

No pooled sign flips between primary and player-game collapse.

A PASS here is review-only. The bucket PASS does not by itself put `game_total` in the model — see the held-out test below.

## Soccer contrast lock (before hit rates)

The three Soccer contrasts are the same named contrasts locked for all team sports in this note (heavy-fav WP >= 0.70 vs pick'em [0.42, 0.58] overs and unders; high vs low total overs), written under **Named contrasts (MLB first; same shape for other team sports)** before any Soccer multi-league hit join. The note file is still untracked in git (no commit hash); the shared names and WP bands were set with the MLB pre-registration on 24 Sep 2026, after MLB results were known but before Soccer hit rates. Soccer total edges **low <= 2.5 / high >= 3.5** were locked from the per-game total histogram after the multi-league line fetch and before joining hits (`lock_total_cuts` in `scripts/score_soccer_game_line_battery.py`) — same process as MLB 7.5/9.0. The 343 vs 1,235 split is that fixed cut on the closing-line pool (effectively 3.5+ vs <=2.5; mid empty because soccer O/U bunches at halves).

## Soccer three-way win probability

Prior Soccer WP used two-way home/(home+away), which ignores the draw and inflates outright win probability (e.g. a -270 favorite near 0.85). Corrected: when drawOdds is present, implied probs are normalized by home+draw+away. WP is the player's team **winning outright**, not 'not losing'. Two-way sports without drawOdds are unchanged. WP contrasts were re-scored after this rewrite; the total contrast is unaffected. This is a fix to the locked rule, not a new contrast.

## Soccer locked total cuts

Set from 1578 closing (or available) per-game totals before joining hits (pooled leagues).
Histogram (rounded): `{"1.5": 139, "2.5": 1096, "3.5": 329, "4.5": 14}`.
**Low <= 2.5** (n=1235 games). **High >= 3.5** (n=343 games). Mid excluded (n=0).

## Soccer pre-score bucket eligibility

Floor is **200 games per treatment bucket and 200 per comparison**, counted from `game_lines` before hits. Counts below use **three-way** outright WP when drawOdds were present.

| Contrast | Treat games | Ctrl games | Status |
|---|---:|---:|---|
| over_heavy_fav_vs_pickem | 127 | 715 | ineligible (below floor) |
| over_high_vs_low_total | 343 | 1235 | eligible |
| under_heavy_fav_vs_pickem | 127 | 715 | ineligible (below floor) |

Counts are unique closing-line games pooled across soccer leagues. Floor is 200 per bucket. After three-way WP, the ≥ 0.70 band falls below the floor (127 treat games) — same class of outcome as MLB's heavy-favorite band. Total high/low remains eligible.

WP >= 0.70 stays fixed. Do not substitute a lower band after seeing these counts; any replacement band is only testable on future games.

## Soccer battery results

Closing-line join via opponent club name. Median split date: **2026-05-15**.
Matched decided props: 81636. Unique games in rows: 524.

### Primary (one row per prop; cluster by game)

#### over_heavy_fav_vs_pickem
- pre-score bucket games: treat 127 / ctrl 715 -> ineligible (below floor)
- **decision: INELIGIBLE (BELOW FLOOR)** (ineligible (below floor)) — not scored; no lift reported.

#### over_high_vs_low_total
- pre-score bucket games: treat 343 / ctrl 1235 -> eligible
- pooled: treat 2920/19357 = 15.1% (129g) vs ctrl 8803/61377 = 14.3% (395g); lift +0.7% SE 0.6% Bonf CI [-0.6%, +2.1%]; excludes0=False
- pre_median: treat 1016/6337 = 16.0% (59g) vs ctrl 3548/21131 = 16.8% (209g); lift -0.8% SE 1.2% Bonf CI [-3.6%, +2.1%]; excludes0=False
- post_median: treat 1904/13020 = 14.6% (70g) vs ctrl 5255/40246 = 13.1% (186g); lift +1.6% SE 0.6% Bonf CI [+0.1%, +3.0%]; excludes0=True
- **decision: FAIL** (rs_ci_includes_zero)

#### under_heavy_fav_vs_pickem
- pre-score bucket games: treat 127 / ctrl 715 -> ineligible (below floor)
- **decision: INELIGIBLE (BELOW FLOOR)** (ineligible (below floor)) — not scored; no lift reported.

### Player-game collapse (diagnostic)

#### over_heavy_fav_vs_pickem
- pre-score bucket games: treat 127 / ctrl 715 -> ineligible (below floor)
- **decision: INELIGIBLE (BELOW FLOOR)** (ineligible (below floor)) — not scored; no lift reported.

#### over_high_vs_low_total
- pre-score bucket games: treat 343 / ctrl 1235 -> eligible
- pooled: treat 487/3319 = 14.7% (129g) vs ctrl 1515/10279 = 14.7% (395g); lift -0.0% SE 0.7% Bonf CI [-1.6%, +1.5%]; excludes0=False
- pre_median: treat 212/1362 = 15.6% (59g) vs ctrl 817/4695 = 17.4% (209g); lift -1.8% SE 1.3% Bonf CI [-4.9%, +1.4%]; excludes0=False
- post_median: treat 275/1957 = 14.1% (70g) vs ctrl 697/5584 = 12.5% (186g); lift +1.6% SE 0.6% Bonf CI [+0.1%, +3.0%]; excludes0=True
- **decision: FAIL** (rs_ci_includes_zero)

#### under_heavy_fav_vs_pickem
- pre-score bucket games: treat 127 / ctrl 715 -> ineligible (below floor)
- **decision: INELIGIBLE (BELOW FLOOR)** (ineligible (below floor)) — not scored; no lift reported.

A PASS here is review-only. Soccer game-line environment does not become a ticket gate from this battery.

## MLB game_total held-out model test

Continuous `game_total` must earn a model slot via held-out games (train through a cutoff; score later dates) log loss / Brier with a **game-clustered** interval on the difference. Base model: logistic on PrizePicks `ml_prob`. Comparison: same + standardized closing `game_total`.

- Cutoff (train through): **2026-05-25**
- Train props: 131323. Test props: 316959 (698 games).
- Base log loss: 0.6013. With total: 0.6025.
- Base Brier: 0.2055. With total: 0.2061 (delta +0.0006).
- Delta log loss (with − base; negative = better): +0.0010 (game-clustered 95% CI [+0.0006, +0.0015]; n_games=698; excludes0=True).
- **Decision: exclude from model — held-out log-loss improvement does not clear a game-clustered 95% interval**

Backfill order after this: **WNBA / CFB** once per-bucket counts clear. Near-tip close check remains on the next MLB slate.
