# Status manifest

Definitions for `status_ledger.md`. The ledger stays one line per item.

- **Resolved basis.** A win rate uses HIT and MISS only. Unresolved rows are counted beside the rate and are not voids.
- **n under 30.** Reported. It does not change a gate.
- **Price.** The broadest cell is the price. A qualifier's cell is the price only when its lift band excludes zero.
- **No regular-season cell.** That leg gets no ticket and no 0.763 floor.
- **Shortlist.** EV above 0 at the median quote. A group is placed only when EV is still above 0 at the scraped payout with every leg two points under its cell.
- **NHL regrade.** Graded HIT/MISS/PUSH rows whose prop canon is `sog` (shots on goal and shots on target) or `hits`. Before = the stored actual. After = `nhl.shots_on_goal` or `nhl.hits` in `data/cache/proporacle_ref.db` on player and date. Unmatched rows keep the stored actual.
- **Soccer not-in-box.** The 264 player-date-prop keys in `soccer_unresolved_multi_event.csv` with bucket `player_not_in_box`. A cell loses every graded HIT/MISS row with that key. Discovery drops those keys.
- **Saves zero check.** The 17 in-box saves whose stored actual is 0. Bench means the ESPN roster marks the keeper `starter=false`.
- **NBA period scale.** Same player and prop, period `Last 5 Avg` / full-game `Last 5 Avg`, and the same for `Last 10 Avg`. Sane band is 0.25–0.40 for NBA1Q and 0.45–0.60 for NBA1H. `L5 Over` is a 0–5 hit count and is not the scale check. Tickets block only when n≥30 and the median is outside the band (`nba_period_l5_ratio.json`).
- **Unlock.** Board is `load_today_board` for 2026-10-08. Buildable is `goblin_70_eligible` and `standard_ticket_eligible` after the season block. Current-season n is `l5_sample_n`, `L5 Sample N`, or `games_used` when the board has one of those fields.
- **Scraped payout.** `user_entries.scraped_n_correct_x` is `payouts[parlay_count].multiplier`. A 1st-place figure is never stored there. `entry_legs.pp_result` and `entry_legs.board_opponent` are filled from the prediction payload when that payload has them.
- **NHL shots shadow.** Goblin OVER shots on goal at line 0.5. No ticket. Forward log is `nhl_sog_line05_shadow.jsonl`. Wire only after 100 forward legs if the Wilson low clears 0.769 (3-leg at the 2.2x median) or 0.845 (2-leg at the 1.4x median).
- **Faceoff check.** Graded `faceoffs_won` lines versus `nhl.faceoffs_won` on the same player and date. A scraped payout is an N-correct multiplier on a slip that contains that prop.
- **Retirement readings.** Played games, and the same score with the set in progress completed for the player who won the match. Both must be the same HIT or the same MISS. A difference is UNRESOLVED. A walkover has no Games Won or Total Games grade. The 71/127 book is the resolved slips in `gated_goblin_slip_manifest_20261007.json`.
- **NHL excluded props.** Faceoffs, plus any prop whose stored actual matches the box on under 98% of joined rows. They are out of cells, gates, and tickets.
- **DNP.** A zero actual with minutes, TOI, plate appearances, or batters faced equal to 0. Stored as NO_ACTUAL, reason dnp. A zero with no minutes source is minutes_unknown and stays out of cells.
- **Fetch gap.** A leg with no actual and a blank or no-match reason. Postponed, retirement, walkover, and DNP are separate. Only fetch gap is a measurement problem.
- **R7.** A new gate is wired only after 100 forward legs with Wilson low above break-even at the median quote, or an out-of-sample n of at least 150 that passes the split-balance flag with Wilson low above break-even. The 0.769 and 0.846 figures are Goblin 3-leg bars (2.2x median and 1.65x 25th percentile).
- **Pick-type break-even.** Goblin: 0.845 at 1.40x (2-leg) and 0.769 at 2.20x (3-leg). Standard: 0.577 (2-leg) and 0.550 (3-leg). Every discovery cell prints the base rate beside the cell rate.
- **Attach path.** Catalog is the uncorrected category file. On-row is the stored grade with no opponent check. Corrected is a non-retired ESPN match to the board opponent.
- **Shadow slip.** Built from the best available legs, quoted, and logged. Nothing is submitted. A payout-only shadow slip is an N-correct quote. It is not in the 30-slip ROI sample and it is not mixed into the 71/127 book.
- **Postseason tag.** MLB and WNBA (including WNBA period slates) ESPN season type 3. Allowed on tickets at the regular-season cell minus 3 points. Tagged postseason=1. Graded on gated_goblin_postseason. Out of the 71/127 book. One slip a day until 30 are graded. Preseason, unknown game type, and every other league's postseason stay blocked and do not use the 0.763 floor.
