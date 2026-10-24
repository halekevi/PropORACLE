# CFB gap catalog

What a complete per-prop decision record requires, versus what exists.
Not a build order.

Status words:

- **Live** — engine or board uses it today
- **Audited** — HIT/MISS measured on graded tape
- **On board** — step8 column present when the pipeline ran
- **Built, empty** — code exists; table is empty until a key/source is filled
- **Not built**
- **Permanent hole** — cannot be recovered

Tape window for every HIT/MISS number here: `graded_props` on **2026-08-28, 08-29, 09-03, 09-04, 09-05, 09-12**. **3,855** decided lines. **2026-09-19 is a permanent hole** (no step8 in this checkout's `outputs/`, `line_history` last CFB archive 2026-09-11, PrizePicks lines gone). Week 0 step8 (**08-28 / 08-29**) has **no `Projected Margin` column**, so those 691 decided lines do not join any margin/script cut.

The weekly O/D markdown (`Sports/CFB/data/cfb_od_rankings_week{N}.md`) is a **different path**: latest step8 pairings (this repo, then `PropORACLE_main_cp`) + ESPN YPG + optional line-secondary z. It can print 9/19 slate teams from main_cp step8 while 9/19 stays ungradable. Authoritative for “who is on this week’s board / what is the percentile tier.” **Not** authoritative for HIT/MISS. When `ol_index` / `dl_index` / `secondary_index` get a CFBD key, correlate vs grades by joining the composite onto `graded_props_*.json`, not by reading the markdown tiers.

---

## 1. Cross-cutting layers (apply to many props)

### 1.1 Form / gates

| Layer | Code | Live? | Audited on tape? | Notes |
|---|---|---|---|---|
| Ungated book × prop HIT/MISS | graded JSON | — | **Yes, full 3,855** | See §2 |
| L5 ≥ 4 list gate | `rank_best_props_today` + `cfb_list_eligible` | Yes, except Rush Att / Kick Pts Goblin OVER | Week-1 `week_grades` only; **not re-cut on 3,855** | Engine applies; per-prop L5≥4 HR not refreshed |
| L5=5 + L10≥8 + D + cover ticket stack | `ticket_70_pool` + `cfb_ticket_fade` / `cfb_pass_yards_goblin_skip_d` | Yes as default, with the five keep-gate exceptions | **Per-prop ticket HR only for the five keep-gate chips + Rush Yards as D control** | Other props get the stack without a published ticket HR |
| Cover floor | `PROP_COVER_UNIT` CFB: pass 15, rush/rec 8, rec 0.8, TDs 0.5, … | Live as ticket floor | **Never tape-audited for CFB** (WNBA audit: mostly redundant with D) | Floor exists; “does cover add lift on CFB” does not |
| Directional D (Weak\|Below OVER, Elite\|Above UNDER) | `d_aligned` + ESPN unit ranks (`cfb_team_unit_rankings.csv`) | Live on tickets except Pass Yards Goblin OVER skip | **Audited: Pass Yards Goblin OVER (skip — D hurts) and Rush Yards Goblin OVER (keep — 84% L5=5+D).** Rec/kick/INT/longest/attempts **not** chip-audited | Unit map is live: pass/rec → pass D, rush → rush D, TDs → TD D, kick → kick D, INT → INT-forced. **National** opp D only for the gate |
| Conference Off/D ranks (same CSV `*_rank_conf`) | step3 attaches `*_tier_conf` / matchup conf fields | On board (soft) | **Diagnostic only — no miss cluster.** Keep-gate L5=5: Rush UNDER misses in disagreement 1/4 (3/4 agree); Rush Goblin OVER 2/5 misses vs 14/22 hits in disagreement; Pass Goblin OVER 3/6 misses but disagreement cell is n=4. Rush Goblin OVER’s mildly counterintuitive lean (dis 14/16 vs agree 8/11) is **not distinguishable from noise**: Wilson95% [64.0, 96.5] vs [43.4, 90.3] overlap; diff +14.8 pp with 95% CI [−16, +46]; Fisher p=0.37; bootstrap P(diff≤0)=0.19. Pure `oppD:conf_stronger` is 5/5 — thinner still, and the wrong direction for “conference-strong D that national undersells hurts overs.” Nat≠conf band is a third axis (agreement/disagreement), not a column swap. **Available, not yet cut** — same disposition as OL/DL/secondary before CFBD fill | Do not run as a parallel gate cut at current n. Revisit only if a later tape shows misses clustering in nat_stronger / conf_stronger rows |
| Badge D N/A | `cfb_pass_yards_goblin_skip_d` | Pass Yards Goblin OVER only | Same | Not a Silver miss |
| Current-season L5 / L10 (`l5_sample_n`, `l10_current_n`) | step5b `_cfb_face_vals` + `_face_window_current_n` | Face L5/L8/L10 are live; neither count is a ticket block | **Observe-only.** One face series feeds all three windows. Last year is appended while current games are under 8. At Week 6 a regular starter with 5–6 games has an all-current L5 and an L10 that is still partly last year, and `PRIOR_SEASON_FILL` stays set. The L5 checklist is **`l5_sample_n` < 5**. `l10_current_n` labels how many L10 slots are current. Once 8 current games exist, L10 is current-only even if shorter than 10 — grade “8 current” and “10 current” as the same fully-current bucket. | Do not return False from `goblin_70_eligible`. |
| Live ticket stack (no L10) | `ticket_gate_passes` skips L10 for CFB/CFB1H | **This is the card.** L5=5 + D + cover. Pass Yards Goblin OVER is L5=5 + cover (floor 15). Rush/Rec cover floor 8. Signed `Edge`, not Abs Edge. | Unique player-games, tape 08-28 through 10-10 except the two dead Saturdays (09-19, 10-03). Collapse is one-day only: a later row replaces an earlier one when it is the next calendar day and the line is within 1 (Hall 8/28–8/29). A later Saturday at the same line stays. **Rush Goblin OVER D-kept 13/17 vs D-dropped 31/36. Rec Goblin OVER 15/22 vs 38/49. Rush Std UNDER 9/10 vs 12/22.** Cover removes nobody on either Goblin. Pass Goblin OVER is 23/28 with no D cut. D has not added anything on either Goblin (pooled D-kept 28/39 vs D-dropped 69/85). Rush UNDER 9/10 vs 12/22 is a lead, not a settled cut. The 9/22 ranks are pre-game for 09-26 and 10-10. No gate change. Pooled L10≥8 on the live set: pass 40/49, fail 20/28. **Unproven, not rejected.** | Flagged-vs-unflagged grading uses this live set, not L5=5+L10≥8. Do not drop D on these cells. |

Keep-gate exceptions already shipped (`utils/cfb_keep_gates.py`):

1. Pass Yards Goblin OVER — skip D (list L5≥4, tickets L5=5+cover). L10≥8 is not a CFB ticket gate.
2. Player TDs — off L5=5 ticket stack; list L5≥4 stays
3. Standard Rec Yards UNDER — off cover-stack tickets; list stays
4. Standard Rush Yards UNDER — **keep** full stack (do not copy Rec U)
5. Rush Attempts / Kicking Points Goblin OVER — off list and tickets

### 1.2 Script / identity (on board from 09-03 onward)

| Layer | Code | Live? | Audited? | Notes |
|---|---|---|---|---|
| Projected Margin, Blowout 14/17/21, implied team total | `utils/cfb_implied_totals.py` → step8 | On board 09-03+ | **Pass Yards Goblin OVER + Rush Yards Goblin OVER only**, joined Week 1 **and 09-12** | Week 0 cannot join. 09-19 missing entirely |
| Team / Opp Rush Rate, Clock Script, Clock Mult, TOP | unit rankings + `cfb_clock_script` | On board | Pass Yards: run-heavy vs not inside blow-fav. Rush: D+ by margin band | Box-score rush rate includes garbage time |
| Neutral-script / standard-downs rate (garbage-time stripped) | — | Not built | — | CFBD standard-downs would replace box-score identity |
| Hypothetical Pass Yards D-veto (blowout ∩ run-heavy ∩ D+) | `would_veto_pass_yards_goblin_d` | Observe-only, **not a gate** | Week 1 = 2/3 unique (09-05 trio). 09-12 had RH×blow-fav (Becht, Stockton, both D− HIT) and **zero D+ rows** — veto intersection was not re-tested. Do not graduate | Run-heavy blow-fav without requiring D+ is 8/9 unique |

**Margin × D already re-run** (Week 1 + 09-12). Unique player-game = one `(date, player)` after collapsing Goblin line variants. Mixed-line players use **majority** (not last-line-wins). That majority rule is why Week 1 Rush D+ blow-fav is **7/15** here, not the earlier 8/15: Turbo Richard 09-05 was `[MISS, MISS, HIT]`.

**Pass Yards Goblin OVER — veto cell is untested on Week 2, not reconfirmed.**
The intersection is blow-fav (m≥17) ∩ run-heavy (Team Rush Rate ≥ 0.58) ∩ D+. Week 1: Weigman HIT / Becht HIT / Hawkins MISS = **2/3 unique** (three line reprints each, 09-05 only). Week 2 (09-12) had **90** Pass Yards Goblin OVER decided and **did** have blow-fav × run-heavy: Becht 169.5/179.5/189.5 and Stockton 174.5/179.5, both HIT, **both D-fail**. D+ rows in that intersection on 09-12: **0**. Week 2 therefore did not re-test the veto cell; it only added two unique D− run-heavy blow-fav HITs. Combined run-heavy blow-fav is 8/9 unique because those two join Week 1’s 6/7. Do not read “2/3, unchanged” as Week 2 confirming the trio.

**Rush Yards Goblin OVER D+ — 14/17 and 14/28 are Week 1+2 sums, not restatements of Week 1.**
- Close \|m\|<8: Week 1 **12/15 unique** (same 12/15 as the pre-09-12 cut; three mixed-line players, all majority HIT) + Week 2 **2/2** (Adam Mohammed, Gio Lopez) = **14/17**.
- Blow-fav m≥17: Week 1 **7/15 unique** (majority; Turbo Richard mixed → MISS) + Week 2 **7/13** = **14/28**.
Direction is the same either week: close holds, blow-fav is a coin flip. Blow-dog remains 3/4 unique (thin). Not a gate.

### 1.3 Usage / role

**Disposition (2026-10-23).** The share-floor test and the hand role check answer different questions. Floor = “does higher share predict higher hit rate?” → no (inverted at 25–50%). Role check = “is this player on the field getting the ball at all?” → that is what caught Glenn (best 16 vs 64.5) and Newberry (best 11 vs 54.5). The inversion does not retire the role check. Prefer **raw carries / pass attempts / targets per game** for role (a back with 12 carries × 5 is a lead back whatever share says; MacIntyre 10/3/2/7 behind a high line is the fail shape). Raw counts also avoid the coverage hole that blanked 10/21 rush-share rows on the 9/05 slate. Both role backtests land where the share test did: **no gate**; role stay **by hand** (junk-volume skip on overs only). Do **not** wire share, committee, shared-WR, median-carry, or tgt/g floors into keep gates.

**Coverage bias (cite filled beside base).** Bucket tables do not add to the base — missing rows are not random. Rush Goblin OVER L5=5: **base 149/201 (74%)**, **filled 103/150 (69%)**, unfilled 46/51 (~90%). Committee YES+NO covers 135/201; the other 66 hit ~89%. On rows with real windows the rate is closer to **69%** than the 74% base. Rec Goblin OVER L5=5: gap runs the other way — **base 229/293 (78%)**, **filled 176/216 (82%)**, unfilled 53/77 (~69%); filled+D 43/54 (**80%**). That L5=5 filled rate **beats Rush Goblin’s 69%** — do not treat receiving as the weakest Goblin chip off early ungated book rates; WRs with real windows belong in the Goblin mix (hand-skip ghost roles). The &lt;2 receptions 11/11 (100%) cell is a **low-line** effect, not a role signal. Std UNDER: unfilled is the optimistic cell. “Does it sort?” is only answerable on filled rows; always quote **filled-only** next to base.

**Rush Std UNDER — evidence demoted (2026-10-23).** Role-tape unique L5=5 is **base 24/37 (65%) · filled 7/16 (44%)**; the other 17/21 blank-window rows hit **~81%**. The earlier “~83% at L5=5” / unique 17/21 (81%) chip quotes were **blank-inflated**. Players with real carry windows (e.g. Patterson, Lee-Knicely) should be judged against the **44% filled** figure, with two caveats: filled rows only exist from 9/26 on this ESPN cache (filled vs unfilled is also later vs earlier), and n=16 is thin — **44% is not proof the chip is bad; it means the evidence is no longer strong.** Keep gate stays L5=5+D+cover8 (D = Elite|Above). Ticket priority: **below** the Goblin mix; only if D tags and N-correct payout clear $40 on a $20 entry.

**15+ carries vs cushion (keep separate).** Heavy-carry Goblin overs are the worst median bucket (23/40 57%) because lines are higher and % cushions thinner (15+ mean line **50.1** / pct **22%** vs 5–9 line **14.0** / pct **68%**) — that is a **line/cushion** fact, not a role fact. Role = is the player on the field getting carries. Cushion = (worst-of-5 yards − line) / line. Do not fold one into the other.

**Pct cushion — observe only; do not trust the 25% cut yet** (`cfb_rush_pct_cushion_holdout.json`, yard-filled n=136). The full-sample pct≥25% split (54/70 **77%** vs 40/66 **61%**, z≈2.1) is the largest effect in this thread, but: (1) the cut was picked after seeing the data — flat 8 yards was pre-registered, so the comparison favors %; (2) pre-registered buckets do **not** rise steadily — &lt;15% 33/50 (66%), **15–25% 7/16 (44%)**, 25–40% 13/18 (72%), 40%+ 41/52 (79%); (3) inside line-size buckets the ≥25% cut mostly dies (line &lt;20: 83% vs 88% **flips**; 20–39: 71% vs 57% z≈0.9) — % cushion partly proxies line size; (4) requested holdout (cut on 8/28–9/12, score 9/26+10/10) **cannot run** — ESPN-filled rows on this cache start 9/26 (train n=0). Alt: cut on 9/26, score 10/10 → both sides hot (100% vs 90%); score 10/10+10/17 → 84% vs 70% (z≈1.6). Flat gap≥8 on the same yard-filled set does not separate (68% vs 70%). **Stay observe-only.** If a later OOS clears, use % cushion as a **tiebreaker when two lines are both L5=5** (prefer the lower line / higher cushion) — not a gate, not a replacement for “highest line still 5/5.”

**UNDER role.** Thin volume / low maxY on Standard UNDER is **support for the under** (Glenn/Newberry best games under the line), not an OVER-style skip. Starter-vs-backup still decides. **Rush UNDER only if the window is filled, D is Elite or Above, and the cover clears 8.** Rec UNDER stays faded (base 17/33 52%, filled 4/11 36%).

**This-week ticket order (role-tape revision).** (1) Goblin mix of rush + pass + **receiving** legs first (Rec L5=5 filled ≥ Rush filled). (2) Standard rush UNDER pair last — only if D + payout clear.

| Layer | Code | Live? | Audited? | Notes |
|---|---|---|---|---|
| Season-mean rush att / pass att / rec tgt share | `build_cfb_player_volume.py` → `cfb_player_volume.csv`; step8 Rush/Pass/Tgt Share | On board | **Inverted / non-monotonic** on Rush Yards, Rec Yards, Receptions (tested floors 25–50%). Held, not gated | Floor finding stands; it is not a role-check finding |
| Rush Yards Std UNDER L5=5 × share overlay | same volume CSV + step8 Rush Share | On board when filled | **Untestable, not tested-and-clean** on the L5=5 unique pool (n=21 after 8/28=8/29 stamp collapse). **10/21 blank** — almost all 9/05 — empty step8 Rush Share and no usable 2025 volume row. The 11 filled rows are not a closed “share doesn’t sort” finding; they are a coverage hole on the slate that is half the sample. Turner 100% 2025 fill is junk, not a 50%+ tail. Do **not** miscoded this as a second inversion alongside the floor result above | Prerequisite: populate `rush_att_share` on 9/05-style slates before re-asking. No share floor. No joint Off×D require-both (opposite strong×leaky cell is 6/7 — throws away hits) |
| Hand role check (raw volume) | Manual; ESPN `rushingAttempts` / yards (box `RUSH_ATT` nearly empty) | Hand only | Matches tape (Glenn, Newberry, MacIntyre). **2026-10-23** (`cfb_role_volume_backtest.json`): Goblin OVER L5=5 **base 149/201 (74%) · filled 103/150 (69%)** · unfilled 46/51 (~90%). Median-carry buckets on filled do **not** sort up (5–9 34/45 76%, 10–14 46/65 71%, 15+ 23/40 57%). Role-fail almost never fires on L5=5 overs. Junk-volume skip stays hand-only — not a hit-rate floor | Prefer per-game counts over team share. Low/erratic count behind a high line = skip **overs** |
| Rec Yards WR role (receptions + season tgt/g) | ESPN gamelog `receptions` / `receivingYards`; season `rec_tgt_pg` from `cfb_player_volume.csv`. Box `REC_TGT` ~empty; ESPN gamelog has **no** `receivingTargets` | Hand / observe | **2026-10-23** (`cfb_rec_role_volume_backtest.json`): Goblin OVER L5=5 **base 229/293 (78%) · filled 176/216 (82%) · filled+D 43/54 (80%)** — **ahead of Rush Goblin filled 69%**; keep WRs in the Goblin mix. Median receptions do not sort up; &lt;2 11/11 is low-line not role. Season tgt/g hump (5–8 92%) hand only. Std UNDER faded | Game-level targets need PBP/box fill. Ghost-role hand skip still applies |
| Rush cover: gap vs % cushion | `cfb_rush_gap_cover_backtest.json` + `cfb_rush_pct_cushion_holdout.json` | Observe only | Post-hoc pct≥25% is the biggest split in-thread but fails trust checks: buckets not monotonic (15–25% 44%), within line-size weak/flip, requested 8/28–9/12 train empty (ESPN fill from 9/26). Alt OOS 9/26→10/10+10/17 keeps ~14pp at 25%. Flat 8 does not separate on yard-filled set | Tiebreaker only if two L5=5 lines available — prefer higher % cushion / lower line. Role ≠ cushion. No gate |
| Bucketed share re-test | — | Blocked | Not run | Needs blanks filled first. Spec: **last-4 share + raw carries**, buckets under 15% / 15–25% / 25–40% / 40%+, on **live-rule rows only**. Until coverage is fixed, “share doesn’t sort” is partly “share couldn’t be tested” |
| Last-4 vs season-mean usage gap | — | Not built | — | Recency; same fill prerequisite as bucketed share |
| Committee / shared-backfield flag | Observe cut in role backtest | Observe only | Goblin OVER L5=5: YES 47/69 (68%) vs NO 43/66 (65%) on the 135 classified rows — no separation. The other 66 (blank/unk) hit ~89%. Stays out of gates | Cheap rule: **≥2 backs with ≥8 carries in each of the last 3 games**. Print next to rush legs; re-grade when teammate coverage denser |
| Shared WR room flag | Observe cut in rec role backtest | Observe only | Goblin OVER L5=5: shared YES 2/3 (67%) vs NO 159/191 (83%) — YES almost empty; no gate signal | Cheap rule: **≥2 teammates with ≥5 receptions in each of the last 3 games**. Needs denser teammate fetch before re-ask |
| Std UNDER L5=5 × maxY vs line | Same ESPN windows | Observe only | Rush role-tape: **base 24/37 (65%) · filled 7/16 (44%)**; blanks ~81% — demotes the old ~83%/17/21 chip quote. Rec: base 17/33 (52%) · filled 4/11 (36%). Unfilled = optimistic cell | Rush UNDER only if **filled + D Elite\|Above + cover≥8**. Ticket below Goblins. Thin volume / low maxY = under support, not skip |
| Snap % | checklist `required_extra` for CFB | Not a CFB board column | — | NFL has snaps; CFB infers from box participation after the fact |
| QB dropback / play-action / RPO flag | — | Not built | — | Moves pass and rush props together |
| Depth chart (before the game) | — | Not built | — | Role change only visible after boxes exist |
| Route / alignment (slot vs outside) | — | Not built | — | Likely PFF-gated |

### 1.4 Line / secondary (CFBD)

| Layer | Code | Live? | Audited? | Notes |
|---|---|---|---|---|
| OL run-block z (line yards, stuff rate, power success) | `utils/cfb_line_secondary.py` `OL_FIELDS` | Observe-only attach in step8 | Never — table empty | **Needs `CFBD_API_KEY`**. Equal-weight placeholder, not a live gate |
| DL / front-seven z (def line yards, stuff, `havoc.frontSeven`) | `DL_FIELDS` | Same, empty | Never | `havoc.frontSeven` is mixed; **not confirmed sack/pressure vs run-stop** |
| Secondary z (`havoc.db`, pass explosiveness allowed, pass success allowed) | `SEC_FIELDS` | Same, empty | Never | Natural pairing for Rec Yards / Completions / INTs |
| Pass-pro (sack rate allowed, pressure rate, time-to-throw) | — | **Not in the composite** | — | OL composite is run-block only. New source fields from CFBD or other |
| Opponent pass-rush (sacks/pressure generated) | Partial overlap with `havoc.frontSeven` only | Empty | Never | Needs an explicit pass-rush index, not the run DL mix |

`Sports/CFB/scripts/pipeline/build_cfb_line_secondary.py` exits 2 with `CANNOT-OBTAIN` when no key. `attach_line_secondary` is a no-op on an empty table.

### 1.5 Availability / environment / market

| Layer | Code | Live? | Audited? | Notes |
|---|---|---|---|---|
| ESPN injury CSV auto-load | `espn_injuries.auto_injuries_csv_from_outputs(..., "CFB")` → `injuries_cfb_<date>.csv`; step6 penalty | Wired after CBB copy-paste fix | Not audited | File is often **missing**; then no-op. No CFB-native freshness (out this week vs last week) |
| Backup QB / starter-out as blowout nullifier | Flagged in overlay notes | Not built as a CFB injury feature | — | |
| Wind / weather | NFL Open-Meteo exists | **Not pointed at CFB stadiums** | — | Completions, air yards, INT, FG |
| Sharp player-prop line movement | `utils/line_movement.py` sport `CFB` → Odds API `americanfootball_ncaaf` | Feed exists | Not attached to CFB step8 | Markets wired: pass yds, rush yds, rec yds, receptions, pass TDs, anytime TD. **Not** completions, rush att, longest, kick, INT, combos |
| Game total / implied team total | step8 Implied Team Total, Opp Implied Total, Spread | On board 09-03+ | Only as margin buckets for Pass/Rush Goblin OVER | Not cross-tabbed vs Completions, Rec, Kick, TDs |

### 1.6 Red zone / kicking behavior

| Layer | Code | Live? | Notes |
|---|---|---|---|
| Red-zone / goal-line touch share | NFL checklist `recommended_extra`; **CFB not built** | — | Needed for Player/Rush/Rec TDs; also inflates rush att without yards |
| Goal-line personnel (dedicated short-yardage back) | Not built | — | Needs PBP formation |
| Team RZ → FG-attempt vs go-for-it | Not built | — | Kicking Points / FG Made |
| Kicker distance splits (make% by range) | Not built | — | CFBD/ESPN box-derivable, not pulled |
| PAT attempt rate / 2-pt rate / kicker identity | Not built | — | |
| Garbage-time kicking (no FGs in a TD blowout; backup kicker) | Margin buckets exist; **never pointed at kick props** | — | |
| Holder / snapper | Not sourced | — | College ST variance; likely too thin |

### 1.7 Pipeline / tape holes (not a prop layer)

| Hole | Status |
|---|---|
| 2026-09-19 full Saturday board | **Permanent.** No step8, no line_history after 09-11 |
| Week 0 step8 missing Projected Margin | 08-28/29 decided rows cannot join script cuts until those boards are rebuilt with current step8 |
| STEP A skip list omitted CFB (and tennis/golf/WCBB/period NBA/WNBA) | **Fixed** in `run_daily.ps1` (`step8GradePairs`). CFB1H still has **no `slate_grader` path** — not in the skip list on purpose |
| CFB1H grades | No `graded_cfb1h`. 1H L5 is Q1+Q2 PBP; do not mix with full-game actuals |
| Combos (`+` name) | Skipped on tickets already. List/print only |

---

## 2. Per-prop record

Ungated HR = full 3,855-line tape (line variants). Unique player-game in parentheses when it differs.

A **complete** record for a chip is: ungated + L5≥4 + ticket L5=5+L10≥8+D+cover (or documented skip) + cover lift vs no-cover + D split + margin×identity×D + any category-specific layers in this section.

### 2.1 Passing volume

#### Pass Yards — Goblin OVER (174/250, 69.6%; unique 72/101, 71.3%)

| Requirement | Status |
|---|---|
| Ungated | Audited, full tape (down from Week-1 73.6% 67/91) |
| L5≥4 list | Live; list HR not re-cut on 3,855 |
| Ticket L5=5+L10≥8+cover, **skip D** | Live keep-gate |
| Cover lift on CFB | Not audited |
| D split | Audited: D hurts (D-fail ~90% on the tape that shipped skip). Blow-fav join: D− 14/15 vs D+ 9/13 unique |
| Margin × run-heavy × D | **Done on Week 1 + 09-12.** Veto intersection (fav×RH×D+) is still only the 09-05 trio (2/3). 09-12 had RH×blow-fav but **0 D+ rows** — Week 2 did not re-test the cell. Run-heavy blow-fav (any D) 8/9. Do not graduate veto |
| Pass-pro / pass-rush composites | Not built / CFBD empty |
| Secondary composite | Built, empty |
| QB dropback / PA / RPO | Not built |
| Backup QB / injury | Wired CSV path; often no file |
| Wind | Not pointed at CFB |
| Implied total × this chip | Not cross-tabbed except via margin |
| Line movement | Feed has `player_pass_yds`; not on CFB card |

#### Pass Yards — Standard OVER (13/29) / Standard UNDER (57/118)

Same missing stack as Goblin except: **no D skip**, no margin×D cut, no keep-gate. Ticket engine would apply full D. Under n is usable; Over n is thin.

#### Pass Completions — Goblin OVER (16/28) / Standard OVER (9/12) / Standard UNDER (29/67)

No D audit, no margin cut, no cover audit, no pass-pro, no wind, no line-movement market (Odds API CFB list omits completions). Standard UNDER 29/67 is the only cell with modest n.

#### Pass Attempts — Goblin OVER (19/36) / Standard OVER (7/14) / Standard UNDER (10/24)

Same as Completions. Thin. Script (dropbacks in blowouts) unexamined.

#### Pass TDs — Goblin OVER (63/91, 69.2%) / Standard OVER (4/8) / Standard UNDER (8/15)

Goblin n is large enough for a D + margin cut that has **not** been run. Red-zone share not built. 0.5-line TD family is faded from the L5=5 **player TD** stack; Pass TDs are a different chip and still on the default ticket stack without a chip audit.

#### Longest Completion — Standard UNDER (24/43) / Standard OVER (6/13)

No D, no margin, no cover audit. Explosive-pass / secondary explosiveness-allowed would be the natural pairing (CFBD empty).

#### Pass+Rush Yards — Goblin OVER (20/36) / Standard OVER (7/11) / Standard UNDER (23/33)

Combo: needs Pass Yards **and** Rush Yards stacks plus a **correlation term** (legs move together vs offset). Tickets skip `+` names. Standard UNDER 23/33 is the least-thin combo cell.

#### Pass Yards combo — Standard UNDER (4/5)

Thin. Same combo gap. Observational only.

---

### 2.2 Rushing volume

#### Rush Yards — Goblin OVER (279/453, 61.6%; unique 124/211, 58.8%)

| Requirement | Status |
|---|---|
| Ungated | Audited, full tape (flat vs Week-1 61.1%) |
| L5≥4 | Live; not re-cut on 3,855 |
| Ticket L5=5+L10≥8+**D**+cover | Live. D control **84% at L5=5+D (16/19)** on the tape that shipped keep-D |
| Cover lift | Not CFB-audited |
| Margin × D | **Done on Week 1 + 09-12.** Unique = `(date, player)`, majority on mixed lines. Close = Week 1 **12/15** + Week 2 **2/2** → **14/17** (14/17 is not a restatement of 12/15). Blow-fav = Week 1 **7/15** (not the earlier last-line 8/15; Turbo Richard mixed majority MISS) + Week 2 **7/13** → **14/28**. D does not absorb script. Not a gate |
| Usage share | On board; inverted at tested floors; needs buckets |
| Usage recency / committee flag | Not built |
| OL run-block / opp DL | Built, empty (CFBD key) |
| Neutral-script rush rate | Not built (current Team Rush Rate is garbage-time-included) |
| Red-zone/goal-line share | Not built (attempts without yards) |
| Wind as pass-weather → rush volume | Not built |
| Line movement | Feed has `player_rush_yds`; not on card |

#### Rush Yards — Standard UNDER (118/183, 64.5%) / Standard OVER (41/89, 46.1%)

UNDER is a shipped keep (full stack L5=5+D+cover8, do not lump with Rec U). Ticket HR on the original tape was 71% 2/2 — **n=2, not re-measured on 3,855**. Unique L5=5 after stamp-dupe collapse was quoted **17/21 (81%)** — that figure is **blank-inflated**: role-tape **base 24/37 (65%) · filled 7/16 (44%)**, with 17/21 no-window rows at ~81%. Filled exists from 9/26 only on the ESPN cache; n=16 is thin — evidence **no longer strong**, not a gate tear-down. Ticket priority below Goblin mix; need D Elite|Above and payout clear. Joint Off×D 2×2: aligned weak-Off×stingy-D **6/6**; opposite strong-Off×leaky-D **6/7** — against a require-both gate. Share overlay **untestable** (10/21 blank on 9/05-heavy pool). OVER has no chip audit. Do not copy the joint/share cut onto Rush Goblin or Pass Goblin.

#### Rush Attempts — Goblin OVER (18/37, 48.6%)

**Off list and tickets** (keep-gate). No D, no margin. Committee + script are the layers that would replace a fade if this is ever reopened.

#### Rush Attempts — Standard OVER (8/16) / Standard UNDER (59/102, 57.8%)

UNDER has n; no D/margin audit. Goblin fade does not automatically apply to Standard UNDER.

#### Longest Rush — Standard UNDER (30/54) / Standard OVER (27/57) / Goblin OVER (0/1)

No D, no margin, no DL/OL join. Thin Goblin.

#### Rush+Rec Yards — Goblin OVER (6/8) / Standard UNDER (20/29) / Standard OVER (8/17)

Combo of Rush Yards + Rec Yards + correlation. Tickets skip `+`. Standard UNDER 20/29 is usable n without a stack audit.

#### Rush Yards combo — Standard UNDER (15/24)

Thin-to-modest. Combo gap.

---

### 2.3 Receiving volume

**No receiving chip has a D split, margin×D cut, or cover-lift audit.** Goblin OVER book-level script movement is not a substitute.

#### Rec Yards — Goblin OVER (388/611, 63.5%; unique 205/321, 63.9%)

**L5=5 role-tape upgrade (2026-10-23).** Ungated book ~64% undersold the gated pool: unique L5=5 **base 229/293 (78%) · filled 176/216 (82%) · filled+D 43/54 (80%)** — ahead of Rush Goblin filled 69%. Keep receiving legs in the Goblin mix when windows check (ghost-role hand skip still applies). &lt;2 median receptions 11/11 is low-line, not a role floor.

Largest cell on the tape. Missing: D (opp pass D is on the board via unit ranks but **not** HIT/MISS-split), margin×identity, cover lift, secondary composite (empty), usage buckets (inverted 60.7% vs 55.4% at tested floors), tgt-share recency, wind, QB-context cross-tab (does WR OVER rate change when the QB clears Pass Yards), line movement (`player_reception_yds` in Odds API, not on card).

#### Rec Yards — Standard UNDER (131/262, 50.0%) / Standard OVER (78/168, 46.4%)

UNDER is **off the cover-stack ticket path** (keep-gate; L5=5 was 0/5 vs 41% on the original tape). Ungated full-tape UNDER is a coin flip. List L5≥4 still on. No D/margin re-audit on 3,855.

#### Rec Yards combo — Standard UNDER (11/17)

Thin. Combo gap.

#### Receptions — Goblin OVER (136/205, 66.3%; unique 126/190)

No D/margin. Usage inverted (70.1% below-floor vs 52.9% at/above). Secondary empty. Line movement market exists (`player_receptions`). Wind more relevant than for rush.

#### Receptions — Standard UNDER (39/78) / Standard OVER (19/38)

No chip audit.

#### Longest Reception — Standard UNDER (52/86, 60.5%) / Standard OVER (40/85, 47.1%)

No D/margin. Secondary explosiveness-allowed is the natural pairing (empty).

#### Rec TDs — Goblin OVER (2/3)

Thin. Same TD/red-zone gap as Player TDs.

---

### 2.4 TD / short yardage

Yards-style L5=5 is the wrong stack (0.5-line). Player TDs **faded from tickets**, list L5≥4 remains.

#### Player TDs — Goblin OVER (75/151, 49.7%) / Standard OVER (2/3) / Standard UNDER (2/6)

| Requirement | Status |
|---|---|
| Ungated | Audited; ~50% Goblin |
| L5=5 ticket stack | **Faded** (keep-gate) |
| List L5≥4 | Still on; not a working TD gate |
| Red-zone / goal-line touch share | **Not built** — the replacement stack, not optional flavor |
| Goal-line personnel | Not built |
| D / margin | Not run on this chip (Week-1 book-level had 8/11 close vs 25/50 blow-fav Goblin; not unique×D) |
| Line movement | `player_anytime_td` in Odds API; not on card |

#### Rush TDs — Goblin OVER (20/41) / Standard UNDER (1/2)

Not in the Player TD fade (different canon). No RZ share, no D/margin. On default ticket stack without audit.

#### Rec TDs — Goblin OVER (2/3)

Thin. Same RZ gap.

#### PAT Made — Goblin OVER (4/7) / Standard UNDER (9/15) / Standard OVER (1/3)

Kicker identity, team PAT vs 2-pt rate, blocked/missed extras: **not modeled**. Near-certainty prop with rare miss modes uncaptured.

---

### 2.5 Kicking

No kicking chip has D, margin, wind, or RZ-to-FG tendency.

#### Kicking Points — Goblin OVER (10/17, 58.8%)

**Off list and tickets** (keep-gate; original fade 20% ungated). Reopening would need: wind, team RZ→FG vs go, kicker range splits, **margin/garbage-time** (blowout TDs replace FGs; backup kicker). None built.

#### Kicking Points — Standard UNDER (28/51, 54.9%) / Standard OVER (9/16)

UNDER has n; no stack audit. Goblin list-fade does not apply to Standard UNDER automatically.

#### FG Made — Goblin OVER (24/44, 54.5%) / Standard UNDER (0/3)

No distance split, no wind, no RZ tendency, no margin. Standard UNDER unusable n.

#### PAT Made

See §2.4.

---

### 2.6 Turnovers

#### INTs — Goblin OVER (10/24, 41.7%) / Standard OVER (4/8) / Standard UNDER (5/8)

No D, no margin, no usage layer. Secondary + pass-rush composites are the relevant empty CFBD layers. QB turnover-worthy throw rate is not in CFBD (proxy from INT rate is circular). **n=24 Goblin total may never support a gate** even if composites populate.

---

### 2.7 Combos (ticket-skipped)

| Chip | Ungated | Missing for a complete record |
|---|---|---|
| Pass Yards combo Std U | 4/5 | Both pass-yard stacks + correlation |
| Rush+Rec Goblin O / Std U / Std O | 6/8, 20/29, 8/17 | Rush Yards + Rec Yards stacks + correlation |
| Pass+Rush Goblin O / Std U / Std O | 20/36, 23/33, 7/11 | Pass + Rush stacks + correlation |
| Rec Yards combo Std U | 11/17 | Rec Yards stack + correlation |
| Rush Yards combo Std U | 15/24 | Rush Yards stack + correlation |

Nothing computes whether the two legs **covary or offset**. Closing combos only matters for list/print.

---

## 3. CFB1H

Same prop families, separate board (`outputs/<date>/cfb1h/`). L5 is **Q1+Q2 ESPN PBP only**. There is **no grader** (`graded_cfb1h` does not exist). STEP A does not require it (requiring it would never skip). Do not join full-game actuals to 1H lines. Every gap in §1–§2 is also open here, plus the grade tape itself.

---

## 4. What is already closed (do not re-open as “missing”)

- CBB-string injury/usage copy-paste in CFB step6 (tests in `tests/test_cfb_cbb_copypaste.py`)
- Five keep-gates in `cfb_keep_gates.py`
- Pass Yards Goblin OVER skip-D as a live ticket/badge rule
- Rush Yards Goblin OVER keep-D as a live ticket rule
- Pass Yards D+RH-in-blowout **veto not graduated**. Week 1 cell = 2/3 unique. 09-12 did not re-test that intersection (RH×blow-fav existed, D+ = 0)
- Rush Yards D-blind-to-script **logged, not a gate**. Close unique Week 1 12/15 + Week 2 2/2 = 14/17; blow-fav 7/15 + 7/13 = 14/28 (majority unique)
- STEP A `graded_cfb` (and tennis/golf/WCBB/period NBA/WNBA/NFL) skip-list
- 09-12 CFB grades in `graded_props_2026-09-12.json`
- 09-19 logged as permanent loss

Usage-share **floors** were tested and found inverted — that is a closed *finding*, not a closed *layer*. The layer still needs bucket-level work if it is ever used.
