# Period prop assessment (NBA1Q / NBA1H / WNBA1H / CFB1H)

Working note next to `prop_skeleton.md`. **All held** until a
period-level history drives L5 and the projection. Full-game numbers
on a quarter line are how the phantom NBA1Q slips happened.

## 1. Where periods stand

NBA1Q tickets are held in `utils/ticket_sport_holds.py`
(`PROPORACLE_NBA1Q_TICKETS`). Early-October cards were a May Finals
leftover, not a leak-cost lesson. 2026-10-20 was the first real 1Q
slate (two slips) — not enough to price the leak.

The stale-board gate that caught those slips stays on. Period fetch
exists; period L5 / `proj_mu` do not.

WNBA1H / WNBA1Q and CFB1H share the same defect shape: if period
boxes are missing, step4 will pad from the full game (Gafford Goblin
Points 1.5 vs a ~16 full-game projection).

## 2. Why the full-game layer is not enough

Opportunity in a quarter is not “minutes / 4.” Starters play most of
Q1; blowouts empty Q4; CFB1H sit patterns are not NFL. A calibrated
NBA full-game model will still misprice 1Q until it sees 1Q boxes
only, games strictly before *G*.

## 3. Gaps — revisit after full-game layers

1. Period box ingest that cannot fall back to the full-game actual
   (`scripts/grading/period_actuals_guard.py` already guards grades;
   L5 / projection need the same wall).
2. Stale-board protection: `game_date` / `fetched_at` / opponent must
   match the slate. The OKC@SAS May Finals copy is the test case.
3. Port the sport’s full-game adapter with a period box index, then
   write a calibration test (train / test by season, Brier / ECE on
   1Q points / PRA / CFB1H pass yards) **before** the run.
4. Re-enable one board at a time with an env flag. NBA1Q first after
   NBA full-game minutes snapshots have a month of `pre_tip` rows.

Do not mix period legs onto Goblin-70 or the mixer while the hold is
on. Do not treat a full-game L5=5 as a 1Q keep.
