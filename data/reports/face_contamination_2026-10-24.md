# Face contamination audit — 2026-10-24

Live NFL `step1_fetch_prizepicks_nfl.py` did not persist `pick_type` before
2026-10-23. step7 (`df.get("pick_type", "Standard")`) and
`utils/nfl_prop_gate_ledger.py:73` defaulted the blank to **Standard**, so every
Goblin and Demon rung on those boards was graded and analyzed as a Standard
face. This note lists what the label defect touched, what was fixed, and what
is still open. Nothing on the ticket path was changed by the audit except the
`unverified_face` stamp on archived slips.

## How a bad date is detected (`utils/face_ladder.py`)

Two sport-date signals over the leg ledger (`proporacle.data.leg_ledger.current_legs`):

| signal | rule | meaning |
|---|---|---|
| `default_labels` / `blank_labels` | n >= 100 graded rows and **zero** Goblin/Demon | pick_type was defaulted (or blank) downstream |
| `laddered_standard` | >= 100 Standard rows and >= 20% of them sit on a player-prop-date with 2+ Standard lines whose spread is >= 1.0 **and** >= 20% of the line | rungs graded as Standard even though the column exists |

Baseline on real-face days is 0–7% (NHL runs 10–19% because the grader merges
several intraday fetches). The spread rule keeps intraday moves (TOI
21.2→22.0, faceoffs 9→10, points 24.5→26.5) out of the ladder; rungs are wide
(sacks 0.5 vs 1.5, rec yds 4.5 vs 58.5, saves 19.5…26.5). Rows whose prop name
is blank / em dash are counted as `garbage_prop` and excluded from the share.

Per-leg verdicts for tickets: `standard_line != line` → rung by sign; 3+ lines
→ min = `goblin_rung`, max = `demon_rung`; 2 lines → cross-book nearest else
`ambiguous_rung`; 1 line → `single_line_unverified`; none → `no_board_rows`.
Only rung verdicts stamp `invalid_reason = unverified_face` (never VOID).

## Sport-dates that fail (1021 sport-dates scanned, 138 fail)

| sport | status | dates | span | rows |
|---|---|---|---|---|
| NFL | default_labels | 16 | 09-09, 09-10, 09-13, 09-14, 09-17, 09-27 .. 10-19 | 13,612 |
| NHL | default_labels | 45 | 2026-02-25 .. 06-15 | 22,586 |
| NHL | laddered_standard | 3 | 03-06 .. 03-08 (50%) | 5,344 |
| Soccer | default_labels | 35 | 05-09 .. 06-16 | 64,230 |
| Soccer | laddered_standard | 11 | 03-26 .. 04-11 (36%) | 37,197 |
| Tennis | default_labels | 14 | 04-11 .. 07-08 | 14,152 |
| NBA1H | default_labels | 14 | 02-19 .. 04-23 | 2,736 |

NFL real-face dates in the sample: **09-20, 09-21, 09-24** (and 10-21+ once
step1 persisted pick_type). Everything NFL from 09-27 to 10-19 is label-only.

Cleared by the spread / garbage rules (they were false positives of the first
pass): CBB 02-26..03-15 (1310–6467 rows per day with an em dash for the prop
name — a grading-file defect, not a face defect; see `garbage_prop` warning),
CFB 09-05 (same em-dash defect on 1310 rows), NHL 10-08 / 10-09 / 10-21 and
NBA Apr–Jun playoff dates (intraday line moves), MLB 06-28, Tennis 05-26,
WNBA 05-03.

NHL 10-10 was a real hit, but from a stale copy: this checkout's
`ui_runner/templates/graded_props_2026-10-10.json` (committed by "Daily slate
2026-10-11 [auto]") had all 3082 NHL rows as Standard while the live tree had
Demon 2489 / Standard 334 / Goblin 259. The 2748 wrong labels were restored
from the live copy by exact (player, prop, line, direction) match and the file
re-ingested; `current_legs()` now keeps only the newest sha of each source file
so superseded rows drop out (leg_id includes pick_norm, so a plain leg_id dedup
could not retire them).

## 1. NFL keep gates (`utils/nfl_keep_gates.py`)

Yes — the measured lift was computed on mislabeled faces. Same ledger, same
props, split by face status:

| Standard UNDER | default-label dates | real-face dates (09-20/21/24) |
|---|---|---|
| receptions | 224/287 = 78.0% | 25/42 = 59.5% |
| receiving_yards | 242/318 = 76.1% | 36/81 = 44.4% |
| rushing_yards | 138/194 = 71.1% | 23/36 = 63.9% |
| sacks | 287/320 = 89.7% | no real-face sample |
| tackles | 210/306 = 68.6% | no real-face sample |
| longest_reception | 100/125 = 80.0% | no real-face sample |
| kicking_points | 53/66 = 80.3% | no real-face sample |

The W1+W2 Standard UNDER keeps (receptions 41/44, rec yds 44/55, sacks,
tackles_ast, kick pts, longest reception, pass volume) are Demon rungs graded
as Standard UNDER hits. The Goblin OVER wave-1 flip cells (fg_made 31/42, rec
yds 311/448, receptions 174/243, pass yds 91/127, rush yds 142/238) come only
from the three real-face dates and survive. Module now carries
`NFL_KEEP_GATE_FACES_VERIFIED = False` and the table above in its docstring;
ticket behavior unchanged — re-measure on 10-23+ boards before trusting any
NFL Standard UNDER keep.

## 2. The 10-11 decisions

`scripts/observe_playbook_shadows.py` (live tree) builds `nfl_cells()` by
filtering graded JSON on `pick_type == "Standard"` — label-only. The
"NFL gated Standard UNDER on 3-leg Flex" observe path and the
rule-4-for-NFL ruling (sacks UNDER 0.5 fragility, "the ungated hit rate does
not waive it") both read those rows. Added `face_label_caveat` to the frozen
criteria JSON, the rule-4 block, the diagnostic report, the observe ledger
(`forward_legs_require: face_ladder status == verified`) and the printed
output. Thresholds untouched; forward counter still 0. The pre-registered
replacement that requires a persisted Standard face is
`ml/prereg/nfl_standard_under_2026-10-24.md` (`utils/nfl_std_under_shadow.py`).

## 3. Live NFL tickets (`scripts/report_unverified_face_tickets.py --sport NFL`)

91 archived NFL tickets / 182 legs checked, 09-04 .. 10-19.

| verdict | legs |
|---|---|
| goblin_rung (labeled Standard, was the Goblin line) | 83 |
| single_line_unverified | 39 |
| labeled_goblin (correct) | 31 |
| no_board_rows | 29 |
| demon_rung / ambiguous_rung | 0 |

**48 tickets stamped `invalid_reason = unverified_face`** across 17 archive
files (`combined_slate_tickets_*.json`, ticket_runs). All are `graded_main`
groups, `placed = None`, 0 rows in the accounts DB `placed_slips` — none was
submitted. Examples: 09-09 Mack Hollins receiving_yards OVER 4.5
(standard_line 58.5) and Craig Woodson tackles_ast OVER 3.5 (vs 9.5) on
"NFL Standard 2-Leg" groups — Goblin legs priced as Standard. No Standard UNDER
leg turned out to be a Demon rung on a ticket (Demon is OVER-only, so a
Demon-as-Standard UNDER could not have been built). `unverified_face` is in the
`utils.proporacle_data_root` invalid skip set next to `stale_board` /
`published_after_start`, so book / winrate / forward exclude them. Live
`tickets_latest.json` is never touched by the stamper — the builder owns it.

## 4. discover_gates NFL rows

`ml/discover_gates.py` now drops rows on unverified sport-dates before
scoring (`drop_unverified_face_dates`, recorded per book as `face_filter`).
NFL re-run: 3051 rows → 1974 dropped on 15 dates → 1077 kept. The previous
Goblin OVER n=576 figure was computed on label-only rows; the Standard UNDER
84.6% n=136 figure is gone (real-face Standard UNDER n=211, generic stack
0%). `summary.md` carries the caveat. NHL / Soccer / Tennis / NBA1H spans
above are filtered the same way on their next run.

## 5. General guards (all sports)

- `utils/pick_type_persist.assert_pick_type_persisted(out, sport=...)` runs
  after the final write in every step1 (CBB, CFB, Golf, MLB, NBA, NFL, NHL,
  Soccer, Tennis, WNBA; both trees). Fatal on missing column / all blank /
  >= 100 rows 100% Standard; `PROPORACLE_PICK_TYPE_GUARD=warn` downgrades;
  Golf is Standard-only by design. Dry-run on live 10-23 / 10-24 boards: all pass.
- `proporacle/data/ledger_checks.py` (both trees) emits warn-only
  `pick_type_default`, `standard_ladder`, and `garbage_prop` warnings per
  sport-date. Since 10-01 it fires on the nine NFL default dates and nothing
  else. The live tree has no ledger parquet (`H:\PropORACLE_main_cp\data\ledger`
  is empty), so those checks are a no-op there until the ledger is shared.
- CFB: pool logs showed goblin=0 std=0 because the pool was empty, not because
  faces were mislabeled; CFB step1 does persist pick_type (every CFB date since
  09-03 is `verified` once the em-dash rows are excluded).
- `scripts/check_scheduled_imports.py` flags `missing_module` as well as
  `missing_name`; fixed on the live tree: guarded `utils.l5_ranked_by_cell`
  import in `report_track_comparison.py`, copied `utils/nhl_excluded_props.py`.
  One pre-existing problem remains on both trees:
  `scripts/compare_winrate_goblin_opt3_shadow.py:35` imports
  `scripts.ticket_slip_grader`, which does not exist.

## 6. Did mislabeling cause the 0.97 → 0.36 Standard top decile?

Reproduced the May–June table (`scripts/standard_decile_face_verified.py
--book standard --prob-col ml_prob --from 2026-05-01 --to 2026-06-30`;
stored `ml_prob` on `data/retrain_dataset.csv` Standard rows), then split by
face status:

| slice | n | top decile pred | realized | gap |
|---|---|---|---|---|
| all (original table) | 74,978 | 0.974 | 0.354 | +0.620 |
| face-verified dates | 39,123 | 0.979 | 0.520 | +0.458 |
| unverified dates | 35,859 | 0.976 | 0.206 | +0.770 |

Rows with p ≥ 0.90: verified 3,431 realize 0.519; default-label 3,307 realize
0.217 (Soccer default-label 2,680 rows realize 0.154; NHL 627 realize 0.486).
Unverified "Standard" rows realize 8–16% in the lower deciles — Demon rungs
(not Goblin) labeled Standard; NHL and Soccer boards are Demon-heavy.

Answer: half and half. Mislabeling is what made the tail *realize* 0.35 —
on real faces the same decile realizes 0.52, the Standard base rate. But the
model still predicts 0.97–0.98 on clean faces (WNBA verified p ≥ 0.90:
2,162 rows at 0.512), so the over-confidence lump itself is not a label
artifact; the pick-type / thin-history diagnosis stands for that half.
Re-scored with the current pickle on all Standard rows (`--book standard`,
no `--prob-col`): verified deciles are monotone with gap +0.03..+0.16;
unverified rows realize 0.12–0.38 across the board. Any retrain must build
from `analysis_legs()` / face-verified dates only.

## 7. Keep gates outside NFL (`scripts/keep_gate_face_split.py`)

| sport | marker | finding |
|---|---|---|
| Soccer | `SOCCER_KEEP_GATE_FACES_VERIFIED = False` | 35 default-label dates 05-09..06-16 + 11 laddered 03-26..04-11 (28,301 legs). Goblin cells (Shots 16/21, SOT 46/68, Saves 205/343) were Goblin-labeled rows, not inflated by inclusion, but could not be re-measured from the ledger (soccer stores `last5_over`; ledger `l5_over` blank — verified L5=5 Shots Goblin 7/11). Any soccer Standard cell from that window is label-only (Standard UNDER Shots 69.7% default vs 53.2% real). |
| Tennis | `TENNIS_KEEP_GATE_FACES_VERIFIED = True`, `TENNIS_STANDARD_FACES_VERIFIED = False` | 14 default-label dates 04-11..07-08. Goblin cells were measured 07-13..09-14, after the span; verified ledger Goblin GW 3676/5200 = 70.7%, Total Games 2839/4589 = 61.9% but L5=5+L10≥8 only 35/62 = 56.5% (below the 70.2% price — re-check on a step8 join). Standard OVER GW (12/16, 30/40) is label-era. |
| NHL | `NHL_STANDARD_UNDER_CELL_FACES_VERIFIED = False` (`utils/ticket_sport_holds.py`) | 45 default-label dates 02-25..06-15 (17,437 legs). Standard UNDER L5=5+L10≥8 on those dates: points 130/162 = 80.2%, assists 133/162 = 82.1%, goals 107/126 = 84.9%; real-face dates: 5/6, 1/2, 0/0. The 70.9% n=1266 cell was Demon rungs — same shape as NFL sacks; explains the 53.4% forward read. Goblin OVER (sog 62.4%, points 60.4% verified) stands. |

## 8. Face verification is first-class

- `proporacle.data.leg_ledger`: `face_status` ledger table
  (`data/ledger/face_status/face_status.parquet`, one row per sport-date,
  `status` + `face_verified`), refreshed after every `ingest_graded` and when
  a new sport-date appears. `current_legs()` always carries
  `face_status` / `face_verified`; **`analysis_legs()`** is the default entry
  for hit-rate / gate / calibration work and drops unverified rows unless
  `include_unverified=True` is passed, printing what it held back.
- `utils/nfl_prop_gate_ledger.prepare_decided` fails closed: blank
  `pick_type` → `Unverified` (never Standard); a date with ≥100 decided rows
  and zero Goblin/Demon → whole date `Unverified`; `face_verified` column.
  The W1+W2 keep tables rebuild without the default-label dates.
- `ml/discover_gates.py`, `scripts/keep_gate_face_split.py`,
  `scripts/standard_decile_face_verified.py` read the stored table.
- Tests: `tests/test_leg_ledger.py::test_face_status_is_first_class_and_analysis_legs_filters`,
  `tests/test_nfl_prop_gate_ledger.py::test_blank_or_defaulted_pick_type_never_becomes_standard`.

## Still open

- NFL Standard UNDER has **no** trustworthy cell; sacks / tackles / longest
  reception / kicking points have zero real-face rows. Accumulate via the
  pre-registered shadow.
- NHL Feb–Jun (45 dates) and Soccer Mar–Jun (46 dates) default / laddered
  labels mean the NHL Standard UNDER `L10>=8` 70.9% cell and any soccer
  Standard cell from that window are label-only too.
- CBB 02-26..03-15 and CFB 09-05 graded files lost prop names (em dash); those
  legs cannot be keyed and are excluded from ladder math. Regrade if the
  step8 sources still exist.
- `scripts.ticket_slip_grader` missing import (above).
- Soccer OVER grading looks depressed on verified dates too (Standard OVER
  Shots 28%, Goblin OVER Shots 38.6% ungated) — likely DNP/missing actual
  graded as 0 → MISS, the mirror of the tennis aces inflation. Not a face
  issue; needs its own check before soccer OVER cells are trusted.
- Soccer / tennis keep cells (Shots L5=5+L10≥8, SOT L5≥4+Off, GW discount,
  Total Games L5≥4+L10≥8) still need a face-verified step8 re-measure; the
  ledger lacks the inputs.
- Retrain `edge_model_*` from `analysis_legs()` rows only; the current
  pickles were fit with 37,000 default-label "Standard" rows (42% of that book).
