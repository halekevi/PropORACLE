# NFL Tuesday post-week cadence (Week 2 → Week 3)

After Week 2 grades land (Tue AM). Do **not** run this Monday while MNF is live.

**Week 2 grade window (only these count as complete):**
- `2026-09-20` (Sun) — full slate
- `2026-09-21` (Mon) — NYG @ LAR

`outputs/2026-09-18` / `09-19` / `09-22` empty stubs do **not** count. Backfill skips empty Box Raw; do not treat those folders as “Week 2 complete.”

## Checklist

1. **Run the pack** (preferred — refreshes CSVs + current O/D md)
   ```powershell
   pwsh -File Sports\NFL\scripts\run_tuesday_post_week.ps1 -Season 2026 -Week 2
   ```
   Or step-by-step:
   ```powershell
   py -3.14 Sports\NFL\scripts\build_nfl_route_participation.py --season 2026 --week 2
   py -3.14 Sports\NFL\scripts\build_nfl_unit_line_ranks.py --season 2026
   py -3.14 Sports\NFL\scripts\build_nfl_pace_ranks.py --season 2026
   py -3.14 Sports\NFL\scripts\build_nfl_qb_mobility.py --seasons 2025,2026
   py -3.14 Sports\NFL\scripts\build_nfl_red_zone_rates.py --seasons 2025,2026
   py -3.14 Sports\NFL\scripts\build_nfl_division_ranks.py
   py -3.14 Sports\NFL\scripts\build_nfl_od_rankings_md.py --season 2026 --week 3
   ```
2. **Confirm live file**
   - Read `Sports/NFL/data/nfl_od_rankings_current.md` (not sealed week2)
   - Check `nfl_od_rankings_meta.json` → `built_at` is today
3. **Week 2 prop-gate ledger** (first real Goblin sample + WOPR validation)
   ```powershell
   py -3.14 Sports\NFL\scripts\backfill_nfl_week1_prop_gates.py --skip-grade --dates 2026-09-20,2026-09-21 --ledger-tag week2
   ```
   Writes `Sports/NFL/data/nfl_prop_gate_ledger_week2.{csv,json}`.
4. **Odds API before NFL step6b**
   - Confirm `ODDS_API_KEY` in env / `.env`
   - Confirm `x-requests-remaining` > 0 (401 `OUT_OF_USAGE_CREDITS` = quota, not a dead key — refresh/upgrade at the-odds-api.com or wait for monthly reset)
   - step6b skips blank fallback caches and uses ESPN totals when Odds API is out of credits

## Current vs sealed

| File | Role |
|------|------|
| `nfl_od_rankings_current.md` | **Live** — tickets, hand-checks, agents |
| `nfl_od_rankings_meta.json` | `built_at`, source seasons, pairing week |
| `nfl_od_rankings_week2.md` | Sealed Week-2 archive (do not use after Week-3 refresh) |
| `nfl_od_rankings_week3.md` | Written by Tuesday pack when week 3 leans are built |

Pipeline `run_nfl_pipeline.ps1` also regenerates `current.md` after division ranks so soft context stays current mid-week.

## Ledger must answer

| Question | Where to look |
|----------|----------------|
| Does WOPR tier stratify UNDER hit rate above the 82–87% baseline? | Week-2 ledger × Low/Med/High WOPR on rec UNDER keep props |
| Does the first Goblin sample show OVER or UNDER skew? | `goblin_diagnostic_report` / Goblin OVER unique-game hit rate vs Standard UNDER |
| Do longest rush / rush attempts UNDER earn allowlist consideration? | Unique-game n + hit rate vs keep floors; only promote if clears with honest n |
| Does pace tier correlate with receptions volume as expected? | `nfl_pace_ranks` × receptions actuals / targets on Week-2 graded board |
| Soft pass UNDER — L5=5 volume after W3? | Today n=1–3; fail-closed on missing Pass D already tested |
| `rushing_yards` OVER gated cell (Elite OL + Weak Rush D)? | Reopen only if gated HR holds with usable n (not ungated 40% / L5=5 2/3) |
| `passing_yards` / `passing_tds` Standard OVER L5=5 n≥10? | Today 1/1 each — still watch |
| Goblin wave-1 only on flag flip? | `NFL_GOBLIN_OVER_KEEP_FLIP`; pass-side stays `PASS_LATER` |
| Goblin flag GO/NO-GO? | `nfl_goblin_wave1_flip_go`: each wave-1 chip ≥75% unique Goblin OVER with n≥5 — **not** overall Goblin HR |

## Gate map (from current O/D md + keep_fade)

- Rec / receptions / longest rec **UNDER** → Opp Pass D (+ Low WOPR pack bump)
- Pass volume **UNDER** → soft Pass D Elite (± NOT Elite Pass Off / DL pass); **fail-closed** if missing
- Rush **UNDER** → watch; Opp Rush D + not-elite off/RB
- Rush **OVER** → **watch**; soft Elite OL + Weak Rush D
- Pass yards / Pass TDs **OVER** → **watch** until L5=5 n≥10
- Punts / YPC / longest completion **OVER** → soft keep
- Sacks **UNDER** → keep
- Tackles+Ast **UNDER** → keep; axis **opp rush OFF**
- Goblin → tickets OFF; wave-1 = fg / rec_yds / longest_rec / rush_yds

## Status

- [x] Week 2 grades complete (Sun + Mon) — NO_ACTUAL=0; voids = DNP / not-on-roster / NO_FG_MADE only
- [ ] Tuesday pack run (route + unit + pace + QB mobility + RZ + division + current.md)
- [ ] `nfl_od_rankings_meta.json` `built_at` is fresh
- [ ] Week 2 ledger published
- [ ] Odds API remaining > 0 before step6b (else ESPN schedule/venue only — **no implied totals**)
- [x] Soft-gated pass UNDER keep + fail-closed missing Pass D (tests)
- [x] Thin OVER demoted to watch: rush yards / pass yards / pass TDs Standard OVER
- [x] Goblin staging: wave-1 FLIP vs PASS_LATER (pass-side not on flag flip)
- [x] Receptions UNDER Option C: pack soft-rank bump (WOPR low + Elite|Above Pass D); allowlist unchanged; harden only after ticket D = opp_pass_d_tier + Week 3+ hold
- [ ] **Week 3 receptions UNDER Option C ledger split** (after grades):
  - Bumped (WOPR low + Elite|Above Pass D) HR — expect ~96%; hold **90%+** with added n = Option A unlock *candidate*
  - Non-bumped HR — expect ~75% base
  - If bumped → ~80% while non-bumped holds ~75%, stack is losing signal → do **not** harden Option A
  - Option A still blocked until `opp_pass_d_tier` is ticket D (not badge-only)
- [ ] Week 3 rush UNDER watch: NOT Elite Rush Off + NOT Elite RB + Elite Rush D (soft; track grades) — W1+W2 9/9
- [ ] Week 3 rush OVER watch: Elite OL + Weak Rush D (opp_rush_d_tier) (soft; track grades)
- [ ] Week 3 rec UNDER watch: Elite Pass D (`opp_pass_d_tier`); best stack NOT Elite Pass Off + Elite Pass D (rec U 41/44=93%, rec yds U 44/55=80%) — WOPR low adds on receptions (42/44=96%)
- [ ] Week 3 rec OVER watch: Weak Pass D (`opp_pass_d_tier`); Fast pace + Weak Pass D on rec yds (14/23=61%); secondary alone is noise/hurt
- [ ] Week 3 pass yards UNDER watch: Elite Pass D (8/10=80%); NOT Elite Pass Off + Elite Pass D (7/8=88%) — thin
- [ ] Week 3 sacks_taken UNDER watch: Weak opp `dl_pass_tier` (4/5=80%) + Elite own pass block (6/9=67%) — soft; DL pass axis correct here
- [x] `nfl_dl_ranks.csv` has `dl_pass_tier` + `dl_run_tier` (rush props: prefer `opp_rush_d_tier`; DL run tier soft-only — Elite run-stop did not lift UNDER in W1+W2)
- [ ] Week 3 step1 board present (Thu–Mon slate folder under `outputs/`)

**Grades are in (Sun 9/20 + Mon 9/21).** Run this list and answer the four ledger questions in chat.
