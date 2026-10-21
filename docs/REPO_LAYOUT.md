# PropORACLE layout (safe cleanup contract)

## Canonical live slate

| Role | Path |
|------|------|
| Live step1–step8 per sport | `outputs/<YYYY-MM-DD>/<sport>/` |
| Combined / Goblin-70 / publish inputs | dated `outputs/` + `ui_runner/` templates |
| Worktree used for slate ranking | usually `PropORACLE_main_cp\outputs\<date>\` after 1AM/8AM |

Do **not** treat `Sports/<SPORT>/step8_*.xlsx` as the live board. Those copies are leftovers or sport-tree mirrors.

## `Sports/` is not “code only”

Classify by **what writes the file**, not by folder name. Do not purge or relocate these live writer targets:

### NFL (mid-pipeline stays under Sports)

- `Sports/NFL/data/outputs/step2_clean_props.csv`
- `Sports/NFL/data/outputs/step3_nfl_with_defense.csv`
- `Sports/NFL/data/outputs/step5_nfl_with_stats.csv`
- `Sports/NFL/data/outputs/step6_*.csv` (hit rates / game context)
- `Sports/NFL/outputs/step7_nfl_ranked.xlsx`, `step8_nfl_direction_clean.xlsx`
- Dated copies under `Sports/NFL/outputs/<date>/` when the pipeline writes them
- Caches: `Sports/NFL/data/cache/`, defense / OD ranking artifacts under `Sports/NFL/data/`

### Step8 (or step7) mirrors other sports still write under Sports

- MLB: `Sports/MLB/outputs/step8_mlb_direction_clean.xlsx`
- Soccer: `Sports/Soccer/outputs/step8_soccer_direction_clean.xlsx` (standalone soccer runner also uses this tree)
- Tennis: `Sports/Tennis/outputs/step8_tennis_direction_clean.xlsx`
- NFL: mirrors above

### Caches (all sports) — stay put

ESPN / boxscore / id / injury / pace caches under each `Sports/<SPORT>/` tree.

## Scratch quarantine

One-off probes live in `archive/scratch/<batch>/` (see `MANIFEST.md` in each batch).

Rules for future cleanup:

1. Grep every `.py` / `.ps1` / `.bat` / `.cmd` / `.json` / scheduled-task XML for the basename before moving.
2. Keep anything referenced outside the scratch cluster.
3. Never move NFL (or other) **pipeline writer** outputs based on path heuristics alone.
4. Prefer `git mv` (or move + `git add -f` for previously untracked files) in one commit.

## Package code

| Tree | Role |
|------|------|
| `proporacle/` | shared libraries (e.g. `table_io` step8 meta / STALE banner) |
| `utils/` | keep gates, correlation, sport helpers |
| `scripts/` | pipelines, daily windows, graders, publish |
| `ui_runner/` | Flask / Railway site |
| `tests/` | pytest |
