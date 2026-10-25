# CBB / WCBB prop assessment

Working note next to `prop_skeleton.md` and `Sports/CBB/README.md`.
No keep file. Full parallel runs skip both boards until **2026-11-01**.
Opening night is about a week out. The job this week is a **dry pass**,
not a gate search.

## 1. Where CBB stands

2025-26 ended early April. Off-season 2026-04-07 through 2026-10-31.
`MAIN` and the parallel job resume 2026-11-01 (override
`PROPORACLE_CBB_RESUME` or `-ForceCBB`).

Feb–Mar graded files lost the prop name to an em dash on **1,310–6,467
rows per day** (2026-02-26..03-15). That is a grading-file defect, not
a face defect. `utils/face_ladder.py` already excludes those
(`garbage_prop`) from ladder math. The writer that produced the dash
must not come back on opening night.

WCBB shares the men’s defense file until a women’s table exists.
Torvik is mens only.

## 2. Factors behind every CBB prop

| Layer | Factors | Why |
|---|---|---|
| Opportunity | Minutes, short vs deep rotation | Largest lever, same as NBA |
| Tempo | Possessions (Torvik / T-Rank) | Volume for every counting stat |
| Script | Spread — non-conference blowouts | Starters sit earlier than NBA |
| Context | Conference vs non-conference | Early November is almost all non-con |
| Data quality | `pick_type`, em-dash props, stale athlete master | Last year’s scramble was NBA; do not repeat it here |

Logical order once boards post:

1. Drop garbage / blank prop names. Do not grade them.
2. Prefer minutes-stable rotation players over 8-man junk minutes.
3. Veto OVER into a 20-point non-con sit.
4. Price at N-correct. CBB is out of season for keep cells — no silent
   fallback to last March.

## 3. What moves each prop

| Prop | Drivers |
|---|---|
| Points / PRA | Minutes, usage, tempo, blowout sit |
| Rebounds | Minutes, opponent miss rate, lineup size |
| Assists | Ball-handler minutes, teammate shooting |
| 3PM / FGA | Volume first. Makes are noisier |
| Steals / blocks | High variance. Leave off early tickets |

## 4. What is already attached

| Piece | Where | Status |
|---|---|---|
| Season window | `Sports/CBB/README.md` | Resume 2026-11-01 |
| League IDs | `prizepicks_league_ids.py` | CBB 20, WCBB 176 fallback |
| Torvik | `docs/TORVIK_RATINGS.md` | Mens only; not a gate |
| Em-dash exclude | `utils/face_ladder.GARBAGE_PROPS` | Ladder only — writer still needs a fix |
| Box caches | `cbb_boxscore_cache.csv`, `wcbb_boxscore_cache.csv` | Week-1 L5 needs last season |

## 5. Gaps — dry pass this week (urgent)

Run before 2026-11-01 so opening night is not another NBA-style scramble.

```powershell
.\run_pipeline.ps1 -CBBOnly -ForceCBB
```

Checklist:

1. **step1 persists `pick_type`** — refuse a ≥100-row board that is
   100% Standard (same assert as NFL). Empty / dash is not a face.
2. **Import smoke test passes** — athletes master (women’s file is
   separate), box caches, defense ranks.
3. **Faces verify** on whatever exhibition / early-DI board posts.
4. **Prop-name writer** — no em dash / replacement character on
   `prop` in graded JSON or step8. Confirm with a fixture that used
   to emit `\u2014`.
5. Confirm live WCBB league id when the tab is dark vs posted.

Force is for the dry pass only. Do not publish CBB tickets from it.

## 6. How the ML fits

```
minutes × rotation length
        ×  per-possession rate (Torvik tempo / efficiency)
        ×  spread (non-con blowout)
        →  distribution
        →  P(over) on each rung
        →  EV at the scraped N-correct quote
```

Port the NBA projection layer after the dry pass is green. Calibrate
on past NCAA seasons over the winter if November volume is too thin
to learn live. KenPom is the paid alternative to Torvik; do not block
on it.
