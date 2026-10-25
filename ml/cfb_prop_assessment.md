# CFB prop assessment

Working note next to `prop_skeleton.md` and `Sports/CFB/GAP_CATALOG.md`.
Keep file: `utils/cfb_keep_gates.py`. Game-script and OL/DL overlays
are **observe-only**. `CFBD_API_KEY` is not set, so the line /
secondary composite is empty.

## 1. Where CFB stands

Saturday sport. Keep exceptions already shipped:

- Pass Yards Goblin OVER — L5=5+cover, no D (D fails 90% on the old cut)
- Player TDs — list L5≥4, **off** the L5=5 ticket stack (0.5-line)
- Standard Rec Yards UNDER — off the cover-stack (L5=5 is 0/5 vs 41% base)
- Standard Rush Yards UNDER — keep L5=5+D+cover
- Rush Attempts / Kicking Points Goblin OVER — off list and tickets

Usage floors tested **non-monotonic**. Share-floor inverted at 25–50%.
Role stays **by hand** (junk-volume skip on overs). Do not wire share
or committee floors into keep gates. Player TDs stay broken until
red-zone share exists.

L10≥8 is not a CFB ticket gate. Face window can include last year
early; `l5_sample_n` stays current-season. `PRIOR_SEASON_FILL` is
Gold at most.

## 2. Factors behind every CFB prop

| Layer | Factors | Why |
|---|---|---|
| Script | Spread, total, blowout sit | Extreme vs NFL. Starters sit in the third quarter |
| Opportunity | Target / rush share, raw carries and targets | Role check, not a share floor |
| Pace | Plays, clock script | Volume for every yardage prop |
| QB / OL | QB status, line yards, stuff rate | Pass yards and sacks |
| Opponent | National unit ranks; conference ranks soft | D skip on Pass Yards Goblin OVER |
| Data quality | Missing CFBD, dead Saturdays (09-19, 10-03) | Line composite cannot populate |

Logical order on a slate:

1. Drop Rush Att / Kick Pts Goblin OVER and Player TDs from the
   ticket stack.
2. Prefer Goblin rush + pass + receiving with a filled window.
3. Rush Standard UNDER only with filled window + Elite|Above + cover 8.
4. Veto OVER into a blowout sit — observe the script overlay; do not
   graduate it from Week 1’s 2/3 cell.
5. Price at N-correct. CFB is weekly; do not invent midweek cells.

## 3. What moves each prop

| Prop | Drivers |
|---|---|
| Pass yards | Pace, implied total, blowout sit, OL. D is N/A on Goblin OVER |
| Rush yards | Carry share, leading-team script, opp rush D |
| Rec yards / receptions | Target share, QB, junk-volume skip by hand |
| Player TDs | Red-zone share. Broken without it |
| Kicking points | Implied total. Goblin OVER off |
| Sacks / INT | Thin. DL composite empty without CFBD |

## 4. What is already attached

| Piece | Where | Status |
|---|---|---|
| Keep file | `utils/cfb_keep_gates.py` | Live exceptions above |
| Script / implied totals | `utils/cfb_implied_totals.py` → step8 | On board 09-03+; observe |
| Unit ranks | `cfb_team_unit_rankings.csv` | Live D except Pass Yards Goblin skip |
| OL / DL / secondary | `utils/cfb_line_secondary.py` | Empty until `CFBD_API_KEY` |
| Usage cache | `build_cfb_usage_cache.py` | Same key. Role by hand |
| Gap catalog | `Sports/CFB/GAP_CATALOG.md` | Source of truth for what is audited |

## 5. Gaps, in build order

1. **Set `CFBD_API_KEY`** (free). Unlocks SP+, PPA, advanced line
   stats, red-zone. First use: red-zone share so Player TDs can be
   judged instead of faded.
2. Fill OL/DL/secondary and correlate vs `graded_props`, not vs the
   weekly O/D markdown.
3. Neutral-script / standard-downs rate (garbage-time stripped).
4. Snapshot pre-kick script + usage the same as NFL volume.

## 6. How the ML fits

```
plays × role (carries / targets)
        ×  shrunk yards per touch
        ×  spread / total / sit risk
        →  yardage distribution
        →  P(over) on each rung
        →  EV at the scraped N-correct quote
```

CFB blowouts are larger than NFL. Opportunity goes to zero when the
starter sits. Script is not optional context.
