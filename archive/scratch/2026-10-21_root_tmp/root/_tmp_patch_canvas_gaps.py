#!/usr/bin/env python3
"""Patch canvas: close cheap Soccer/Tennis gaps + MLB board coverage."""
from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd

CANVAS = Path(
    r"C:/Users/halek/.cursor/projects/h-PropORACLE/canvases/active-sport-prop-hit-rates.canvas.tsx"
)
_REPO = Path(r"H:/PropORACLE")
MAIN = _REPO.parent / "PropORACLE_main_cp"
cat = json.loads(
    (_REPO / "data/reports/all_ranked_prop_hit_rates.json").read_text(encoding="utf-8")
)

# --- MLB graded presence for Doubles/Triples/SB ---
import sys

sys.path[:0] = [str(_REPO / "scripts"), str(_REPO)]
import prop_hit_tiers as T  # noqa: E402

dirs = [
    MAIN / "ui_runner" / "templates",
    MAIN / "mobile" / "www",
    _REPO / "ui_runner" / "templates",
    _REPO / "mobile" / "www",
]
by_date = {}
for d in dirs:
    if not d.is_dir():
        continue
    for p in sorted(d.glob("graded_props_20*.json")):
        if ".bak_" in p.name:
            continue
        date = p.stem.replace("graded_props_", "")[:10]
        if date not in by_date:
            by_date[date] = p

from collections import Counter

mlb_gap = Counter()
for path in by_date.values():
    raw = json.loads(path.read_text(encoding="utf-8"))
    for r in raw.get("props") or raw.get("rows") or []:
        if not isinstance(r, dict):
            continue
        if T.norm_sport(str(r.get("sport") or "")) != "MLB":
            continue
        prop = str(r.get("prop") or r.get("prop_type") or "")
        low = prop.lower()
        if any(
            x in low
            for x in (
                "double",
                "triple",
                "stolen",
                "home run",
                "homer",
                "rbi",
                "combo",
            )
        ):
            res = str(r.get("result") or "").strip().upper() or "?"
            mlb_gap[(prop, res)] += 1

print("MLB board-gap props in graded_props:")
for (prop, res), n in sorted(mlb_gap.items(), key=lambda x: (-x[1], x[0])):
    print(f"  {prop!r:40} {res:6} n={n}")

# catalog check
print("\nCatalog MLB props matching double/triple/sb/hr/rbi:")
for r in cat["rows"]:
    if r["sport"] != "MLB":
        continue
    blob = f"{r.get('prop_display','')} {r.get('prop','')}".lower()
    if any(x in blob for x in ("double", "triple", "stolen", "home", "rbi", "combo")):
        hr = float(r["hr"])
        if hr <= 1:
            hr *= 100
        print(
            f"  {r['book']:18} {r['prop_display']:30} n={r['n']:5} hr={hr:.1f}% prop={r['prop']}"
        )

t = CANVAS.read_text(encoding="utf-8")

# Update COVERAGE constant
old_cov = """const COVERAGE = [
  { sport: "WNBA", covered: "25/29", note: "strongest" },
  { sport: "Soccer", covered: "11/15", note: "best active book" },
  { sport: "Tennis", covered: "7/11", note: "incl. unsupported 1st-set" },
  { sport: "CFB", covered: "7/24", note: "post grader-fix caution" },
  { sport: "NFL", covered: "9/40", note: "Pass Yards gap" },
  { sport: "MLB", covered: "—", note: "no live board check yet" },
] as const;"""

new_cov = """const COVERAGE = [
  { sport: "WNBA", covered: "25/29", note: "strongest" },
  { sport: "Soccer", covered: "13/15", note: "Tackles/Crosses pulled in" },
  { sport: "Tennis", covered: "9/11", note: "Sets/Tie Breaks pulled in" },
  { sport: "MLB", covered: "19/22", note: "Sep 18 step1 board check" },
  { sport: "CFB", covered: "7/24", note: "post grader-fix caution" },
  { sport: "NFL", covered: "9/40", note: "Pass Yards gap" },
] as const;"""

if old_cov not in t:
    raise SystemExit("COVERAGE block not found")
t = t.replace(old_cov, new_cov, 1)

# Add Tackles/Crosses into SOCCER_MAIN before closing ]; of SOCCER_MAIN
# Insert after Offsides / before Shots UNDER — actually add after Shots UNDER as thin tracked
marker = """  {
    prop: "Shots UNDER",
    ungated: "61.9% (297/480)",
    gate: "L5≥4",
    gated: "66.8% (129/193); L5=5 74.3% (n=74)",
  },
];

const SOCCER_EXCL"""

insert_soccer = """  {
    prop: "Shots UNDER",
    ungated: "61.9% (297/480)",
    gate: "L5≥4",
    gated: "66.8% (129/193); L5=5 74.3% (n=74)",
  },
  {
    prop: "Tackles",
    ungated: "Goblin 23.1% (3/13) · Std OVER 37.5% (6/16) · Demon 20.6% (167/810)",
    gate: "none — thin live books",
    gated: "pulled in this pass; Demon volume only useful for base rate",
  },
  {
    prop: "Crosses",
    ungated: "Std OVER 16.7% (2/12)",
    gate: "none — thin",
    gated: "pulled in this pass; n=12 only",
  },
];

const SOCCER_EXCL"""

if marker not in t:
    raise SystemExit("SOCCER_MAIN marker not found")
t = t.replace(marker, insert_soccer, 1)

# Update SOCCER_COV — move Tackles/Crosses from write-up miss to tracked thin
old_scov = """const SOCCER_COV: CoverageBucket[] = [
  {
    label: "Real gate or fade status (9)",
    tone: "success",
    items:
      "Passes Attempted (excluded), Shots, SOT, Saves, Goals, Clearances (excluded), G+A, Assists, Fouls",
  },
  {
    label: "In catalog, thin live books — write-up miss (2)",
    tone: "info",
    items:
      "Tackles (Goblin n=13 / Std O n=16 / Demon n=810), Crosses (Std O n=12). Already in master catalog.",
  },
  {
    label: "Tracked but too thin (2)",
    tone: "warning",
    items: "Attempted Dribbles, Shots Assisted",
  },
  {
    label: "Fantasy — intentional drop + mostly VOID (2)",
    tone: "neutral",
    items:
      "Outfield / Goalie Fantasy Score: catalog drops fantasy; graded rows almost all VOID (858 / 195) — not config-only.",
  },
];"""

new_scov = """const SOCCER_COV: CoverageBucket[] = [
  {
    label: "Real gate or fade / thin status (13)",
    tone: "success",
    items:
      "Passes Attempted (excluded), Shots, SOT, Saves, Goals, Clearances (excluded), G+A, Assists, Fouls, Fouls Drawn, Offsides, Tackles, Crosses — Tackles/Crosses pulled into rates this pass",
  },
  {
    label: "Tracked but too thin (2)",
    tone: "warning",
    items: "Attempted Dribbles, Shots Assisted",
  },
  {
    label: "Still not graded usefully (2)",
    tone: "neutral",
    items:
      "Outfield / Goalie Fantasy Score: catalog drops fantasy; graded rows almost all VOID (858 / 195).",
  },
];"""

if old_scov not in t:
    raise SystemExit("SOCCER_COV not found")
t = t.replace(old_scov, new_scov, 1)

# Add tennis Demon / thin rows after TENNIS_STD_UNDER Total Games
marker_t = """  {
    prop: "Total Games",
    ungated: "52.3% (225/430)",
    gate: "none established",
    gated: "added this pass (was missed)",
  },
];

const TENNIS_COV"""

# Find actual TENNIS_COV start after STD_UNDER
idx = t.find('gated: "added this pass (was missed)"')
if idx < 0:
    raise SystemExit("tennis under marker missing")
# find the ]; after that which closes TENNIS_STD_UNDER
close = t.find("];\n\nconst TENNIS_COV", idx)
if close < 0:
    raise SystemExit("TENNIS_COV after under not found")
block_end = close + len("];")
insert = """];

const TENNIS_DEMON_THIN: RateRow[] = [
  {
    prop: "Total Sets",
    ungated: "Demon OVER 45.8% (38/83)",
    gate: "none — Demon-only volume",
    gated: "pulled in this pass; no Goblin/Std sample",
  },
  {
    prop: "Total Tie Breaks",
    ungated: "Demon OVER 33.3% (29/87) · Std UNDER 50% (1/2) · Std OVER 0% (0/1)",
    gate: "none — Demon-heavy",
    gated: "pulled in this pass; live books n≤2",
  },
];

const TENNIS_COV"""
# replace from the ]; before TENNIS_COV
t = t[:close] + insert + t[close + len("];\n\nconst TENNIS_COV") :]

# Update TENNIS_COV labels
old_tcov = """const TENNIS_COV: CoverageBucket[] = [
  {
    label: "Covered (7)",
    tone: "success",
    items:
      "Total Games, Total Games Won, Aces, Double Faults, 1st Set Aces / Total Games / Games Won (unsupported_prop=1, not real rates)",
  },
  {
    label: "In catalog, Demon-heavy — write-up miss (2)",
    tone: "info",
    items:
      "Total Sets (Demon n=83, 45.8%), Total Tie Breaks (Demon n=87; live Std n=1–2). Already graded — not missing from data.",
  },
  {
    label: "Truly absent from graded history (1)",
    tone: "danger",
    items: "Break Points Won — zero HIT/MISS/VOID rows across 207 graded dates.",
  },
  {
    label: "Fantasy — intentional drop + all VOID (1)",
    tone: "neutral",
    items: "Fantasy Score: catalog filter drops fantasy; 52 VOID, 0 HIT/MISS.",
  },
];"""

new_tcov = """const TENNIS_COV: CoverageBucket[] = [
  {
    label: "Covered with rates (9)",
    tone: "success",
    items:
      "Total Games, Total Games Won, Aces, Double Faults, Total Sets (Demon), Total Tie Breaks (Demon), 1st Set Aces / Total Games / Games Won (unsupported_prop=1)",
  },
  {
    label: "Still not graded / tracked (2)",
    tone: "danger",
    items:
      "Break Points Won — zero graded rows in 207 dates. Fantasy Score — intentional drop + 52 VOID only.",
  },
];"""

if old_tcov not in t:
    raise SystemExit("TENNIS_COV not found")
t = t.replace(old_tcov, new_tcov, 1)

# MLB board coverage — replace neutral callout
old_mlb_cov = """          <Callout tone="neutral" title="Board coverage">
            Not yet checked against a live MLB Popular-tab screenshot.
          </Callout>"""

new_mlb_cov = """          <H2>Board coverage (Sep 18 step1 — 22 unique props)</H2>
          <Callout tone="success" title="19 of 22 matched to catalog rates">
            Live board fetched_at 2026-09-18T13:15 ET. Matched: Hits, Hits Allowed,
            H+R+RBI, Hitter/Pitcher Ks, ERA, Walks Allowed, Pitching Outs, Pitches
            Thrown, Total Bases, Singles, Runs, Walks, PA, Home Runs, RBIs,
            1st-Inning Walks/Runs Allowed. Pitcher Strikeouts (Combo) fuzzy-matches
            onto Pitcher Ks — no separate combo cell.
          </Callout>
          <Callout tone="warning" title="Board-only — not in catalog (3)">
            Doubles, Triples, Stolen Bases — on today&apos;s board, no master-catalog
            cells (keep list already FADE/off for 2B/3B/SB). Graded history for these
            labels is empty or unmapped — not a write-up miss.
          </Callout>
          <Callout tone="info" title="Precision gap still open">
            Four of five pitcher L10-primary gates still show L10-alone (~X%+) not the
            wired gate with pitch-tier qualifier. Separate from board coverage.
          </Callout>"""

if old_mlb_cov not in t:
    raise SystemExit("MLB board callout not found")
t = t.replace(old_mlb_cov, new_mlb_cov, 1)

# Overview stats + tally text
old_stats = """          <Row gap={12} wrap>
            <Stat value="25/29" label="WNBA board" tone="success" />
            <Stat value="11/15" label="Soccer board" tone="success" />
            <Stat value="7/11" label="Tennis board" tone="info" />
            <Stat value="7/24" label="CFB board" tone="warning" />
            <Stat value="9/40" label="NFL board" tone="warning" />
            <Stat value="—" label="MLB board" tone="info" />
          </Row>
          <Callout tone="info" title="Board coverage tally">
            WNBA 25/29 · Soccer 11/15 · Tennis 7/11 · CFB 7/24 · NFL 9/40. MLB is
            the only active sport not yet checked against a live Popular-tab
            screenshot.
          </Callout>"""

new_stats = """          <Row gap={12} wrap>
            <Stat value="25/29" label="WNBA board" tone="success" />
            <Stat value="19/22" label="MLB board" tone="success" />
            <Stat value="13/15" label="Soccer board" tone="success" />
            <Stat value="9/11" label="Tennis board" tone="success" />
            <Stat value="7/24" label="CFB board" tone="warning" />
            <Stat value="9/40" label="NFL board" tone="warning" />
          </Row>
          <Callout tone="info" title="Board coverage tally (updated)">
            WNBA 25/29 · MLB 19/22 · Soccer 13/15 · Tennis 9/11 · CFB 7/24 · NFL 9/40.
            Cheap Soccer Tackles/Crosses + Tennis Sets/Tie Breaks pulled into rates.
            MLB board check done from Sep 18 step1 (not a screenshot — live CSV).
          </Callout>"""

if old_stats not in t:
    raise SystemExit("overview stats not found")
t = t.replace(old_stats, new_stats, 1)

# Wire TENNIS_DEMON_THIN into Tennis sport view
old_tennis_view = """          <H2>Standard UNDER</H2>
          {rateTable(TENNIS_STD_UNDER)}
          <H3>1st-Set props</H3>"""

new_tennis_view = """          <H2>Standard UNDER</H2>
          {rateTable(TENNIS_STD_UNDER)}
          <H2>Demon / thin — pulled in this pass</H2>
          {rateTable(TENNIS_DEMON_THIN)}
          <H3>1st-Set props</H3>"""

if old_tennis_view not in t:
    raise SystemExit("tennis view block not found")
t = t.replace(old_tennis_view, new_tennis_view, 1)

# Update callout #3 text slightly
old3 = """          <Callout tone="info" title="3. Checked — not a fixed allowlist">
            Catalog aggregates every HIT/MISS except fantasy (intentional) and
            grading_suspect. Soccer Tackles/Crosses + Tennis Total Sets/Tie Breaks
            already in master — write-up misses. WNBA Fantasy has 1,942 graded
            HIT/MISS filtered out on purpose. Break Points Won: zero graded rows
            in 207 dates. Soccer/Tennis Fantasy: mostly VOID, not a config flip.
          </Callout>"""

new3 = """          <Callout tone="success" title="3. Cheap gaps closed this pass">
            Soccer Tackles/Crosses + Tennis Total Sets/Tie Breaks now in the rate
            tables. MLB board check: 19/22 matched (Doubles/Triples/SB board-only,
            not in catalog). Still open: WNBA Quarters audit, Fantasy VOID check,
            NFL/CFB infrastructure, pitcher L10+tier re-measure.
          </Callout>"""

if old3 not in t:
    raise SystemExit("callout 3 not found")
t = t.replace(old3, new3, 1)

CANVAS.write_text(t, encoding="utf-8")
print("canvas patched ok")
