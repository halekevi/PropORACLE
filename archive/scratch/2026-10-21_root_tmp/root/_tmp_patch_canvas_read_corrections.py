#!/usr/bin/env python3
"""Patch canvas: covered≠playable, Ks combo audit, canvas-as-record."""
from pathlib import Path

p = Path(
    r"C:/Users/halek/.cursor/projects/h-PropORACLE/canvases/active-sport-prop-hit-rates.canvas.tsx"
)
t = p.read_text(encoding="utf-8")

# --- 1. Soften Tackles/Crosses/Sets/TieBreaks gated column to FADE ---
repls = [
    (
        """  {
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
  },""",
        """  {
    prop: "Tackles",
    ungated: "Goblin 23.1% (3/13) · Std OVER 37.5% (6/16) · Demon 20.6% (167/810)",
    gate: "none",
    gated: "FADE — covered≠playable; weak base (Demon 20.6% is the honest n)",
  },
  {
    prop: "Crosses",
    ungated: "Std OVER 16.7% (2/12)",
    gate: "none",
    gated: "FADE — covered≠playable; n=12 only",
  },""",
    ),
    (
        """const TENNIS_DEMON_THIN: RateRow[] = [
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
];""",
        """const TENNIS_DEMON_THIN: RateRow[] = [
  {
    prop: "Total Sets",
    ungated: "Demon OVER 45.8% (38/83)",
    gate: "none — Demon-only",
    gated: "FADE — covered≠playable; 45.8% is a number not a play",
  },
  {
    prop: "Total Tie Breaks",
    ungated: "Demon OVER 33.3% (29/87) · Std UNDER 50% (1/2) · Std OVER 0% (0/1)",
    gate: "none — Demon-heavy",
    gated: "FADE — covered≠playable; Demon 33.3%",
  },
];""",
    ),
]

for old, new in repls:
    if old not in t:
        raise SystemExit(f"block missing:\n{old[:80]}")
    t = t.replace(old, new, 1)

# --- 2. Overview: record decision + covered≠playable + Ks combo ---
old_success = """          <Callout tone="success" title="3. Cheap gaps closed this pass">
            Soccer Tackles/Crosses + Tennis Total Sets/Tie Breaks now in the rate
            tables. MLB board check: 19/22 live-book matched; Doubles/Triples/SB/HR
            are Demon-only catalog (huge n, keep FADE). Still open: WNBA Quarters
            audit, Fantasy VOID check, NFL/CFB infrastructure, pitcher L10+tier
            re-measure.
          </Callout>"""

# may have been edited - try flexible
if old_success not in t:
    # find by title
    i = t.find('title="3. Cheap gaps closed this pass"')
    if i < 0:
        raise SystemExit("callout 3 title missing")
    # find Callout start before title
    start = t.rfind("<Callout", 0, i)
    end = t.find("</Callout>", i) + len("</Callout>")
    old_success = t[start:end]

new_success = """          <Callout tone="warning" title="Covered ≠ playable">
            The four newly pulled rates (Soccer Tackles/Crosses, Tennis Total Sets /
            Tie Breaks) are visible FADE candidates — weak bases, not hidden lift.
            Closing a coverage gap does not mean finding a new play.
          </Callout>
          <Callout tone="danger" title="Pitcher Ks (Combo) = computation failure (audited)">
            Not the same market as plain Pitcher Strikeouts. Raw labels stay
            separate: canon pitcher_ks vs strikeouts_combo. Combo is 100% VOID
            (1,011/1,011), void_reason=NO_ACTUAL, actual always null — players are
            two-name strings (A + B). Same shape as Soccer Passes/Clearances null
            cascade, not WNBA Fantasy intentional drop. Site may show Ks vs Pitcher
            Ks for the plain market; Combo is a different prop.
          </Callout>
          <Callout tone="info" title="Record of truth: this canvas">
            All Sep 18 revisions (cheap pulls, MLB board check, gap taxonomy, Ks
            combo audit) live here. The earlier Doc/Notion draft ends at pre-pull
            WNBA and is intentionally stale unless you sync it. Do not treat the
            Doc as current.
          </Callout>"""

t = t.replace(old_success, new_success, 1)

# --- 3. MLB board combo callout fix ---
old_combo = """Pitcher Strikeouts (Combo) has no
            separate cell — fuzzy onto Pitcher Ks; combo rows are mostly VOID."""
new_combo = """Pitcher Strikeouts (Combo) is a SEPARATE canon
            (strikeouts_combo) — 100% VOID / NO_ACTUAL on two-pitcher legs; do not
            fuzzy onto plain Pitcher Ks."""
if old_combo in t:
    t = t.replace(old_combo, new_combo, 1)
else:
    print("WARN: mlb combo sentence not found")

# --- 4. Add MLB row for combo under fade ---
fade_insert_after = """  {
    prop: "Home Runs (Demon)",
    ungated: "11.6% (4,025/34,666)",
    gate: "—",
    gated: "FADE — Demon-only",
  },"""
if fade_insert_after not in t:
    raise SystemExit("HR demon row missing")
t = t.replace(
    fade_insert_after,
    fade_insert_after
    + """
  {
    prop: "Pitcher Strikeouts (Combo)",
    ungated: "0 decided — 100% VOID (1,011/1,011)",
    gate: "—",
    gated: "BROKEN — NO_ACTUAL cascade on A+B legs; not plain pitcher_ks",
  },""",
    1,
)

# --- 5. Tally note ---
old_tally = """            Cheap Soccer Tackles/Crosses + Tennis Sets/Tie Breaks pulled into rates.
            MLB board check done from Sep 18 step1 (not a screenshot — live CSV)."""
new_tally = """            Cheap pulls are FADE visibility only. MLB board from Sep 18 step1 CSV.
            Canvas is the record; Doc draft is stale on purpose until you sync."""
if old_tally in t:
    t = t.replace(old_tally, new_tally, 1)

p.write_text(t, encoding="utf-8")
print("canvas patched")

# Persist audit
out = Path(r"H:/PropORACLE/data/reports/audit_mlb_pitcher_ks_combo_void.json")
out.write_text(
    __import__("json").dumps(
        {
            "audited": "2026-09-18",
            "question": "Are Pitcher Ks and Pitcher Ks (Combo) the same market? Is VOID real?",
            "verdict": {
                "same_market": False,
                "plain_canon": "pitcher_ks",
                "combo_canon": "strikeouts_combo",
                "plain_raw": "Pitcher Strikeouts",
                "combo_raw": "Pitcher Strikeouts (Combo)",
                "combo_void_rate": "100% (1011/1011)",
                "combo_void_reason": "NO_ACTUAL",
                "combo_actuals": "all null/blank",
                "combo_player_shape": "Pitcher A + Pitcher B (two-leg sum never filled)",
                "classification": "computation_cascade_failure",
                "not": "intentional_fantasy_drop_or_display_alias",
                "plain_void_pct": "19.4% (5000/25746) with mostly nonzero actuals — different phenomenon",
                "site_display_note": "Plain market may show as Ks or Pitcher Strikeouts; both map to pitcher_ks. Combo stays strikeouts_combo.",
            },
        },
        indent=2,
    )
    + "\n",
    encoding="utf-8",
)
print("wrote", out)
