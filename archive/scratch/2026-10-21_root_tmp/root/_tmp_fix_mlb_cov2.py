from pathlib import Path

p = Path(r"C:/Users/halek/.cursor/projects/h-PropORACLE/canvases/active-sport-prop-hit-rates.canvas.tsx")
t = p.read_text(encoding="utf-8")

old = """          <H2>Board coverage (Sep 18 step1 — 22 unique props)</H2>
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

new = """          <H2>Board coverage (Sep 18 step1 — 22 unique props)</H2>
          <Callout tone="success" title="19/22 have live-book (Goblin/Std) catalog rates">
            Live board fetched_at 2026-09-18T13:15 ET. Keep/fade props matched:
            Hits, Hits Allowed, H+R+RBI, Hitter/Pitcher Ks, ERA, Walks Allowed,
            Pitching Outs, Pitches Thrown, Total Bases, Singles, Runs, Walks, PA,
            RBIs, 1st-Inning Walks/Runs Allowed. Pitcher Strikeouts (Combo) has no
            separate cell — fuzzy onto Pitcher Ks; combo rows are mostly VOID.
          </Callout>
          <Callout tone="info" title="Demon-only catalog — not a data gap (3+)">
            Doubles Demon 14.5% (n=26,785), Triples Demon 3.2% (n=2,622), Stolen
            Bases Demon 8.1% (n=25,663), Home Runs Demon 11.6% (n=34,666). Huge
            graded history — but almost no Goblin/Std cells. Keep list already FADE
            for these. Board shows them; live-book rates do not exist to pull.
          </Callout>
          <Callout tone="warning" title="Precision gap still open">
            Four of five pitcher L10-primary gates still show L10-alone (~X%+) not
            the wired gate with pitch-tier qualifier. Separate from board coverage.
          </Callout>"""

if old not in t:
    # show nearby
    i = t.find("Board coverage (Sep 18")
    print("FOUND" if i >= 0 else "MISSING", repr(t[i : i + 200] if i >= 0 else ""))
    raise SystemExit("mlb block not found")
t = t.replace(old, new, 1)

# Fix coverage note for MLB
t = t.replace(
    '{ sport: "MLB", covered: "19/22", note: "Sep 18 step1 board check" }',
    '{ sport: "MLB", covered: "19/22", note: "live books; 2B/3B/SB Demon-only" }',
    1,
)

# Add Demon fade rates under MLB goblin fade section if present
fade_marker = """  {
    prop: "1B/2B/3B, HR, SB, 1st-inning",
    ungated: "—",
    gate: "—",
    gated: "FADE (off)",
  },
];"""

fade_new = """  {
    prop: "Doubles (Demon)",
    ungated: "14.5% (3,905/26,785)",
    gate: "—",
    gated: "FADE — Demon-only; no Goblin/Std cell",
  },
  {
    prop: "Triples (Demon)",
    ungated: "3.2% (87/2,622)",
    gate: "—",
    gated: "FADE — Demon-only",
  },
  {
    prop: "Stolen Bases (Demon)",
    ungated: "8.1% (2,078/25,663)",
    gate: "—",
    gated: "FADE — Demon-only",
  },
  {
    prop: "Home Runs (Demon)",
    ungated: "11.6% (4,025/34,666)",
    gate: "—",
    gated: "FADE — Demon-only",
  },
  {
    prop: "1st-inning extras",
    ungated: "thin / mixed books",
    gate: "—",
    gated: "FADE (off keep list)",
  },
];"""

if fade_marker not in t:
    raise SystemExit("fade marker missing")
t = t.replace(fade_marker, fade_new, 1)

p.write_text(t, encoding="utf-8")
print("mlb coverage fixed")
