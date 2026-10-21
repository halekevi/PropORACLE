from pathlib import Path

p = Path(r"C:/Users/halek/.cursor/projects/h-PropORACLE/canvases/active-sport-prop-hit-rates.canvas.tsx")
t = p.read_text(encoding="utf-8")
a = t.find("const WNBA_COV")
b = t.find("function rateTable")
assert a > 0 and b > a
new = """const WNBA_COV: CoverageBucket[] = [
  {
    label: "Fantasy intentional catalog drop, data exists (1)",
    tone: "info",
    items:
      "Fantasy Score: 1,942 HIT/MISS in graded_props (52.3%) — filtered by is_fantasy_prop_label. Only cheap config flip among fantasy gaps (product decision).",
  },
  {
    label: "Unconfirmed risk (3)",
    tone: "danger",
    items:
      "Quarters with 3+/4+/5+ Points — same period-split family as NFL props that fuzzy-matched onto full-game columns. Audit step2/step5 before trusting any rate.",
  },
];

"""
p.write_text(t[:a] + new + t[b:], encoding="utf-8")
print("replaced WNBA_COV ok")
