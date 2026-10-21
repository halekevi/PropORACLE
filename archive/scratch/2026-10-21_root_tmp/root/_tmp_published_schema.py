"""Inspect published-stream sources for leg-level fields."""
from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path

ROOT = Path(r"H:\PropORACLE")

# --- grade_history ---
gh = json.loads((ROOT / "ui_runner/templates/grade_history.json").read_text(encoding="utf-8"))
print("=== grade_history ===")
print("rows", len(gh), "keys", sorted(gh[0].keys()))
print("has legs/tickets?", any("leg" in k.lower() or k == "tickets" for k in gh[0]))

# --- ticket_eval HTML ---
html_path = ROOT / "ui_runner/templates/ticket_eval_2026-05-15.html"
text = html_path.read_text(encoding="utf-8", errors="replace")
assigns = Counter(re.findall(r"window\.([A-Z_][A-Z0-9_]*)\s*=", text))
print("\n=== ticket_eval_2026-05-15.html ===")
print("window assigns", dict(assigns))
print("size MB", round(html_path.stat().st_size / 1e6, 2))

for name in assigns:
    m = re.search(rf"window\.{name}\s*=\s*", text)
    if not m:
        continue
    start = m.end()
    while start < len(text) and text[start].isspace():
        start += 1
    try:
        obj, _ = json.JSONDecoder().raw_decode(text, start)
    except json.JSONDecodeError as exc:
        print(f"  {name}: parse fail {exc}")
        continue
    print(f"  {name}: type={type(obj).__name__}", end="")
    if isinstance(obj, list):
        print(f" len={len(obj)}", end="")
        if obj and isinstance(obj[0], dict):
            print(f" keys0={sorted(obj[0].keys())[:12]}", end="")
            has_legs = sum(1 for x in obj if isinstance(x, dict) and x.get("legs"))
            print(f" has_legs={has_legs}", end="")
    elif isinstance(obj, dict):
        print(f" keys={list(obj.keys())[:15]}", end="")
    print()

# Search for embedded ticket objects with legs arrays
leg_hits = 0
for m in re.finditer(r'"legs"\s*:\s*\[', text):
    leg_hits += 1
print("raw \"legs\":[ occurrences", leg_hits)

# Try to find a ticket-like object near first legs
idx = text.find('"legs":[')
if idx < 0:
    idx = text.find('"legs": [')
if idx >= 0:
    # walk back to nearest {
    brace = text.rfind("{", 0, idx)
    chunk = text[brace : brace + 2500]
    # crude: show field names around
    fields = sorted(set(re.findall(r'"([a-zA-Z0-9_]+)"\s*:', chunk[:1200])))
    print("near first legs, fields", fields[:40])

# --- ticket_runs coverage ---
runs = ROOT / "ui_runner/data/ticket_runs"
dates = sorted(d.name for d in runs.iterdir() if d.is_dir()) if runs.exists() else []
print("\n=== ticket_runs ===")
print("dates", dates)
print("covers May/June 2026?", any(d.startswith("2026-05") or d.startswith("2026-06") for d in dates))

# --- June graded on_ticket ---
gp = json.loads((ROOT / "ui_runner/templates/graded_props_2026-06-01.json").read_text(encoding="utf-8"))
props = gp.get("props") or []
on = [r for r in props if r.get("on_ticket") is True]
print("\n=== graded_props on_ticket (2026-06-01) ===")
print("on_ticket True", len(on), "of", len(props))
if on:
    print("sample keys", sorted(on[0].keys())[:25])
    print(
        "sample",
        {
            k: on[0].get(k)
            for k in [
                "player",
                "prop",
                "direction",
                "line",
                "sport",
                "team",
                "opp_team",
                "ml_prob",
                "hit",
                "result",
                "ticket_id",
                "on_ticket",
            ]
        },
    )
    tids = {r.get("ticket_id") for r in on if r.get("ticket_id")}
    print("unique ticket_ids among on_ticket", len(tids))
