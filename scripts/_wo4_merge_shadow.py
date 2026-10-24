import json
from pathlib import Path

root = Path("data/reports")
shadow = json.loads((root / "shadow_slips_2026-10-08.json").read_text(encoding="utf-8"))
cap = json.loads((root / "shadow_slips_2026-10-08_capture.json").read_text(encoding="utf-8"))
by_id = {}
rows = cap.get("slips") or cap.get("captured") or cap.get("results") or []
if isinstance(cap, dict) and not rows:
    for key in cap:
        if isinstance(cap[key], list) and cap[key] and isinstance(cap[key][0], dict):
            rows = cap[key]
            break
for rec in rows:
    if isinstance(rec, dict) and rec.get("ticket_id"):
        by_id[rec["ticket_id"]] = rec
live = []
for slip in shadow["tickets"]:
    rec = by_id.get(slip["ticket_id"]) or {}
    x = rec.get("power_min_x")
    if rec.get("status") == "ok" and x:
        slip["live_n_correct_x"] = x
        slip["quote_source"] = "live_cdp"
        live.append(float(x))
    else:
        slip["scrape_status"] = rec.get("status") or rec.get("error") or "missing"
shadow["live_scraped"] = len(live)
shadow["live_n_correct_x"] = sorted(set(live))
shadow["submit"] = False
(root / "shadow_slips_2026-10-08.json").write_text(json.dumps(shadow, indent=2), encoding="utf-8")
print("captured_rows", len(rows), "keys", list(cap)[:12] if isinstance(cap, dict) else type(cap))
print("live", len(live), sorted(set(live)))
