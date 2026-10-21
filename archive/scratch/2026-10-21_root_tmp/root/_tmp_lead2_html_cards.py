"""Count ticket-cards in ticket_eval HTML vs grade_history; peek leg structure."""
from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(r"H:/PropORACLE")
TEMPLATES = ROOT / "ui_runner" / "templates"


def load_gh() -> dict[str, dict]:
    from utils.proporacle_data_root import load_best_grade_history_runs

    runs = load_best_grade_history_runs(ROOT, templates_dir=TEMPLATES)
    by: dict[str, dict] = {}
    for r in runs or []:
        if not isinstance(r, dict):
            continue
        d = str(r.get("date") or "")[:10]
        if not (d.startswith("2026-05") or d.startswith("2026-06")):
            continue
        track = str(r.get("track") or "main").lower()
        if track in ("main", "", "default", "combined", "graded_main", "graded-main", "mixer"):
            by[d] = r
        elif d not in by:
            by[d] = r
    return by


def main() -> None:
    gh = load_gh()
    files = sorted(TEMPLATES.glob("ticket_eval_2026-05*.html")) + sorted(
        TEMPLATES.glob("ticket_eval_2026-06*.html")
    )
    # exclude variants
    files = [f for f in files if re.match(r"ticket_eval_2026-0[56]-\d{2}\.html$", f.name)]
    print(f"ticket_eval main HTML files: {len(files)}")
    print(f"{'date':10} {'cards':>6} {'gh_n':>5} {'gh_w':>4} {'dn':>6}  MB")
    diffs = []
    close = 0
    for f in files:
        d = f.name.replace("ticket_eval_", "").replace(".html", "")
        text = f.read_text(encoding="utf-8", errors="replace")
        cards = len(re.findall(r'class="ticket-card', text))
        # also try summary line
        m = re.search(r"(\d+)\s+tickets</div>", text)
        summary_n = int(m.group(1)) if m else None
        g = gh.get(d) or {}
        ghn = g.get("n_tickets")
        ghw = g.get("wins")
        dn = (cards - int(ghn)) if ghn is not None else None
        print(
            f"{d:10} {cards:6d} {str(ghn):>5} {str(ghw):>4} {str(dn):>6}  {f.stat().st_size/1e6:.2f}  sum={summary_n}"
        )
        if ghn is not None:
            diffs.append(abs(cards - int(ghn)))
            if abs(cards - int(ghn)) / max(int(ghn), 1) <= 0.15:
                close += 1
            if summary_n is not None and abs(summary_n - int(ghn)) <= 2:
                pass  # summary matches history (expected — same writer)

    if diffs:
        print(f"overlap={len(diffs)} mean_|cards-gh_n|={sum(diffs)/len(diffs):.1f} within15%={close}/{len(diffs)}")
        if sum(diffs) / len(diffs) < 40 or close >= len(diffs) // 2:
            print("HIT: ticket_eval HTML card count ≈ grade_history — parse cards for Lever B legs.")
        else:
            print("MISS on card count vs gh (cards may include pending/ungraded).")

    # Structure peek May 15
    p = TEMPLATES / "ticket_eval_2026-05-15.html"
    t = p.read_text(encoding="utf-8", errors="replace")
    # first ticket card snippet
    i = t.find('class="ticket-card')
    print("\n=== first ticket-card snippet (800 chars) ===")
    print(t[i : i + 800].replace("\n", " ")[:800])
    # leg rows
    print("leg-row count", len(re.findall(r"leg-row|ticket-leg|class=\"leg", t)))
    # SLATE_DATA
    m = re.search(r"window\.SLATE_DATA\s*=\s*(\[)", t)
    if m:
        # parse carefully with raw_decode
        start = m.start(1)
        decoder = json.JSONDecoder()
        arr, _ = decoder.raw_decode(t[start:])
        print(f"SLATE_DATA n={len(arr)} keys={sorted(arr[0].keys())[:25] if arr else None}")
        print("row0", {k: arr[0].get(k) for k in list(arr[0])[:18]})


if __name__ == "__main__":
    main()
