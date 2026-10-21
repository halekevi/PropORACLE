"""Finalize Lead2 hit stats + leg structure peek."""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(r"H:/PropORACLE")
TEMPLATES = ROOT / "ui_runner" / "templates"
sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def main() -> None:
    from utils.proporacle_data_root import load_best_grade_history_runs

    gh: dict[str, dict] = {}
    for r in load_best_grade_history_runs(ROOT, templates_dir=TEMPLATES) or []:
        d = str(r.get("date") or "")[:10]
        if not (d.startswith("2026-05") or d.startswith("2026-06")):
            continue
        track = str(r.get("track") or "main").lower()
        if track in ("main", "", "default", "combined", "graded_main", "graded-main", "mixer") or d not in gh:
            gh[d] = r

    diffs_n: list[int] = []
    diffs_w: list[int] = []
    exact = 0
    within5 = 0
    within10pct = 0

    for f in sorted(TEMPLATES.glob("ticket_eval_*.html")):
        if not re.match(r"ticket_eval_2026-0[56]-\d{2}\.html$", f.name):
            continue
        d = f.name[len("ticket_eval_") : -5]
        g = gh.get(d)
        if not g:
            continue
        text = f.read_text(encoding="utf-8", errors="replace")
        arts = re.findall(r'<article class="ticket-card[^"]*">.*?</article>', text, flags=re.S)
        win = sum(1 for a in arts if "all-hit" in a[:120])
        loss = sum(1 for a in arts if "card-missed" in a[:120])
        dec = win + loss
        ghn = int(g["n_tickets"])
        ghw = int(g.get("wins") or 0)
        dn = abs(dec - ghn)
        dw = abs(win - ghw)
        diffs_n.append(dn)
        diffs_w.append(dw)
        if dn == 0 and dw == 0:
            exact += 1
        if dn <= 5:
            within5 += 1
        if dn / max(ghn, 1) <= 0.10:
            within10pct += 1

    diffs_n.sort()
    diffs_w.sort()
    print(
        f"Lead2 HTML articles vs grade_history: n={len(diffs_n)} "
        f"mean_|n|={sum(diffs_n)/len(diffs_n):.2f} median_|n|={diffs_n[len(diffs_n)//2]} "
        f"mean_|w|={sum(diffs_w)/len(diffs_w):.2f} median_|w|={diffs_w[len(diffs_w)//2]}"
    )
    print(f"exact n&w={exact}/{len(diffs_n)}  within5_n={within5}/{len(diffs_n)}  within10%_n={within10pct}/{len(diffs_n)}")
    print("HIT" if (sum(diffs_n) / len(diffs_n) < 25 and within10pct >= len(diffs_n) // 2) else "SOFT")

    # leg structure
    t = (TEMPLATES / "ticket_eval_2026-05-15.html").read_text(encoding="utf-8", errors="replace")
    arts = re.findall(r'<article class="ticket-card[^"]*">.*?</article>', t, flags=re.S)
    a = next(x for x in arts if "all-hit" in x[:120])
    print("\nclasses:", re.findall(r'class="([^"]+)"', a)[:35])
    plain = re.sub(r"<[^>]+>", " | ", a)
    plain = re.sub(r"\s+", " ", plain)
    print("PLAIN:", plain[:900])
    # count grade badges
    print("HIT span", len(re.findall(r">HIT<", a)))
    print("MISS span", len(re.findall(r">MISS<", a)))
    # player lines often in <div class="player"> or similar
    for pat in ["leg-player", "leg-meta", "leg-stat", "leg-result", "prop-type", "direction", "actual"]:
        print(pat, a.lower().count(pat))


if __name__ == "__main__":
    main()
