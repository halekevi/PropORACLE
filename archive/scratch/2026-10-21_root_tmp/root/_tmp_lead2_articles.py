"""Count <article class=\"ticket-card...\"> decided outcomes vs grade_history."""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(r"H:/PropORACLE")
TEMPLATES = ROOT / "ui_runner" / "templates"
sys.stdout.reconfigure(encoding="utf-8", errors="replace")


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


def articles(html: str) -> list[str]:
    return re.findall(
        r'<article class="ticket-card[^"]*">.*?</article>',
        html,
        flags=re.S,
    )


def classify(article: str) -> str:
    head = article[:200].lower()
    if "all-hit" in head:
        return "win"
    if "card-missed" in head:
        return "loss"
    if "card-warning" in head:
        return "warn"
    if "pending" in article.lower() or "ungraded" in article.lower():
        return "pending"
    return "other"


def extract_legs(article: str) -> list[dict]:
    """Best-effort leg scrape from ticket-eval article HTML."""
    legs = []
    # typical row: player + prop + line + direction + grade badge
    # look for repeated blocks inside article
    for m in re.finditer(
        r'<div class="[^"]*leg[^"]*"[^>]*>(.*?)</div>',
        article,
        flags=re.S | re.I,
    ):
        chunk = re.sub(r"<[^>]+>", " ", m.group(1))
        chunk = re.sub(r"\s+", " ", chunk).strip()
        if chunk:
            legs.append({"raw": chunk[:120]})
    if legs:
        return legs
    # fallback: table rows
    for m in re.finditer(r"<tr[^>]*>(.*?)</tr>", article, flags=re.S | re.I):
        cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", m.group(1), flags=re.S | re.I)
        if len(cells) >= 3:
            plain = [re.sub(r"<[^>]+>", " ", c) for c in cells]
            plain = [re.sub(r"\s+", " ", c).strip() for c in plain]
            legs.append({"cells": plain[:6]})
    return legs


def main() -> None:
    gh = load_gh()
    files = [
        f
        for f in sorted(TEMPLATES.glob("ticket_eval_*.html"))
        if re.match(r"ticket_eval_2026-0[56]-\d{2}\.html$", f.name)
    ]
    print(f"files={len(files)}")
    print(
        f"{'date':10} {'art':>5} {'win':>4} {'loss':>5} {'dec':>5} {'gh_n':>5} {'gh_w':>4} {'dn':>5} {'dw':>4}"
    )
    diffs_n = []
    diffs_w = []
    close = 0
    for f in files:
        d = f.name[len("ticket_eval_") : -5]
        text = f.read_text(encoding="utf-8", errors="replace")
        arts = articles(text)
        counts = {"win": 0, "loss": 0, "warn": 0, "pending": 0, "other": 0}
        leg_ns = []
        for a in arts:
            counts[classify(a)] += 1
        dec = counts["win"] + counts["loss"]
        # sample legs from first win
        if not leg_ns:
            for a in arts:
                if classify(a) == "win":
                    leg_ns.append(len(extract_legs(a)))
                    break
        g = gh.get(d) or {}
        ghn = g.get("n_tickets")
        ghw = g.get("wins")
        dn = (dec - int(ghn)) if ghn is not None else None
        dw = (counts["win"] - int(ghw)) if ghw is not None else None
        print(
            f"{d:10} {len(arts):5d} {counts['win']:4d} {counts['loss']:5d} {dec:5d} "
            f"{str(ghn):>5} {str(ghw):>4} {str(dn):>5} {str(dw):>4}  other={counts['other']} pend={counts['pending']} warn={counts['warn']}"
        )
        if ghn is not None:
            diffs_n.append(abs(dec - int(ghn)))
            if abs(dec - int(ghn)) / max(int(ghn), 1) <= 0.10:
                close += 1
        if ghw is not None:
            diffs_w.append(abs(counts["win"] - int(ghw)))

    if diffs_n:
        mean_n = sum(diffs_n) / len(diffs_n)
        mean_w = sum(diffs_w) / len(diffs_w) if diffs_w else None
        print(
            f"\noverlap={len(diffs_n)} mean_|dec-gh_n|={mean_n:.1f} "
            f"mean_|win-gh_w|={mean_w:.1f if mean_w is not None else 'n/a'} within10%={close}/{len(diffs_n)}"
        )
        if mean_n < 25 or close >= len(diffs_n) // 2:
            print("HIT: article decided counts ≈ grade_history.")
        else:
            print("MISS vs strict hit bar — but check if closer than Lead1.")

    # structure peek
    p = TEMPLATES / "ticket_eval_2026-05-15.html"
    text = p.read_text(encoding="utf-8", errors="replace")
    for a in articles(text):
        if classify(a) == "win":
            plain = re.sub(r"<[^>]+>", " | ", a)
            plain = re.sub(r"\s+", " ", plain)
            print("\nWIN ARTICLE PLAIN:", plain[:700])
            legs = extract_legs(a)
            print("extract_legs n=", len(legs), "sample=", legs[:4])
            # show inner class names
            print("inner classes:", re.findall(r'class="([^"]+)"', a)[:30])
            break


if __name__ == "__main__":
    main()
