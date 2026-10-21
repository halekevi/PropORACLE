"""Filter ticket_eval HTML to decided cards; compare to grade_history; check legs."""
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


def split_cards(html: str) -> list[str]:
    parts = re.split(r'(?=<div class="ticket-card)', html)
    return [p for p in parts if p.startswith('<div class="ticket-card')]


def card_is_decided(card: str) -> bool | None:
    """True=decided money, False=pending/ungraded, None=unknown."""
    low = card.lower()
    # pending markers
    if "pending" in low or "ungraded" in low or "no actual" in low:
        # could still have a money result elsewhere — check payout result first
        pass
    # money outcome badges from build_ticket_eval
    if re.search(r"\b(WIN|SWEEP|LOSS|VOID_LOSS|MIN GUARANTEE|MIN.?GUARANTEE)\b", card, re.I):
        if "pending" in low and not re.search(r"result[^<]*(WIN|LOSS|SWEEP)", card, re.I):
            return False
        return True
    if "card-missed" in card or "all-hit" in card:
        return True
    if "pending" in low or "ungraded" in low:
        return False
    return None


def count_legs(card: str) -> int:
    # common patterns in ticket_eval HTML
    n = len(re.findall(r'class="[^"]*leg[^"]*"', card))
    if n:
        return n
    return len(re.findall(r"<tr[^>]*leg|leg-player|prop-line", card, re.I))


def main() -> None:
    gh = load_gh()
    files = [
        f
        for f in sorted(TEMPLATES.glob("ticket_eval_2026-0[56]-*.html"))
        if re.match(r"ticket_eval_2026-0[56]-\d{2}\.html$", f.name)
    ]
    print(f"{'date':10} {'all':>5} {'dec':>5} {'pend':>5} {'unk':>5} {'gh_n':>5} {'dn_dec':>6} legs/card")
    diffs = []
    close = 0
    for f in files:
        d = f.name[len("ticket_eval_") : -len(".html")]
        text = f.read_text(encoding="utf-8", errors="replace")
        cards = split_cards(text)
        dec = pend = unk = 0
        leg_counts = []
        for c in cards:
            flag = card_is_decided(c)
            if flag is True:
                dec += 1
                leg_counts.append(count_legs(c))
            elif flag is False:
                pend += 1
            else:
                unk += 1
        ghn = (gh.get(d) or {}).get("n_tickets")
        ghw = (gh.get(d) or {}).get("wins")
        dn = (dec - int(ghn)) if ghn is not None else None
        avg_legs = (sum(leg_counts) / len(leg_counts)) if leg_counts else 0
        print(
            f"{d:10} {len(cards):5d} {dec:5d} {pend:5d} {unk:5d} {str(ghn):>5} {str(dn):>6} avg_legs={avg_legs:.1f}"
        )
        if ghn is not None:
            diffs.append(abs(dec - int(ghn)))
            if abs(dec - int(ghn)) / max(int(ghn), 1) <= 0.10:
                close += 1

    if diffs:
        mean = sum(diffs) / len(diffs)
        print(f"\noverlap={len(diffs)} mean_|dec-gh_n|={mean:.1f} within10%={close}/{len(diffs)}")
        if mean < 25 or close >= len(diffs) // 2:
            print("HIT: decided HTML cards ≈ grade_history — build Lever B parser on ticket_eval HTML.")
        else:
            print("SOFT: closer than combined_slate/git but not a clean hit yet — inspect card markers.")

    # Peek one decided card structure (ascii-safe)
    p = TEMPLATES / "ticket_eval_2026-05-15.html"
    text = p.read_text(encoding="utf-8", errors="replace")
    for c in split_cards(text):
        if card_is_decided(c) is True:
            # strip tags lightly for structure
            plain = re.sub(r"<[^>]+>", " | ", c)
            plain = re.sub(r"\s+", " ", plain)
            print("\nDECIDED CARD PLAIN (600 chars):")
            print(plain[:600])
            # show class attrs
            classes = re.findall(r'class="([^"]+)"', c)
            print("classes sample:", classes[:25])
            break

    # Also check KPI line for exact n
    m = re.search(r"(\d+)\s+tickets", text)
    print("any 'N tickets' in may15:", m.group(0) if m else None)
    for m in re.finditer(r"[^\n]{0,60}\d+\s+tickets[^\n]{0,60}", text):
        print(" line:", m.group(0)[:120])
        if m.start() > 500000:
            break


if __name__ == "__main__":
    main()
