"""Lead 1: ticket_id coverage vs on_ticket=True; validate ticket_id-only reconstruction."""
from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(r"H:\PropORACLE")
TEMPLATES = ROOT / "ui_runner" / "templates"


def _fold_result(r: dict) -> str:
    res = str(r.get("result") or "").strip().upper()
    if res in {"HIT", "MISS", "VOID", "PUSH"}:
        return "VOID" if res == "PUSH" else res
    h = r.get("hit")
    if h is True or h == 1 or str(h).lower() in {"1", "true"}:
        return "HIT"
    if h is False or h == 0 or str(h).lower() in {"0", "false"}:
        return "MISS"
    return "UNGRADED"


def _is_flex(group_name: str, n_legs: int) -> bool:
    return n_legs >= 3 and "flex" in str(group_name or "").lower()


def _pays_money(group_name: str, grades: list[str]) -> bool | None:
    if not grades or any(g == "UNGRADED" for g in grades):
        return None
    n = len(grades)
    h = sum(1 for g in grades if g == "HIT")
    m = sum(1 for g in grades if g == "MISS")
    v = sum(1 for g in grades if g == "VOID")
    eff = n - v
    if eff < 2:
        return False
    if _is_flex(group_name, n):
        if eff < 3:
            return m == 0 and h == eff
        return m <= 1 and h >= eff - 1
    return m == 0 and h == eff


def _group_name(tid: str) -> str:
    parts = str(tid).split("|")
    if len(parts) >= 2:
        return re.sub(r"\s*#\d+\s*$", "", parts[1]).strip()
    return str(tid)


def _slate_date(tid: str) -> str:
    return str(tid).split("|")[0][:10]


def main() -> None:
    # --- coverage compare ---
    print("=== Lead 1: ticket_id vs on_ticket coverage (May–June) ===")
    print(f"{'date':10} {'props':>6} {'on_T':>5} {'tid':>6} {'tid_only':>8} {'both':>5} {'ratio':>6}")
    tot_on = tot_tid = tot_props = 0
    for path in sorted(TEMPLATES.glob("graded_props_2026-0[56]*.json")):
        day = path.stem.replace("graded_props_", "")[:10]
        props = json.loads(path.read_text(encoding="utf-8")).get("props") or []
        on = sum(
            1
            for r in props
            if r.get("on_ticket") is True or str(r.get("on_ticket")).lower() == "true"
        )
        tid = sum(1 for r in props if r.get("ticket_id"))
        both = sum(
            1
            for r in props
            if r.get("ticket_id")
            and (r.get("on_ticket") is True or str(r.get("on_ticket")).lower() == "true")
        )
        tid_only = tid - both
        ratio = tid / max(1, on) if on else (float("inf") if tid else 0.0)
        tot_on += on
        tot_tid += tid
        tot_props += len(props)
        if on or tid:
            print(
                f"{day:10} {len(props):6d} {on:5d} {tid:6d} {tid_only:8d} {both:5d} "
                f"{ratio:6.1f}x"
            )
    print(
        f"{'TOTAL':10} {tot_props:6d} {tot_on:5d} {tot_tid:6d} "
        f"{'':8} {'':5} {tot_tid/max(1,tot_on):6.1f}x"
    )

    if tot_tid <= tot_on * 1.25:
        print("\n[Lead 1] ticket_id NOT meaningfully richer than on_ticket — skip re-validate.")
        return

    print("\n[Lead 1] ticket_id is richer — reconstructing on ticket_id alone…")

    # load all ticket_id rows
    by_tid: dict[str, list] = defaultdict(list)
    for path in sorted(TEMPLATES.glob("graded_props_2026-0[56]*.json")):
        props = json.loads(path.read_text(encoding="utf-8")).get("props") or []
        for r in props:
            tid = r.get("ticket_id")
            if not tid:
                continue
            by_tid[str(tid)].append(r)

    tickets = []
    for tid, legs in by_tid.items():
        gname = _group_name(tid)
        grades = [_fold_result(leg) for leg in legs]
        pays = _pays_money(gname, grades)
        if pays is None:
            continue
        tickets.append(
            {
                "ticket_id": tid,
                "slate_date": _slate_date(tid),
                "n_legs": len(legs),
                "product": "flex" if _is_flex(gname, len(legs)) else "power",
                "ticket_hit": pays,
            }
        )
    print(f"reconstructed decided tickets={len(tickets)} unique tids={len(by_tid)}")
    print("product", dict(Counter(t["product"] for t in tickets)))

    by_day: dict[str, dict] = defaultdict(lambda: {"n": 0, "wins": 0})
    for t in tickets:
        by_day[t["slate_date"]]["n"] += 1
        by_day[t["slate_date"]]["wins"] += int(bool(t["ticket_hit"]))

    gh_by_date: dict[str, dict] = {}
    for r in json.loads((TEMPLATES / "grade_history.json").read_text(encoding="utf-8")):
        d = str(r.get("date") or "")[:10]
        if not d:
            continue
        prev = gh_by_date.get(d)
        if prev is None or int(r.get("n_tickets") or 0) >= int(prev.get("n_tickets") or 0):
            gh_by_date[d] = r

    print("\n=== ticket_id-only vs grade_history ===")
    print(f"{'date':10} {'recon_n':>7} {'recon_w':>7} {'gh_n':>6} {'gh_w':>5} {'dn':>6} {'dw':>5}")
    overlap = 0
    sum_dn = sum_dw = 0
    for d in sorted(set(by_day) | set(gh_by_date)):
        if not (d.startswith("2026-05") or d.startswith("2026-06")):
            continue
        rec = by_day.get(d)
        gh = gh_by_date.get(d)
        if not rec or not gh:
            continue
        rn, rw = rec["n"], rec["wins"]
        gn = int(gh.get("n_tickets") or 0)
        gw = int(gh.get("wins") or 0) + int(gh.get("guarantees") or 0)
        overlap += 1
        sum_dn += abs(rn - gn)
        sum_dw += abs(rw - gw)
        print(f"{d:10} {rn:7d} {rw:7d} {gn:6d} {gw:5d} {rn-gn:6d} {rw-gw:5d}")

    if overlap:
        mean_dn = sum_dn / overlap
        mean_dw = sum_dw / overlap
        print(f"\noverlap={overlap} mean_|n|={mean_dn:.1f} mean_|w|={mean_dw:.1f}")
        # success: roughly close — e.g. mean |n| under ~20% of mean gh_n
        mean_gh_n = sum(
            int(gh_by_date[d].get("n_tickets") or 0)
            for d in by_day
            if d in gh_by_date and (d.startswith("2026-05") or d.startswith("2026-06"))
        ) / max(1, overlap)
        print(f"mean gh_n={mean_gh_n:.0f} |n|/gh_n={mean_dn/max(1,mean_gh_n):.1%}")
        if mean_dn <= max(20, 0.2 * mean_gh_n) and mean_dw <= max(10, 0.25 * mean_gh_n * 0.25):
            print("HIT: ticket_id-only reconstruction is close enough to build on.")
        else:
            print("MISS: still too far from grade_history — go to Lead 2.")


if __name__ == "__main__":
    main()
