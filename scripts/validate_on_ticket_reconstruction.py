"""
Reconstruct published tickets from graded_props on_ticket=True + ticket_id,
validate daily win counts vs grade_history, segment Power vs Flex.

Cash rules mirror scripts/build_ticket_eval._ticket_pays_money:
  - Flex only when group name contains 'flex' (case-insensitive) and n>=3
  - else Power: all non-void legs must HIT
"""
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
        if res == "PUSH":
            return "VOID"
        return res
    h = r.get("hit")
    if h is True or h == 1 or str(h).lower() in {"1", "true"}:
        return "HIT"
    if h is False or h == 0 or str(h).lower() in {"0", "false"}:
        return "MISS"
    return "UNGRADED"


def _is_flex(group_name: str, n_legs: int) -> bool:
    if n_legs < 3:
        return False
    return "flex" in str(group_name or "").lower()


def _pays_money(group_name: str, grades: list[str]) -> bool | None:
    """True/False cash; None if any UNGRADED."""
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


def _group_name_from_tid(tid: str) -> str:
    # ticket_id: date|Group Name #n|copy
    parts = str(tid).split("|")
    if len(parts) >= 2:
        return re.sub(r"\s*#\d+\s*$", "", parts[1]).strip()
    return str(tid)


def _slate_date_from_tid(tid: str) -> str:
    return str(tid).split("|")[0][:10]


def load_on_ticket_rows() -> list[dict]:
    rows = []
    for path in sorted(TEMPLATES.glob("graded_props_2026-0[56]*.json")):
        file_date = path.stem.replace("graded_props_", "")[:10]
        props = json.loads(path.read_text(encoding="utf-8")).get("props") or []
        for r in props:
            if r.get("on_ticket") is not True and str(r.get("on_ticket")).lower() != "true":
                continue
            tid = r.get("ticket_id")
            if not tid:
                continue
            rows.append({**r, "_file_date": file_date, "ticket_id": str(tid)})
    return rows


def reconstruct_tickets(rows: list[dict]) -> list[dict]:
    by_tid: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_tid[str(r["ticket_id"])].append(r)

    tickets = []
    for tid, legs in by_tid.items():
        gname = _group_name_from_tid(tid)
        grades = [_fold_result(leg) for leg in legs]
        n = len(legs)
        flex = _is_flex(gname, n)
        pays = _pays_money(gname, grades)
        tickets.append(
            {
                "ticket_id": tid,
                "slate_date": _slate_date_from_tid(tid),
                "group_name": gname,
                "n_legs": n,
                "product": "flex" if flex else "power",
                "ticket_hit": pays,  # None if incomplete
                "n_hit": sum(1 for g in grades if g == "HIT"),
                "n_miss": sum(1 for g in grades if g == "MISS"),
                "n_void": sum(1 for g in grades if g == "VOID"),
                "n_ungraded": sum(1 for g in grades if g == "UNGRADED"),
                "legs": legs,
            }
        )
    return tickets


def main() -> None:
    rows = load_on_ticket_rows()
    tickets = reconstruct_tickets(rows)
    print(f"on_ticket legs={len(rows)} reconstructed tickets={len(tickets)}")

    prod = Counter(t["product"] for t in tickets)
    print("product mix", dict(prod))
    print(
        "flex share",
        f"{prod.get('flex', 0) / max(1, len(tickets)):.1%}",
        "(only names containing 'flex')",
    )

    decided = [t for t in tickets if t["ticket_hit"] is not None]
    incomplete = [t for t in tickets if t["ticket_hit"] is None]
    print(f"decided={len(decided)} incomplete/ungraded={len(incomplete)}")

    # Daily win table keyed by slate_date (from ticket_id)
    by_day: dict[str, dict] = defaultdict(lambda: {"n": 0, "wins": 0, "power_n": 0, "power_w": 0, "flex_n": 0, "flex_w": 0})
    for t in decided:
        d = t["slate_date"]
        by_day[d]["n"] += 1
        if t["ticket_hit"]:
            by_day[d]["wins"] += 1
        if t["product"] == "power":
            by_day[d]["power_n"] += 1
            by_day[d]["power_w"] += int(bool(t["ticket_hit"]))
        else:
            by_day[d]["flex_n"] += 1
            by_day[d]["flex_w"] += int(bool(t["ticket_hit"]))

    gh_raw = json.loads((TEMPLATES / "grade_history.json").read_text(encoding="utf-8"))
    # Keep best / last run per date? Multiple tracks may exist — sum or take max n?
    # Use one row per date: prefer largest n_tickets (main board).
    gh_by_date: dict[str, dict] = {}
    for r in gh_raw:
        d = str(r.get("date") or "")[:10]
        if not d:
            continue
        prev = gh_by_date.get(d)
        if prev is None or int(r.get("n_tickets") or 0) >= int(prev.get("n_tickets") or 0):
            gh_by_date[d] = r

    print("\n=== validation: reconstructed vs grade_history (slate_date) ===")
    print(
        f"{'date':10} {'recon_n':>7} {'recon_w':>7} {'gh_n':>6} {'gh_w':>5} "
        f"{'n_delta':>7} {'w_delta':>7} {'power_n':>7} {'flex_n':>6}"
    )
    overlap_days = 0
    sum_abs_n = 0
    sum_abs_w = 0
    for d in sorted(set(by_day) | set(gh_by_date)):
        if not (d.startswith("2026-05") or d.startswith("2026-06")):
            continue
        rec = by_day.get(d)
        gh = gh_by_date.get(d)
        if not rec and not gh:
            continue
        rn = rec["n"] if rec else 0
        rw = rec["wins"] if rec else 0
        gn = int(gh.get("n_tickets") or 0) if gh else 0
        gw = int(gh.get("wins") or 0) + int(gh.get("guarantees") or 0) if gh else 0
        # grade_history win_rate uses wins+guarantees
        if rec and gh:
            overlap_days += 1
            sum_abs_n += abs(rn - gn)
            sum_abs_w += abs(rw - gw)
        print(
            f"{d:10} {rn:7d} {rw:7d} {gn:6d} {gw:5d} "
            f"{rn-gn:7d} {rw-gw:7d} "
            f"{(rec or {}).get('power_n', 0):7d} {(rec or {}).get('flex_n', 0):6d}"
        )

    if overlap_days:
        print(
            f"\noverlap_days={overlap_days} "
            f"mean_|n_delta|={sum_abs_n/overlap_days:.1f} "
            f"mean_|w_delta|={sum_abs_w/overlap_days:.1f}"
        )

    # Also: AND-of-legs vs flex-aware for flex-named tickets only
    flex_tix = [t for t in decided if t["product"] == "flex"]
    if flex_tix:
        and_wins = sum(1 for t in flex_tix if t["n_miss"] == 0 and t["n_ungraded"] == 0)
        flex_wins = sum(1 for t in flex_tix if t["ticket_hit"])
        print(
            f"\nFlex-named tickets: n={len(flex_tix)} "
            f"AND-of-legs wins={and_wins} flex-aware wins={flex_wins} "
            f"(delta={flex_wins - and_wins})"
        )
    else:
        print("\nNo ticket_ids containing 'Flex' in May–June on_ticket set "
              "— Mixed/Goblin names score as Power under grade_history rules.")


if __name__ == "__main__":
    main()
