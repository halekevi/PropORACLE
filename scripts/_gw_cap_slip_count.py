"""Count archived GW slips that lose EV>0 when Games Won p drops to 0.62."""
from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

from report_gated_construction_efficiency import discover_g70_archives, payload_from_g70  # noqa: E402
from utils.gated_construction_tracks import GATED_GOBLIN, construction_track  # noqa: E402
from utils.prop_norm import canon_prop  # noqa: E402
from utils.ticket_70_pool import P_GOBLIN_L5EQ5, P_TENNIS_GAMES_WON_GOBLIN_LIVE  # noqa: E402


def _gw_count(legs: list[dict]) -> int:
    n = 0
    for leg in legs:
        sport = str(leg.get("sport") or "").upper()
        prop = canon_prop("TENNIS", str(leg.get("prop") or leg.get("prop_type") or ""))
        if sport.startswith("TENNIS") and prop == "games_won":
            n += 1
    return n


def _all_hit_x(ticket: dict, n: int) -> float | None:
    pay = ticket.get("payout") if isinstance(ticket.get("payout"), dict) else {}
    nc = ticket.get("n_correct") if isinstance(ticket.get("n_correct"), dict) else {}
    pay_nc = pay.get("n_correct") if isinstance(pay.get("n_correct"), dict) else {}
    for src in (nc, pay_nc):
        for key in (n, str(n)):
            if key in src:
                try:
                    return float(src[key])
                except (TypeError, ValueError):
                    pass
    for key in ("sweep_payout_x", "all_hit_x", "payout_x", "min_x", "ev_n_correct"):
        raw = ticket.get(key)
        if raw is None:
            raw = pay.get(key)
        try:
            if raw is not None:
                val = float(raw)
                # ev_n_correct is already a multiple on archive tickets
                if key == "ev_n_correct" and val > 0:
                    return val
                if key != "ev_n_correct":
                    return val
        except (TypeError, ValueError):
            pass
    n_g = sum(
        1
        for leg in (ticket.get("legs") or [])
        if "goblin" in str(leg.get("pick_type") or "").lower()
    )
    floors = {
        (2, 2): 1.5,
        (3, 3): 1.65,
        (2, 1): 2.0,
        (3, 2): 2.5,
        (3, 1): 3.25,
    }
    return floors.get((n, n_g))


def _joint(ps: list[float]) -> float:
    out = 1.0
    for p in ps:
        out *= p
    return out


def _leg_p(leg: dict, *, live_gw: bool) -> float:
    sport = str(leg.get("sport") or "").upper()
    prop = canon_prop("TENNIS", str(leg.get("prop") or leg.get("prop_type") or ""))
    try:
        stored = float(leg.get("p") or leg.get("leg_prob_used"))
    except (TypeError, ValueError):
        stored = None
    if sport.startswith("TENNIS") and prop == "games_won":
        if live_gw:
            return P_TENNIS_GAMES_WON_GOBLIN_LIVE
        if stored is not None and 0 < stored <= 1:
            return stored
        return P_GOBLIN_L5EQ5
    if stored is not None and 0 < stored <= 1:
        return stored
    return P_GOBLIN_L5EQ5


def main() -> None:
    rows = []
    for date_s, path in discover_g70_archives(start=None, end=None):
        payload = payload_from_g70(path, date_s)
        tickets = []
        if payload.get("groups"):
            for group in payload["groups"]:
                gname = str(group.get("group_name") or "")
                for ticket in group.get("tickets") or []:
                    tickets.append((gname, ticket))
        else:
            for ticket in payload.get("tickets") or []:
                tickets.append((str(ticket.get("group_name") or ticket.get("name") or ""), ticket))
        for gname, ticket in tickets:
            if not isinstance(ticket, dict):
                continue
            if construction_track(ticket, gname) != GATED_GOBLIN:
                continue
            legs = [leg for leg in (ticket.get("legs") or []) if isinstance(leg, dict)]
            if not legs:
                continue
            n_gw = _gw_count(legs)
            if n_gw == 0:
                continue
            n = len(legs)
            px = _all_hit_x(ticket, n)
            ps_old = [_leg_p(leg, live_gw=False) for leg in legs]
            ps_new = [_leg_p(leg, live_gw=True) for leg in legs]
            if px is None or px < 1.4:
                status = "no_payout"
                old_ev = new_ev = None
            else:
                old_ev = _joint(ps_old) * px - 1.0
                new_ev = _joint(ps_new) * px - 1.0
                if new_ev > 0:
                    status = "kept"
                elif old_ev > 0:
                    status = "killed"
                else:
                    status = "never"
            rows.append(
                {
                    "date": date_s,
                    "id": ticket.get("id") or ticket.get("ticket_id"),
                    "n": n,
                    "n_gw": n_gw,
                    "px": px,
                    "ps_old": ps_old,
                    "ps_new": ps_new,
                    "old_ev": old_ev,
                    "new_ev": new_ev,
                    "status": status,
                    "players": [leg.get("player") for leg in legs],
                }
            )

    print(f"GW slips on gated archives: {len(rows)}")
    print("by status", dict(Counter(r["status"] for r in rows)))
    print("by n_legs", dict(Counter(r["n"] for r in rows)))
    print("by n_gw", dict(Counter(r["n_gw"] for r in rows)))
    killed = [r for r in rows if r["status"] == "killed"]
    kept = [r for r in rows if r["status"] == "kept"]
    never = [r for r in rows if r["status"] == "never"]
    print(f"killed (old EV>0 -> new EV<=0): {len(killed)}")
    print(f"kept (still EV>0 at 0.62): {len(kept)}")
    print(f"never (already EV<=0 before cap): {len(never)}")
    for r in killed:
        print(
            f"  KILL {r['date']} {r['id']} n={r['n']} gw={r['n_gw']} px={r['px']} "
            f"old_ev={r['old_ev']:.3f} new_ev={r['new_ev']:.3f} {r['players']}"
        )
    for r in kept:
        print(
            f"  KEEP {r['date']} {r['id']} n={r['n']} gw={r['n_gw']} px={r['px']} "
            f"old_ev={r['old_ev']:.3f} new_ev={r['new_ev']:.3f} {r['players']}"
        )
    two = [r for r in rows if r["n"] == 2]
    two_dead = [
        r
        for r in two
        if r["status"] in {"killed", "never"}
        or (r["new_ev"] is not None and r["new_ev"] <= 0)
    ]
    print(f"2-leg GW slips: {len(two)}; non-positive EV at 0.62: {len(two_dead)}")


if __name__ == "__main__":
    main()
