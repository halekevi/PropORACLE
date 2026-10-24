"""Quote sensitivity on the latest board, and Saves passes on a past board."""
from __future__ import annotations

import sys
from itertools import combinations
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT.parent / "PropORACLE_main_cp"
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

import build_goblin70_tickets as g70  # noqa: E402
from utils.prop_norm import canon_prop  # noqa: E402
from utils.soccer_keep_gates import soccer_keep_eligible, soccer_ticket_gate_passes  # noqa: E402
from utils.ticket_70_pool import goblin_70_eligible, goblin_ticket_p  # noqa: E402


def _p(*a):
    print(*a, flush=True)


def _latest_board_date() -> str:
    for root in (MAIN / "outputs", ROOT / "outputs"):
        if not root.is_dir():
            continue
        days = sorted(p.name for p in root.iterdir() if p.is_dir() and p.name[:4] == "2026")
        if days:
            return days[-1]
    return ""


def _clears(ps: list[float], quote: float) -> bool:
    joint = 1.0
    for p in ps:
        joint *= p
    return joint * quote - 1.0 > 0 and quote >= 1.4


def quote_table(day: str) -> None:
    _p(f"\n=== QUOTE SENSITIVITY board {day} ===")
    board = g70.load_today_board(day, skip_golf=True)
    gob = []
    for r in board:
        if not goblin_70_eligible(r):
            continue
        if g70.fold_name(r.get("player")) in g70.SKIP_PLAYERS:
            continue
        rec = dict(r)
        rec["p"] = goblin_ticket_p(rec)
        gob.append(rec)
    _p(f"eligible goblin legs {len(gob)}")
    gw = sum(
        1
        for r in gob
        if str(r.get("sport") or "").upper().startswith("TENNIS")
        and canon_prop("TENNIS", str(r.get("prop") or "")) == "games_won"
    )
    _p(f"of which Games Won {gw}")
    # Cap combinations: if pool is huge, sample is wrong. Count exactly for n<=40 else report too big.
    rows = gob
    if len(rows) > 40:
        _p(f"pool {len(rows)} > 40; counting is exact but may be slow")
    quotes = [("2-leg", 2, 1.5), ("2-leg", 2, 1.7), ("3-leg", 3, 1.65), ("3-leg", 3, 2.2)]
    for label, n, quote in quotes:
        if len(rows) < n:
            _p(f"  {label} {quote}x: pool too small")
            continue
        if len(rows) > 25 and n == 3:
            # combinations of 25 is 2300; 40 is 9880. 60^3 is too big.
            # Use greedy: any n-tuple whose min p still clears.
            need = quote ** (-1 / n)
            _p(f"  {label} {quote}x needs per-leg ~{need:.1%} if equal")
            strong = [r for r in rows if float(r["p"]) >= need]
            _p(f"    legs at/above equal-rate bar: {len(strong)} (not a built-slip count)")
            continue
        n_clear = 0
        best = None
        for combo in combinations(rows, n):
            ps = [float(r["p"]) for r in combo]
            if _clears(ps, quote):
                n_clear += 1
                ev = 1.0
                for p in ps:
                    ev *= p
                ev = ev * quote - 1
                if best is None or ev > best:
                    best = ev
        _p(f"  {label} @ {quote}x: {n_clear} positive-EV groups; best EV {best}")


def saves_on(day: str) -> None:
    _p(f"\n=== SAVES GATE on {day} ===")
    import pandas as pd

    path = MAIN / "outputs" / day / "soccer" / "step8_soccer_direction.csv"
    if not path.is_file():
        path = ROOT / "outputs" / day / "soccer" / "step8_soccer_direction.csv"
    if not path.is_file():
        _p("  no soccer step8")
        return
    if path.suffix == ".parquet":
        df = pd.read_parquet(path)
    elif path.suffix in {".xlsx", ".xls"}:
        df = pd.read_excel(path)
    else:
        df = pd.read_csv(path, dtype=str, encoding_errors="replace")
    cols = {str(c).lower(): c for c in df.columns}
    seen = set()
    passed = blocked_lineup = blocked_history = blocked_other = 0
    for _, r in df.iterrows():
        prop = canon_prop("SOCCER", str(r.get(cols.get("prop_type") or cols.get("prop") or "") or ""))
        if prop != "saves":
            continue
        player = str(r.get(cols.get("player") or "") or "")
        if not player or player in seen:
            continue
        # one row per player: prefer goblin over
        pick = str(r.get(cols.get("pick_type") or "") or "")
        side = str(r.get(cols.get("side") or r.get(cols.get("direction") or "") or "") or "")
        if "goblin" not in pick.lower() or side.upper() not in {"", "OVER"}:
            continue
        seen.add(player)
        row = {str(c): r.get(c) for c in df.columns}
        row["sport"] = "SOCCER"
        row["player"] = player
        row["prop"] = "Goalie Saves"
        row["side"] = "OVER"
        row["pick_type"] = "Goblin"
        ok = soccer_keep_eligible(row)
        ticket = soccer_ticket_gate_passes(row)
        if ticket:
            passed += 1
            _p(f"  PASS {player}")
        else:
            from utils.soccer_board_coverage import (
                competition_has_box_history,
                keeper_lineup_status,
            )

            if not competition_has_box_history(row):
                blocked_history += 1
            elif keeper_lineup_status(row) == "not_starter":
                blocked_lineup += 1
                _p(f"  LINEUP {player} not starter")
            else:
                blocked_other += 1
    _p(
        f"unique goblin saves players {len(seen)} ticket-pass {passed} "
        f"blocked_history {blocked_history} blocked_lineup {blocked_lineup} "
        f"blocked_other_gates {blocked_other}"
    )


def main() -> None:
    day = _latest_board_date()
    _p(f"latest outputs day {day}")
    if day:
        quote_table(day)
    saves_on("2026-09-27")


if __name__ == "__main__":
    main()
