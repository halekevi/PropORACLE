"""Regrade checkable tennis rows by board opponent, log grade flips."""
from __future__ import annotations

import json
import sys
from collections import Counter
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT.parent / "PropORACLE_main_cp"
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

from utils.grade_corrections import WRONG_MATCH_ATTACH, log_correction  # noqa: E402
from utils.prop_norm import canon_prop  # noqa: E402
from _wrong_match_and_cap_effect import (  # noqa: E402
    _board_matches,
    _load_board_opp_map,
    _norm,
    _opp_match,
)

OUT = ROOT / "data" / "reports" / "regrade_by_opp_20261007.json"


def _p(*a):
    print(*a, flush=True)


def _pairs() -> list[dict]:
    pairs: dict[tuple[str, str], dict] = {}
    seen = set()
    for root in (MAIN / "outputs", ROOT / "outputs"):
        if not root.is_dir():
            continue
        for path in sorted(root.glob("**/actuals_tennis_*.csv")):
            if path.name in seen:
                continue
            seen.add(path.name)
            day = path.stem.replace("actuals_tennis_", "")[:10]
            try:
                df = pd.read_csv(path, dtype=str)
            except Exception:
                continue
            if df.empty or "player" not in df.columns:
                continue
            for _, r in df.iterrows():
                prop = canon_prop("TENNIS", str(r.get("prop_type") or ""))
                if prop not in {"games_won", "match_total_games"}:
                    continue
                try:
                    actual = float(r.get("actual"))
                except (TypeError, ValueError):
                    continue
                player = str(r.get("player") or "").strip()
                key = (_norm(player), day)
                rec = pairs.setdefault(key, {"player": player, "day": day})
                rec[prop] = actual
    return [r for r in pairs.values() if "games_won" in r]


def main() -> None:
    rows = _pairs()
    _p(f"player-days {len(rows)}")
    days = sorted({r["day"] for r in rows})
    pull = set()
    for d in days:
        dd = date.fromisoformat(d)
        for off in (-1, 0, 1):
            pull.add((dd + timedelta(days=off)).isoformat())
    boards = {}
    for i, d in enumerate(sorted(pull)):
        boards[d] = _board_matches(d)
        if (i + 1) % 20 == 0 or i + 1 == len(pull):
            _p(f"  espn {i+1}/{len(pull)}")
    maps = {}
    for i, d in enumerate(sorted(pull)):
        maps[d] = _load_board_opp_map(d)
        if (i + 1) % 20 == 0 or i + 1 == len(pull):
            _p(f"  board {i+1}/{len(pull)}")

    def board_opp(day: str, player: str) -> str:
        key = _norm(player)
        for d in (
            day,
            (date.fromisoformat(day) - timedelta(days=1)).isoformat(),
            (date.fromisoformat(day) + timedelta(days=1)).isoformat(),
        ):
            found = (maps.get(d) or {}).get(key, "")
            if found and "UNKNOWN" not in found.upper():
                return found
        return ""

    def cands_for(day: str, player: str) -> list[dict]:
        key = _norm(player)
        out = []
        for d in (
            day,
            (date.fromisoformat(day) + timedelta(days=1)).isoformat(),
            (date.fromisoformat(day) - timedelta(days=1)).isoformat(),
        ):
            out.extend(boards.get(d, {}).get(key) or [])
        return out

    checked = flips = agree = no_board = no_opp_match = 0
    flip_rows = []
    correct_gw: dict[tuple[str, str], float] = {}
    for r in rows:
        board = board_opp(r["day"], r["player"])
        if not board:
            no_board += 1
            continue
        cands = cands_for(r["day"], r["player"])
        # Prior checkable set: some ESPN row's games match the stored actual.
        stored = float(r["games_won"])
        if not any(abs(c["games"] - stored) < 0.01 for c in cands):
            continue
        checked += 1
        matched = [
            c
            for c in cands
            if _opp_match(board, c["opp"]) and not c["retired"]
        ]
        if not matched:
            matched = [c for c in cands if _opp_match(board, c["opp"])]
        if not matched:
            no_opp_match += 1
            continue
        matched.sort(key=lambda c: (c["retired"], 0 if c["ymd"] == r["day"] else 1))
        hand = matched[0]
        correct_gw[(_norm(r["player"]), r["day"])] = float(hand["games"])
        if abs(hand["games"] - stored) < 0.01:
            agree += 1
        else:
            flips += 1
            if len(flip_rows) < 25:
                flip_rows.append(
                    {
                        "day": r["day"],
                        "player": r["player"],
                        "stored": stored,
                        "correct": hand["games"],
                        "board_opp": board,
                        "espn_opp": hand["opp"],
                        "status": hand["status"],
                    }
                )

    _p(f"checked {checked} agree {agree} flip_actual {flips} no_opp_match {no_opp_match} no_board {no_board}")

    # Grade flips on graded_props Games Won rows whose player-day flipped.
    grade_flip = grade_same = grade_seen = 0
    logged = 0
    templates = MAIN / "ui_runner" / "templates"
    if not templates.is_dir():
        templates = ROOT / "ui_runner" / "templates"
    for path in sorted(templates.glob("graded_props_2026-*.json")):
        day = path.stem.replace("graded_props_", "")[:10]
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        for row in raw.get("props") or []:
            if not isinstance(row, dict):
                continue
            if not str(row.get("sport") or "").upper().startswith("TENNIS"):
                continue
            if canon_prop("TENNIS", str(row.get("prop") or "")) != "games_won":
                continue
            res = str(row.get("result") or "").upper()
            if res not in {"HIT", "MISS"}:
                continue
            key = (_norm(row.get("player")), day)
            if key not in correct_gw:
                continue
            try:
                line = float(row.get("line"))
            except (TypeError, ValueError):
                continue
            side = str(row.get("direction") or "OVER").upper()
            gw = correct_gw[key]
            if side == "UNDER":
                new = "HIT" if gw < line else "MISS"
            else:
                new = "HIT" if gw > line else "MISS"
            grade_seen += 1
            if new == res:
                grade_same += 1
                continue
            grade_flip += 1
            log_correction(
                {
                    "date": day,
                    "player": row.get("player"),
                    "prop": row.get("prop"),
                    "direction": side,
                    "line": row.get("line"),
                    "sport": "TENNIS",
                },
                old=res,
                new=new,
                reason=WRONG_MATCH_ATTACH,
                note=f"board-opp games_won {gw} vs stored actual",
            )
            logged += 1

    _p(f"graded GW rows with a corrected actual: {grade_seen} same {grade_same} flip {grade_flip} logged {logged}")

    # Filled GW legs on correct attach only.
    legs_path = ROOT / "data" / "reports" / "gw_hand_branch_20261007.json"
    filled_ok = filled_bad = filled_hit = filled_n = same_hit = same_n = 0
    if legs_path.is_file():
        legs = json.loads(legs_path.read_text(encoding="utf-8"))["legs"]
        for rec in legs:
            key = (_norm(rec["player"]), rec["date"])
            # start-fill uses the fill day when present
            src = str(rec.get("source") or "")
            if src.startswith("fill:"):
                key = (_norm(rec["player"]), src.split(":", 1)[1])
            gw = correct_gw.get(key)
            if gw is None:
                continue
            stored = rec.get("actual")
            try:
                line = float(rec["line"])
                stored_f = float(stored)
            except (TypeError, ValueError):
                continue
            attached_ok = abs(stored_f - gw) < 0.01
            grade = "HIT" if gw > line else "MISS"
            if src.startswith("fill:"):
                if not attached_ok:
                    filled_bad += 1
                    continue
                filled_ok += 1
                filled_n += 1
                if grade == "HIT":
                    filled_hit += 1
            else:
                if not attached_ok:
                    continue
                same_n += 1
                if grade == "HIT":
                    same_hit += 1
        _p(
            f"filled correct-attach {filled_hit}/{filled_n} "
            f"(skipped wrong-attach fills {filled_bad}); "
            f"same-day correct-attach {same_hit}/{same_n}"
        )

    # Slip book: replay manifest grades, swapping a Games Won leg when the
    # board-opponent match changes HIT/MISS.
    from report_gated_construction_efficiency import discover_g70_archives, payload_from_g70
    from utils.gated_construction_tracks import GATED_GOBLIN, construction_track
    import build_ticket_eval as te

    man = json.loads(
        (ROOT / "data" / "reports" / "gated_goblin_slip_manifest_20261007.json").read_text(
            encoding="utf-8"
        )
    )
    leg_fix: dict[tuple[str, str, str], str] = {}
    for date_s, path in discover_g70_archives(start=None, end=None):
        payload = payload_from_g70(path, date_s)
        tickets = []
        if payload.get("groups"):
            for g in payload["groups"]:
                for t in g.get("tickets") or []:
                    tickets.append(t)
        else:
            tickets.extend(payload.get("tickets") or [])
        for ticket in tickets:
            if not isinstance(ticket, dict):
                continue
            fam = str(ticket.get("family") or "")
            track = str(ticket.get("ticket_track") or "")
            if (
                construction_track(ticket, str(ticket.get("group_name") or "Power 3 · goblin"))
                != GATED_GOBLIN
                and "goblin" not in fam.lower()
                and "goblin" not in track.lower()
            ):
                continue
            tid = str(ticket.get("id") or ticket.get("ticket_id") or "")
            for leg in ticket.get("legs") or []:
                if not isinstance(leg, dict):
                    continue
                if not str(leg.get("sport") or "").upper().startswith("TENNIS"):
                    continue
                if canon_prop("TENNIS", str(leg.get("prop") or "")) != "games_won":
                    continue
                key = (_norm(leg.get("player")), date_s)
                gw = correct_gw.get(key)
                if gw is None:
                    continue
                try:
                    line = float(leg.get("line"))
                except (TypeError, ValueError):
                    continue
                side = str(leg.get("side") or leg.get("direction") or "OVER").upper()
                new = "HIT" if (gw < line if side == "UNDER" else gw > line) else "MISS"
                leg_fix[(date_s, tid, _norm(leg.get("player")))] = new
    wins = resolved = flipped_slips = 0
    for slip in man["slips"]:
        grades = list(slip.get("grades") or [])
        players = slip.get("players") or []
        changed = False
        for i, player in enumerate(players):
            fix = leg_fix.get((slip["date"], str(slip.get("ticket_id") or "").split("|")[-1], _norm(player)))
            # ticket id on manifest is "date|date|P3-1" and archive id may be "P3-1" or full
            if fix is None:
                fix = leg_fix.get((slip["date"], str(slip.get("ticket_id") or ""), _norm(player)))
            if fix is None:
                short = str(slip.get("ticket_id") or "").split("|")[-1]
                fix = leg_fix.get((slip["date"], short, _norm(player)))
            if fix and i < len(grades) and grades[i] in {"HIT", "MISS"} and grades[i] != fix:
                # only swap when this slot is the tennis GW — props align
                props = slip.get("props") or []
                prop = props[i] if i < len(props) else ""
                if canon_prop("TENNIS", prop) != "games_won":
                    continue
                grades[i] = fix
                changed = True
        if changed:
            flipped_slips += 1
        if all(g in {"HIT", "MISS"} for g in grades) and grades:
            resolved += 1
            if te._ticket_pays_money(slip.get("group") or "", grades):
                wins += 1
    _p(f"rescore slips wins {wins}/{resolved} flipped_slips {flipped_slips} (was 71/127)")
    # We don't have per-leg grades stored as objects with lines on the manifest.
    # Use gw_hand_branch grades, swapping when correct_gw flips the grade.
    slip_note = {
        "manifest_resolved_wins": sum(1 for s in man["slips"] if s["resolved"] and s["win"]),
        "manifest_resolved": man["resolved"],
        "grade_flips_logged": logged,
        "actual_flips_on_checkable": flips,
        "checkable": checked,
    }
    _p(
        f"manifest still {slip_note['manifest_resolved_wins']}/{slip_note['manifest_resolved']} "
        f"until slip legs are re-scored; catalog grade flips logged={logged}"
    )
    OUT.write_text(
        json.dumps(
            {
                "checked": checked,
                "agree": agree,
                "actual_flips": flips,
                "no_opp_match": no_opp_match,
                "no_board": no_board,
                "grade_seen": grade_seen,
                "grade_same": grade_same,
                "grade_flip": grade_flip,
                "logged": logged,
                "filled_correct": f"{filled_hit}/{filled_n}",
                "same_day_correct": f"{same_hit}/{same_n}",
                "flip_samples": flip_rows,
                "slip_note": slip_note,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    _p(f"WROTE {OUT}")


if __name__ == "__main__":
    main()
