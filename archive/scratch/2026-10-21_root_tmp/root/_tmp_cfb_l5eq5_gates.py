"""CFB L5=5 family per chip x book on the 3,855-line tape."""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(r"H:\PropORACLE")
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

import discover_gates_sport as D  # noqa: E402
import prop_hit_tiers as T  # noqa: E402
from utils.prop_norm import display_prop  # noqa: E402
from utils.ticket_70_pool import goblin_70_eligible, standard_ticket_eligible  # noqa: E402

TAPE = {
    "2026-08-28",
    "2026-08-29",
    "2026-09-03",
    "2026-09-04",
    "2026-09-05",
    "2026-09-12",
}

GATES = (
    "ungated",
    "L5=5",
    "L5=5+L10>=8",
    "L5=5+D",
    "L5=5+L10>=8+D",
    "live ticket",
)

ORDER = [
    "pass_yds",
    "pass_cmp",
    "pass_att",
    "pass_td",
    "pass_long",
    "pass_rush_yds",
    "pass_yds_combo",
    "rush_yds",
    "rush_att",
    "rush_long",
    "rush_rec_yds",
    "rush_yds_combo",
    "rec_yds",
    "rec",
    "rec_long",
    "rec_yds_combo",
    "rec_td",
    "player_td",
    "rush_td",
    "pat_made",
    "kick_pts",
    "fg_made",
    "int",
]


def live_ticket(rec: dict) -> bool:
    row = {
        "sport": "CFB",
        "player": rec.get("player"),
        "prop": rec["prop"],
        "side": rec["side"],
        "pick_type": "Goblin" if rec["book"].startswith("Goblin") else "Standard",
        "line": rec.get("line"),
        "l5_over": rec.get("l5_over"),
        "l5_under": rec.get("l5_under"),
        "l10_over": rec.get("l10_over"),
        "l10_under": rec.get("l10_under"),
        "def": rec.get("def_tier"),
        "cover": rec.get("cover"),
        "dist_l5": rec.get("cover"),
    }
    if rec["book"] == "Goblin OVER":
        return goblin_70_eligible(row)
    if rec["book"] in {"Standard OVER", "Standard UNDER"}:
        return standard_ticket_eligible(row)
    return False


def raw_rows() -> tuple[list[dict], dict]:
    """Same join as discover_gates_sport, tape dates only, keep line variants."""
    cfg = D.SPORT_CFG["CFB"]
    accept = cfg["accept"]
    by_date = D.graded_by_date()
    rows: list[dict] = []
    n_no_l5 = 0
    n_step8 = 0
    dates_used = []
    for date in sorted(by_date):
        if date not in TAPE:
            continue
        path = by_date[date]
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        step_path = D.find_step8("CFB", date)
        step_idx = D.load_step8_index(step_path, "CFB") if step_path else {}
        if step_path:
            n_step8 += 1
            dates_used.append(f"{date}:{step_path.name}")
        else:
            dates_used.append(f"{date}:NO_STEP8")
        for r in raw.get("props") or []:
            if not isinstance(r, dict):
                continue
            sport = T.norm_sport(str(r.get("sport") or ""))
            if sport not in accept:
                continue
            res = str(r.get("result") or "").strip().upper()
            if res not in ("HIT", "MISS"):
                continue
            rec_join = None
            # reuse iter_sport_rows field build by calling the same keying
            # Minimal copy of the join body:
            prop = T.canon_prop("CFB", str(r.get("prop") or r.get("prop_type") or ""))
            if not prop:
                continue
            if cfg.get("exclude_period") and (
                D.is_football_period_split_prop(prop)
                or D.is_football_period_split_prop(r.get("prop"))
            ):
                continue
            side = D._side(r.get("direction") or r.get("over_under"))
            if side is None:
                continue
            book = D._pick_book(r.get("pick_type"), side)
            if book not in D.BOOKS:
                continue
            line = D._num(r.get("line"))
            player_norm = D._norm_player(r.get("player"))
            pick = (
                "Goblin"
                if "goblin" in str(r.get("pick_type") or "").lower()
                else (
                    "Demon"
                    if "demon" in str(r.get("pick_type") or "").lower()
                    else "Standard"
                )
            )
            key = (player_norm, prop, pick, line, side)
            s8 = step_idx.get(key) or {}
            rec_join = {
                "date": date,
                "player": r.get("player"),
                "player_norm": player_norm,
                "prop": prop,
                "book": book,
                "side": side,
                "line": line,
                "hit": 1 if res == "HIT" else 0,
                "l5_over": r.get("l5_over")
                if r.get("l5_over") not in (None, "")
                else s8.get("l5_over"),
                "l5_under": r.get("l5_under")
                if r.get("l5_under") not in (None, "")
                else s8.get("l5_under"),
                "l10_over": r.get("l10_over")
                if r.get("l10_over") not in (None, "")
                else s8.get("l10_over"),
                "l10_under": r.get("l10_under")
                if r.get("l10_under") not in (None, "")
                else s8.get("l10_under"),
                "def_tier": r.get("def_tier")
                or r.get("def")
                or s8.get("def_tier")
                or "",
                "cover": r.get("cover") or r.get("dist_l5") or s8.get("cover"),
            }
            if rec_join["l5_over"] in (None, ""):
                rec_join["l5_over"] = r.get("last5_over")
            if rec_join["l5_under"] in (None, ""):
                rec_join["l5_under"] = r.get("last5_under")
            atoms = D.build_atoms(rec_join)
            rec_join["atoms"] = atoms
            rec_join["live"] = live_ticket(rec_join)
            if not atoms["L5=5"] and D.C.l5_dir(rec_join, rec_join["side"]) is None:
                n_no_l5 += 1
            rows.append(rec_join)
    unique = D.unique_game_dedupe(rows)
    for rec in unique:
        rec["atoms"] = D.build_atoms(rec)
        rec["live"] = live_ticket(rec)
    meta = {
        "raw_n": len(rows),
        "unique_n": len(unique),
        "step8_dates": dates_used,
        "n_no_l5_raw": n_no_l5,
    }
    return rows, unique, meta


def cell(rows: list[dict], pred) -> tuple[int, int]:
    h = n = 0
    for r in rows:
        if pred(r):
            n += 1
            h += r["hit"]
    return h, n


def fmt(h: int, n: int) -> str:
    if not n:
        return "—"
    return f"{100 * h / n:.1f}% {h}/{n}"


def main() -> None:
    raw, unique, meta = raw_rows()
    print("META", json.dumps(meta, indent=2))
    books = ("Goblin OVER", "Standard OVER", "Standard UNDER")
    props = sorted({r["prop"] for r in unique}, key=lambda p: (ORDER.index(p) if p in ORDER else 99, p))
    print()
    hdr = f"{'chip':22} {'book':16} {'ungated':14} {'L5=5':14} {'L5=5+L10':14} {'L5=5+D':14} {'L5=5+L10+D':14} {'live ticket':14}"
    print("UNIQUE PLAYER-GAME")
    print(hdr)
    table = []
    for prop in props:
        for book in books:
            pool = [r for r in unique if r["prop"] == prop and r["book"] == book]
            if not pool:
                continue
            ung = cell(pool, lambda r: True)
            l5 = cell(pool, lambda r: r["atoms"]["L5=5"])
            l5l10 = cell(pool, lambda r: r["atoms"]["L5=5"] and r["atoms"]["L10>=8"])
            l5d = cell(pool, lambda r: r["atoms"]["L5=5"] and r["atoms"]["D_ok"])
            stack = cell(
                pool,
                lambda r: r["atoms"]["L5=5"] and r["atoms"]["L10>=8"] and r["atoms"]["D_ok"],
            )
            live = cell(pool, lambda r: r["live"])
            label = display_prop(prop, "CFB")
            row = {
                "prop": prop,
                "label": label,
                "book": book,
                "ungated": {"h": ung[0], "n": ung[1]},
                "l5eq5": {"h" : l5[0], "n": l5[1]},
                "l5_l10": {"h": l5l10[0], "n": l5l10[1]},
                "l5_d": {"h": l5d[0], "n": l5d[1]},
                "stack": {"h": stack[0], "n": stack[1]},
                "live": {"h": live[0], "n": live[1]},
            }
            table.append(row)
            print(
                f"{label:22} {book:16} {fmt(*ung):14} {fmt(*l5):14} {fmt(*l5l10):14} "
                f"{fmt(*l5d):14} {fmt(*stack):14} {fmt(*live):14}"
            )
    print()
    print("LINE VARIANTS (same join, no unique-game collapse)")
    print(hdr)
    raw_table = []
    for prop in props:
        for book in books:
            pool = [r for r in raw if r["prop"] == prop and r["book"] == book]
            if not pool:
                continue
            ung = cell(pool, lambda r: True)
            l5 = cell(pool, lambda r: r["atoms"]["L5=5"])
            l5l10 = cell(pool, lambda r: r["atoms"]["L5=5"] and r["atoms"]["L10>=8"])
            l5d = cell(pool, lambda r: r["atoms"]["L5=5"] and r["atoms"]["D_ok"])
            stack = cell(
                pool,
                lambda r: r["atoms"]["L5=5"] and r["atoms"]["L10>=8"] and r["atoms"]["D_ok"],
            )
            live = cell(pool, lambda r: r["live"])
            label = display_prop(prop, "CFB")
            raw_table.append(
                {
                    "prop": prop,
                    "label": label,
                    "book": book,
                    "ungated": {"h": ung[0], "n": ung[1]},
                    "l5eq5": {"h": l5[0], "n": l5[1]},
                    "l5_l10": {"h": l5l10[0], "n": l5l10[1]},
                    "l5_d": {"h": l5d[0], "n": l5d[1]},
                    "stack": {"h": stack[0], "n": stack[1]},
                    "live": {"h": live[0], "n": live[1]},
                }
            )
            print(
                f"{label:22} {book:16} {fmt(*ung):14} {fmt(*l5):14} {fmt(*l5l10):14} "
                f"{fmt(*l5d):14} {fmt(*stack):14} {fmt(*live):14}"
            )
    out = ROOT / "data" / "reports" / "_tmp_cfb_l5eq5_gates.json"
    out.write_text(
        json.dumps({"meta": meta, "unique": table, "variants": raw_table}, indent=2),
        encoding="utf-8",
    )
    print("wrote", out)


if __name__ == "__main__":
    main()
