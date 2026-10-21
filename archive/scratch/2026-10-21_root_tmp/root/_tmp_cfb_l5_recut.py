"""CFB-only L5=5 recut: pick the graded file that actually has CFB."""
from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

ROOT = Path(r"H:\PropORACLE")
MAIN = Path(r"H:\PropORACLE_main_cp")
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

import prop_hit_tiers as T  # noqa: E402
from utils.defense_tiers import normalize_def_tier_label  # noqa: E402
from utils.prop_norm import display_prop  # noqa: E402
from utils.ticket_70_pool import goblin_70_eligible, standard_ticket_eligible  # noqa: E402

TAPE = [
    "2026-08-28",
    "2026-08-29",
    "2026-09-03",
    "2026-09-04",
    "2026-09-05",
    "2026-09-12",
    "2026-09-26",
    "2026-10-10",
]
BOOKS = ("Goblin OVER", "Standard OVER", "Standard UNDER")
PROD = {"Weak", "Below Avg"}
STINGY = {"Elite", "Above Avg"}
LABEL = {
    "pass_yds": "Pass Yards",
    "pass_cmp": "Pass Completions",
    "passcompletions": "Pass Completions",
    "pass_att": "Pass Attempts",
    "pass_td": "Pass TDs",
    "pass_long": "Longest Completion",
    "pass_rush_yds": "Pass+Rush Yards",
    "pass_yds_combo": "Pass Yards combo",
    "rush_yds": "Rush Yards",
    "rush_att": "Rush Attempts",
    "rush_long": "Longest Rush",
    "rush_rec_yds": "Rush+Rec Yards",
    "rush_yds_combo": "Rush Yards combo",
    "rec_yds": "Rec Yards",
    "rec": "Receptions",
    "rec_long": "Longest Reception",
    "rec_yds_combo": "Rec Yards combo",
    "rec_td": "Rec TDs",
    "player_td": "Player TDs",
    "rush_td": "Rush TDs",
    "pat_made": "PAT Made",
    "kick_pts": "Kicking Points",
    "fg_made": "FG Made",
    "int": "INTs",
}
ORDER = list(LABEL)


def is_cfb(row: dict) -> bool:
    s = T.norm_sport(str(row.get("sport") or row.get("league") or ""))
    return s in {"CFB", "NCAAF"} or str(row.get("sport") or "").strip().upper() == "CFB"


def num(v):
    if v in (None, ""):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def first_num(*vals):
    for v in vals:
        n = num(v)
        if n is not None:
            return n
    return None


def norm_player(s) -> str:
    import re

    x = re.sub(r"[^a-z0-9 ]+", " ", str(s or "").lower())
    return re.sub(r"\s+", " ", x).strip()


def side_of(raw) -> str | None:
    s = str(raw or "").strip().upper()
    if s in {"O", "OVER"}:
        return "OVER"
    if s in {"U", "UNDER"}:
        return "UNDER"
    return None


def pick_of(raw) -> str:
    low = str(raw or "").lower()
    if "goblin" in low:
        return "Goblin"
    if "demon" in low:
        return "Demon"
    return "Standard"


def book_of(pick: str, side: str) -> str | None:
    if pick == "Goblin" and side == "OVER":
        return "Goblin OVER"
    if pick == "Standard" and side in {"OVER", "UNDER"}:
        return f"Standard {side}"
    return None


def cfb_count(path: Path) -> int:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return 0
    n = 0
    for r in raw.get("props") or []:
        if not isinstance(r, dict) or not is_cfb(r):
            continue
        if str(r.get("result") or "").upper() in {"HIT", "MISS"}:
            n += 1
    return n


def graded_for(date: str) -> Path | None:
    cands = []
    for root in (ROOT, MAIN):
        p = root / "ui_runner" / "templates" / f"graded_props_{date}.json"
        if p.exists():
            cands.append((cfb_count(p), str(p), p))
    if not cands:
        return None
    cands.sort(reverse=True)
    return cands[0][2]


def find_step8(date: str) -> Path | None:
    names = (
        f"step8_cfb_direction_clean_{date}.xlsx",
        "step8_cfb_direction_clean.xlsx",
    )
    for root in (ROOT, MAIN):
        d = root / "outputs" / date / "cfb"
        for n in names:
            p = d / n
            if p.exists():
                return p
    return None


def load_step8(path: Path) -> dict[tuple, dict]:
    df = pd.read_excel(path)
    cm = {str(c).strip().lower(): c for c in df.columns}

    def col(*names):
        for n in names:
            if n.lower() in cm:
                return cm[n.lower()]
        return None

    c_player = col("player", "Player")
    c_prop = col("prop", "Prop", "prop_type", "prop_norm")
    c_pick = col("pick type", "Pick Type", "pick_type")
    c_line = col("line", "Line")
    c_dir = col(
        "direction",
        "Direction",
        "bet_direction",
        "final_bet_direction",
        "recommended_side",
    )
    c_l5o = col("last5_over", "l5_over", "L5 Over", "line_hits_over_5")
    c_l5u = col("last5_under", "l5_under", "L5 Under", "line_hits_under_5")
    c_l10o = col("line_hits_over_10", "l10_over", "L10 Over", "last10_over")
    c_l10u = col("line_hits_under_10", "l10_under", "L10 Under", "last10_under")
    c_def = col("def tier", "Def Tier", "def_tier", "opp_def_tier")
    c_cover = col("dist_l5", "cover", "Abs Edge", "avg_L5_vs_line")
    c_flag = col("season_l5_flag", "Season L5 Flag")
    out: dict[tuple, dict] = {}
    for row in df.to_dict("records"):
        prop = T.canon_prop("CFB", str(row.get(c_prop) or ""))
        if not prop:
            continue
        side = side_of(row.get(c_dir) if c_dir else None)
        if side is None:
            continue
        pick = pick_of(row.get(c_pick) if c_pick else "")
        line = num(row.get(c_line) if c_line else None)
        key = (norm_player(row.get(c_player) if c_player else None), prop, pick, line, side)
        rec = {
            "l5_over": num(row.get(c_l5o) if c_l5o else None),
            "l5_under": num(row.get(c_l5u) if c_l5u else None),
            "l10_over": num(row.get(c_l10o) if c_l10o else None),
            "l10_under": num(row.get(c_l10u) if c_l10u else None),
            "def_tier": row.get(c_def) if c_def else "",
            "cover": num(row.get(c_cover) if c_cover else None),
            "season_l5_flag": str(row.get(c_flag) or "") if c_flag else "",
        }
        if key not in out:
            out[key] = rec
        else:
            for k, v in rec.items():
                if out[key].get(k) in (None, "") and v not in (None, ""):
                    out[key][k] = v
    return out


def l5_dir(rec, side):
    return rec.get("l5_over") if side == "OVER" else rec.get("l5_under")


def l10_dir(rec, side):
    return rec.get("l10_over") if side == "OVER" else rec.get("l10_under")


def d_ok(side, tier) -> bool:
    t = normalize_def_tier_label(tier) or ""
    if side == "OVER":
        return t in PROD
    return t in STINGY


def live_ok(rec) -> bool:
    row = {
        "sport": "CFB",
        "player": rec.get("player"),
        "prop": rec["prop"],
        "side": rec["side"],
        "pick_type": rec["pick"],
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
    return standard_ticket_eligible(row)


def unique_dedupe(rows: list[dict]) -> list[dict]:
    groups: dict[tuple, list] = defaultdict(list)
    for r in rows:
        groups[(r["date"], r["player_norm"], r["prop"], r["side"], r["book"])].append(r)
    out = []
    for pool in groups.values():
        lines = [x["line"] for x in pool if x.get("line") is not None]
        med = sorted(lines)[len(lines) // 2] if lines else 0
        best = min(pool, key=lambda r: abs((r.get("line") or med) - med))
        out.append(best)
    return out


def fmt(h, n) -> str:
    if not n:
        return "never fires"
    return f"{100 * h / n:.1f}% {h}/{n}"


def cell(rows, pred):
    h = n = 0
    for r in rows:
        if pred(r):
            n += 1
            h += r["hit"]
    return h, n


def main() -> None:
    rows = []
    meta_dates = []
    n_join = n_l5 = n_l10 = 0
    for date in TAPE:
        gpath = graded_for(date)
        spath = find_step8(date)
        n_cfb = cfb_count(gpath) if gpath else 0
        meta_dates.append(
            {
                "date": date,
                "graded": str(gpath) if gpath else None,
                "cfb_decided": n_cfb,
                "step8": str(spath) if spath else None,
            }
        )
        if not gpath or not spath:
            print(f"{date} SKIP graded={gpath} step8={spath} cfb={n_cfb}")
            continue
        idx = load_step8(spath)
        raw = json.loads(gpath.read_text(encoding="utf-8"))
        day_n = day_hit = 0
        for r in raw.get("props") or []:
            if not isinstance(r, dict) or not is_cfb(r):
                continue
            res = str(r.get("result") or "").upper()
            if res not in {"HIT", "MISS"}:
                continue
            prop = T.canon_prop("CFB", str(r.get("prop") or r.get("prop_type") or ""))
            if not prop:
                continue
            side = side_of(r.get("direction") or r.get("over_under") or r.get("side"))
            if side is None:
                continue
            pick = pick_of(r.get("pick_type"))
            book = book_of(pick, side)
            if book not in BOOKS:
                continue
            line = num(r.get("line"))
            key = (norm_player(r.get("player")), prop, pick, line, side)
            s8 = idx.get(key) or {}
            day_n += 1
            if s8:
                day_hit += 1
                n_join += 1
            rec = {
                "date": date,
                "player": r.get("player"),
                "player_norm": key[0],
                "prop": prop,
                "book": book,
                "pick": pick,
                "side": side,
                "line": line,
                "hit": 1 if res == "HIT" else 0,
                "l5_over": first_num(r.get("l5_over"), r.get("last5_over"), s8.get("l5_over")),
                "l5_under": first_num(r.get("l5_under"), r.get("last5_under"), s8.get("l5_under")),
                "l10_over": first_num(
                    r.get("l10_over"), r.get("line_hits_over_10"), s8.get("l10_over")
                ),
                "l10_under": first_num(
                    r.get("l10_under"), r.get("line_hits_under_10"), s8.get("l10_under")
                ),
                "def_tier": r.get("def_tier") or r.get("def") or s8.get("def_tier") or "",
                "cover": first_num(r.get("cover"), r.get("dist_l5"), s8.get("cover")),
                "joined": bool(s8),
            }
            if l5_dir(rec, side) is not None:
                n_l5 += 1
            if l10_dir(rec, side) is not None:
                n_l10 += 1
            rec["l5"] = l5_dir(rec, side)
            rec["l10"] = l10_dir(rec, side)
            rec["d"] = d_ok(side, rec["def_tier"])
            rec["live"] = live_ok(rec)
            rows.append(rec)
        print(f"{date} cfb_std/gob={day_n} join={day_hit} step8_keys={len(idx)} file={gpath.name}")

    unique = unique_dedupe(rows)
    meta = {
        "dates": meta_dates,
        "variants": len(rows),
        "unique": len(unique),
        "joined": n_join,
        "has_l5": n_l5,
        "has_l10": n_l10,
        "join_pct": round(100 * n_join / max(len(rows), 1), 1),
        "l5_pct": round(100 * n_l5 / max(len(rows), 1), 1),
    }
    print("META", json.dumps(meta, indent=2))

    props = sorted({r["prop"] for r in rows}, key=lambda p: (ORDER.index(p) if p in ORDER else 99, p))

    def dump(pool, title):
        print(f"\n{title}")
        print(
            f"{'chip':22} {'book':16} {'ungated':16} {'L5=5':16} {'L5=5+L10':16} "
            f"{'L5=5+D':16} {'L5=5+L10+D':16} {'live':16}"
        )
        table = []
        for prop in props:
            for book in BOOKS:
                sub = [r for r in pool if r["prop"] == prop and r["book"] == book]
                if not sub:
                    continue
                ung = cell(sub, lambda r: True)
                l5 = cell(sub, lambda r: r["l5"] == 5)
                l5l10 = cell(sub, lambda r: r["l5"] == 5 and r["l10"] is not None and r["l10"] >= 8)
                l5d = cell(sub, lambda r: r["l5"] == 5 and r["d"])
                stack = cell(
                    sub,
                    lambda r: r["l5"] == 5 and r["l10"] is not None and r["l10"] >= 8 and r["d"],
                )
                live = cell(sub, lambda r: r["live"])
                label = LABEL.get(prop) or display_prop(prop, "CFB")
                table.append(
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
                    f"{label:22} {book:16} {fmt(*ung):16} {fmt(*l5):16} {fmt(*l5l10):16} "
                    f"{fmt(*l5d):16} {fmt(*stack):16} {fmt(*live):16}"
                )
        return table

    variants = dump(rows, "LINE VARIANTS")
    unique_t = dump(unique, "UNIQUE PLAYER-GAME")
    out = ROOT / "data" / "reports" / "cfb_l5eq5_gates_recut.json"
    out.write_text(
        json.dumps({"meta": meta, "variants": variants, "unique": unique_t}, indent=2),
        encoding="utf-8",
    )
    print("wrote", out)


if __name__ == "__main__":
    main()
