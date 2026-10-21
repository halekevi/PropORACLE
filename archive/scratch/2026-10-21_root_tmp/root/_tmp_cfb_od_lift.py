"""CFB O/D lift on top of L5=5 gates. Observe-only. Does not change keep-gates."""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

ROOT = Path(r"H:\PropORACLE")
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

import _tmp_cfb_l5_recut as R  # noqa: E402
from utils.cfb_playoff_metadata import cfb_abbr_lookup_keys  # noqa: E402
from utils.cfb_prop_defense import prop_def_axis  # noqa: E402
from utils.cfb_unit_line_ranks import percentile_tier  # noqa: E402
from utils.defense_tiers import normalize_def_tier_label  # noqa: E402

RANK_CSV = ROOT / "Sports" / "CFB" / "data" / "reference" / "cfb_team_unit_rankings.csv"
OUT = ROOT / "data" / "reports" / "cfb_l5eq5_od_lift.json"

AXIS_COLS = {
    "pass": ("off_pass_rank", "def_pass_rank"),
    "rush": ("off_rush_rank", "def_rush_rank"),
    "points": ("off_points_rank", "def_points_rank"),
    "td": ("off_td_rank", "def_td_rank"),
    "pass_td": ("off_pass_td_rank", "def_pass_td_rank"),
    "rush_td": ("off_rush_td_rank", "def_rush_td_rank"),
    "rec_td": ("off_rec_td_rank", "def_rec_td_rank"),
    "kick": ("off_kick_pts_rank", "def_kick_pts_rank"),
    "fg": ("off_fg_rank", "def_fg_rank"),
    "pat": ("off_pat_rank", "def_pat_rank"),
    "int": ("off_int_rank", "def_int_rank"),
}

STRONG = {"Elite", "Above Avg", "Above"}
WEAK = {"Weak", "Below Avg", "Below"}
FOCUS = [
    ("rush_yds", "Goblin OVER"),
    ("pass_yds", "Goblin OVER"),
    ("rec_yds", "Goblin OVER"),
    ("rec", "Goblin OVER"),
    ("pass_td", "Goblin OVER"),
    ("rush_yds", "Standard UNDER"),
    ("rec_yds", "Standard UNDER"),
    ("pass_yds", "Standard UNDER"),
]


def band(t: str) -> str:
    t = normalize_def_tier_label(t) or str(t or "").strip()
    if t in STRONG or t in {"Elite", "Above Avg"}:
        return "strong"
    if t in WEAK or t in {"Weak", "Below Avg"}:
        return "weak"
    if t in {"Avg"}:
        return "avg"
    if t in {"Above"}:
        return "strong"
    if t in {"Below"}:
        return "weak"
    return "blank"


def load_ranks() -> tuple[dict[str, dict], int]:
    df = pd.read_csv(RANK_CSV)
    n = int(df["team_abbr"].nunique())
    by: dict[str, dict] = {}
    for rec in df.to_dict("records"):
        abbr = str(rec.get("team_abbr") or "").strip().upper()
        if not abbr:
            continue
        by[abbr] = rec
        for k in cfb_abbr_lookup_keys(abbr):
            by.setdefault(k, rec)
    return by, n


def lookup_team(by: dict, raw) -> dict | None:
    s = str(raw or "").strip().upper()
    if not s:
        return None
    if s in by:
        return by[s]
    for k in cfb_abbr_lookup_keys(s):
        if k in by:
            return by[k]
    return None


def tier_of(row: dict | None, col: str, n: int) -> str:
    if not row:
        return ""
    return percentile_tier(row.get(col), n)


def load_step8_teams(path: Path) -> dict:
    idx = R.load_step8(path)
    df = pd.read_excel(path)
    cm = {str(c).strip().lower(): c for c in df.columns}

    def col(*names):
        for n in names:
            if n.lower() in cm:
                return cm[n.lower()]
        return None

    import prop_hit_tiers as T

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
    c_team = col("team", "Team")
    c_opp = col("opp", "Opp", "opp_team")
    c_share = col("rush share", "Rush Share", "rush_att_share")
    c_player_disp = c_player
    for row in df.to_dict("records"):
        prop = T.canon_prop("CFB", str(row.get(c_prop) or ""))
        if not prop:
            continue
        side = R.side_of(row.get(c_dir) if c_dir else None)
        if side is None:
            continue
        pick = R.pick_of(row.get(c_pick) if c_pick else "")
        line = R.num(row.get(c_line) if c_line else None)
        key = (R.norm_player(row.get(c_player) if c_player else None), prop, pick, line, side)
        rec = idx.get(key)
        if rec is None:
            continue
        rec["team"] = row.get(c_team) if c_team else ""
        rec["opp"] = row.get(c_opp) if c_opp else ""
        rec["player"] = row.get(c_player_disp) if c_player_disp else ""
        share_raw = row.get(c_share) if c_share else None
        rec["rush_share_step8"] = R.num(share_raw)
    return idx


def collect_rows() -> tuple[list[dict], int]:
    import prop_hit_tiers as T

    ranks, n_fbs = load_ranks()
    rows = []
    for date in R.TAPE:
        gpath = R.graded_for(date)
        spath = R.find_step8(date)
        if not gpath or not spath:
            continue
        idx = load_step8_teams(spath)
        raw = json.loads(gpath.read_text(encoding="utf-8"))
        for r in raw.get("props") or []:
            if not isinstance(r, dict) or not R.is_cfb(r):
                continue
            res = str(r.get("result") or "").upper()
            if res not in {"HIT", "MISS"}:
                continue
            prop = T.canon_prop("CFB", str(r.get("prop") or r.get("prop_type") or ""))
            if not prop:
                continue
            side = R.side_of(r.get("direction") or r.get("over_under") or r.get("side"))
            if side is None:
                continue
            pick = R.pick_of(r.get("pick_type"))
            book = R.book_of(pick, side)
            if book not in R.BOOKS:
                continue
            line = R.num(r.get("line"))
            key = (R.norm_player(r.get("player")), prop, pick, line, side)
            s8 = idx.get(key) or {}
            rec = {
                "date": date,
                "player_norm": key[0],
                "prop": prop,
                "book": book,
                "side": side,
                "line": line,
                "hit": 1 if res == "HIT" else 0,
                "l5": None,
                "l10": None,
                "team": s8.get("team") or r.get("team") or "",
                "opp": s8.get("opp") or r.get("opp_team") or "",
                "player": s8.get("player") or r.get("player") or "",
                "rush_share_step8": s8.get("rush_share_step8"),
                "board_d": normalize_def_tier_label(
                    r.get("def_tier") or r.get("def") or s8.get("def_tier") or ""
                ),
            }
            rec["l5_over"] = R.first_num(r.get("l5_over"), r.get("last5_over"), s8.get("l5_over"))
            rec["l5_under"] = R.first_num(r.get("l5_under"), r.get("last5_under"), s8.get("l5_under"))
            rec["l10_over"] = R.first_num(
                r.get("l10_over"), r.get("line_hits_over_10"), s8.get("l10_over")
            )
            rec["l10_under"] = R.first_num(
                r.get("l10_under"), r.get("line_hits_under_10"), s8.get("l10_under")
            )
            rec["l5"] = rec["l5_over"] if side == "OVER" else rec["l5_under"]
            rec["l10"] = rec["l10_over"] if side == "OVER" else rec["l10_under"]
            axis = prop_def_axis(prop)
            off_c, def_c = AXIS_COLS.get(axis, AXIS_COLS["points"])
            own = lookup_team(ranks, rec["team"])
            opp = lookup_team(ranks, rec["opp"])
            rec["axis"] = axis
            rec["own_off"] = tier_of(own, off_c, n_fbs)
            rec["opp_def"] = tier_of(opp, def_c, n_fbs)
            rec["own_def"] = tier_of(own, def_c, n_fbs)
            rec["opp_off"] = tier_of(opp, off_c, n_fbs)
            rec["own_off_b"] = band(rec["own_off"])
            rec["opp_def_b"] = band(rec["opp_def"])
            rec["own_def_b"] = band(rec["own_def"])
            rec["opp_off_b"] = band(rec["opp_off"])
            rec["board_d_b"] = band(rec["board_d"])
            rows.append(rec)
    return R.unique_dedupe(rows), n_fbs


def pack(label, pool) -> dict:
    n = len(pool)
    h = sum(r["hit"] for r in pool)
    return {
        "label": label,
        "h": h,
        "n": n,
        "hr": round(100 * h / n, 1) if n else None,
    }


def lift(cell, base) -> float | None:
    if not cell["n"] or base.get("hr") is None or cell.get("hr") is None:
        return None
    return round(cell["hr"] - base["hr"], 1)


def main() -> None:
    unique, n_fbs = collect_rows()
    print("unique", len(unique), "FBS", n_fbs)
    print(
        "own_off filled",
        sum(1 for r in unique if r["own_off_b"] != "blank"),
        "opp_def filled",
        sum(1 for r in unique if r["opp_def_b"] != "blank"),
        "board_d filled",
        sum(1 for r in unique if r["board_d_b"] != "blank"),
    )
    report = {"n_fbs": n_fbs, "unique": len(unique), "chips": []}
    seen = set(FOCUS)
    extra = []
    by = defaultdict(list)
    for r in unique:
        by[(r["prop"], r["book"])].append(r)
    for (prop, book), pool in sorted(by.items()):
        if (prop, book) in seen:
            continue
        if sum(1 for r in pool if r["l5"] == 5) >= 8:
            extra.append((prop, book))
    chips = list(FOCUS) + extra
    print("chips", chips)
    for prop, book in chips:
        pool = [r for r in unique if r["prop"] == prop and r["book"] == book]
        l5 = [r for r in pool if r["l5"] == 5]
        l5l10 = [r for r in l5 if r["l10"] is not None and r["l10"] >= 8]
        over = book.endswith("OVER")
        want_d = "weak" if over else "strong"  # leaky D for OVER, stingy for UNDER
        want_off = "strong" if over else "weak"
        base = pack("L5=5", l5)
        cuts = [
            pack("ungated", pool),
            base,
            pack("L5=5+L10>=8", l5l10),
            pack("L5=5 + board D aligned", [r for r in l5 if r["board_d_b"] == want_d]),
            pack("L5=5 + opp D aligned (CSV)", [r for r in l5 if r["opp_def_b"] == want_d]),
            pack("L5=5 + own Off aligned", [r for r in l5 if r["own_off_b"] == want_off]),
            pack(
                "L5=5 + own Off x opp D aligned",
                [r for r in l5 if r["own_off_b"] == want_off and r["opp_def_b"] == want_d],
            ),
            pack(
                "L5=5+L10 + own Off x opp D",
                [
                    r
                    for r in l5l10
                    if r["own_off_b"] == want_off and r["opp_def_b"] == want_d
                ],
            ),
            pack("L5=5 + own Off opposite", [r for r in l5 if r["own_off_b"] == ("weak" if want_off == "strong" else "strong")]),
            pack("L5=5 + opp D opposite", [r for r in l5 if r["opp_def_b"] == ("strong" if want_d == "weak" else "weak")]),
            pack("L5=5 + opp Off strong (script)", [r for r in l5 if r["opp_off_b"] == "strong"]),
            pack("L5=5 + own D leaky", [r for r in l5 if r["own_def_b"] == "weak"]),
        ]
        label = R.LABEL.get(prop, prop)
        print(f"\n==== {label} {book} unique n={len(pool)} L5=5 n={len(l5)} ====")
        for c in cuts:
            lf = lift(c, base) if c["label"] != "L5=5" else 0.0
            extra = f"  lift {lf:+.1f}" if lf is not None and c["label"] != "L5=5" else ""
            hr = f"{c['hr']:.1f}%" if c["hr"] is not None else "—"
            print(f"  {c['label']:36} {hr:8} {c['h']}/{c['n']}{extra}")
        # 3x3 own Off × opp D on L5=5
        grid = []
        for ob in ("strong", "avg", "weak"):
            for db in ("weak", "avg", "strong"):
                sub = [r for r in l5 if r["own_off_b"] == ob and r["opp_def_b"] == db]
                grid.append(pack(f"own Off {ob} x opp D {db}", sub))
        report["chips"].append(
            {
                "prop": prop,
                "label": label,
                "book": book,
                "axis": pool[0]["axis"] if pool else "",
                "cuts": [{**c, "lift_vs_l5eq5": lift(c, base)} for c in cuts],
                "grid": grid,
            }
        )
    OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print("wrote", OUT)


if __name__ == "__main__":
    main()
