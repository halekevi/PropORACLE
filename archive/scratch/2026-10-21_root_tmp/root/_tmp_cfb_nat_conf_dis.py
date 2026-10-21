"""Nat vs conf rank disagreement diagnostic on keep-gate L5=5 cells.

Not a new cut. Flags games where national band != conference band on
own Off and/or opp D, then checks whether misses cluster there.
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(r"H:\PropORACLE")
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

import _tmp_cfb_l5_recut as R  # noqa: E402
from _tmp_cfb_od_lift import (  # noqa: E402
    AXIS_COLS,
    band,
    load_ranks,
    load_step8_teams,
    lookup_team,
    pack,
    tier_of,
)
from utils.cfb_prop_defense import prop_def_axis  # noqa: E402
from utils.defense_tiers import normalize_def_tier_label  # noqa: E402

OUT = ROOT / "data" / "reports" / "cfb_nat_conf_disagreement.json"

# Keep-gate (+ Rush UNDER keep, Rec UNDER faded control, Pass/Rec Goblin for miss pool)
CHIPS = [
    ("rush_yds", "Goblin OVER"),
    ("pass_yds", "Goblin OVER"),
    ("rec_yds", "Goblin OVER"),
    ("rush_yds", "Standard UNDER"),
    ("rec_yds", "Standard UNDER"),
    ("pass_yds", "Standard UNDER"),
]

CONF_AXIS = {
    "pass": ("off_pass_rank_conf", "def_pass_rank_conf"),
    "rush": ("off_rush_rank_conf", "def_rush_rank_conf"),
    "points": ("off_points_rank_conf", "def_points_rank_conf"),
    "td": ("off_td_rank_conf", "def_td_rank_conf"),
    "pass_td": ("off_pass_td_rank_conf", "def_pass_td_rank_conf"),
    "rush_td": ("off_rush_td_rank_conf", "def_rush_td_rank_conf"),
    "rec_td": ("off_rec_td_rank_conf", "def_rec_td_rank_conf"),
    "kick": ("off_kick_pts_rank_conf", "def_kick_pts_rank_conf"),
    "fg": ("off_fg_rank_conf", "def_fg_rank_conf"),
    "pat": ("off_pat_rank_conf", "def_pat_rank_conf"),
    "int": ("off_int_rank_conf", "def_int_rank_conf"),
}


def collapse_stamp_dupes(pool: list[dict]) -> list[dict]:
    groups: dict[tuple, list] = defaultdict(list)
    for r in pool:
        groups[(r["player_norm"], r["prop"], r["book"], r["side"])].append(r)
    out = []
    for g in groups.values():
        g = sorted(g, key=lambda x: x["date"])
        kept = []
        for r in g:
            if kept and abs((r.get("line") or 0) - (kept[-1].get("line") or 0)) <= 1.0:
                kept[-1] = r
            else:
                kept.append(r)
        out.extend(kept)
    return out


def disagree_kind(nat_b: str, conf_b: str) -> str:
    """Return disagreement shape, or 'agree' / 'blank'."""
    if nat_b == "blank" or conf_b == "blank":
        return "blank"
    if nat_b == conf_b:
        return "agree"
    # Direction of disagreement
    order = {"strong": 2, "avg": 1, "weak": 0}
    if nat_b not in order or conf_b not in order:
        return "disagree_other"
    if order[nat_b] > order[conf_b]:
        return "nat_stronger"  # looks better nationally than in-conference
    return "conf_stronger"  # looks better in-conference than nationally


def collect() -> list[dict]:
    ranks, n_fbs = load_ranks()
    rows = []
    for date in R.TAPE:
        gpath = R.graded_for(date)
        spath = R.find_step8(date)
        if not gpath or not spath:
            continue
        idx = load_step8_teams(spath)
        raw = json.loads(gpath.read_text(encoding="utf-8"))
        import prop_hit_tiers as T

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
            l5_over = R.first_num(r.get("l5_over"), r.get("last5_over"), s8.get("l5_over"))
            l5_under = R.first_num(r.get("l5_under"), r.get("last5_under"), s8.get("l5_under"))
            l5 = l5_over if side == "OVER" else l5_under
            if l5 != 5:
                continue
            team = s8.get("team") or r.get("team") or ""
            opp = s8.get("opp") or r.get("opp_team") or ""
            axis = prop_def_axis(prop)
            off_c, def_c = AXIS_COLS.get(axis, AXIS_COLS["points"])
            off_cc, def_cc = CONF_AXIS.get(axis, CONF_AXIS["points"])
            own = lookup_team(ranks, team)
            opp_row = lookup_team(ranks, opp)
            conf_sz = int(R.num(own.get("conference_size")) or 0) if own else 0
            opp_conf_sz = int(R.num(opp_row.get("conference_size")) or 0) if opp_row else 0
            own_off_nat = tier_of(own, off_c, n_fbs)
            own_off_conf = tier_of(own, off_cc, conf_sz) if conf_sz else ""
            opp_def_nat = tier_of(opp_row, def_c, n_fbs)
            opp_def_conf = tier_of(opp_row, def_cc, opp_conf_sz) if opp_conf_sz else ""
            own_off_nb = band(own_off_nat)
            own_off_cb = band(own_off_conf)
            opp_def_nb = band(opp_def_nat)
            opp_def_cb = band(opp_def_conf)
            rec = {
                "date": date,
                "player": s8.get("player") or r.get("player") or "",
                "player_norm": key[0],
                "prop": prop,
                "book": book,
                "side": side,
                "line": line,
                "hit": 1 if res == "HIT" else 0,
                "team": team,
                "opp": opp,
                "axis": axis,
                "own_conf": str(own.get("conference_name") or "") if own else "",
                "opp_conf": str(opp_row.get("conference_name") or "") if opp_row else "",
                "own_off_nat": own_off_nat,
                "own_off_conf": own_off_conf,
                "own_off_nb": own_off_nb,
                "own_off_cb": own_off_cb,
                "own_off_nat_rk": R.num(own.get(off_c)) if own else None,
                "own_off_conf_rk": R.num(own.get(off_cc)) if own else None,
                "opp_def_nat": opp_def_nat,
                "opp_def_conf": opp_def_conf,
                "opp_def_nb": opp_def_nb,
                "opp_def_cb": opp_def_cb,
                "opp_def_nat_rk": R.num(opp_row.get(def_c)) if opp_row else None,
                "opp_def_conf_rk": R.num(opp_row.get(def_cc)) if opp_row else None,
                "own_off_dis": disagree_kind(own_off_nb, own_off_cb),
                "opp_def_dis": disagree_kind(opp_def_nb, opp_def_cb),
            }
            # Any meaningful disagreement on either side for this prop's axes
            sides = []
            if rec["own_off_dis"] not in {"agree", "blank"}:
                sides.append(f"ownOff:{rec['own_off_dis']}")
            if rec["opp_def_dis"] not in {"agree", "blank"}:
                sides.append(f"oppD:{rec['opp_def_dis']}")
            rec["dis_any"] = bool(sides)
            rec["dis_label"] = "+".join(sides) if sides else "agree"
            # Cross-conference matchup (conf ranks not comparable across leagues)
            rec["cross_conf"] = bool(
                rec["own_conf"] and rec["opp_conf"] and rec["own_conf"] != rec["opp_conf"]
            )
            rows.append(rec)
    unique = R.unique_dedupe(rows)
    return collapse_stamp_dupes(unique)


def hn(d: dict) -> str:
    if not d["n"]:
        return "empty"
    return f"{d['hr']:.1f}% {d['h']}/{d['n']}"


def main() -> None:
    pool = collect()
    print(f"keep-chip L5=5 unique (stamp-collapsed) n={len(pool)}")
    report = {"chips": [], "misses_disagreement": [], "hits_disagreement": []}

    for prop, book in CHIPS:
        sub = [r for r in pool if r["prop"] == prop and r["book"] == book]
        if not sub:
            continue
        base = pack("L5=5", sub)
        agree = pack("agree both axes", [r for r in sub if not r["dis_any"]])
        dis = pack("any disagreement", [r for r in sub if r["dis_any"]])
        own_dis = pack("own Off disagrees", [r for r in sub if r["own_off_dis"] not in {"agree", "blank"}])
        opp_dis = pack("opp D disagrees", [r for r in sub if r["opp_def_dis"] not in {"agree", "blank"}])
        nat_str = pack("own Off nat_stronger", [r for r in sub if r["own_off_dis"] == "nat_stronger"])
        conf_str = pack("own Off conf_stronger", [r for r in sub if r["own_off_dis"] == "conf_stronger"])
        d_nat = pack("opp D nat_stronger", [r for r in sub if r["opp_def_dis"] == "nat_stronger"])
        d_conf = pack("opp D conf_stronger", [r for r in sub if r["opp_def_dis"] == "conf_stronger"])
        cross = pack("cross-conference game", [r for r in sub if r["cross_conf"]])
        same = pack("same-conference game", [r for r in sub if not r["cross_conf"]])

        label = R.LABEL.get(prop, prop)
        print(f"\n==== {label} {book} n={len(sub)} base {hn(base)} ====")
        for c in (agree, dis, own_dis, opp_dis, nat_str, conf_str, d_nat, d_conf, same, cross):
            print(f"  {c['label']:32} {hn(c)}")

        misses = [r for r in sub if not r["hit"]]
        hits = [r for r in sub if r["hit"]]
        miss_dis = sum(1 for r in misses if r["dis_any"])
        hit_dis = sum(1 for r in hits if r["dis_any"])
        print(
            f"  misses in disagreement: {miss_dis}/{len(misses)}  "
            f"hits in disagreement: {hit_dis}/{len(hits)}"
        )

        chip = {
            "prop": prop,
            "book": book,
            "label": label,
            "base": base,
            "agree": agree,
            "disagree": dis,
            "own_off_dis": own_dis,
            "opp_def_dis": opp_dis,
            "miss_n": len(misses),
            "miss_dis_n": miss_dis,
            "hit_n": len(hits),
            "hit_dis_n": hit_dis,
            "miss_roster": [],
            "dis_roster": [],
        }
        for r in sorted(sub, key=lambda x: (x["date"], str(x.get("player")))):
            if not r["hit"] or r["dis_any"]:
                entry = {
                    "date": r["date"],
                    "player": r["player"],
                    "team": r["team"],
                    "opp": r["opp"],
                    "line": r["line"],
                    "hit": r["hit"],
                    "own_off": f"{r['own_off_nat']}/{r['own_off_conf']} ({r['own_off_nb']}/{r['own_off_cb']})",
                    "opp_def": f"{r['opp_def_nat']}/{r['opp_def_conf']} ({r['opp_def_nb']}/{r['opp_def_cb']})",
                    "own_off_rk": f"nat={r['own_off_nat_rk']} conf={r['own_off_conf_rk']}",
                    "opp_def_rk": f"nat={r['opp_def_nat_rk']} conf={r['opp_def_conf_rk']}",
                    "dis": r["dis_label"],
                    "cross_conf": r["cross_conf"],
                    "own_conf": r["own_conf"],
                    "opp_conf": r["opp_conf"],
                }
                if not r["hit"]:
                    chip["miss_roster"].append(entry)
                    report["misses_disagreement"].append({**entry, "chip": f"{label} {book}"})
                if r["dis_any"]:
                    chip["dis_roster"].append(entry)
                    if r["hit"]:
                        report["hits_disagreement"].append({**entry, "chip": f"{label} {book}"})
                mark = "HIT" if r["hit"] else "MISS"
                if not r["hit"] or r["dis_any"]:
                    print(
                        f"  {r['date']} {mark:4} {(r['player'] or '')[:20]:20} "
                        f"Off {r['own_off_nb']}/{r['own_off_cb']} "
                        f"D {r['opp_def_nb']}/{r['opp_def_cb']} "
                        f"[{r['dis_label']}]"
                    )
        report["chips"].append(chip)

    OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print("wrote", OUT)


if __name__ == "__main__":
    main()
