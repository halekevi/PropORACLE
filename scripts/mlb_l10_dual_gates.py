"""MLB Goblin OVER: L10>=8 stacked on recency, then own + opponent rank both-align.

  py -3.14 scripts/mlb_l10_dual_gates.py
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path

OPEN = Path(r"H:\PropORACLE")
MAIN = Path(r"H:\PropORACLE_main_cp")
sys.path[:0] = [str(OPEN), str(OPEN / "scripts")]

import all_ranked_prop_hit_rates as C  # noqa: E402
import mlb_counting_gate_grid as G  # noqa: E402
import prop_hit_tiers as T  # noqa: E402
from utils.defense_tiers import normalize_def_tier_label  # noqa: E402
from utils.mlb_prop_matchup import pitcher_offense_axis  # noqa: E402

OUT = OPEN / "data" / "reports" / "mlb_l10_dual_gates.json"
PROD = frozenset({"Weak", "Below Avg"})
STINGY = frozenset({"Elite", "Above Avg"})
PITCHER = frozenset({
    "hits_allowed",
    "earned_runs",
    "pitcher_ks",
    "walks_allowed",
    "pitches_thrown",
    "pitching_outs",
    "first_inning_walks",
    "first_inning_runs",
})
DISPLAY = {
    "walks_allowed": "Walks Allowed",
    "pitches_thrown": "Pitches Thrown",
    "earned_runs": "Earned Runs Allowed",
    "pitcher_ks": "Pitcher Strikeouts",
    "hits_allowed": "Hits Allowed",
    "pitching_outs": "Pitching Outs",
    "hits+runs+rbis": "Hits+Runs+RBIs",
    "hitter_ks": "Hitter Strikeouts",
    "hits": "Hits",
    "total_bases": "Total Bases",
    "runs": "Runs",
    "singles": "Singles",
    "walks": "Walks (batter)",
    "plate_appearances": "Plate Appearances",
    "rbis": "RBIs",
    "first_inning_walks": "1st Inning Walks Allowed",
    "first_inning_runs": "1st Inning Runs Allowed",
}
ORDER = [
    "walks_allowed",
    "pitches_thrown",
    "earned_runs",
    "pitcher_ks",
    "hits_allowed",
    "pitching_outs",
    "hits+runs+rbis",
    "hitter_ks",
    "hits",
    "total_bases",
    "runs",
    "singles",
    "walks",
    "plate_appearances",
    "rbis",
    "first_inning_walks",
    "first_inning_runs",
]


def pct(h, n):
    if n <= 0:
        return None
    return round(100.0 * h / n, 1)


def cell():
    return {"n": 0, "h": 0}


def add(c, hit):
    c["n"] += 1
    c["h"] += hit


def rate(c):
    return {"n": c["n"], "hits": c["h"], "hr": pct(c["h"], c["n"])}


def load_step8_over(path) -> dict[tuple, tuple]:
    import pandas as pd

    df = pd.read_csv(path, low_memory=False)
    out: dict[tuple, tuple] = {}
    for row in df.to_dict("records"):
        prop = T.canon_prop("MLB", str(row.get("prop_type") or row.get("prop_norm") or ""))
        if not prop:
            continue
        if G._pick(row.get("pick_type")) != "Goblin":
            continue
        side = G._side(row.get("bet_direction") or row.get("final_bet_direction") or row.get("direction"))
        if side != "OVER":
            continue
        l5 = C.num(row.get("last5_over") if row.get("last5_over") not in (None, "") else row.get("line_hits_over_5"))
        l10 = C.num(row.get("l10_over") if row.get("l10_over") not in (None, "") else row.get("line_hits_over_10"))
        key = (
            G._norm_player(row.get("player") or row.get("player_name")),
            prop,
            G._num(row.get("line")),
        )
        prev = out.get(key)
        if prev is None:
            out[key] = (l5, l10)
        else:
            a, b = prev
            out[key] = (a if a is not None else l5, b if b is not None else l10)
    return out


def load_def():
    import pandas as pd

    d = pd.read_csv(OPEN / "Sports" / "MLB" / "mlb_defense_summary.csv")
    d["TEAM_ABBREVIATION"] = d["TEAM_ABBREVIATION"].astype(str).str.strip().str.upper()
    alias = {"ARI": "AZ", "CHW": "CWS", "OAK": "ATH", "WSN": "WSH", "WAS": "WSH"}
    out = {}
    for r in d.to_dict("records"):
        rec = {
            "pitch": normalize_def_tier_label(r.get("DEF_TIER") or r.get("def_tier")),
            "hits": normalize_def_tier_label(r.get("OFF_HITS_TIER")),
            "runs": normalize_def_tier_label(r.get("OFF_RUNS_TIER")),
            "so": normalize_def_tier_label(r.get("OFF_SO_TIER")),
        }
        ab = str(r["TEAM_ABBREVIATION"]).upper()
        out[ab] = rec
        for a, b in alias.items():
            if ab == b:
                out[a] = rec
            if ab == a:
                out[b] = rec
    return out


def own_opp_ok(canon: str, own: dict | None, opp: dict | None) -> tuple[bool, bool]:
    if not own or not opp:
        return False, False
    if canon == "hitter_ks":
        return own["so"] in PROD, opp["pitch"] in STINGY
    if canon in PITCHER:
        axis = pitcher_offense_axis(canon, "pitcher")
        if axis == "opp_offense_hits" or canon in ("walks_allowed", "pitches_thrown", "first_inning_walks"):
            opp_ok = opp["hits"] in PROD
        elif axis == "opp_offense_runs" or canon == "first_inning_runs":
            opp_ok = opp["runs"] in PROD
        elif axis == "opp_offense_ks":
            opp_ok = opp["so"] in PROD
        else:
            opp_ok = opp["hits"] in PROD
        if canon in ("pitcher_ks", "pitching_outs"):
            own_ok = own["pitch"] in STINGY
        else:
            own_ok = own["pitch"] in PROD
        return own_ok, opp_ok
    # hitter counting
    return own["hits"] in PROD, opp["pitch"] in PROD


def bucket(own_ok: bool, opp_ok: bool) -> str:
    if own_ok and opp_ok:
        return "both"
    if opp_ok:
        return "opp_only"
    if own_ok:
        return "own_only"
    return "neither"


def catalog_joints():
    p = json.loads((OPEN / "data" / "reports" / "all_ranked_prop_hit_rates.json").read_text(encoding="utf-8"))
    out = {}
    for r in p["rows"]:
        if r.get("sport") != "MLB" or r.get("book") != "Goblin OVER":
            continue
        out[r["prop"]] = {
            "ungated": {"n": r["n"], "hr": None if r.get("hr") is None else round(100 * r["hr"], 1)},
            "l5ge4": {"n": r.get("listed_n"), "hr": None if r.get("listed_hr") is None else round(100 * r["listed_hr"], 1)},
            "l5eq5": {"n": r.get("l5eq5_n"), "hr": None if r.get("l5eq5_hr") is None else round(100 * r["l5eq5_hr"], 1)},
            "l10ge8": {"n": r.get("l10ge8_n"), "hr": None if r.get("l10ge8_hr") is None else round(100 * r["l10ge8_hr"], 1)},
            "l5ge4_l10ge8": {
                "n": r.get("l5ge4_l10ge8_n"),
                "hr": None if r.get("l5ge4_l10ge8_hr") is None else round(100 * r["l5ge4_l10ge8_hr"], 1),
            },
            "l5eq5_l10ge8": {
                "n": r.get("l5eq5_l10ge8_n"),
                "hr": None if r.get("l5eq5_l10ge8_hr") is None else round(100 * r["l5eq5_l10ge8_hr"], 1),
            },
            "l5ge4_l10ge8_d": {
                "n": r.get("l5ge4_l10ge8_d_n"),
                "hr": None if r.get("l5ge4_l10ge8_d_hr") is None else round(100 * r["l5ge4_l10ge8_d_hr"], 1),
            },
            "l5eq5_l10ge8_d": {
                "n": r.get("l5eq5_l10ge8_d_n"),
                "hr": None if r.get("l5eq5_l10ge8_d_hr") is None else round(100 * r["l5eq5_l10ge8_d_hr"], 1),
            },
        }
    return out


def main() -> None:
    defs = load_def()
    # prop -> recency -> rank_bucket -> cell
    acc: dict[str, dict[str, dict[str, dict]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(cell))
    )
    era05: dict[str, dict[str, dict]] = defaultdict(lambda: defaultdict(cell))
    n_rows = 0
    miss = 0
    seen: set[str] = set()
    paths = sorted((MAIN / "ui_runner" / "templates").glob("graded_props_20*.json"))
    paths += sorted((OPEN / "ui_runner" / "templates").glob("graded_props_20*.json"))
    for path in paths:
        date = path.stem.replace("graded_props_", "")[:10]
        if date in seen:
            continue
        seen.add(date)
        step_path = G.find_step8_csv(date)
        step_idx = load_step8_over(step_path) if step_path else {}
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        for r in raw.get("props") or []:
            if T.norm_sport(str(r.get("sport") or "")) != "MLB":
                continue
            if str(r.get("result") or "").strip().upper() not in ("HIT", "MISS"):
                continue
            side = str(r.get("direction") or r.get("over_under") or "").strip().upper()
            if side in ("O", "OVER"):
                side = "OVER"
            else:
                continue
            if C.pick_book(str(r.get("pick_type") or ""), side) != "Goblin OVER":
                continue
            canon = T.canon_prop("MLB", str(r.get("prop") or r.get("prop_type") or ""))
            if not canon:
                continue
            hit = 1 if str(r.get("result")).upper() == "HIT" else 0
            n_rows += 1
            l5 = C.l5_dir(r, "OVER")
            l10 = C.l10_dir(r, "OVER")
            if l5 is None or l10 is None:
                s8 = step_idx.get(
                    (
                        G._norm_player(r.get("player")),
                        canon,
                        C.num(r.get("line")),
                    )
                )
                if s8:
                    if l5 is None:
                        l5 = s8[0]
                    if l10 is None:
                        l10 = s8[1]
            team = str(r.get("team") or "").strip().upper()
            opp = str(r.get("opp_team") or "").strip().upper()
            own = defs.get(team)
            opp_d = defs.get(opp)
            if not own or not opp_d:
                miss += 1
            own_ok, opp_ok = own_opp_ok(canon, own, opp_d)
            b = bucket(own_ok, opp_ok)
            rec_keys = ["all"]
            l5ge4 = l5 is not None and l5 >= 4
            l5eq5 = l5 is not None and l5 == 5
            l10ge8 = l10 is not None and l10 >= 8
            if l10ge8:
                rec_keys.append("l10ge8")
            if l5ge4:
                rec_keys.append("l5ge4")
            if l5ge4 and l10ge8:
                rec_keys.append("l5ge4_l10ge8")
            if l5eq5:
                rec_keys.append("l5eq5")
            if l5eq5 and l10ge8:
                rec_keys.append("l5eq5_l10ge8")
            for k in rec_keys:
                add(acc[canon][k]["all"], hit)
                add(acc[canon][k][b], hit)
            line = C.num(r.get("line"))
            if canon == "earned_runs" and line is not None and abs(line - 0.5) < 1e-9:
                for k in rec_keys:
                    add(era05[k]["all"], hit)
                    add(era05[k][b], hit)

    props_out = []
    for canon in ORDER + sorted(set(acc) - set(ORDER)):
        if canon not in acc:
            continue
        recs = {}
        for gk, buckets in acc[canon].items():
            recs[gk] = {bk: rate(c) for bk, c in buckets.items()}
        props_out.append(
            {
                "prop": canon,
                "display": DISPLAY.get(canon, canon),
                "gates": recs,
            }
        )

    report = {
        "source": "graded_props Goblin OVER + mlb_defense_summary.csv as of 2026-09-03",
        "n_decided": n_rows,
        "miss_team": miss,
        "n_dates": len(seen),
        "own_axis": "pitcher props: own pitching; hitter counting: own OFF_HITS; hitter Ks: own OFF_SO",
        "opp_axis": "pitcher HA/walks/pitches: opp OFF_HITS; ERA: opp OFF_RUNS; pitcher Ks: opp OFF_SO; hitters: opp pitching",
        "align_over": "PROD Weak|Below except hitter Ks opp pitching Elite|Above and pitcher Ks/outs own pitching Elite|Above",
        "catalog": catalog_joints(),
        "era_0p5": {gk: {bk: rate(c) for bk, c in bks.items()} for gk, bks in era05.items()},
        "props": props_out,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print("wrote", OUT, "n", n_rows, "dates", len(seen), "miss", miss)

    print(f"\n{'#':>3} {'prop':26} {'L5>=4+L10':>16} {'+both':>16} {'L5=5+L10':>16} {'+both':>16}")
    for i, p in enumerate(props_out, 1):
        g = p["gates"]

        def fmt(gate, bk="all"):
            c = g.get(gate, {}).get(bk) or {}
            if not c.get("n"):
                return "—"
            return f"{c['hr']}% n={c['n']}"

        print(
            f"{i:3} {p['display']:26} {fmt('l5ge4_l10ge8'):>16} {fmt('l5ge4_l10ge8','both'):>16} "
            f"{fmt('l5eq5_l10ge8'):>16} {fmt('l5eq5_l10ge8','both'):>16}"
        )
    print("\nERA 0.5")
    for gk in ("all", "l10ge8", "l5ge4", "l5ge4_l10ge8", "l5eq5", "l5eq5_l10ge8"):
        a = report["era_0p5"].get(gk, {}).get("all") or {}
        b = report["era_0p5"].get(gk, {}).get("both") or {}
        print(f"  {gk:16} all {a}  both {b}")


if __name__ == "__main__":
    main()
