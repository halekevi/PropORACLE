"""Grade 2026-09-20 listed keep-gate, reconstructed G70 pool, mixer tickets."""
from __future__ import annotations

import json
import sys
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

ROOT = Path(r"H:\PropORACLE")
MAIN = Path(r"H:\PropORACLE_main_cp")
DATE = "2026-09-20"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from rank_best_props_today import (  # noqa: E402
    _clears_list_gate,
    load_nfl,
    load_sport,
    recs,
)
from utils.prop_norm import canon_prop  # noqa: E402
from utils.ticket_70_pool import goblin_70_eligible, standard_ticket_eligible  # noqa: E402

OUT = ROOT / "_tmp_grade_sep20_out.json"


def pname(s: object) -> str:
    t = unicodedata.normalize("NFKD", str(s or ""))
    t = "".join(c for c in t if not unicodedata.combining(c))
    t = t.casefold().replace(".", " ").replace("'", "").replace("’", "")
    for tok in (" jr", " sr", " ii", " iii", " iv"):
        if t.endswith(tok):
            t = t[: -len(tok)]
    return " ".join(t.split())


def pflt(v) -> float | None:
    try:
        if v in (None, "", "nan"):
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def attach_start(rows: list[dict], df: pd.DataFrame) -> None:
    if df is None or df.empty:
        return
    for rec, (_, r) in zip(rows, df.iterrows()):
        rec["start_time"] = r.get("start_time")
        rec["game_date"] = r.get("game_date") if "game_date" in df.columns else None


def on_slate_et(rec: dict, date: str) -> bool:
    st = rec.get("start_time")
    if st not in (None, "", "nan"):
        ts = pd.to_datetime(st, errors="coerce", utc=True)
        if pd.notna(ts):
            return ts.tz_convert("America/New_York").strftime("%Y-%m-%d") == date
    gd = str(rec.get("game_date") or "")[:10]
    return gd == date or gd in ("", "nan")


def load_all() -> list[dict]:
    rows: list[dict] = []
    for sport, folder, fname in (
        ("WNBA", "wnba", "step8_wnba_direction.csv"),
        ("MLB", "mlb", "step8_mlb_direction.csv"),
        ("SOCCER", "soccer", "step8_soccer_direction.csv"),
        ("TENNIS", "tennis", "step8_tennis_direction.csv"),
    ):
        df = load_sport(MAIN, DATE, sport, folder, fname)
        print(f"load {sport} {0 if df is None else len(df)}")
        if df is None or df.empty:
            continue
        chunk = recs(df)
        attach_start(chunk, df)
        rows.extend(chunk)
    df_nfl = load_nfl(MAIN, DATE)
    print(f"load NFL step8 {0 if df_nfl is None else len(df_nfl)}")
    if df_nfl is None or df_nfl.empty:
        p = MAIN / "outputs" / DATE / "nfl" / "step5_nfl_with_stats.csv"
        if p.is_file():
            df_nfl = pd.read_csv(p, low_memory=False)
            df_nfl["sport"] = "NFL"
            if "model_dir" not in df_nfl.columns and "model_dir_5" in df_nfl.columns:
                df_nfl["model_dir"] = df_nfl["model_dir_5"]
            if "player" not in df_nfl.columns and "player_name" in df_nfl.columns:
                df_nfl["player"] = df_nfl["player_name"]
            print(f"load NFL step5 {len(df_nfl)}")
    if df_nfl is not None and not df_nfl.empty:
        chunk = recs(df_nfl)
        attach_start(chunk, df_nfl)
        rows.extend(chunk)
    return rows


def result_of(actual: float | None, line: float | None, side: str) -> str | None:
    if actual is None or line is None:
        return None
    if abs(actual - line) < 1e-9:
        return "PUSH"
    hit = actual > line if side == "OVER" else actual < line
    return "HIT" if hit else "MISS"


def load_grades() -> tuple[dict, dict]:
    gp = json.loads(
        (MAIN / "ui_runner" / "templates" / f"graded_props_{DATE}.json").read_text(
            encoding="utf-8"
        )
    )
    exact: dict[tuple, dict] = {}
    loose: dict[tuple, list] = defaultdict(list)
    for r in gp.get("props") or []:
        sport = str(r.get("sport") or "").strip().upper()
        if sport == "SOCCER":
            sport = "SOCCER"
        player = pname(r.get("player"))
        prop = canon_prop(sport, str(r.get("prop") or r.get("prop_type") or ""))
        line = pflt(r.get("line"))
        pt = str(r.get("pick_type") or "").strip().title()
        side = str(r.get("direction") or r.get("side") or "").strip().upper()
        res = str(r.get("result") or "").strip().upper()
        actual = pflt(r.get("actual_value") if r.get("actual_value") is not None else r.get("actual"))
        row = {"result": res, "actual": actual, "sport": sport, "prop": prop, "player": r.get("player")}
        exact[(sport, player, prop, line, pt, side)] = row
        loose[(sport, player, prop, pt, side)].append(row)
        loose[(sport, player, prop)].append(row)
    return exact, loose


def load_soccer_actuals() -> dict[tuple, float]:
    p = MAIN / "outputs" / DATE / f"actuals_soccer_{DATE}.csv"
    out: dict[tuple, float] = {}
    if not p.is_file():
        return out
    df = pd.read_csv(p)
    for _, r in df.iterrows():
        player = pname(r.get("player"))
        prop = canon_prop("SOCCER", str(r.get("prop_type") or ""))
        act = pflt(r.get("actual"))
        if act is None:
            continue
        out[(player, prop)] = act
    return out


def load_nfl_actuals() -> dict[tuple, float]:
    p = MAIN / "outputs" / DATE / f"actuals_nfl_{DATE}.csv"
    out: dict[tuple, float] = {}
    if not p.is_file():
        return out
    df = pd.read_csv(p)
    for _, r in df.iterrows():
        player = pname(r.get("player"))
        prop = canon_prop("NFL", str(r.get("prop_type") or ""))
        act = pflt(r.get("actual"))
        if act is None:
            continue
        out[(player, prop)] = act
    return out


def lookup(
    rec: dict,
    exact: dict,
    loose: dict,
    soccer_act: dict,
    nfl_act: dict,
) -> dict:
    sport = str(rec.get("sport") or "").strip().upper()
    if sport == "SOCCER":
        sport = "SOCCER"
    player = pname(rec.get("player"))
    prop = canon_prop(sport, str(rec.get("prop") or ""))
    line = pflt(rec.get("line"))
    pt = str(rec.get("pick_type") or "").strip().title()
    side = str(rec.get("side") or "").strip().upper()
    hit = exact.get((sport, player, prop, line, pt, side))
    if hit and hit.get("result") in {"HIT", "MISS", "PUSH", "VOID"}:
        return hit
    cands = loose.get((sport, player, prop, pt, side)) or []
    if len(cands) == 1 and cands[0].get("result") in {"HIT", "MISS", "PUSH", "VOID"}:
        return cands[0]
    decided = [c for c in cands if c.get("result") in {"HIT", "MISS", "PUSH"}]
    if len(decided) == 1:
        return decided[0]
    if sport == "SOCCER":
        act = soccer_act.get((player, prop))
        res = result_of(act, line, side)
        if res:
            return {"result": res, "actual": act}
    if sport == "NFL":
        act = nfl_act.get((player, prop))
        res = result_of(act, line, side)
        if res:
            return {"result": res, "actual": act}
    return {}


def tally(items: list[dict]) -> dict:
    c = Counter(x["grade"] for x in items)
    hit = c["HIT"]
    miss = c["MISS"]
    decided = hit + miss
    return {
        "n": len(items),
        "hit": hit,
        "miss": miss,
        "push": c["PUSH"],
        "void": c["VOID"],
        "open": c["OPEN"],
        "decided": decided,
        "rate": (round(100.0 * hit / decided, 1) if decided else None),
        "rate_s": (
            f"{100.0 * hit / decided:.1f}% ({hit}/{decided})" if decided else "—"
        ),
    }


def book_of(r: dict) -> str:
    return f"{r.get('pick_type')} {r.get('side')}"


def dedup(items: list[dict]) -> list[dict]:
    seen = set()
    out = []
    for x in items:
        k = (
            str(x.get("sport") or "").upper(),
            pname(x.get("player")),
            str(x.get("prop") or "").lower(),
            pflt(x.get("line")),
            str(x.get("pick_type") or "").title(),
            str(x.get("side") or "").upper(),
        )
        if k in seen:
            continue
        seen.add(k)
        out.append(x)
    return out


def prop_tally(items: list[dict]) -> dict:
    groups: dict[str, list] = defaultdict(list)
    for x in items:
        groups[str(x.get("prop") or "")].append(x)
    scored = {k: tally(v) for k, v in groups.items()}
    return dict(sorted(scored.items(), key=lambda kv: -kv[1]["decided"]))


def main() -> None:
    rows = load_all()
    exact, loose = load_grades()
    soccer_act = load_soccer_actuals()
    nfl_act = load_nfl_actuals()
    print("graded exact", len(exact), "soccer act", len(soccer_act), "nfl act", len(nfl_act))

    listed = []
    g70 = []
    for r in rows:
        g = lookup(r, exact, loose, soccer_act, nfl_act)
        rec = {
            **{k: r.get(k) for k in (
                "sport", "player", "prop", "line", "pick_type", "side",
                "l5_over", "l5_under", "l10_over", "l10_under", "start_time",
                "season_l5_flag", "l5_sample_n", "games_used",
            )},
            "grade": g.get("result") or "OPEN",
            "actual": g.get("actual"),
            "list_ok": _clears_list_gate(r),
            "g70_ok": goblin_70_eligible(r) or standard_ticket_eligible(r),
            "gob70": goblin_70_eligible(r),
            "std70": standard_ticket_eligible(r),
            "sunday": on_slate_et(r, DATE),
            "combo": "+" in str(r.get("player") or ""),
        }
        if rec["list_ok"]:
            listed.append(rec)
        if rec["g70_ok"]:
            g70.append(rec)

    listed = dedup(listed)
    g70 = dedup(g70)
    four = {"WNBA", "MLB", "SOCCER", "TENNIS"}
    listed_four = [x for x in listed if str(x["sport"]).upper() in four]
    listed_sunday = [
        x
        for x in listed_four
        if x["sunday"] or (str(x["sport"]).upper() == "TENNIS" and x["sunday"])
    ]
    listed_sunday = [x for x in listed_four if x["sunday"]]
    tennis_later = [
        x for x in listed if str(x["sport"]).upper() == "TENNIS" and not x["sunday"]
    ]
    g70_four = [x for x in g70 if str(x["sport"]).upper() in four]
    g70_sunday = [x for x in g70_four if x["sunday"]]

    by_sport = {}
    for sport in ("WNBA", "MLB", "SOCCER", "TENNIS", "NFL"):
        pool = [x for x in listed if str(x["sport"]).upper() == sport]
        if sport == "TENNIS":
            sun = [x for x in pool if x["sunday"]]
            later = [x for x in pool if not x["sunday"]]
            by_sport[sport] = {**tally(sun), "later_n": tally(later)["n"], "later": tally(later)}
        elif sport == "NFL":
            noc = [x for x in pool if x["sunday"] and not x["combo"]]
            cur = [
                x
                for x in noc
                if str(x.get("season_l5_flag") or "").upper()
                not in {"PRIOR_SEASON_FILL", "PRIOR_SEASON_L5", "NO_CURRENT_SEASON"}
                and not str(x.get("season_l5_flag") or "").upper().startswith("THIN_")
                and (pflt(x.get("l5_sample_n")) or 0) >= 5
            ]
            by_sport[sport] = {
                "padded_no_combo": tally(noc),
                "current_season": tally(cur),
            }
        else:
            by_sport[sport] = tally([x for x in pool if x["sunday"]])

    by_book = {}
    for b in ("Goblin OVER", "Standard UNDER", "Standard OVER"):
        by_book[b] = tally([x for x in listed_sunday if book_of(x) == b])

    g70_sport = {}
    for sport in ("WNBA", "MLB", "SOCCER", "TENNIS", "NFL"):
        pool = [x for x in g70 if str(x["sport"]).upper() == sport and x["sunday"]]
        g70_sport[sport] = tally(pool)

    soccer_props = prop_tally([x for x in listed_sunday if str(x["sport"]).upper() == "SOCCER"])
    nfl_noc = [
        x
        for x in listed
        if str(x["sport"]).upper() == "NFL" and x["sunday"] and not x["combo"]
    ]
    nfl_book = {b: tally([x for x in nfl_noc if book_of(x) == b]) for b in ("Goblin OVER", "Standard UNDER", "Standard OVER")}

    misses_list = [
        x
        for x in listed_sunday
        if x["grade"] == "MISS" and str(x["sport"]).upper() in four
    ]
    misses_g70 = [x for x in g70_sunday if x["grade"] == "MISS"]

    # Mixer tickets
    tpath = MAIN / "ui_runner" / "data" / "backups" / "tickets_latest.bak_20260920_164144.json"
    raw = json.loads(tpath.read_text(encoding="utf-8"))
    tickets_out = []
    for g in raw.get("groups") or []:
        gname = g.get("group_name") or ""
        for tix in g.get("tickets") or []:
            payout = tix.get("payout") or {}
            n_corr = payout.get("min_payout_x") or payout.get("payout") or payout.get("sweep_payout_x")
            ttype = str(payout.get("ticket_type") or tix.get("mode") or "power").lower()
            legs_src = tix.get("picks") or tix.get("legs") or []
            legs = []
            for lg in legs_src:
                rec = {
                    "sport": str(lg.get("sport") or "").strip().upper() or "SOCCER",
                    "player": lg.get("player"),
                    "prop": lg.get("prop_type") or lg.get("prop"),
                    "line": lg.get("line"),
                    "pick_type": lg.get("pick_type"),
                    "side": str(lg.get("direction") or lg.get("side") or "").upper(),
                }
                if rec["sport"] == "SOCCER":
                    rec["sport"] = "SOCCER"
                gnd = lookup(rec, exact, loose, soccer_act, nfl_act)
                rec["grade"] = gnd.get("result") or "OPEN"
                rec["actual"] = gnd.get("actual")
                legs.append(rec)
            grades = [lg["grade"] for lg in legs]
            n_hit = grades.count("HIT")
            n_miss = grades.count("MISS")
            n_open = grades.count("OPEN")
            n_push = grades.count("PUSH") + grades.count("VOID")
            n_legs = len(legs)
            power = ttype.startswith("power") or ttype == "power"
            if n_miss and power:
                result = "LOSS"
                units = -1.0
            elif n_open:
                result = "OPEN"
                units = 0.0
            elif power:
                if n_hit == n_legs - n_push and n_hit > 0:
                    result = "SWEEP"
                    units = float(n_corr or 0) - 1.0
                else:
                    result = "VOID"
                    units = 0.0
            else:
                # Flex: N-correct all-hit only if we don't have 5/4 lines; Power-like
                if n_miss == 0 and n_hit >= n_legs - n_push:
                    result = "SWEEP"
                    units = float(n_corr or 0) - 1.0
                else:
                    result = "LOSS"
                    units = -1.0
            tickets_out.append(
                {
                    "group": gname,
                    "id": tix.get("ticket_id"),
                    "n_legs": n_legs,
                    "type": ttype,
                    "n_correct_x": n_corr,
                    "score": f"{n_hit}/{n_legs}",
                    "n_hit": n_hit,
                    "n_miss": n_miss,
                    "n_open": n_open,
                    "result": result,
                    "units": round(units, 2),
                    "legs": [
                        f"{lg['player']} {lg['side'][:1] if lg['side'] else ''}{lg['line']} {lg['prop']} {lg['grade']}"
                        for lg in legs
                    ],
                    "leg_rows": legs,
                }
            )

    decided_tix = [t for t in tickets_out if t["result"] in {"SWEEP", "LOSS"}]
    cashed = [t for t in decided_tix if t["result"] == "SWEEP"]
    units = round(sum(t["units"] for t in decided_tix), 2)

    payload = {
        "listed_four_sunday": tally(listed_sunday),
        "listed_four_all_window": tally(listed_four),
        "g70_four_sunday": tally(g70_sunday),
        "g70_four_all_window": tally(g70_four),
        "by_sport": by_sport,
        "by_book": by_book,
        "g70_sport": g70_sport,
        "soccer_props": soccer_props,
        "tennis_later": tally(tennis_later),
        "nfl_listed": tally(nfl_noc),
        "nfl_book": nfl_book,
        "nfl_g70": tally([x for x in g70 if str(x["sport"]).upper() == "NFL"]),
        "list_misses": [
            {
                "sport": x["sport"],
                "player": x["player"],
                "prop": x["prop"],
                "side": x["side"],
                "line": x["line"],
                "actual": x["actual"],
                "book": book_of(x),
            }
            for x in misses_list[:40]
        ],
        "g70_misses": [
            {
                "sport": x["sport"],
                "player": x["player"],
                "prop": x["prop"],
                "side": x["side"],
                "line": x["line"],
                "actual": x["actual"],
            }
            for x in misses_g70
        ],
        "tickets": tickets_out,
        "tickets_decided": len(decided_tix),
        "tickets_cashed": len(cashed),
        "tickets_units": units,
        "n_listed": len(listed),
        "n_g70": len(g70),
        "notes": {
            "g70_file": "none for 2026-09-20; reconstructed from step8",
            "mixer": "tickets_latest.bak_20260920_164144 graded_main",
            "nfl_step8": "missing; list from step5",
            "cfb": "step1 only, no Sunday step8",
        },
    }
    OUT.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    print("LISTED FOUR SUNDAY", payload["listed_four_sunday"])
    print("BY SPORT", json.dumps(by_sport, indent=2))
    print("BY BOOK", json.dumps(by_book, indent=2))
    print("G70 FOUR SUNDAY", payload["g70_four_sunday"])
    print("G70 SPORT", json.dumps(g70_sport, indent=2))
    print("SOCCER PROPS", json.dumps(soccer_props, indent=2))
    print("NFL listed no-combo", payload["nfl_listed"], "book", json.dumps(nfl_book, indent=2), "g70", payload["nfl_g70"])
    print("G70 misses", len(misses_g70))
    for m in misses_g70[:25]:
        print("  G70 MISS", m["sport"], m["player"], m["prop"], m["side"], m["line"], m["actual"])
    print("TICKETS", len(cashed), "/", len(decided_tix), "units", units, "open", sum(1 for t in tickets_out if t["result"]=="OPEN"))
    for t in tickets_out:
        print(t["result"], t["score"], t["n_correct_x"], t["group"], " | ".join(t["legs"]))
    print("wrote", OUT)


if __name__ == "__main__":
    main()
