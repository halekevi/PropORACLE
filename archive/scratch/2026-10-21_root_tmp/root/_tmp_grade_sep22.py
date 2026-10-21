"""Grade 2026-09-22 listed keep-gate, G70 pool, and live dual-card tickets."""
from __future__ import annotations

import json
import sys
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

ROOT = Path(r"H:\PropORACLE")
MAIN = Path(r"H:\PropORACLE_main_cp")
DATE = "2026-09-22"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from rank_best_props_today import _clears_list_gate, load_sport, recs  # noqa: E402
from utils.prop_norm import canon_prop  # noqa: E402
from utils.ticket_70_pool import goblin_70_eligible, standard_ticket_eligible  # noqa: E402

try:
    from build_goblin70_tickets import attach_wnba_ticket_priors  # noqa: E402
except Exception:
    attach_wnba_ticket_priors = None

OUT = ROOT / "_tmp_grade_sep22_out.json"


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


def sport_u(s: object) -> str:
    t = str(s or "").strip().upper()
    if t in {"SOC", "SOCCER"}:
        return "SOCCER"
    if t in {"TENNIS"}:
        return "TENNIS"
    return t


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


def load_board() -> list[dict]:
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
        if attach_wnba_ticket_priors and sport == "WNBA":
            for r in chunk:
                attach_wnba_ticket_priors(r, DATE)
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
    n_dec = 0
    for r in gp.get("props") or []:
        sport = sport_u(r.get("sport"))
        player = pname(r.get("player"))
        prop = canon_prop(sport, str(r.get("prop") or r.get("prop_type") or ""))
        line = pflt(r.get("line"))
        pt = str(r.get("pick_type") or "").strip().title()
        side = str(r.get("direction") or r.get("side") or "").strip().upper()
        res = str(r.get("result") or "").strip().upper()
        actual = pflt(
            r.get("actual_value") if r.get("actual_value") not in (None, "") else r.get("actual")
        )
        row = {
            "result": res,
            "actual": actual,
            "sport": sport,
            "prop": prop,
            "player": r.get("player"),
        }
        exact[(sport, player, prop, line, pt, side)] = row
        loose[(sport, player, prop, pt, side)].append(row)
        loose[(sport, player, prop)].append(row)
        if res in {"HIT", "MISS", "PUSH", "VOID"}:
            n_dec += 1
    print("graded_props", gp.get("count"), "decided-ish", n_dec)
    return exact, loose


def load_csv_actuals(sport: str) -> dict[tuple, float]:
    p = MAIN / "outputs" / DATE / f"actuals_{sport.lower()}_{DATE}.csv"
    out: dict[tuple, float] = {}
    if not p.is_file() or p.stat().st_size < 80:
        return out
    df = pd.read_csv(p)
    for _, r in df.iterrows():
        player = pname(r.get("player"))
        prop = canon_prop(sport, str(r.get("prop_type") or r.get("prop") or ""))
        act = pflt(r.get("actual") if r.get("actual") is not None else r.get("actual_value"))
        if act is None:
            continue
        out[(player, prop)] = act
    return out


def lookup(rec: dict, exact: dict, loose: dict, extra: dict[str, dict]) -> dict:
    sport = sport_u(rec.get("sport"))
    player = pname(rec.get("player"))
    prop = canon_prop(sport, str(rec.get("prop") or rec.get("prop_type") or ""))
    line = pflt(rec.get("line"))
    pt = str(rec.get("pick_type") or "").strip().title()
    side = str(
        rec.get("side") or rec.get("direction") or rec.get("over_under") or ""
    ).strip().upper()
    hit = exact.get((sport, player, prop, line, pt, side))
    if hit and hit.get("result") in {"HIT", "MISS", "PUSH", "VOID"}:
        return hit
    cands = loose.get((sport, player, prop, pt, side)) or []
    decided = [c for c in cands if c.get("result") in {"HIT", "MISS", "PUSH", "VOID"}]
    if len(decided) == 1:
        return decided[0]
    if len(decided) > 1 and line is not None:
        close = [
            c
            for c in decided
            if c.get("actual") is not None
        ]
        if len(close) == 1:
            return close[0]
    act_map = extra.get(sport) or {}
    act = act_map.get((player, prop))
    res = result_of(act, line, side)
    if res:
        return {"result": res, "actual": act}
    return {}


def tally(items: list[dict]) -> dict:
    c = Counter(x.get("grade") for x in items)
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
            sport_u(x.get("sport")),
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


def n_correct_map(tix: dict) -> dict[int, float]:
    raw = tix.get("n_correct") or {}
    if isinstance(raw, dict) and raw:
        out = {}
        for k, v in raw.items():
            try:
                out[int(k)] = float(v)
            except (TypeError, ValueError):
                continue
        if out:
            return out
    payout = tix.get("payout") or {}
    for key in ("n_correct", "n_correct_pays"):
        blob = payout.get(key)
        if isinstance(blob, dict):
            out = {}
            for k, v in blob.items():
                try:
                    out[int(k)] = float(v)
                except (TypeError, ValueError):
                    continue
            if out:
                return out
    x = payout.get("min_payout_x") or tix.get("display_min_x") or tix.get("power_payout")
    try:
        if x is not None:
            n = int(tix.get("n_legs") or len(tix.get("legs") or tix.get("picks") or []))
            return {n: float(x)}
    except (TypeError, ValueError):
        pass
    return {}


def grade_ticket(tix: dict, gname: str, exact, loose, extra) -> dict:
    payout = tix.get("payout") or {}
    ttype = str(
        tix.get("product") or tix.get("play") or payout.get("ticket_type") or tix.get("mode") or ""
    ).strip()
    if not ttype:
        gl = gname.lower()
        ttype = "Flex" if "flex" in gl else "Power"
    power = "flex" not in ttype.lower()
    legs_src = tix.get("picks") or tix.get("legs") or []
    legs = []
    for lg in legs_src:
        rec = {
            "sport": sport_u(lg.get("sport")),
            "player": lg.get("player"),
            "prop": lg.get("prop_type") or lg.get("prop"),
            "line": lg.get("line"),
            "pick_type": lg.get("pick_type"),
            "side": str(lg.get("direction") or lg.get("side") or "").upper(),
        }
        gnd = lookup(rec, exact, loose, extra)
        rec["grade"] = gnd.get("result") or "OPEN"
        rec["actual"] = gnd.get("actual")
        legs.append(rec)
    grades = [lg["grade"] for lg in legs]
    n_hit = grades.count("HIT")
    n_miss = grades.count("MISS")
    n_open = grades.count("OPEN")
    n_push = grades.count("PUSH") + grades.count("VOID")
    n_legs = len(legs)
    live = n_legs - n_push
    pays = n_correct_map(tix)
    yolo = "yolo" in gname.lower() or bool(tix.get("exclude_from_winrate"))
    if n_open:
        result, units, paid = "OPEN", 0.0, None
    elif power:
        if live > 0 and n_hit == live and n_miss == 0:
            result, paid = "SWEEP", True
            units = float(pays.get(live) or pays.get(n_legs) or 0) - 1.0
        else:
            result, paid, units = "LOSS", False, -1.0
    else:
        # Flex: N-correct.  n-1 is still a cash if that tier pays > 0.
        x = pays.get(n_hit)
        if x is None and n_miss == 0 and n_hit >= live:
            x = pays.get(live) or pays.get(n_legs)
        if x is not None and float(x) > 0:
            result, paid = ("SWEEP" if n_miss == 0 else "CASH"), True
            units = float(x) - 1.0
        else:
            result, paid, units = "LOSS", False, -1.0
    return {
        "group": gname,
        "id": tix.get("ticket_id") or tix.get("id"),
        "n_legs": n_legs,
        "type": ttype,
        "yolo": yolo,
        "track": tix.get("ticket_track") or "",
        "score": f"{n_hit}/{n_legs}",
        "n_hit": n_hit,
        "n_miss": n_miss,
        "n_open": n_open,
        "n_push": n_push,
        "result": result,
        "paid": paid,
        "units": round(units, 2),
        "legs": [
            f"{lg['player']} {lg['side'][:1] if lg['side'] else ''}{lg['line']} {lg['prop']} {lg['grade']}"
            for lg in legs
        ],
    }


def summarize_tickets(tickets_out: list[dict], *, include_yolo: bool) -> dict:
    pool = tickets_out if include_yolo else [t for t in tickets_out if not t.get("yolo")]
    decided = [t for t in pool if t["result"] in {"SWEEP", "CASH", "LOSS"}]
    paid = [t for t in decided if t.get("paid")]
    return {
        "n": len(pool),
        "decided": len(decided),
        "paid": len(paid),
        "loss": sum(1 for t in decided if t["result"] == "LOSS"),
        "open": sum(1 for t in pool if t["result"] == "OPEN"),
        "hit_rate": (
            f"{100.0 * len(paid) / len(decided):.1f}% ({len(paid)}/{len(decided)})"
            if decided
            else "—"
        ),
        "units": round(sum(t["units"] for t in decided), 2),
    }


def main() -> None:
    rows = load_board()
    exact, loose = load_grades()
    extra = {
        "SOCCER": load_csv_actuals("Soccer"),
        "NFL": load_csv_actuals("NFL"),
        "TENNIS": load_csv_actuals("Tennis"),
        "WNBA": load_csv_actuals("WNBA"),
        "MLB": load_csv_actuals("MLB"),
    }
    print("extra actuals", {k: len(v) for k, v in extra.items()})

    listed = []
    g70 = []
    for r in rows:
        g = lookup(r, exact, loose, extra)
        rec = {
            **{
                k: r.get(k)
                for k in (
                    "sport",
                    "player",
                    "prop",
                    "line",
                    "pick_type",
                    "side",
                    "l5_over",
                    "l5_under",
                    "l10_over",
                    "l10_under",
                )
            },
            "grade": g.get("result") or "OPEN",
            "actual": g.get("actual"),
            "list_ok": _clears_list_gate(r),
            "gob70": goblin_70_eligible(r),
            "std70": standard_ticket_eligible(r),
            "tuesday": on_slate_et(r, DATE),
        }
        rec["g70_ok"] = rec["gob70"] or rec["std70"]
        if rec["list_ok"]:
            listed.append(rec)
        if rec["g70_ok"]:
            g70.append(rec)

    listed = dedup(listed)
    g70 = dedup(g70)
    four = {"WNBA", "MLB", "SOCCER", "TENNIS"}
    listed_tue = [
        x for x in listed if sport_u(x["sport"]) in four and x["tuesday"]
    ]
    g70_tue = [x for x in g70 if sport_u(x["sport"]) in four and x["tuesday"]]
    g70_gob = [x for x in g70_tue if x.get("gob70")]

    by_sport = {
        s: tally([x for x in listed_tue if sport_u(x["sport"]) == s])
        for s in ("WNBA", "MLB", "SOCCER", "TENNIS")
    }
    by_book = {
        b: tally([x for x in listed_tue if book_of(x) == b])
        for b in ("Goblin OVER", "Standard UNDER", "Standard OVER")
    }
    g70_sport = {
        s: tally([x for x in g70_gob if sport_u(x["sport"]) == s])
        for s in ("WNBA", "MLB", "SOCCER", "TENNIS")
    }
    wnba_props = prop_tally(
        [x for x in g70_gob if sport_u(x["sport"]) == "WNBA"]
    )
    mlb_props = prop_tally([x for x in g70_gob if sport_u(x["sport"]) == "MLB"])
    tennis_later = tally(
        [x for x in listed if sport_u(x["sport"]) == "TENNIS" and not x["tuesday"]]
    )

    tpath = (
        MAIN
        / "outputs"
        / DATE
        / "canonical"
        / "platform_ui"
        / "tickets_latest.json"
    )
    raw = json.loads(tpath.read_text(encoding="utf-8"))
    tickets_out = []
    for g in raw.get("groups") or []:
        gname = g.get("group_name") or ""
        for tix in g.get("tickets") or []:
            tickets_out.append(grade_ticket(tix, gname, exact, loose, extra))

    g70_tix = [
        t
        for t in tickets_out
        if "goblin-70" in t["group"].lower() or t.get("track") == "goblin70"
    ]
    mixer_tix = [t for t in tickets_out if t not in g70_tix]
    wr = summarize_tickets(g70_tix, include_yolo=False)
    wr_y = summarize_tickets(g70_tix, include_yolo=True)
    mix = summarize_tickets(mixer_tix, include_yolo=False)

    payload = {
        "listed_four_tuesday": tally(listed_tue),
        "g70_goblin_tuesday": tally(g70_gob),
        "g70_any_tuesday": tally(g70_tue),
        "by_sport": by_sport,
        "by_book": by_book,
        "g70_sport": g70_sport,
        "wnba_g70_props": wnba_props,
        "mlb_g70_props": mlb_props,
        "tennis_later": tennis_later,
        "tickets_g70_winrate": wr,
        "tickets_g70_with_yolo": wr_y,
        "tickets_mixer": mix,
        "g70_misses": [
            {
                "sport": x["sport"],
                "player": x["player"],
                "prop": x["prop"],
                "side": x["side"],
                "line": x["line"],
                "actual": x["actual"],
            }
            for x in g70_gob
            if x["grade"] == "MISS"
        ],
        "tickets": tickets_out,
    }
    OUT.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    print("\nLISTED FOUR TUE", payload["listed_four_tuesday"]["rate_s"], payload["listed_four_tuesday"])
    print("BY SPORT", json.dumps(by_sport, indent=2))
    print("BY BOOK", json.dumps(by_book, indent=2))
    print("G70 GOBLIN TUE", payload["g70_goblin_tuesday"]["rate_s"], payload["g70_goblin_tuesday"])
    print("G70 SPORT", json.dumps(g70_sport, indent=2))
    print("WNBA G70 PROPS", json.dumps(wnba_props, indent=2))
    print("MLB G70 PROPS", json.dumps(mlb_props, indent=2))
    print("TENNIS LATER", tennis_later)
    print("G70 TICKETS (YOLO off)", wr)
    print("G70 TICKETS +YOLO", wr_y)
    print("MIXER", mix)
    print("G70 misses", len(payload["g70_misses"]))
    for m in payload["g70_misses"][:30]:
        print("  G70 MISS", m["sport"], m["player"], m["prop"], m["side"], m["line"], m["actual"])
    print("\n--- TICKETS ---")
    for t in tickets_out:
        flag = " YOLO" if t["yolo"] else ""
        print(
            f"{t['result']:6} {t['score']:5} {t['group']}{flag} | "
            + " | ".join(t["legs"])
        )
    print("wrote", OUT)


if __name__ == "__main__":
    main()
