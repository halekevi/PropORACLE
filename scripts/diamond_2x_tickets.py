#!/usr/bin/env python3
"""Diamond 2x ticket card: daily build, walk-forward backtest, monthly rollup.

Pool: Goblin OVER, directional L5 >= 4 (WNBA family L5 = 5), Gold then Silver.
Promo: Diamond (Gold + L10>=8 AND season>70%) first, then Platinum L10-only,
then remaining Gold. Skip ERA 0.5. One player per slip. Mix sports.
Never use 1st place — N-correct only.

Products:
  4-Flex  4=6x / 3=1.5x   primary 2x + cash
  5-Flex  5=10x / 4=2x    payout sleeve
  3-Power 3/3=6x          Power 2x sleeve (cash = sweep)
Skip 2-Power and 3-Flex (cap 1.58x even at Diamond 72.6%).

  py -3.14 scripts/diamond_2x_tickets.py --mode daily --date 2026-08-23
  py -3.14 scripts/diamond_2x_tickets.py --mode backtest --from 2026-03-10 --to 2026-08-22
  py -3.14 scripts/diamond_2x_tickets.py --mode report
"""
from __future__ import annotations

import argparse
import json
import math
import re
import sys
import unicodedata
from collections import defaultdict, deque
from datetime import datetime, timedelta
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO / "scripts") not in sys.path:
    sys.path.insert(0, str(_REPO / "scripts"))
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

import rank_best_props_today as R  # noqa: E402

KEEP = {"WNBA", "MLB", "SOCCER", "TENNIS", "NFL", "CFB"}
OUT_DIR = _REPO / "data" / "reports" / "diamond_2x"
GRADED_DIRS = [
    _REPO.parent / "PropORACLE_main_cp" / "ui_runner" / "templates",
    _REPO.parent / "PropORACLE_main_cp" / "mobile" / "www",
    _REPO / "ui_runner" / "templates",
    _REPO / "mobile" / "www",
]
FLEX = {4: {4: 6.0, 3: 1.5}, 5: {5: 10.0, 4: 2.0}}
POWER = {3: {3: 6.0}}
P_DIA = 0.726
P_PLAT_L10 = 0.709
P_GOLD = 0.690
P_SILVER = 0.660
CARD = (
    ("F4-1", 4, True),
    ("F4-2", 4, True),
    ("F5-1", 5, True),
    ("P3-1", 3, False),
    ("P3-2", 3, False),
)


def tennis_match_key(mu: object) -> str:
    s = str(mu or "").split("(")[0]
    parts = re.split(r"\s+vs\.?\s+", s, flags=re.I)
    if len(parts) == 2:
        a, b = fold_name(parts[0]), fold_name(parts[1])
        if a and b:
            return " | ".join(sorted((a, b)))
    return fold_name(s)


def fold_name(s: object) -> str:
    t = unicodedata.normalize("NFKD", str(s or ""))
    t = "".join(c for c in t if not unicodedata.combining(c))
    t = t.casefold()
    t = re.sub(r"\b(jr|sr|iii|ii|iv)\b\.?", " ", t)
    t = re.sub(r"[^a-z0-9]+", " ", t)
    return " ".join(t.split())


def fold_prop(s: object) -> str:
    t = str(s or "").casefold()
    for a, b in (
        ("points", "pts"),
        ("rebounds", "rebs"),
        ("assists", "asts"),
        ("hits+runs+rbis", "hrr"),
        ("total bases", "tb"),
        ("strikeouts", "k"),
        ("pitching outs", "outs"),
        ("earned runs allowed", "er"),
        ("total games", "tg"),
        ("games won", "gw"),
        ("goalie saves", "saves"),
        ("shots on target", "sot"),
    ):
        t = t.replace(a, b)
    return re.sub(r"[^a-z0-9]+", "", t)


def sport_of(s: object) -> str:
    u = str(s or "").strip().upper()
    if u in ("SOC", "FOOTBALL"):
        return "SOCCER"
    if u in ("NFLP", "NFL PRESEASON"):
        return "NFL"
    return u


def pick_of(s: object) -> str:
    t = str(s or "").strip().lower()
    if "dem" in t:
        return "Demon"
    if "gob" in t:
        return "Goblin"
    return "Standard"


def dir_of(s: object) -> str:
    t = str(s or "").strip().upper()
    if t in ("OVER", "O", "HIGHER"):
        return "OVER"
    if t in ("UNDER", "U", "LOWER"):
        return "UNDER"
    return ""


def num(v) -> float | None:
    if v is None or v == "":
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    if math.isnan(x):
        return None
    return x


def hit_of(v) -> int | None:
    if v is True or v == 1:
        return 1
    if v is False or v == 0:
        return 0
    t = str(v or "").strip().upper()
    if t in ("HIT", "WIN", "W", "1", "TRUE"):
        return 1
    if t in ("MISS", "LOSS", "L", "0", "FALSE"):
        return 0
    return None


def binomial(n: int, k: int, p: float) -> float:
    if k < 0 or k > n:
        return 0.0
    return math.comb(n, k) * (p**k) * ((1 - p) ** (n - k))


def ticket_ev(n: int, p: float, flex: bool) -> dict:
    table = FLEX[n] if flex else POWER[n]
    cash = sum(binomial(n, k, p) for k, m in table.items() if m > 0)
    ev = sum(binomial(n, k, p) * m for k, m in table.items())
    return {
        "leg_p": round(p, 4),
        "cash_pct": round(100 * cash, 1),
        "ev": round(ev, 3),
        "clears_2x": ev >= 2.0,
        "sweep_pct": round(100 * binomial(n, n, p), 1),
        "payouts": {str(k): v for k, v in table.items()},
    }


def p_of(r: dict) -> float:
    if r.get("promo") == "Diamond":
        return P_DIA
    if r.get("promo") == "Platinum" and (r.get("l10") or 0) >= 8:
        return P_PLAT_L10
    if r.get("badge") == "Gold":
        return P_GOLD
    return P_SILVER


def skip_era(r: dict) -> bool:
    return "earned run" in str(r.get("prop") or "").lower() and float(r.get("line") or 99) <= 0.5


def promo_of(r: dict) -> str:
    gold = r.get("badge") == "Gold"
    l10_ok = r.get("l10") is not None and r["l10"] >= 8
    szn_ok = (
        r.get("season_hr") is not None
        and (r.get("season_n") or 0) >= 10
        and r["season_hr"] > 0.70
    )
    if gold and l10_ok and szn_ok:
        return "Diamond"
    if gold and (l10_ok != szn_ok):
        return "Platinum"
    return r.get("badge") or ""


def promo_rank(r: dict) -> int:
    if r.get("promo") == "Diamond":
        return 0
    if r.get("promo") == "Platinum" and (r.get("l10") or 0) >= 8:
        return 1
    if r.get("badge") == "Gold":
        return 2
    if r.get("promo") == "Platinum":
        return 3
    return 4


def graded_paths() -> list[Path]:
    by_date: dict[str, Path] = {}
    for d in GRADED_DIRS:
        if not d.is_dir():
            continue
        for p in sorted(d.glob("graded_props_*.json")):
            if ".bak_" in p.name:
                continue
            date = p.stem.replace("graded_props_", "")[:10]
            if date not in by_date:
                by_date[date] = p
    return [by_date[k] for k in sorted(by_date)]


def build_hist_rows() -> list[dict]:
    hist: dict[tuple, deque] = defaultdict(deque)
    out: list[dict] = []
    for path in graded_paths():
        date = path.stem.replace("graded_props_", "")[:10]
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        props = raw.get("props") or raw.get("rows") or []
        if not isinstance(props, list):
            continue
        for r in props:
            if not isinstance(r, dict):
                continue
            sp = sport_of(r.get("sport"))
            if sp not in KEEP:
                continue
            hit = hit_of(r.get("result") if r.get("result") not in (None, "") else r.get("hit"))
            if hit is None:
                continue
            player = fold_name(r.get("player"))
            prop = fold_prop(r.get("prop") or r.get("prop_type"))
            if not player or not prop:
                continue
            side = dir_of(r.get("direction") or r.get("over_under"))
            pick = pick_of(r.get("pick_type"))
            if pick == "Demon":
                continue
            line = num(r.get("line"))
            actual = num(r.get("actual_value"))
            key = (sp, player, prop)
            prior = [x for x in hist[key] if x[0] < date]
            l5 = l10 = None
            season_hr = None
            season_n = 0
            season_avg = None
            if line is not None and prior:
                acts = [a for _, a in prior if a is not None]
                if acts:
                    season_avg = sum(acts) / len(acts)
                    flags = []
                    for a in acts:
                        if side == "OVER":
                            flags.append(int(a > line))
                        elif side == "UNDER":
                            flags.append(int(a < line))
                    season_n = len(flags)
                    if season_n:
                        season_hr = sum(flags) / season_n
                    if len(flags) >= 5:
                        l5 = sum(flags[-5:])
                    if len(flags) >= 10:
                        l10 = sum(flags[-10:])
            rec = {
                "date": date,
                "sport": sp,
                "player": str(r.get("player") or "").strip(),
                "prop": str(r.get("prop") or "").strip(),
                "line": line,
                "pick_type": pick,
                "side": side,
                "model_dir": dir_of(
                    r.get("model_dir") or r.get("bet_direction") or r.get("final_bet_direction")
                ),
                "l5_over": l5 if side == "OVER" else None,
                "l5_under": l5 if side == "UNDER" else None,
                "cover": None
                if season_avg is None or line is None
                else round(season_avg - line, 2),
                "def": str(r.get("def_tier") or r.get("d") or "").strip(),
                "def_rank": None,
                "hit": hit,
                "season_hr": season_hr,
                "season_n": season_n,
                "l5": l5,
                "l10": l10,
                "matchup": str(r.get("matchup") or r.get("game") or "").strip(),
            }
            rk = num(r.get("def_rank") or r.get("opp_def_rank"))
            rec["def_rank"] = None if rk is None else int(rk)
            rec.update(R._badge(rec, None))
            rec["promo"] = promo_of(rec)
            out.append(rec)
            if actual is not None:
                hist[key].append((date, actual))
                if len(hist[key]) > 80:
                    hist[key].popleft()
    return out


def eligible(r: dict) -> bool:
    if r.get("pick_type") != "Goblin" or r.get("side") != "OVER":
        return False
    if r.get("badge") not in ("Gold", "Silver"):
        return False
    if skip_era(r) or "+" in str(r.get("player") or ""):
        return False
    l5 = r.get("l5")
    if l5 is None:
        l5 = r.get("l5_over")
    need = R.WNBA_L5_MIN if R.is_wnba_family(r.get("sport")) else 4
    return l5 is not None and l5 >= need


def pool_for_day(rows: list[dict]) -> list[dict]:
    seen: set[tuple] = set()
    pool: list[dict] = []
    for r in rows:
        if not eligible(r):
            continue
        rec = dict(r)
        rec["l5"] = rec.get("l5") if rec.get("l5") is not None else rec.get("l5_over")
        rec["l10"] = rec.get("l10") if rec.get("l10") is not None else rec.get("l10_over")
        rec["promo"] = promo_of(rec)
        key = (fold_name(rec["player"]), fold_prop(rec["prop"]), rec.get("line"))
        if key in seen:
            continue
        seen.add(key)
        pool.append(rec)
    pool.sort(
        key=lambda x: (
            promo_rank(x),
            0 if (x.get("l5") or 0) >= 5 else 1,
            -(x.get("l10") or 0),
            str(x.get("player")),
        )
    )
    used: set[str] = set()
    uniq: list[dict] = []
    for r in pool:
        pn = fold_name(r["player"])
        if pn in used:
            continue
        used.add(pn)
        uniq.append(r)
    return uniq


def pack_card(pool: list[dict]) -> list[dict]:
    taken: set[str] = set()
    tickets: list[dict] = []

    def take(n: int) -> list[dict]:
        combo: list[dict] = []
        sports: set[str] = set()
        wnba_games: set[str] = set()
        tennis_matches: set[str] = set()
        for prefer_new_sport in (True, False):
            combo, sports, wnba_games, tennis_matches = [], set(), set(), set()
            for r in pool:
                pn = fold_name(r["player"])
                if pn in taken or any(fold_name(x["player"]) == pn for x in combo):
                    continue
                mu = str(r.get("matchup") or "")
                tk = tennis_match_key(mu) if r["sport"] == "TENNIS" else ""
                if r["sport"] == "WNBA" and mu and mu in wnba_games:
                    continue
                if r["sport"] == "TENNIS" and tk and tk in tennis_matches:
                    continue
                if prefer_new_sport and r["sport"] in sports and len(combo) < n - 1:
                    continue
                combo.append(r)
                sports.add(r["sport"])
                if r["sport"] == "WNBA" and mu:
                    wnba_games.add(mu)
                if r["sport"] == "TENNIS" and tk:
                    tennis_matches.add(tk)
                if len(combo) == n:
                    return combo
        return combo if len(combo) == n else []

    for tid, n, flex in CARD:
        combo = take(n)
        if not combo:
            continue
        p = sum(p_of(x) for x in combo) / len(combo)
        math_d = ticket_ev(n, p, flex)
        if not math_d.get("clears_2x"):
            continue
        for r in combo:
            taken.add(fold_name(r["player"]))
        tickets.append(
            {
                "id": tid,
                "n": n,
                "product": "Flex" if flex else "Power",
                "mean_leg_p": round(p, 4),
                **math_d,
                "sports": sorted({x["sport"] for x in combo}),
                "legs": [
                    {
                        "sport": x["sport"],
                        "player": x["player"],
                        "prop": x["prop"],
                        "side": x.get("side") or "OVER",
                        "line": x.get("line"),
                        "l5": x.get("l5"),
                        "l10": x.get("l10"),
                        "badge": x.get("badge"),
                        "promo": x.get("promo"),
                        "season_hr": None
                        if x.get("season_hr") is None
                        else round(100 * x["season_hr"], 1),
                        "p": p_of(x),
                        "hit": x.get("hit"),
                        "matchup": x.get("matchup") or "",
                    }
                    for x in combo
                ],
            }
        )
    return tickets


def settle(ticket: dict) -> dict:
    legs = ticket["legs"]
    if any(x.get("hit") not in (0, 1) for x in legs):
        return {**ticket, "settled": False}
    hits = sum(int(x["hit"]) for x in legs)
    table = ticket.get("payouts") or {}
    mult = float(table.get(str(hits), table.get(hits, 0)) or 0)
    return {
        **ticket,
        "settled": True,
        "hits": hits,
        "mult": mult,
        "cashed": mult > 0,
        "swept": hits == ticket["n"],
    }


def summarize_tickets(tickets: list[dict]) -> dict:
    done = [settle(t) if not t.get("settled") else t for t in tickets]
    done = [t for t in done if t.get("settled")]
    out: dict = {"n": len(done), "by_product": {}}
    if not done:
        return out
    out["cash_pct"] = round(100 * sum(int(t["cashed"]) for t in done) / len(done), 1)
    out["mean_mult"] = round(sum(float(t["mult"]) for t in done) / len(done), 3)
    out["sweep_pct"] = round(100 * sum(int(t["swept"]) for t in done) / len(done), 1)
    out["clears_2x_realized"] = out["mean_mult"] >= 2.0
    by: dict[str, list] = defaultdict(list)
    for t in done:
        by[f"{t['product'][0]}{t['n']}"].append(t)
    for k, xs in by.items():
        out["by_product"][k] = {
            "n": len(xs),
            "cash_pct": round(100 * sum(int(t["cashed"]) for t in xs) / len(xs), 1),
            "mean_mult": round(sum(float(t["mult"]) for t in xs) / len(xs), 3),
            "sweep_pct": round(100 * sum(int(t["swept"]) for t in xs) / len(xs), 1),
        }
    return out


def load_today_board(date: str) -> list[dict]:
    candidates = [_REPO]
    main_cp = _REPO.parent / "PropORACLE_main_cp"
    if main_cp.is_dir():
        candidates.append(main_cp)
    root = R._choose_step8_root(candidates, date)
    if root is None:
        return []
    rows: list[dict] = []
    for sport, folder, fname in R.SPORTS:
        df = R.load_sport(root, date, sport, folder, fname)
        if df.empty:
            continue
        rows.extend(R.recs(df))
    df_nfl = R.load_nfl(root, date)
    if not df_nfl.empty:
        rows.extend(R.recs(df_nfl))
    df_cfb = R.load_cfb(root, date)
    if not df_cfb.empty:
        rows.extend(R.recs(df_cfb))
    return rows


def attach_hist_to_board(board: list[dict], hist_rows: list[dict], date: str) -> list[dict]:
    last: dict[tuple, dict] = {}
    for r in hist_rows:
        if r["date"] >= date:
            continue
        last[(r["sport"], fold_name(r["player"]), fold_prop(r["prop"]))] = r
    out = []
    for t in board:
        rec = dict(t)
        rec["l5"] = rec.get("l5_over") if rec.get("side") == "OVER" else rec.get("l5_under")
        rec["l10"] = rec.get("l10_over") if rec.get("side") == "OVER" else rec.get("l10_under")
        rec["season_n"] = rec.get("season_n") or 0
        h = last.get((rec.get("sport"), fold_name(rec.get("player")), fold_prop(rec.get("prop"))))
        if h:
            if rec.get("l10") is None:
                rec["l10"] = h.get("l10")
            rec["season_hr"] = h.get("season_hr")
            rec["season_n"] = h.get("season_n") or 0
        rec["promo"] = promo_of(rec)
        out.append(rec)
    return out


def write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def append_ledger(rows: list[dict]) -> None:
    path = OUT_DIR / "ledger.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


def cmd_build(date: str, hist: list[dict] | None = None) -> dict:
    if hist is None:
        print("loading graded history…")
        hist = build_hist_rows()
        print(f"graded rows {len(hist):,}")
    board = load_today_board(date)
    if not board:
        print("no step8 board for", date)
        return {}
    today = attach_hist_to_board(board, hist, date)
    pool = pool_for_day(today)
    tickets = pack_card(pool)
    payload = {
        "date": date,
        "method": "diamond_2x_v1",
        "pool_n": len(pool),
        "promo_counts": {
            k: sum(1 for r in pool if r.get("promo") == k)
            for k in ("Diamond", "Platinum", "Gold", "Silver")
        },
        "tickets": tickets,
    }
    write_json(OUT_DIR / f"tickets_{date}.json", payload)
    print("wrote", OUT_DIR / f"tickets_{date}.json", "pool", len(pool))
    for t in tickets:
        legs = " · ".join(f"{x['sport'][:3]} {x['player']} {x['promo']}" for x in t["legs"])
        print(f"  {t['id']} {t['product']}{t['n']} EV {t['ev']:.2f}x cash {t['cash_pct']}%  {legs}")
    return payload


def cmd_backtest(d0: str, d1: str) -> dict:
    print("loading graded history…")
    hist = build_hist_rows()
    print(f"graded rows {len(hist):,}")
    by_date: dict[str, list] = defaultdict(list)
    for r in hist:
        by_date[r["date"]].append(r)
    daily = []
    all_tix = []
    for d in sorted(by_date):
        if d < d0 or d > d1:
            continue
        tickets = [settle(t) for t in pack_card(pool_for_day(by_date[d]))]
        if not tickets:
            continue
        summ = summarize_tickets(tickets)
        daily.append({"date": d, "pool_n": len(pool_for_day(by_date[d])), **summ})
        all_tix.extend({"date": d, **t} for t in tickets)
    rollup = summarize_tickets(all_tix)
    payload = {
        "method": "diamond_2x_v1",
        "from": d0,
        "to": d1,
        "n_days": len(daily),
        "rollup": rollup,
        "daily": daily,
    }
    write_json(OUT_DIR / f"backtest_{d0}_to_{d1}.json", payload)
    print("wrote", OUT_DIR / f"backtest_{d0}_to_{d1}.json")
    print("days", len(daily), "tickets", rollup.get("n"))
    print(
        "cash",
        rollup.get("cash_pct"),
        "mean_mult",
        rollup.get("mean_mult"),
        "sweep",
        rollup.get("sweep_pct"),
        "2x",
        rollup.get("clears_2x_realized"),
    )
    for k, v in (rollup.get("by_product") or {}).items():
        print(f"  {k}: n={v['n']} cash={v['cash_pct']}% mean_mult={v['mean_mult']}x")
    return payload


def cmd_grade(date: str, hist: list[dict] | None = None) -> dict:
    path = OUT_DIR / f"tickets_{date}.json"
    if not path.is_file():
        print("no tickets file", path)
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if hist is None:
        hist = build_hist_rows()
    by_key = {}
    for r in hist:
        if r["date"] != date:
            continue
        by_key[(r["sport"], fold_name(r["player"]), fold_prop(r["prop"]))] = r
    tickets = []
    ledger_rows = []
    for t in payload.get("tickets") or []:
        legs = []
        for x in t["legs"]:
            h = by_key.get((x["sport"], fold_name(x["player"]), fold_prop(x["prop"])))
            legs.append({**x, "hit": None if h is None else h.get("hit")})
        settled = settle({**t, "legs": legs})
        tickets.append(settled)
        if settled.get("settled"):
            ledger_rows.append(
                {
                    "date": date,
                    "id": settled["id"],
                    "product": settled["product"],
                    "n": settled["n"],
                    "hits": settled["hits"],
                    "mult": settled["mult"],
                    "cashed": settled["cashed"],
                    "swept": settled["swept"],
                    "ev_exante": settled.get("ev"),
                    "live": True,
                }
            )
    payload["tickets"] = tickets
    payload["grade"] = summarize_tickets(tickets)
    write_json(path, payload)
    if ledger_rows:
        append_ledger(ledger_rows)
    print("graded", date, payload["grade"])
    return payload


def cmd_report() -> dict:
    live: list[dict] = []
    ledger = OUT_DIR / "ledger.jsonl"
    if ledger.is_file():
        for line in ledger.read_text(encoding="utf-8").splitlines():
            if line.strip():
                live.append(json.loads(line))
    payload: dict = {"live_n": len(live), "live": {}, "backtests": [], "monthly": {}}
    if live:
        fake = []
        for r in live:
            n = int(r["n"])
            hits = int(r.get("hits") or 0)
            fake.append(
                {
                    "product": r["product"],
                    "n": n,
                    "payouts": {str(k): v for k, v in (FLEX.get(n) or POWER.get(n) or {}).items()},
                    "legs": [{"hit": 1}] * hits + [{"hit": 0}] * (n - hits),
                    "settled": True,
                    "hits": hits,
                    "mult": r.get("mult"),
                    "cashed": r.get("cashed"),
                    "swept": r.get("swept"),
                }
            )
        payload["live"] = summarize_tickets(fake)
    by_month: dict[str, list] = defaultdict(list)
    for p in sorted(OUT_DIR.glob("backtest_*.json")):
        b = json.loads(p.read_text(encoding="utf-8"))
        payload["backtests"].append(
            {
                "file": p.name,
                "from": b.get("from"),
                "to": b.get("to"),
                "n_days": b.get("n_days"),
                "rollup": b.get("rollup"),
            }
        )
        for d in b.get("daily") or []:
            by_month[str(d["date"])[:7]].append(d)
    monthly = {}
    for m, days in sorted(by_month.items()):
        n = sum(int(d.get("n") or 0) for d in days)
        cash_n = sum((d.get("cash_pct") or 0) / 100.0 * (d.get("n") or 0) for d in days)
        mult_s = sum((d.get("mean_mult") or 0) * (d.get("n") or 0) for d in days)
        monthly[m] = {
            "days": len(days),
            "tickets": n,
            "cash_pct": None if not n else round(100 * cash_n / n, 1),
            "mean_mult": None if not n else round(mult_s / n, 3),
        }
    payload["monthly"] = monthly
    write_json(OUT_DIR / "report_latest.json", payload)
    print("live tickets", payload["live_n"], payload.get("live"))
    print("monthly")
    for m, v in monthly.items():
        print(
            f"  {m}: days={v['days']} tix={v['tickets']} "
            f"cash={v['cash_pct']}% mean_mult={v['mean_mult']}x"
        )
    print("After September, re-run --mode report and compare 2026-08 vs 2026-09 vs 2026-03..08 backtest.")
    return payload


def cmd_daily(date: str) -> int:
    yest = (datetime.strptime(date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    print("loading graded history…")
    hist = build_hist_rows()
    print(f"graded rows {len(hist):,}")
    if (OUT_DIR / f"tickets_{yest}.json").is_file():
        cmd_grade(yest, hist)
    cmd_build(date, hist)
    cmd_report()
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=("daily", "build", "backtest", "grade", "report"), default="daily")
    ap.add_argument("--date", default="")
    ap.add_argument("--from", dest="d0", default="2026-03-10")
    ap.add_argument("--to", dest="d1", default="")
    args = ap.parse_args()
    date = (args.date or datetime.now().strftime("%Y-%m-%d"))[:10]
    if args.mode == "daily":
        return cmd_daily(date)
    if args.mode == "build":
        cmd_build(date)
        return 0
    if args.mode == "grade":
        cmd_grade(date)
        return 0
    if args.mode == "report":
        cmd_report()
        return 0
    cmd_backtest(args.d0[:10], (args.d1 or date)[:10])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
