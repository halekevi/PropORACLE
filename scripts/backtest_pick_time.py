#!/usr/bin/env python3
"""Hit rates by when the PrizePicks line was pulled.

Scores line_history snapshots against graded actuals using THAT snapshot's
line (not the overnight grader line). One row per prop per lock window
(latest fetch in that window). Two list gates:

  L5 >= 4 Gold/Silver  (live list)
  L5 == 5 Gold/Silver/Bronze  (strict L5, full badge split)

Books: Goblin OVER / Standard OVER / Standard UNDER.

Windows (America/New_York):
  day_ahead     fetched calendar date < game_date
  early_am      same day, hour < 8
  lock_8am      same day, 8:00-9:59 (primary same-day lock)
  late_morning  same day, 10:00-11:59
  afternoon     same day, 12:00-16:59
  evening       same day, 17:00+ and not last hour
  last_hour     0-60 minutes before start_time (wins over clock buckets)

  py -3.14 scripts/backtest_pick_time.py
"""
from __future__ import annotations

import json
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO / "scripts") not in sys.path:
    sys.path.insert(0, str(_REPO / "scripts"))
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

import diamond_2x_tickets as D  # noqa: E402
import prop_hit_tiers as T  # noqa: E402
import rank_best_props_today as R  # noqa: E402

ET = ZoneInfo("America/New_York")
DB = _REPO / "data" / "line_history.db"
OUT = _REPO / "data" / "reports" / "pick_time_backtest.json"
SPORTS = ("WNBA", "MLB", "SOCCER")
BOOKS = ("Goblin OVER", "Standard OVER", "Standard UNDER")
PROMOS = (
    "Diamond",
    "Platinum",
    "Platinum L10-only",
    "Platinum season-only",
    "Gold remain",
    "Silver",
    "Bronze",
    "Gold all",
    "Listed",
)
PRINT_PROMOS = (
    "Listed",
    "Diamond",
    "Platinum",
    "Gold remain",
    "Gold all",
    "Silver",
    "Bronze",
)
WINDOWS = (
    "day_ahead",
    "early_am",
    "lock_8am",
    "late_morning",
    "afternoon",
    "evening",
    "last_hour",
)
PRIMARY = ("day_ahead", "early_am", "lock_8am", "afternoon", "last_hour")
PROP_PRINT_WINS = ("day_ahead", "lock_8am", "afternoon", "last_hour")
PROP_MIN_N = 15
PROP_MOVE_N = 12
SKIP_PROPS = {"fantasyscore", "fantasy"}
PAIR_WINDOWS = (
    ("day_ahead", "lock_8am"),
    ("day_ahead", "afternoon"),
    ("day_ahead", "last_hour"),
    ("early_am", "lock_8am"),
    ("lock_8am", "afternoon"),
    ("lock_8am", "last_hour"),
    ("afternoon", "last_hour"),
)


def empty():
    return {"n": 0, "hits": 0, "hr": None}


def add(cell, hit: int) -> None:
    cell["n"] += 1
    cell["hits"] += int(hit)
    cell["hr"] = round(cell["hits"] / cell["n"], 4) if cell["n"] else None


def fmt(c) -> str:
    if not c or not c["n"]:
        return "--"
    return f"{100 * c['hits'] / c['n']:.1f}%  {c['hits']}/{c['n']}"


def new_promo_block():
    return {k: empty() for k in PROMOS}


def book_of(pick: str, side: str) -> str | None:
    if pick == "Goblin" and side == "OVER":
        return "Goblin OVER"
    if pick == "Standard" and side == "OVER":
        return "Standard OVER"
    if pick == "Standard" and side == "UNDER":
        return "Standard UNDER"
    return None


def promo_bucket(r: dict) -> str:
    p = r.get("promo") or ""
    if p == "Diamond":
        return "Diamond"
    if p == "Platinum":
        l10_ok = r.get("l10") is not None and r["l10"] >= 8
        return "Platinum L10-only" if l10_ok else "Platinum season-only"
    if r.get("badge") == "Gold":
        return "Gold remain"
    if r.get("badge") == "Bronze":
        return "Bronze"
    return "Silver"


def parse_dt(raw: object) -> datetime | None:
    s = str(raw or "").strip()
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        try:
            dt = datetime.fromisoformat(s[:19])
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ET)
    return dt.astimezone(ET)


def window_of(fetched: datetime, game_date: str, start: datetime | None) -> str | None:
    gd = str(game_date or "")[:10]
    fd = fetched.date().isoformat()
    if start is not None:
        hours = (start - fetched).total_seconds() / 3600.0
        if hours < 0:
            return None
        if hours <= 1.0:
            return "last_hour"
    if not gd:
        return None
    if fd < gd:
        return "day_ahead"
    if fd > gd:
        return None
    hour = fetched.hour
    if hour < 8:
        return "early_am"
    if hour < 10:
        return "lock_8am"
    if hour < 12:
        return "late_morning"
    if hour < 17:
        return "afternoon"
    return "evening"


def would_hit(actual: float, line: float, side: str) -> int | None:
    if actual > line:
        return 1 if side == "OVER" else 0
    if actual < line:
        return 0 if side == "OVER" else 1
    return None


def load_grades() -> tuple[dict, dict, dict]:
    """actuals, sides meta, priors by (sport, player, prop)."""
    actuals: dict[tuple, float] = {}
    sides: dict[tuple, dict] = {}
    by_player: dict[tuple, list[tuple[str, float]]] = defaultdict(list)
    seen_act: set[tuple] = set()
    for path in D.graded_paths():
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
            sp = D.sport_of(r.get("sport"))
            if sp not in SPORTS:
                continue
            player = D.fold_name(r.get("player"))
            prop = D.fold_prop(r.get("prop") or r.get("prop_type"))
            if not player or not prop or prop in SKIP_PROPS:
                continue
            pick = D.pick_of(r.get("pick_type"))
            if pick == "Demon":
                continue
            side = D.dir_of(r.get("direction") or r.get("over_under"))
            actual = D.num(r.get("actual_value"))
            pkey = (sp, player, prop)
            akey = (sp, player, prop, date)
            if actual is not None and akey not in actuals:
                actuals[akey] = actual
            if actual is not None and (sp, player, prop, date) not in seen_act:
                by_player[pkey].append((date, actual))
                seen_act.add((sp, player, prop, date))
            if not side:
                continue
            skey = (sp, player, prop, pick, date, side)
            if skey in sides:
                continue
            sides[skey] = {
                "def": str(r.get("def_tier") or r.get("d") or "").strip(),
                "model_dir": D.dir_of(
                    r.get("model_dir") or r.get("bet_direction") or r.get("final_bet_direction")
                ),
                "def_rank": D.num(r.get("def_rank") or r.get("opp_def_rank")),
                "graded_line": D.num(r.get("line")),
                "player": str(r.get("player") or "").strip(),
                "prop": str(r.get("prop") or "").strip(),
            }
    for k in by_player:
        by_player[k].sort()
    return actuals, sides, by_player


def lstats(priors: list[tuple[str, float]], date: str, line: float, side: str) -> dict:
    acts = [a for d, a in priors if d < date]
    flags: list[int] = []
    for a in acts:
        if side == "OVER":
            flags.append(int(a > line))
        else:
            flags.append(int(a < line))
    season_n = len(flags)
    season_avg = (sum(acts) / len(acts)) if acts else None
    season_hr = (sum(flags) / season_n) if season_n else None
    l5 = sum(flags[-5:]) if len(flags) >= 5 else None
    l10 = sum(flags[-10:]) if len(flags) >= 10 else None
    cover = None if season_avg is None else round(season_avg - line, 2)
    return {
        "l5": l5,
        "l10": l10,
        "season_hr": season_hr,
        "season_n": season_n,
        "season_avg": season_avg,
        "cover": cover,
        "l5_over": l5 if side == "OVER" else None,
        "l5_under": l5 if side == "UNDER" else None,
    }


def load_snapshots() -> dict[tuple, dict]:
    """Latest fetch per (sport, player, prop, pick, date, window)."""
    if not DB.is_file():
        raise SystemExit(f"missing {DB}")
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    cur = con.execute(
        """
        SELECT sport, player, prop_type, pick_type, game_date, line, fetched_at, start_time
        FROM line_history
        WHERE sport IN ('WNBA','MLB','SOCCER')
          AND pick_type IN ('Goblin','Standard','goblin','standard')
          AND game_date IS NOT NULL AND game_date != ''
          AND line IS NOT NULL
        """
    )
    best: dict[tuple, dict] = {}
    n_raw = 0
    skipped_win = 0
    for sport, player, prop, pick, game_date, line, fetched_at, start_time in cur:
        n_raw += 1
        fetched = parse_dt(fetched_at)
        if fetched is None:
            skipped_win += 1
            continue
        start = parse_dt(start_time)
        win = window_of(fetched, str(game_date)[:10], start)
        if win is None:
            skipped_win += 1
            continue
        sp = D.sport_of(sport)
        pl = D.fold_name(player)
        pr = D.fold_prop(prop)
        pk = D.pick_of(pick)
        gd = str(game_date)[:10]
        ln = D.num(line)
        if not pl or not pr or pr in SKIP_PROPS or ln is None or pk not in ("Goblin", "Standard"):
            continue
        key = (sp, pl, pr, pk, gd, win)
        prev = best.get(key)
        if prev is None or fetched > prev["fetched"]:
            best[key] = {
                "line": ln,
                "fetched": fetched,
                "start": start,
                "player_raw": str(player or ""),
                "prop_raw": str(prop or ""),
            }
    con.close()
    return best, n_raw, skipped_win


def score_rows(snaps: dict, actuals: dict, sides: dict, priors: dict) -> list[dict]:
    out: list[dict] = []
    for (sp, pl, pr, pk, gd, win), snap in snaps.items():
        akey = (sp, pl, pr, gd)
        actual = actuals.get(akey)
        if actual is None:
            continue
        line = snap["line"]
        want_sides = ("OVER",) if pk == "Goblin" else ("OVER", "UNDER")
        for side in want_sides:
            meta = sides.get((sp, pl, pr, pk, gd, side))
            if meta is None:
                continue
            hit = would_hit(actual, line, side)
            if hit is None:
                continue
            book = book_of(pk, side)
            if book is None:
                continue
            rec = {
                "date": gd,
                "sport": sp,
                "player": meta["player"] or snap["player_raw"],
                "prop": meta["prop"] or snap["prop_raw"],
                "line": line,
                "graded_line": meta.get("graded_line"),
                "pick_type": pk,
                "side": side,
                "window": win,
                "book": book,
                "hit": hit,
                "actual": actual,
                "def": meta.get("def") or "",
                "model_dir": meta.get("model_dir") or "",
                "def_rank": None
                if meta.get("def_rank") is None
                else int(meta["def_rank"]),
            }
            rec.update(lstats(priors.get((sp, pl, pr), []), gd, line, side))
            if D.skip_era(rec):
                continue
            rec.update(R._badge(rec, None))
            rec["promo"] = D.promo_of(rec)
            sport_n = T.norm_sport(sp)
            rec["sport_n"] = sport_n
            rec["prop_canon"] = T.canon_prop(sport_n, rec.get("prop") or "")
            out.append(rec)
    return out


def list_ok(r: dict, *, l5_mode: str = "ge4", badges: tuple[str, ...] = ("Gold", "Silver")) -> bool:
    if r.get("badge") not in badges:
        return False
    l5 = r.get("l5")
    if l5 is None:
        return False
    if l5_mode == "eq5":
        return l5 == 5
    return l5 >= 4


def pred_ge4(r: dict) -> bool:
    return list_ok(r, l5_mode="ge4", badges=("Gold", "Silver"))


def pred_eq5(r: dict) -> bool:
    return list_ok(r, l5_mode="eq5", badges=("Gold", "Silver", "Bronze"))


def nested() -> dict:
    return {
        w: {b: new_promo_block() for b in BOOKS} | {"ALL": new_promo_block()}
        for w in WINDOWS
    }


def _hit_targets(block: dict, by_sport: dict, r: dict, bucket: str) -> list:
    win, book, sp = r["window"], r["book"], r["sport"]
    return [
        block[win][book][bucket],
        block[win]["ALL"][bucket],
        by_sport[sp][win][book][bucket],
        by_sport[sp][win]["ALL"][bucket],
    ]


def fill_block(rows: list[dict], pred) -> dict:
    block = nested()
    by_sport = {sp: nested() for sp in SPORTS}
    for r in rows:
        if not pred(r):
            continue
        hit = r["hit"]
        bucket = promo_bucket(r)
        targets = _hit_targets(block, by_sport, r, bucket)
        if bucket.startswith("Platinum"):
            targets.extend(_hit_targets(block, by_sport, r, "Platinum"))
        if r.get("badge") == "Gold":
            targets.extend(_hit_targets(block, by_sport, r, "Gold all"))
        if r.get("badge") in ("Gold", "Silver"):
            targets.extend(_hit_targets(block, by_sport, r, "Listed"))
        for c in targets:
            add(c, hit)
    return {"overall": block, "by_sport": by_sport}


def _empty_pair_cell() -> dict:
    return {"n": 0, "a_hits": 0, "b_hits": 0, "line_moved": 0}


def _finish_pair_cell(c: dict) -> None:
    if not c["n"]:
        return
    c["a_hr"] = round(c["a_hits"] / c["n"], 4)
    c["b_hr"] = round(c["b_hits"] / c["n"], 4)
    c["delta"] = round(c["b_hr"] - c["a_hr"], 4)


def paired(rows: list[dict], pred) -> dict:
    by_key: dict[tuple, dict[str, dict]] = defaultdict(dict)
    for r in rows:
        if not pred(r):
            continue
        key = (r["sport"], D.fold_name(r["player"]), D.fold_prop(r["prop"]), r["pick_type"], r["side"], r["date"])
        by_key[key][r["window"]] = r
    out: dict[str, dict] = {}
    for a, b in PAIR_WINDOWS:
        label = f"{a} vs {b}"
        cell = {
            "n": 0,
            "a_hits": 0,
            "b_hits": 0,
            "both_hit": 0,
            "a_only": 0,
            "b_only": 0,
            "line_same": 0,
            "line_moved": 0,
            "over_better_at_b": 0,
            "under_better_at_b": 0,
            "by_book": {bk: _empty_pair_cell() for bk in BOOKS},
            "by_promo": {p: _empty_pair_cell() for p in ("Diamond", "Platinum", "Gold remain", "Silver", "Bronze")},
            "by_book_promo": {
                bk: {p: _empty_pair_cell() for p in ("Diamond", "Platinum", "Gold remain", "Silver", "Bronze")}
                for bk in BOOKS
            },
        }
        for recs in by_key.values():
            ra, rb = recs.get(a), recs.get(b)
            if ra is None or rb is None:
                continue
            cell["n"] += 1
            cell["a_hits"] += ra["hit"]
            cell["b_hits"] += rb["hit"]
            if ra["hit"] and rb["hit"]:
                cell["both_hit"] += 1
            elif ra["hit"]:
                cell["a_only"] += 1
            elif rb["hit"]:
                cell["b_only"] += 1
            if ra["line"] == rb["line"]:
                cell["line_same"] += 1
            else:
                cell["line_moved"] += 1
                if ra["side"] == "OVER" and rb["line"] < ra["line"]:
                    cell["over_better_at_b"] += 1
                if ra["side"] == "UNDER" and rb["line"] > ra["line"]:
                    cell["under_better_at_b"] += 1
            bk = ra["book"]
            bucket = promo_bucket(ra)
            plat = "Platinum" if bucket.startswith("Platinum") else bucket
            if bk in cell["by_book"]:
                cell["by_book"][bk]["n"] += 1
                cell["by_book"][bk]["a_hits"] += ra["hit"]
                cell["by_book"][bk]["b_hits"] += rb["hit"]
                if ra["line"] != rb["line"]:
                    cell["by_book"][bk]["line_moved"] += 1
            if plat in cell["by_promo"]:
                cell["by_promo"][plat]["n"] += 1
                cell["by_promo"][plat]["a_hits"] += ra["hit"]
                cell["by_promo"][plat]["b_hits"] += rb["hit"]
                if ra["line"] != rb["line"]:
                    cell["by_promo"][plat]["line_moved"] += 1
            if bk in cell["by_book_promo"] and plat in cell["by_book_promo"][bk]:
                bp = cell["by_book_promo"][bk][plat]
                bp["n"] += 1
                bp["a_hits"] += ra["hit"]
                bp["b_hits"] += rb["hit"]
                if ra["line"] != rb["line"]:
                    bp["line_moved"] += 1
        if cell["n"]:
            cell["a_hr"] = round(cell["a_hits"] / cell["n"], 4)
            cell["b_hr"] = round(cell["b_hits"] / cell["n"], 4)
            cell["delta_b_minus_a"] = round(cell["b_hr"] - cell["a_hr"], 4)
            cell["moved_pct"] = round(cell["line_moved"] / cell["n"], 4)
            for c in cell["by_book"].values():
                _finish_pair_cell(c)
            for c in cell["by_promo"].values():
                _finish_pair_cell(c)
            for bk in cell["by_book_promo"].values():
                for c in bk.values():
                    _finish_pair_cell(c)
        out[label] = cell
    return out


def prop_label(r: dict) -> str:
    return f"{r.get('sport_n') or r['sport']}|{r['book']}|{r.get('prop_canon') or r.get('prop') or '?'}"


def _add_prop_hit(store: dict, r: dict, bucket: str) -> None:
    key = prop_label(r)
    cell = store.setdefault(key, {w: new_promo_block() for w in WINDOWS})
    add(cell[r["window"]][bucket], r["hit"])


def fill_by_prop(rows: list[dict], pred) -> dict:
    store: dict[str, dict] = {}
    for r in rows:
        if not pred(r):
            continue
        bucket = promo_bucket(r)
        _add_prop_hit(store, r, bucket)
        if bucket.startswith("Platinum"):
            _add_prop_hit(store, r, "Platinum")
        if r.get("badge") == "Gold":
            _add_prop_hit(store, r, "Gold all")
        if r.get("badge") in ("Gold", "Silver"):
            _add_prop_hit(store, r, "Listed")
    out = []
    for key, wins in store.items():
        sport, book, prop = key.split("|", 2)
        compact = {}
        for w, block in wins.items():
            kept = {k: v for k, v in block.items() if v.get("n")}
            if kept:
                compact[w] = kept
        if not compact:
            continue
        n8 = (compact.get("lock_8am") or {}).get("Listed") or empty()
        out.append(
            {
                "key": key,
                "sport": sport,
                "book": book,
                "prop": prop,
                "n_lock_listed": n8.get("n") or 0,
                "windows": compact,
            }
        )
    out.sort(key=lambda x: (-x["n_lock_listed"], x["sport"], x["book"], x["prop"]))
    return out


def paired_by_prop(rows: list[dict], pred) -> dict:
    by_key: dict[tuple, dict[str, dict]] = defaultdict(dict)
    for r in rows:
        if not pred(r):
            continue
        key = (
            r.get("sport_n") or r["sport"],
            r["book"],
            r.get("prop_canon") or r.get("prop") or "",
            D.fold_name(r["player"]),
            r["pick_type"],
            r["side"],
            r["date"],
        )
        by_key[key][r["window"]] = r
    out: dict[str, dict] = {}
    for a, b in PAIR_WINDOWS:
        label = f"{a} vs {b}"
        props: dict[str, dict] = defaultdict(_empty_pair_cell)
        for recs in by_key.values():
            ra, rb = recs.get(a), recs.get(b)
            if ra is None or rb is None:
                continue
            pk = f"{ra.get('sport_n') or ra['sport']}|{ra['book']}|{ra.get('prop_canon') or ''}"
            c = props[pk]
            c["n"] += 1
            c["a_hits"] += ra["hit"]
            c["b_hits"] += rb["hit"]
            if ra["line"] != rb["line"]:
                c["line_moved"] += 1
        for c in props.values():
            _finish_pair_cell(c)
        out[label] = {
            k: v for k, v in sorted(props.items(), key=lambda kv: -kv[1]["n"]) if v["n"]
        }
    return out


def window_counts(rows: list[dict], pred) -> dict:
    out = {w: {"listed": 0, "decided_n": 0, "bronze": 0} for w in WINDOWS}
    for r in rows:
        if not pred(r):
            continue
        if r.get("badge") in ("Gold", "Silver"):
            out[r["window"]]["listed"] += 1
        else:
            out[r["window"]]["bronze"] += 1
        out[r["window"]]["decided_n"] += 1
    return out


def print_prop_tables(title: str, by_prop: list, pairs: dict, show_bronze: bool = True) -> None:
    print(f"\n########## {title} ##########")
    for book in BOOKS:
        rows = [p for p in by_prop if p["book"] == book and p["n_lock_listed"] >= PROP_MIN_N]
        if not rows:
            rows = [p for p in by_prop if p["book"] == book and p["n_lock_listed"] >= 8]
        print(f"\n===== {book}  (lock_8am listed n>={PROP_MIN_N if any(p['n_lock_listed'] >= PROP_MIN_N and p['book']==book for p in by_prop) else 8}) =====")
        badges = ("Diamond", "Platinum", "Gold all", "Silver", "Bronze") if show_bronze else ("Diamond", "Platinum", "Gold all", "Silver")
        badge_hdr = ("Dia@8am", "Plat@8am", "Gold@8am", "Sil@8am", "Brz@8am") if show_bronze else ("Dia@8am", "Plat@8am", "Gold@8am", "Sil@8am")
        print(
            f"  {'sport':7} {'prop':22} "
            + "  ".join(f"{w[:10]:>12}" for w in PROP_PRINT_WINS)
            + "  "
            + "  ".join(f"{h:>12}" for h in badge_hdr)
        )
        for p in rows:
            wins = p["windows"]
            bits = []
            for w in PROP_PRINT_WINS:
                bits.append(f"{fmt((wins.get(w) or {}).get('Listed')):>12}")
            lock = wins.get("lock_8am") or {}
            for b in badges:
                bits.append(f"{fmt(lock.get(b)):>12}")
            print(f"  {p['sport']:7} {p['prop'][:22]:22} " + "  ".join(bits))
    print("\n--- window movers (listed, both n>={}) ---".format(PROP_MOVE_N))
    movers = []
    for p in by_prop:
        wins = p["windows"]
        a = (wins.get("day_ahead") or {}).get("Listed") or empty()
        b = (wins.get("lock_8am") or {}).get("Listed") or empty()
        c = (wins.get("last_hour") or {}).get("Listed") or empty()
        if a["n"] >= PROP_MOVE_N and b["n"] >= PROP_MOVE_N and a.get("hr") is not None and b.get("hr") is not None:
            movers.append((abs(b["hr"] - a["hr"]), "day_ahead vs lock_8am", p, a, b))
        if c["n"] >= PROP_MOVE_N and b["n"] >= PROP_MOVE_N and c.get("hr") is not None and b.get("hr") is not None:
            movers.append((abs(c["hr"] - b["hr"]), "lock_8am vs last_hour", p, b, c))
        aft = (wins.get("afternoon") or {}).get("Listed") or empty()
        if aft["n"] >= PROP_MOVE_N and b["n"] >= PROP_MOVE_N and aft.get("hr") is not None and b.get("hr") is not None:
            movers.append((abs(aft["hr"] - b["hr"]), "lock_8am vs afternoon", p, b, aft))
    movers.sort(key=lambda x: (-x[0], -x[3]["n"]))
    movers = [m for m in movers if m[0] >= 0.03]
    if not movers:
        print("  none with both sides n>={} and |d|>=3pp".format(PROP_MOVE_N))
    for _, label, p, left, right in movers[:40]:
        dpp = 100 * ((right.get("hr") or 0) - (left.get("hr") or 0))
        print(
            f"  {p['sport']:7} {p['book']:16} {p['prop'][:20]:20} {label:24} "
            f"{fmt(left)} -> {fmt(right)}  d={dpp:+.1f}pp"
        )
    print("\n--- paired same-name (lock_8am vs afternoon), n>=20 ---")
    pair = pairs.get("lock_8am vs afternoon") or {}
    shown = 0
    for key, cell in pair.items():
        if cell.get("n", 0) < 20:
            continue
        sport, book, prop = key.split("|", 2)
        print(
            f"  {sport:7} {book:16} {prop[:20]:20} n={cell['n']:4}  "
            f"{cell['a_hr']:.1%} -> {cell['b_hr']:.1%}  d={cell['delta']:+.1%}  "
            f"moved {cell['line_moved']}/{cell['n']}"
        )
        shown += 1
    if not shown:
        print("  none")


def print_ge4_vs_eq5(ge4: list, eq5: list) -> None:
    print("\n########## L5>=4 vs L5=5 at lock_8am listed, by prop ##########")
    ge4_map = {p["key"]: p for p in ge4}
    print(f"  {'sport':7} {'book':16} {'prop':22} {'L5>=4 8am':16} {'L5=5 8am':16} {'dpp':7}")
    rows = []
    for p in eq5:
        if p["n_lock_listed"] < PROP_MIN_N:
            continue
        g = ge4_map.get(p["key"])
        e8 = (p["windows"].get("lock_8am") or {}).get("Listed") or empty()
        g8 = ((g or {}).get("windows") or {}).get("lock_8am", {}).get("Listed") or empty()
        if not e8.get("n") or not g8.get("n"):
            continue
        dpp = 100 * ((e8.get("hr") or 0) - (g8.get("hr") or 0))
        rows.append((p, g8, e8, dpp))
    rows.sort(key=lambda x: -abs(x[3]))
    for p, g8, e8, dpp in rows:
        print(
            f"  {p['sport']:7} {p['book']:16} {p['prop'][:22]:22} "
            f"{fmt(g8):16} {fmt(e8):16} {dpp:+.1f}"
        )


def main() -> None:
    print("loading graded actuals…")
    actuals, sides, priors = load_grades()
    print("  actuals", len(actuals), "sides", len(sides), "players", len(priors))
    print("loading line_history snapshots…")
    snaps, n_raw, skipped_win = load_snapshots()
    print("  raw", n_raw, "windows kept", len(snaps), "dropped", skipped_win)
    print("scoring…")
    rows = score_rows(snaps, actuals, sides, priors)
    listed_ge4 = [r for r in rows if pred_ge4(r)]
    listed_eq5 = [r for r in rows if pred_eq5(r) and r.get("badge") in ("Gold", "Silver")]
    print("  scored", len(rows), "L5>=4 Gold/Silver", len(listed_ge4), "L5=5 Gold/Silver", len(listed_eq5))
    tables_ge4 = fill_block(rows, pred_ge4)
    tables_eq5 = fill_block(rows, pred_eq5)
    pairs_ge4 = paired(rows, pred_ge4)
    pairs_eq5 = paired(rows, pred_eq5)
    by_prop_ge4 = fill_by_prop(rows, pred_ge4)
    by_prop_eq5 = fill_by_prop(rows, pred_eq5)
    pairs_prop_ge4 = paired_by_prop(rows, pred_ge4)
    pairs_prop_eq5 = paired_by_prop(rows, pred_eq5)
    dates = sorted({r["date"] for r in listed_ge4}) if listed_ge4 else []
    payload = {
        "generated_at": datetime.now(ET).isoformat(timespec="seconds"),
        "source": str(DB),
        "note": (
            "Each prop is scored once per lock window using the last line_history "
            "print in that window vs the graded actual. L5/L10/season HR vs that "
            "snapshot line. Tennis is not in line_history. Last hour wins when it "
            "overlaps afternoon/evening. l5_ge4 = live list (Gold/Silver, L5>=4). "
            "l5_eq5 = L5 exactly 5, Gold/Silver listed plus Bronze as its own badge. "
            "by_prop uses canon_prop names (sport x book x market)."
        ),
        "from": dates[0] if dates else None,
        "to": dates[-1] if dates else None,
        "n_days": len(dates),
        "n_raw_history": n_raw,
        "n_window_snaps": len(snaps),
        "n_scored": len(rows),
        "n_listed": len(listed_ge4),
        "n_listed_l5_eq5": len(listed_eq5),
        "windows": list(WINDOWS),
        "primary_windows": list(PRIMARY),
        "window_counts": window_counts(rows, pred_ge4),
        "overall": tables_ge4["overall"],
        "by_sport": tables_ge4["by_sport"],
        "by_prop": by_prop_ge4,
        "paired": pairs_ge4,
        "paired_by_prop": pairs_prop_ge4,
        "l5_eq5": {
            "n_listed": len(listed_eq5),
            "window_counts": window_counts(rows, pred_eq5),
            "overall": tables_eq5["overall"],
            "by_sport": tables_eq5["by_sport"],
            "by_prop": by_prop_eq5,
            "paired": pairs_eq5,
            "paired_by_prop": pairs_prop_eq5,
        },
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print("wrote", OUT)
    print_ge4_vs_eq5(by_prop_ge4, by_prop_eq5)
    print_prop_tables("L5 >= 4 by prop  Gold/Silver", by_prop_ge4, pairs_prop_ge4, show_bronze=False)
    print_prop_tables("L5 == 5 by prop  Gold/Silver listed + Bronze", by_prop_eq5, pairs_prop_eq5, show_bronze=True)


if __name__ == "__main__":
    main()
