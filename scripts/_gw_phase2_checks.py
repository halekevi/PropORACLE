"""Phase 2: hand-verify GW, gate branches, same-window, reconcile, 11pm, soccer.

Uses the already-written slip manifest. Does not re-grade the full archive set.
"""
from __future__ import annotations

import json
import sys
import urllib.request
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT.parent / "PropORACLE_main_cp"
sys.path[:0] = [str(ROOT), str(ROOT / "scripts"), str(ROOT / "Sports" / "Tennis" / "scripts")]

import build_ticket_eval as te  # noqa: E402
from report_gated_construction_efficiency import discover_g70_archives, payload_from_g70  # noqa: E402
from utils.eastern_slate_date import eastern_slate_date  # noqa: E402
from utils.gated_construction_tracks import GATED_GOBLIN, construction_track  # noqa: E402
from utils.leg_start_window import is_placeholder_start, usable_start_date  # noqa: E402
from utils.prop_norm import canon_prop  # noqa: E402
from utils.soccer_board_coverage import covered_events, event_matches_sides, match_sides  # noqa: E402
from utils.tennis_keep_gates import (  # noqa: E402
    GAMES_WON_L5_SECOND_WON_MIN,
    GAMES_WON_STD_DISCOUNT_MIN,
    tennis_games_won_big_favorite_fade,
    tennis_games_won_factor_keep,
    tennis_goblin_keep_eligible,
    tennis_l5_second_won_pct,
    tennis_opp_lefty,
    tennis_rank_gap,
    tennis_std_discount,
)
from utils.ticket_70_pool import norm_sport  # noqa: E402

HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
ET = ZoneInfo("America/New_York")
MANIFEST = ROOT / "data" / "reports" / "gated_goblin_slip_manifest_20261007.json"
PREV_MANIFEST = ROOT / "data" / "reports" / "gated_goblin_slip_manifest_prev.json"
OUT = ROOT / "data" / "reports" / "gw_phase2_20261007.json"


def _p(*a):
    print(*a, flush=True)


def _espn_get(url: str):
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode())


def _hand_score(player: str, day: str) -> dict | None:
    want = " ".join(player.casefold().split())
    for tour in ("atp", "wta"):
        for ymd in (day, (date.fromisoformat(day) + timedelta(days=1)).isoformat()):
            url = (
                f"https://site.api.espn.com/apis/site/v2/sports/tennis/{tour}/scoreboard"
                f"?dates={ymd.replace('-', '')}"
            )
            try:
                payload = _espn_get(url)
            except Exception as exc:
                _p(f"  ESPN fail {tour} {ymd}: {exc}")
                continue
            for ev in payload.get("events") or []:
                for comp in ev.get("competitions") or []:
                    comps = list(comp.get("competitors") or [])
                    names = [
                        str(((c.get("athlete") or {}).get("displayName") or ""))
                        for c in comps
                    ]
                    if not any(want in " ".join(n.casefold().split()) for n in names):
                        continue
                    status = str(((comp.get("status") or {}).get("type") or {}).get("name") or "")
                    detail = str(((comp.get("status") or {}).get("type") or {}).get("detail") or "")
                    mine = next(
                        (
                            c
                            for c in comps
                            if want
                            in " ".join(
                                str(((c.get("athlete") or {}).get("displayName") or "")).casefold().split()
                            )
                        ),
                        None,
                    )
                    if mine is None:
                        continue
                    lines = mine.get("linescores") or []
                    games = sum(float((x or {}).get("value") or 0) for x in lines)
                    set_bits = [str(int(float((x or {}).get("value") or 0))) for x in lines]
                    other = next((c for c in comps if c is not mine), None)
                    other_bits = []
                    if other is not None:
                        ols = other.get("linescores") or []
                        other_bits = [str(int(float((x or {}).get("value") or 0))) for x in ols]
                    score_line = "-".join(
                        f"{a}-{b}" for a, b in zip(set_bits, other_bits or ["?"] * len(set_bits))
                    )
                    return {
                        "event": ev.get("id"),
                        "date": ev.get("date"),
                        "status": status,
                        "detail": detail,
                        "games_won_hand": games,
                        "score_line": score_line,
                        "retired": any(
                            tok in (status + " " + detail).upper()
                            for tok in ("RET", "ABANDON", "WALKOVER", "W/O")
                        ),
                    }
    return None


def _starts_for(day: str) -> dict[str, str]:
    by_player: dict[str, Counter] = defaultdict(Counter)
    root = MAIN / "outputs" / day
    files = list(root.glob("tennis/step1_tennis_props.csv")) + list(
        root.glob("step1_tennis_props.csv")
    )
    for path in files:
        try:
            df = pd.read_csv(path, dtype=str, encoding_errors="replace")
        except Exception:
            continue
        cols = {str(c).lower(): c for c in df.columns}
        pc = cols.get("player") or cols.get("player_name")
        sc = cols.get("start_time") or cols.get("game_time")
        if not (pc and sc):
            continue
        for player, start in zip(df[pc], df[sc], strict=False):
            if player is None or start is None:
                continue
            name = str(player).strip().lower()
            stamp = str(start).strip()
            if name and stamp and stamp.lower() != "nan":
                by_player[name][stamp] += 1
    return {n: c.most_common(1)[0][0] for n, c in by_player.items() if c}


_step8_cache: dict[str, pd.DataFrame] = {}


def _step8(day: str) -> pd.DataFrame:
    if day in _step8_cache:
        return _step8_cache[day]
    root = MAIN / "outputs" / day
    files = (
        list(root.glob("step8_*tennis*clean*.parquet"))
        + list(root.glob("tennis/*clean*.parquet"))
        + list(root.glob("step8_*tennis*clean*.xlsx"))
    )
    for path in files:
        try:
            df = pd.read_parquet(path) if path.suffix == ".parquet" else pd.read_excel(path)
            _step8_cache[day] = df
            return df
        except Exception:
            continue
    _step8_cache[day] = pd.DataFrame()
    return _step8_cache[day]


def _row_for(day: str, player: str, prop: str) -> dict:
    df = _step8(day)
    if df.empty:
        return {}
    cols = {str(c).lower(): c for c in df.columns}
    pc = cols.get("player") or cols.get("player_name")
    prop_c = cols.get("prop_type") or cols.get("prop")
    if not (pc and prop_c):
        return {}
    want_p = player.strip().lower()
    want_prop = canon_prop("TENNIS", prop)
    for _, r in df.iterrows():
        if str(r.get(pc) or "").strip().lower() != want_p:
            continue
        if canon_prop("TENNIS", str(r.get(prop_c) or "")) != want_prop:
            continue
        return {str(c): r.get(c) for c in df.columns}
    return {}


def _branch(row: dict) -> dict:
    disc = tennis_std_discount(row)
    lefty = tennis_opp_lefty(row)
    sw = tennis_l5_second_won_pct(row)
    gap = tennis_rank_gap(row)
    fade = tennis_games_won_big_favorite_fade(row)
    factor = tennis_games_won_factor_keep(row)
    keep = tennis_goblin_keep_eligible(row)
    branch = "none"
    inputs_filled = {
        "disc": disc is not None,
        "lefty": lefty is not None,
        "second_won": sw is not None,
        "rank_gap": gap is not None,
        "l10": (row.get("l10_over") or row.get("L10 Over") or row.get("l10")) is not None,
    }
    if fade:
        branch = "fade_50plus"
    elif disc is not None and disc >= GAMES_WON_STD_DISCOUNT_MIN:
        branch = "std_discount"
    elif factor:
        if lefty is True:
            branch = "l10_lefty"
        elif sw is not None and sw >= GAMES_WON_L5_SECOND_WON_MIN:
            branch = "l10_2nd_won"
        else:
            branch = "factor_other"
    return {
        "keep": keep,
        "branch": branch,
        "disc": disc,
        "lefty": lefty,
        "second_won": sw,
        "rank_gap": gap,
        "fade": fade,
        "l5": row.get("l5_over") or row.get("L5 Over") or row.get("l5"),
        "l10": row.get("l10_over") or row.get("L10 Over") or row.get("l10"),
        "inputs_filled": inputs_filled,
        "opp_lefty_raw": row.get("opp_lefty") or row.get("vs_lefty") or row.get("opponent_hand"),
        "second_won_raw": row.get("l5_second_won_pct")
        or row.get("second_won_l5")
        or row.get("L5 2nd Won %"),
    }


def _collect_gw_legs() -> list[dict]:
    """Re-grade only tennis Games Won / Total Games legs on gated slips (fast path)."""
    te.PROP_ALIAS_INDEX = True
    cache = {}
    gw: list[dict] = []
    tg: list[dict] = []
    starts_cache: dict[str, dict[str, str]] = {}

    def indices(day):
        if day not in cache:
            cache[day] = (
                te._load_actuals_indices({}, [day], slate_date=day, payload={}),
                te._injury_dnp_by_sport([day]),
            )
        return cache[day]

    for date_s, path in discover_g70_archives(start=None, end=None):
        payload = payload_from_g70(path, date_s)
        starts = starts_cache.setdefault(date_s, _starts_for(date_s))
        base_idx, base_inj = indices(date_s)
        for group in payload.get("groups") or []:
            gname = str(group.get("group_name") or "")
            for ticket in group.get("tickets") or []:
                if not isinstance(ticket, dict):
                    continue
                if construction_track(ticket, gname) != GATED_GOBLIN:
                    continue
                for leg in ticket.get("legs") or []:
                    if not isinstance(leg, dict):
                        continue
                    if norm_sport(str(leg.get("sport") or "")).lower() != "tennis":
                        continue
                    prop = str(leg.get("prop") or leg.get("prop_type") or "")
                    prop_k = canon_prop("TENNIS", prop)
                    if prop_k not in {"games_won", "match_total_games"}:
                        continue
                    player = str(leg.get("player") or "")
                    start = starts.get(player.lower(), "")
                    use = dict(leg)
                    if not use.get("direction") and use.get("side"):
                        use["direction"] = use["side"]
                    if not use.get("prop_type") and use.get("prop"):
                        use["prop_type"] = use["prop"]
                    matched = te._match_leg_to_row_multi(use, base_idx)
                    try:
                        line = float(use.get("line"))
                    except (TypeError, ValueError):
                        line = None
                    direction = str(use.get("direction") or use.get("side") or "").strip().upper()
                    actual = matched["actual"] if matched else None
                    grade_raw = matched["grade_raw"] if matched else ""
                    void_note = matched.get("void_note", "") if matched else ""
                    g = te._leg_grade(actual, line, direction, grade_raw, void_note)
                    g = te._resolve_void_pending_if_injury_dnp(
                        g, use, actual, line, grade_raw, void_note, base_inj
                    )
                    source = "slate"
                    if g not in {"HIT", "MISS"} and start and not is_placeholder_start(start):
                        day = usable_start_date(start)
                        if day and day != date_s:
                            idx2, inj2 = indices(day)
                            matched2 = te._match_leg_to_row_multi(use, idx2)
                            actual2 = matched2["actual"] if matched2 else None
                            g2 = te._leg_grade(
                                actual2,
                                line,
                                direction,
                                matched2["grade_raw"] if matched2 else "",
                                matched2.get("void_note", "") if matched2 else "",
                            )
                            g2 = te._resolve_void_pending_if_injury_dnp(
                                g2,
                                use,
                                actual2,
                                line,
                                matched2["grade_raw"] if matched2 else "",
                                matched2.get("void_note", "") if matched2 else "",
                                inj2,
                            )
                            if g2 in {"HIT", "MISS"}:
                                g = g2
                                actual = actual2
                                source = f"fill:{day}"
                    if g not in {"HIT", "MISS"}:
                        continue
                    rec = {
                        "date": date_s,
                        "player": player,
                        "prop": prop,
                        "prop_k": prop_k,
                        "line": line,
                        "side": direction or "OVER",
                        "grade": g,
                        "actual": actual,
                        "start": start,
                        "source": source,
                        "pick_type": str(leg.get("pick_type") or "Goblin"),
                    }
                    if prop_k == "games_won":
                        gw.append(rec)
                    else:
                        tg.append(rec)
    return gw, tg


def _legacy_keys_no_start_fill() -> set[str]:
    """Approximate the prior 148 set: alias-on scored slips, slate grades only, voids pay."""
    te.PROP_ALIAS_INDEX = True
    keys: set[str] = set()
    cache = {}

    def indices(day):
        if day not in cache:
            cache[day] = (
                te._load_actuals_indices({}, [day], slate_date=day, payload={}),
                te._injury_dnp_by_sport([day]),
            )
        return cache[day]

    for date_s, path in discover_g70_archives(start=None, end=None):
        payload = payload_from_g70(path, date_s)
        base_idx, base_inj = indices(date_s)
        for group in payload.get("groups") or []:
            gname = str(group.get("group_name") or "")
            for ticket in group.get("tickets") or []:
                if not isinstance(ticket, dict):
                    continue
                if construction_track(ticket, gname) != GATED_GOBLIN:
                    continue
                legs = [leg for leg in (ticket.get("legs") or []) if isinstance(leg, dict)]
                if not legs:
                    continue
                grades = []
                for leg in legs:
                    use = dict(leg)
                    if not use.get("direction") and use.get("side"):
                        use["direction"] = use["side"]
                    if not use.get("prop_type") and use.get("prop"):
                        use["prop_type"] = use["prop"]
                    matched = te._match_leg_to_row_multi(use, base_idx)
                    try:
                        line = float(use.get("line"))
                    except (TypeError, ValueError):
                        line = None
                    direction = str(use.get("direction") or use.get("side") or "").strip().upper()
                    actual = matched["actual"] if matched else None
                    g = te._leg_grade(
                        actual,
                        line,
                        direction,
                        matched["grade_raw"] if matched else "",
                        matched.get("void_note", "") if matched else "",
                    )
                    g = te._resolve_void_pending_if_injury_dnp(
                        g,
                        use,
                        actual,
                        line,
                        matched["grade_raw"] if matched else "",
                        matched.get("void_note", "") if matched else "",
                        base_inj,
                    )
                    grades.append(g)
                if any(g == "UNGRADED" for g in grades):
                    continue
                if all(g == "VOID" for g in grades):
                    continue
                # money outcome (voids pay)
                if te._ticket_pays_money(gname, grades) is None and all(
                    g in {"HIT", "MISS", "VOID"} for g in grades
                ):
                    # still a scored slip if at least one HIT/MISS
                    if not any(g in {"HIT", "MISS"} for g in grades):
                        continue
                tid = str(ticket.get("ticket_id") or ticket.get("id") or "").strip()
                sid = f"{date_s}|{tid}" if tid else f"{date_s}|legs"
                keys.add(sid)
    return keys


def main() -> None:
    result: dict = {}
    man = json.loads(MANIFEST.read_text(encoding="utf-8"))
    slips = {s["id"]: s for s in man["slips"]}
    _p(f"MANIFEST loaded n={len(slips)} resolved={man['resolved']} unresolved={man['unresolved']}")

    # ---- reconcile vs prev / legacy ----
    _p("\n=== RECONCILE ===")
    if PREV_MANIFEST.is_file():
        old = json.loads(PREV_MANIFEST.read_text(encoding="utf-8"))
        old_ids = {s["id"] for s in old.get("slips") or []}
    else:
        _p("building legacy slate-only key set (proxy for prior 148)...")
        old_ids = _legacy_keys_no_start_fill()
        _p(f"legacy proxy n={len(old_ids)}")
        PREV_MANIFEST.write_text(
            json.dumps(
                {
                    "written_at": datetime.now(ET).isoformat(timespec="seconds"),
                    "note": "slate-only alias-on scored proxy for prior set",
                    "n": len(old_ids),
                    "slips": [{"id": i} for i in sorted(old_ids)],
                },
                indent=2,
            ),
            encoding="utf-8",
        )
    new_ids = set(slips)
    added = sorted(new_ids - old_ids)
    removed = sorted(old_ids - new_ids)
    _p(f"ADDED {len(added)}")
    for sid in added:
        s = slips.get(sid, {})
        _p(f"  + {sid} resolved={s.get('resolved')} reason={s.get('unresolved_reason','')}")
    _p(f"REMOVED {len(removed)}")
    for sid in removed[:50]:
        _p(f"  - {sid}")
    result["reconcile"] = {
        "old_n": len(old_ids),
        "new_n": len(new_ids),
        "added": added,
        "removed": removed,
    }

    # ---- collect GW legs ----
    _p("\n=== COLLECT GW LEGS ===")
    gw, tg = _collect_gw_legs()
    _p(f"GATED_GAMES_WON n={len(gw)} hits={sum(1 for r in gw if r['grade']=='HIT')}")
    _p(f"GATED_TOTAL_GAMES n={len(tg)} hits={sum(1 for r in tg if r['grade']=='HIT')}")
    result["gw_n"] = len(gw)
    result["gw_hits"] = sum(1 for r in gw if r["grade"] == "HIT")

    # ---- gate branches ----
    _p("\n=== GATE BRANCHES ===")
    branch_hits: dict[str, Counter] = defaultdict(Counter)
    branch_rows = []
    blank_factor = 0
    for rec in gw:
        row = _row_for(rec["date"], rec["player"], rec["prop"])
        row.setdefault("sport", "TENNIS")
        row.setdefault("pick_type", rec["pick_type"])
        row.setdefault("side", rec["side"])
        row.setdefault("line", rec["line"])
        row.setdefault("player", rec["player"])
        row.setdefault("prop", rec["prop"])
        # prefer archive L5/L10 if present on leg — step8 may differ
        info = _branch(row)
        if info["branch"] in {"l10_lefty", "l10_2nd_won", "factor_other"} and (
            not info["inputs_filled"]["lefty"] and not info["inputs_filled"]["second_won"]
        ):
            blank_factor += 1
        branch_hits[info["branch"]][rec["grade"]] += 1
        branch_rows.append({**rec, **info})
        filled = info["inputs_filled"]
        _p(
            f"  {rec['date']} {rec['player']} line={rec['line']} grade={rec['grade']} "
            f"branch={info['branch']} disc={info['disc']} lefty={info['lefty']} "
            f"2nd={info['second_won']} l10={info['l10']} fade={info['fade']} "
            f"filled={filled} lefty_raw={info['opp_lefty_raw']!r} "
            f"2nd_raw={info['second_won_raw']!r}"
        )
    _p("BRANCH_SUMMARY")
    for b, c in sorted(branch_hits.items()):
        n = c["HIT"] + c["MISS"]
        _p(f"  {b}: {c['HIT']}/{n} ({c['HIT']/n if n else None:.1%})")
    _p(f"factor branches with blank lefty AND blank 2nd-won: {blank_factor}")
    result["branches"] = {
        b: {"HIT": c["HIT"], "MISS": c["MISS"]} for b, c in branch_hits.items()
    }

    # ---- hand verify 10 ----
    _p("\n=== HAND VERIFY 10 ===")
    fills = [r for r in gw if str(r["source"]).startswith("fill")]
    hits = [r for r in gw if r["grade"] == "HIT"]
    misses = [r for r in gw if r["grade"] == "MISS"]
    sample = []
    for bucket in (fills, misses, hits):
        for r in bucket:
            if r not in sample:
                sample.append(r)
            if len(sample) >= 10:
                break
        if len(sample) >= 10:
            break
    hand_rows = []
    for rec in sample:
        day = usable_start_date(rec["start"]) or rec["date"]
        hand = _hand_score(rec["player"], day)
        stored = rec["actual"]
        if hand is None:
            _p(f"  {rec['date']} {rec['player']} stored={stored} ESPN=NOT_FOUND day={day}")
            hand_rows.append({**rec, "hand": None, "match": False})
            continue
        hand_g = hand["games_won_hand"]
        try:
            line = float(rec["line"])
        except (TypeError, ValueError):
            line = None
        hand_grade = "HIT" if line is not None and hand_g > line else (
            "MISS" if line is not None else None
        )
        match = (
            stored is not None
            and abs(float(stored) - hand_g) < 0.01
            and (hand_grade is None or hand_grade == rec["grade"])
        )
        _p(
            f"  {rec['date']} {rec['player']} line={rec['line']} stored={stored} "
            f"hand={hand_g} score={hand['score_line']} status={hand['status']} "
            f"retired={hand['retired']} grade={rec['grade']} hand_grade={hand_grade} "
            f"MATCH={match}"
        )
        hand_rows.append(
            {
                **rec,
                "hand": hand_g,
                "score_line": hand["score_line"],
                "retired": hand["retired"],
                "hand_grade": hand_grade,
                "match": match,
            }
        )
    result["hand_verify"] = hand_rows

    # ---- same window / same line ----
    _p("\n=== SAME WINDOW SAME LINE ===")
    dates = sorted({r["date"] for r in gw})
    gated_keys = {
        (r["date"], r["player"].lower(), float(r["line"]))
        for r in gw
        if r["line"] is not None
    }
    by_line_g: dict[float, Counter] = defaultdict(Counter)
    by_line_u: dict[float, Counter] = defaultdict(Counter)
    gated_all = Counter()
    ungated_all = Counter()
    for day in dates:
        path = MAIN / "ui_runner" / "templates" / f"graded_props_{day}.json"
        if not path.is_file():
            path = ROOT / "ui_runner" / "templates" / f"graded_props_{day}.json"
        if not path.is_file():
            continue
        raw = json.loads(path.read_text(encoding="utf-8"))
        for row in raw.get("props") or []:
            if not isinstance(row, dict):
                continue
            if not str(row.get("sport") or "").upper().startswith("TENNIS"):
                continue
            if "goblin" not in str(row.get("pick_type") or "").lower():
                continue
            if str(row.get("direction") or "").upper() != "OVER":
                continue
            if canon_prop("TENNIS", str(row.get("prop") or "")) != "games_won":
                continue
            res = str(row.get("result") or "").upper()
            if res not in {"HIT", "MISS"}:
                continue
            try:
                line = float(row.get("line"))
            except (TypeError, ValueError):
                continue
            key = (day, str(row.get("player") or "").lower(), line)
            is_gated = key in gated_keys
            (gated_all if is_gated else ungated_all)[res] += 1
            (by_line_g if is_gated else by_line_u)[line][res] += 1
    gh, gn = gated_all["HIT"], gated_all["HIT"] + gated_all["MISS"]
    uh, un = ungated_all["HIT"], ungated_all["HIT"] + ungated_all["MISS"]
    _p(f"  gated catalog rows on slip dates: {gh}/{gn} = {gh/gn if gn else None}")
    _p(f"  ungated catalog rows on slip dates: {uh}/{un} = {uh/un if un else None}")
    _p(f"  slip-leg rate: {result['gw_hits']}/{result['gw_n']}")
    shared = sorted(set(by_line_g) & set(by_line_u))
    _p(f"  shared line levels: {len(shared)}")
    for line in shared:
        g = by_line_g[line]
        u = by_line_u[line]
        gnn = g["HIT"] + g["MISS"]
        unn = u["HIT"] + u["MISS"]
        _p(f"    line {line}: gated {g['HIT']}/{gnn} ungated {u['HIT']}/{unn}")
    templates = MAIN / "ui_runner" / "templates"
    if not templates.is_dir():
        templates = ROOT / "ui_runner" / "templates"
    cat_dates = []
    for path in sorted(templates.glob("graded_props_2026-*.json")):
        day = path.stem.replace("graded_props_", "")[:10]
        if "2026-08-26" <= day <= "2026-09-30":
            cat_dates.append(day)
    _p(
        f"  catalog window {cat_dates[0] if cat_dates else '?'}.."
        f"{cat_dates[-1] if cat_dates else '?'} n={len(cat_dates)}"
    )
    _p(f"  gated slip dates {dates[0]}..{dates[-1]} n={len(dates)}")
    _p(f"  overlap {len(set(dates) & set(cat_dates))}")
    result["same_window"] = {
        "gated": f"{gh}/{gn}",
        "ungated": f"{uh}/{un}",
        "shared_lines": shared,
        "overlap_dates": len(set(dates) & set(cat_dates)),
    }

    # ---- 11pm ----
    _p("\n=== 11PM STAMP ===")
    for day in ("2026-09-20", "2026-09-27", "2026-09-10"):
        step1 = MAIN / "outputs" / day / "tennis" / "step1_tennis_props.csv"
        if not step1.is_file():
            _p(f"  no step1 {day}")
            continue
        df = pd.read_csv(step1, dtype=str)
        col = "start_time" if "start_time" in df.columns else None
        if not col:
            _p(f"  step1 {day}: no start_time col cols={list(df.columns)[:8]}")
            continue
        n23 = df[col].fillna("").str.contains("T23:00:00").sum()
        non = df[~df[col].fillna("").str.contains("T23:00:00")]
        sample_non = non[col].dropna().head(3).tolist()
        _p(f"  step1 {day}: {n23}/{len(df)} are T23:00:00; sample non-23={sample_non}")
    miss = [
        s
        for s in slips.values()
        if s["unresolved_reason"] in {"missing_start", "bad_start", "placeholder_23"}
    ]
    recoverable = []
    for s in miss:
        starts = _starts_for(s["date"])
        for player in s["players"]:
            st = starts.get(player.lower(), "")
            if st and not is_placeholder_start(st) and usable_start_date(st):
                recoverable.append((s["id"], player, st))
                break
    _p(f"unresolved lacking usable start: {len(miss)}")
    _p(f"of those, step1 already has a real non-23 start: {len(recoverable)}")
    for row in recoverable[:15]:
        _p(f"  recover {row[0]} {row[1]} -> {row[2]}")
    # Can we backfill 23:00 from anywhere? Check if archive legs already lack start_time
    no_leg_start = 0
    for date_s, path in discover_g70_archives(start=None, end=None):
        payload = payload_from_g70(path, date_s)
        for group in payload.get("groups") or []:
            for ticket in group.get("tickets") or []:
                for leg in ticket.get("legs") or []:
                    if norm_sport(str(leg.get("sport") or "")).lower() != "tennis":
                        continue
                    if not leg.get("start_time") and not leg.get("game_time"):
                        no_leg_start += 1
    _p(f"tennis archive legs with no start_time field: {no_leg_start}")
    result["stamp"] = {
        "miss_start_slips": len(miss),
        "step1_recoverable": len(recoverable),
        "recoverable": recoverable[:30],
        "tennis_legs_no_start_field": no_leg_start,
    }

    # ---- soccer ----
    _p("\n=== SOCCER MISSES Sep 27 ===")
    day = "2026-09-27"
    day_dir = MAIN / "outputs" / day
    actuals_path = day_dir / f"actuals_soccer_{day}.csv"
    actuals = pd.read_csv(actuals_path, dtype=str)
    have = set(" ".join(str(p).casefold().split()) for p in actuals["player"] if str(p).strip())
    events = covered_events(day) or ()
    slate_files = list(day_dir.glob("**/step8*soccer*")) + list(day_dir.glob("step8*soccer*"))
    rows = []
    for path in slate_files:
        if path.suffix.lower() not in {".parquet", ".csv", ".xlsx"}:
            continue
        try:
            df = (
                pd.read_parquet(path)
                if path.suffix == ".parquet"
                else (
                    pd.read_csv(path, dtype=str)
                    if path.suffix == ".csv"
                    else pd.read_excel(path)
                )
            )
        except Exception:
            continue
        cols = {str(c).lower(): c for c in df.columns}
        pc = cols.get("player") or cols.get("player_name")
        if not pc:
            continue
        for _, r in df.iterrows():
            player = str(r.get(pc) or "").strip()
            if not player or " + " in player:
                continue
            start = r.get(cols.get("start_time") or cols.get("game_time") or "")
            if eastern_slate_date(start) != day:
                continue
            try:
                dt = datetime.fromisoformat(str(start).replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=ZoneInfo("UTC"))
                if dt.astimezone(ET) > datetime.now(ET) and day >= datetime.now(ET).date().isoformat():
                    continue
            except Exception:
                continue
            sides = match_sides(
                {
                    "sport": "SOCCER",
                    "matchup": r.get(cols.get("matchup") or "") if cols.get("matchup") else "",
                    "team": r.get(cols.get("team") or "") if cols.get("team") else "",
                    "opp_team": r.get(cols.get("opp_team") or cols.get("opponent") or "")
                    if (cols.get("opp_team") or cols.get("opponent"))
                    else "",
                    "start_time": start,
                }
            )
            if sides is None or not any(
                event_matches_sides(sides[0], sides[1], ev) for ev in events
            ):
                continue
            name = " ".join(player.casefold().split())
            prop = str(r.get(cols.get("prop_type") or cols.get("prop") or "") or "")
            league = str(r.get(cols.get("league") or cols.get("competition") or "") or "")
            flags = []
            for k, c in cols.items():
                if any(tok in k for tok in ("starter", "lineup", "minutes", "role", "usage", "off")):
                    val = str(r.get(c) or "").strip()
                    if val and val.lower() != "nan":
                        flags.append(f"{k}={val}")
            rows.append(
                {
                    "player": player,
                    "name": name,
                    "prop": prop,
                    "league": league,
                    "flags": "|".join(flags),
                    "missing": name not in have,
                }
            )
        if rows:
            _p(f"  slate file {path.name} rows={len(rows)}")
            break
    by_player = {}
    for r in rows:
        by_player.setdefault(r["name"], r)
    missing = [r for r in by_player.values() if r["missing"]]
    present = [r for r in by_player.values() if not r["missing"]]
    _p(f"started covered players {len(by_player)} missing {len(missing)}")

    def starter_bucket(flags: str) -> str:
        u = flags.upper()
        if any(tok in u for tok in ("STARTER", "=START", "HIGH_VOL", "HIGH", "ELITE")):
            return "starterish"
        if any(tok in u for tok in ("BENCH", "LOW", "DNP", "SIT")):
            return "benchish"
        if not flags:
            return "blank"
        return "other"

    _p("by starter flag (missing):", dict(Counter(starter_bucket(r["flags"]) for r in missing)))
    _p("by starter flag (present):", dict(Counter(starter_bucket(r["flags"]) for r in present)))
    _p("missing by league:", dict(Counter(r["league"] or "?" for r in missing)))
    _p(
        "missing by prop:",
        dict(Counter(canon_prop("SOCCER", r["prop"]) or r["prop"] or "?" for r in missing)),
    )
    # also prop-level among all started covered rows that miss actual (player-prop)
    miss_legs = [r for r in rows if r["missing"]]
    _p(
        "missing legs by prop:",
        dict(Counter(canon_prop("SOCCER", r["prop"]) or r["prop"] or "?" for r in miss_legs)),
    )
    _p("sample missing:")
    for r in missing[:20]:
        _p(f"  {r['player']} {r['prop']} league={r['league']!r} flags={r['flags'][:100]!r}")
    result["soccer"] = {
        "players": len(by_player),
        "missing": len(missing),
        "starter_missing": dict(Counter(starter_bucket(r["flags"]) for r in missing)),
        "league_missing": dict(Counter(r["league"] or "?" for r in missing)),
        "prop_missing_players": dict(
            Counter(canon_prop("SOCCER", r["prop"]) or r["prop"] or "?" for r in missing)
        ),
        "prop_missing_legs": dict(
            Counter(canon_prop("SOCCER", r["prop"]) or r["prop"] or "?" for r in miss_legs)
        ),
    }

    OUT.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    _p(f"\nWROTE {OUT}")


if __name__ == "__main__":
    main()
