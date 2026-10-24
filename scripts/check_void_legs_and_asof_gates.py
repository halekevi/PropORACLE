#!/usr/bin/env python3
"""NO_ACTUAL legs, as-of MLB defense gate refit, and no-void Goblin ROI.

Does not write grade history and does not change ticket pricing.
"""
from __future__ import annotations

import importlib
import json
import math
import sys
import urllib.request
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT.parent / "PropORACLE_main_cp"
sys.path[:0] = [str(ROOT), str(ROOT / "scripts"), str(ROOT / "Sports" / "MLB" / "scripts")]

import build_ticket_eval as te  # noqa: E402
import check_gated_joint_calibration as cal  # noqa: E402
from report_gated_construction_efficiency import (  # noqa: E402
    discover_g70_archives,
    payload_from_g70,
)
from utils.defense_tiers import def_tier_from_overall_rank  # noqa: E402
from utils.gated_construction_tracks import GATED_GOBLIN, construction_track  # noqa: E402
from utils.pp_vegas_gap import is_grading_suspect  # noqa: E402
from utils.ticket_70_pool import canon_prop, norm_sport, ticket_gate_passes  # noqa: E402

import utils.mlb_keep_gates as mkg  # noqa: E402

CUT = "2026-08-26"
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}


def _fetch_json(url: str) -> dict:
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=40) as resp:
        return json.loads(resp.read().decode())


def _num(raw: object) -> float:
    try:
        if raw is None or str(raw).strip() in {"", "-", ".---"}:
            return 0.0
        return float(raw)
    except (TypeError, ValueError):
        return 0.0


def _name_key(raw: object) -> str:
    return "".join(ch for ch in str(raw or "").lower() if ch.isalnum())


def _pick_label(raw: object) -> str:
    kind = str(raw or "").lower()
    if "goblin" in kind:
        return "Goblin"
    if "standard" in kind or kind == "std":
        return "Standard"
    return ""


def _grade_goblin() -> tuple[list[dict], list[dict], list[dict]]:
    """Return goblin slips, no-void forward legs, and void legs."""
    slips: list[dict] = []
    forward: list[dict] = []
    voids: list[dict] = []
    for date_s, path in discover_g70_archives(start=None, end=None):
        payload = payload_from_g70(path, date_s)
        graded_merge_dates, _ = te.resolve_ticket_eval_graded_merge_dates(date_s, payload, [])
        if date_s not in graded_merge_dates:
            graded_merge_dates = [date_s] + list(graded_merge_dates)
        indices = te._load_actuals_indices(
            {}, graded_merge_dates, slate_date=date_s, payload=payload
        )
        injury = te._injury_dnp_by_sport(graded_merge_dates)
        for group in payload.get("groups") or []:
            gname = str(group.get("group_name") or "")
            for ticket in group.get("tickets") or []:
                if not isinstance(ticket, dict):
                    continue
                track = construction_track(ticket, gname)
                if track is None:
                    continue
                legs = [leg for leg in (ticket.get("legs") or []) if isinstance(leg, dict)]
                grades: list[str] = []
                pending: list[dict] = []
                decided_legs: list[dict] = []
                for leg in legs:
                    use = dict(leg)
                    if not use.get("direction") and use.get("side"):
                        use["direction"] = use["side"]
                    if not use.get("prop_type") and use.get("prop"):
                        use["prop_type"] = use["prop"]
                    matched = te._match_leg_to_row_multi(use, indices)
                    try:
                        line = float(use.get("line"))
                    except (TypeError, ValueError):
                        line = None
                    direction = str(use.get("direction") or use.get("side") or "").strip().upper()
                    actual = matched["actual"] if matched else None
                    grade_raw = matched["grade_raw"] if matched else ""
                    void_note = matched.get("void_note", "") if matched else ""
                    if matched and matched.get("line") is not None and line is None:
                        line = matched["line"]
                    g_leg = te._leg_grade(actual, line, direction, grade_raw, void_note)
                    g_leg = te._resolve_void_pending_if_injury_dnp(
                        g_leg, use, actual, line, grade_raw, void_note, injury
                    )
                    grades.append(g_leg)
                    use["grade_raw"] = grade_raw
                    use["void_note"] = void_note
                    rec = {
                        "date": date_s,
                        "player": str(leg.get("player") or leg.get("name") or ""),
                        "sport": str(leg.get("sport") or ""),
                        "prop": str(leg.get("prop") or leg.get("prop_type") or ""),
                        "team": str(leg.get("team") or ""),
                        "opp": str(leg.get("opp") or leg.get("opp_team") or ""),
                        "line": line,
                        "side": direction,
                        "pick": str(leg.get("pick_type") or ""),
                        "grade": g_leg,
                        "grade_raw": grade_raw,
                        "void_note": void_note,
                        "archive_actual": actual,
                    }
                    if g_leg == "VOID":
                        pending.append(rec)
                    elif g_leg in {"HIT", "MISS"}:
                        decided_legs.append(rec)
                if not grades or any(g == "UNGRADED" for g in grades):
                    continue
                if all(g == "VOID" for g in grades):
                    continue
                outcome = te._ticket_eval_money_outcome(gname, grades, ticket)
                if outcome.get("pending"):
                    continue
                result = str(outcome.get("result") or "")
                if result == "VOID_LOSS" and not te._ticket_pays_money(gname, grades):
                    continue
                win = bool(te._ticket_pays_money(gname, grades))
                quote = cal._f(ticket.get("power_payout")) or cal._f(
                    (ticket.get("n_correct") or {}).get(str(len(legs)))
                    or (ticket.get("n_correct") or {}).get(len(legs))
                )
                paid = cal._f(outcome.get("actual_payout"))
                slip = {
                    "date": date_s,
                    "track": track,
                    "n_legs": len(legs),
                    "voids": sum(1 for g in grades if g == "VOID"),
                    "misses": sum(1 for g in grades if g == "MISS"),
                    "win": win,
                    "result": result,
                    "quote_mult": quote,
                    "paid_mult": paid,
                    "net_10": cal._f(outcome.get("net_10")) or 0.0,
                    "payout_source": str(ticket.get("payout_source") or ""),
                }
                if slip["voids"] == 0:
                    forward.extend(decided_legs)
                if track != GATED_GOBLIN:
                    continue
                slips.append(slip)
                for rec in pending:
                    rec["slip_win"] = win
                    rec["slip_misses"] = slip["misses"]
                    rec["quote_mult"] = quote
                    rec["paid_mult"] = paid
                    voids.append(rec)
        print(f"graded {date_s}", flush=True)
    return slips, forward, voids


def _print_no_void_roi(slips: list[dict]) -> None:
    clean = [s for s in slips if s["voids"] == 0]
    voided = [s for s in slips if s["voids"] > 0]
    wins = sum(1 for s in clean if s["win"])
    roi = cal._roi([s["net_10"] for s in clean])
    all_roi = cal._roi([s["net_10"] for s in slips])
    void_roi = cal._roi([s["net_10"] for s in voided])
    paid = [float(s["paid_mult"]) for s in clean if s["win"] and s.get("paid_mult")]
    avg_paid = sum(paid) / len(paid) if paid else 0.0
    print(f"\nGoblin slips: {len(slips)}  with a void: {len(voided)}  no void: {len(clean)}")
    print(f"No-void win rate {wins}/{len(clean)} = {(wins / len(clean) if clean else 0):.1%}")
    print(f"No-void ROI {roi:+.2f}% on the archive quote (flat $10 in net_10)")
    print(f"  Average quoted multiple on those {len(paid)} wins: {avg_paid:.2f}x")
    print(f"  All 132 Goblin slips quote ROI {all_roi:+.2f}%. Void slips alone {void_roi:+.2f}%.")
    print("  No scraped payout is behind this ROI. Winner multiples are the quoted n_correct.")
    same = scaled = 0
    for s in voided:
        if not s["win"]:
            continue
        q, p = s.get("quote_mult"), s.get("paid_mult")
        if q and p and abs(float(p) - float(q)) < 0.05:
            same += 1
        elif q and p and float(p) < float(q) - 0.05:
            scaled += 1
    void_wins = sum(1 for s in voided if s["win"])
    print(
        f"Void-slip payouts are computed, not scraped: {void_wins} recorded wins, "
        f"{scaled} paid below the archive n_correct, {same} paid at that quote."
    )
    losses = [s for s in voided if not s["win"]]
    with_miss = sum(1 for s in losses if s["misses"] > 0)
    print(
        f"Void slips recorded as losses: {len(losses)}. "
        f"{with_miss} of them already have a decided miss, so filling the void leg does not flip the slip."
    )


def _json_index(dates: set[str]) -> dict[tuple, dict]:
    by_date: dict[str, Path] = {}
    for base in (MAIN / "ui_runner" / "templates", ROOT / "ui_runner" / "templates"):
        if not base.is_dir():
            continue
        for d in dates:
            p = base / f"graded_props_{d}.json"
            if p.is_file() and d not in by_date:
                by_date[d] = p
    out: dict[tuple, dict] = {}
    by_name: dict[tuple, list] = defaultdict(list)
    for d, path in by_date.items():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        for raw in payload.get("props") or []:
            if not isinstance(raw, dict):
                continue
            sport = norm_sport(str(raw.get("sport") or ""))
            prop = canon_prop(sport, str(raw.get("prop") or ""))
            key = (d, _name_key(raw.get("player")), sport, prop)
            prev = out.get(key)
            blob = str(raw.get("void_reason") or raw.get("result") or "")
            if prev is None or "NO_ACTUAL" in blob.upper():
                out[key] = raw
            by_name[(d, _name_key(raw.get("player")))].append(raw)
    return out, by_name


def _csv_hits(date_s: str, player: str) -> list[dict]:
    """Every actuals-csv row for this player on this date, any prop."""
    want = _name_key(player)
    found: list[dict] = []
    seen: set[str] = set()
    for d in te._actuals_csv_dirs_for_date(date_s):
        if not d.is_dir():
            continue
        for path in sorted(d.glob(f"actuals_*_{date_s}.csv")):
            key = str(path.resolve())
            if key in seen:
                continue
            seen.add(key)
            try:
                rows = te._load_actuals_csv_rows(path)
            except Exception:
                continue
            for row in rows:
                if _name_key(row.get("player") or row.get("Player")) != want:
                    continue
                found.append(
                    {
                        "file": path.name,
                        "prop": str(row.get("prop") or row.get("prop_type") or row.get("Prop") or ""),
                        "actual": row.get("actual"),
                    }
                )
    return found


def _attach_archive(voids: list[dict]) -> None:
    dates = {v["date"] for v in voids}
    idx, by_name = _json_index(dates)
    for v in voids:
        sport = norm_sport(v["sport"])
        prop = canon_prop(sport, v["prop"])
        raw = idx.get((v["date"], _name_key(v["player"]), sport, prop))
        if raw is None:
            cands = by_name.get((v["date"], _name_key(v["player"]))) or []
            raw = next(
                (
                    r
                    for r in cands
                    if _name_key(r.get("prop")) == _name_key(v["prop"])
                    or "NO_ACTUAL" in str(r.get("void_reason") or "").upper()
                ),
                cands[0] if len(cands) == 1 else None,
            )
        v["json_void_reason"] = str((raw or {}).get("void_reason") or "")
        v["json_result"] = str((raw or {}).get("result") or "")
        v["actual_source"] = str((raw or {}).get("actual_source") or "")
        v["actual_value"] = (raw or {}).get("actual_value")
        v["json_prop"] = str((raw or {}).get("prop") or "")
        v["json_found"] = raw is not None
        v["csv_rows"] = _csv_hits(v["date"], v["player"])


def _refetch_mlb(voids: list[dict]) -> None:
    s2 = importlib.import_module("step2_attach_picktypes_mlb")
    s4 = importlib.import_module("step4_attach_player_stats_mlb")
    id_map = dict(mkg._id_by_name())
    cache = mkg._ID_CACHE
    if not cache.is_file():
        cache = ROOT / "Sports" / "MLB" / "scripts" / "mlb_id_cache.csv"
    if cache.is_file():
        import csv

        with cache.open(encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                nm = s2.norm_name(str(row.get("player_norm") or row.get("player") or ""))
                pid = str(row.get("mlb_player_id") or "").strip()
                if nm and pid:
                    id_map.setdefault(nm, pid)
    logs: dict[tuple[str, str], list] = {}

    def splits(pid: str, group: str) -> list:
        key = (pid, group)
        if key not in logs:
            logs[key] = s4.fetch_game_log(pid, group, "2026")
        return logs[key]

    for v in voids:
        v["refetch"] = ""
        v["refetch_actual"] = None
        v["refetch_grade"] = ""
        if norm_sport(v["sport"]) != "MLB":
            v["refetch"] = "not_mlb"
            continue
        prop_norm = s2.norm_prop(v["prop"])
        ptype = s2.player_type(prop_norm)
        if ptype not in {"pitcher", "hitter"}:
            v["refetch"] = "bad_prop"
            continue
        pid = id_map.get(s2.norm_name(v["player"])) or id_map.get(mkg._norm_name(v["player"]))
        if not pid:
            v["refetch"] = "no_mlb_id"
            continue
        group = "pitching" if ptype == "pitcher" else "hitting"
        target = date.fromisoformat(v["date"])
        chosen = None
        used = ""
        for sp in splits(str(pid), group):
            gd = str(sp.get("date") or "")[:10]
            if gd == v["date"]:
                chosen = sp
                used = "same_day"
                break
        if chosen is None:
            nxt = (target + timedelta(days=1)).isoformat()
            for sp in splits(str(pid), group):
                if str(sp.get("date") or "")[:10] == nxt:
                    chosen = sp
                    used = "next_day"
                    break
        if chosen is None:
            v["refetch"] = "no_game_log"
            continue
        derive = s4.derive_pitcher_stat if ptype == "pitcher" else s4.derive_hitter_stat
        val = float(derive(chosen, prop_norm))
        if math.isnan(val):
            v["refetch"] = f"game_log_{used}_stat_missing"
            continue
        v["refetch"] = f"game_log_{used}"
        v["refetch_actual"] = val
        v["refetch_grade"] = te._leg_grade(val, v["line"], v["side"], "", "")


_PROP_ACTUAL = {
    "totalgames": "matchtotalgames",
    "totalgameswon": "gameswon",
    "gameswon": "gameswon",
    "goaliesaves": "goalkeepersaves",
    "rushyards": "rushingyards",
    "rushingyards": "rushingyards",
    "shots": "shots",
    "shotsontarget": "shotsontarget",
    "hitterstrikeouts": "strikeouts",
    "fgmade": "fgmade",
}


def _csv_value_for_leg(leg: dict) -> float | None:
    want = _PROP_ACTUAL.get(_name_key(leg.get("prop")), _name_key(leg.get("prop")))
    for row in leg.get("csv_rows") or []:
        if _name_key(row.get("prop")) != want:
            continue
        try:
            return float(row.get("actual"))
        except (TypeError, ValueError):
            continue
    return None


def _print_voids(voids: list[dict]) -> None:
    na = [v for v in voids if cal._void_reason(v) == "NO_ACTUAL"]
    other = [v for v in voids if cal._void_reason(v) != "NO_ACTUAL"]
    print(f"\nVoid legs on graded Goblin slips: {len(voids)}  NO_ACTUAL: {len(na)}  other: {len(other)}")
    for v in other:
        print(f"  other {v['date']} {v['sport']} {v['player']} {v['prop']} {cal._void_reason(v)}")
    groups: dict[tuple, list] = defaultdict(list)
    for v in na:
        src = v.get("actual_source") or "(blank)"
        groups[(v["sport"], v["prop"], v["date"], src)].append(v)
    print(f"  {'sport':<8} {'prop':<28} {'date':<12} {'n':>3} source")
    for key, chunk in sorted(groups.items(), key=lambda kv: (kv[0][0], kv[0][2], kv[0][1])):
        print(f"  {key[0]:<8} {key[1][:28]:<28} {key[2]:<12} {len(chunk):3d} {key[3]}")
    json_hit = sum(1 for v in na if v.get("json_found"))
    blank_src = sum(1 for v in na if not str(v.get("actual_source") or "").strip())
    empty_val = sum(
        1
        for v in na
        if v.get("actual_value") in (None, "", "null") and v.get("archive_actual") is None
    )
    reasons: dict[str, int] = defaultdict(int)
    for v in na:
        reasons[v.get("json_void_reason") or "(no json reason)"] += 1
    print(f"  graded json matched {json_hit}/{len(na)}; actual_source blank {blank_src}; archive actual empty {empty_val}")
    print("  json void_reason: " + ", ".join(f"{k}={n}" for k, n in sorted(reasons.items(), key=lambda kv: -kv[1])))
    absent = [v for v in na if not v.get("csv_rows")]
    present = [v for v in na if v.get("csv_rows")]
    print(f"  grade-time actuals csv: player absent {len(absent)}, player present {len(present)}")
    print("  rows the fetch file does contain:")
    for v in present:
        bits = []
        for row in v["csv_rows"]:
            bits.append(f"{row['file']} {row['prop']}={row['actual']}")
        val = _csv_value_for_leg(v)
        grade = ""
        if val is not None:
            grade = te._leg_grade(val, v["line"], v["side"], "", "")
            v["refetch_actual"] = val
            v["refetch_grade"] = grade
            v["refetch"] = "actuals_csv"
        print(
            f"    {v['date']} {v['sport']} {v['player']} {v['prop']} "
            f"-> {'; '.join(bits)}"
            + (f"  grades {grade}" if grade else "")
        )
    refetch: dict[str, int] = defaultdict(int)
    for v in na:
        refetch[v.get("refetch") or "(none)"] += 1
    print("  fresh MLB game log:")
    for k, n in sorted(refetch.items(), key=lambda kv: -kv[1]):
        print(f"    {k}: {n}")
    hit = sum(1 for v in na if v.get("refetch_grade") == "HIT")
    miss = sum(1 for v in na if v.get("refetch_grade") == "MISS")
    print(f"  recovered grades: HIT {hit}  MISS {miss}  still unresolved {len(na) - hit - miss}")
    win_miss = [v for v in na if v.get("slip_win") and v.get("refetch_grade") == "MISS"]
    win_hit = [v for v in na if v.get("slip_win") and v.get("refetch_grade") == "HIT"]
    loss_miss = [v for v in na if not v.get("slip_win") and v.get("refetch_grade") == "MISS"]
    loss_hit = [v for v in na if not v.get("slip_win") and v.get("refetch_grade") == "HIT"]
    print(
        f"  on recorded wins: void leg would have HIT {len(win_hit)}, MISS {len(win_miss)}"
    )
    print(
        f"  on recorded losses: void leg would have HIT {len(loss_hit)}, MISS {len(loss_miss)}"
    )
    win_slips_miss = {
        (v["date"], v["player"], v["prop"]) for v in win_miss
    }
    print(f"  recorded wins whose void leg is a miss: {len(win_slips_miss)}")
    print("  recovered legs:")
    for v in na:
        if not v.get("refetch_grade"):
            continue
        print(
            f"    {v['date']} {v['sport']} {v['player']} {v['prop']} "
            f"line {v['line']} actual {v['refetch_actual']} {v['refetch_grade']} "
            f"slip {'WIN' if v['slip_win'] else 'LOSS'} ({v['refetch']})"
        )


def _ranks(values: dict[str, float], *, low_is_first: bool) -> dict[str, int]:
    ordered = sorted(values, key=lambda k: values[k], reverse=not low_is_first)
    out: dict[str, int] = {}
    prev = None
    rank = 0
    for i, key in enumerate(ordered, start=1):
        if prev is None or values[key] != prev:
            rank = i
        out[key] = rank
        prev = values[key]
    return out


def _load_team_games() -> tuple[list[tuple], list[tuple]]:
    teams = (_fetch_json("https://statsapi.mlb.com/api/v1/teams?sportId=1").get("teams") or [])
    pitch: list[tuple] = []
    hit: list[tuple] = []
    for i, team in enumerate(teams, start=1):
        tid = team.get("id")
        abbr = mkg._abbr(team.get("abbreviation"))
        print(f"  team log {i}/{len(teams)} {abbr}", flush=True)
        for group, bucket in (("pitching", pitch), ("hitting", hit)):
            url = (
                f"https://statsapi.mlb.com/api/v1/teams/{tid}/stats"
                f"?stats=gameLog&group={group}&season=2026&gameType=R"
            )
            data = _fetch_json(url)
            splits = (data.get("stats") or [{}])[0].get("splits") or []
            for sp in splits:
                bucket.append((str(sp.get("date") or "")[:10], abbr, sp.get("stat") or {}))
    return pitch, hit


def _cumul(events: list[tuple], fields: tuple[str, ...]) -> dict[str, list[tuple[str, dict]]]:
    events = sorted((e for e in events if e[0]), key=lambda e: (e[0], e[1]))
    running: dict[str, dict[str, float]] = defaultdict(lambda: {k: 0.0 for k in fields} | {"g": 0.0})
    out: dict[str, list[tuple[str, dict]]] = defaultdict(list)
    for dt, abbr, stat in events:
        rec = running[abbr]
        for k in fields:
            rec[k] += _num(stat.get(k))
        rec["g"] += 1.0
        out[abbr].append((dt, dict(rec)))
    return out


def _before(hist: list[tuple[str, dict]], day: str) -> dict | None:
    # Last game strictly before day. Hist is date-sorted.
    lo, hi = 0, len(hist)
    while lo < hi:
        mid = (lo + hi) // 2
        if hist[mid][0] < day:
            lo = mid + 1
        else:
            hi = mid
    if lo == 0:
        return None
    return hist[lo - 1][1]


def _tiers_for(day: str, pitch_hist, hit_hist) -> dict[str, dict[str, str]]:
    pitch_rates: dict[str, tuple[float, float, float]] = {}
    hit_rates: dict[str, float] = {}
    teams = set(pitch_hist) | set(hit_hist)
    for abbr in teams:
        p = _before(pitch_hist.get(abbr) or [], day)
        h = _before(hit_hist.get(abbr) or [], day)
        if p and p.get("outs", 0) >= 27:
            ip = p["outs"] / 3.0
            era = (p["earnedRuns"] * 9.0 / ip) if ip else None
            whip = ((p["hits"] + p["baseOnBalls"]) / ip) if ip else None
            denom = p["atBats"] + p["baseOnBalls"] + p["hitByPitch"] + p["sacFlies"]
            obp = ((p["hits"] + p["baseOnBalls"] + p["hitByPitch"]) / denom) if denom else None
            if era is not None and whip is not None and obp is not None:
                pitch_rates[abbr] = (era, whip, obp)
        if h and h.get("g", 0) >= 1 and h.get("hits", 0) >= 0:
            hit_rates[abbr] = h["hits"] / h["g"]
    era_r = _ranks({k: v[0] for k, v in pitch_rates.items()}, low_is_first=True)
    whip_r = _ranks({k: v[1] for k, v in pitch_rates.items()}, low_is_first=True)
    obp_r = _ranks({k: v[2] for k, v in pitch_rates.items()}, low_is_first=True)
    score = {
        k: (3.0 * era_r[k] + 2.0 * whip_r[k] + 1.5 * obp_r[k]) / 6.5 for k in pitch_rates
    }
    def_r = _ranks(score, low_is_first=True)
    hit_r = _ranks(hit_rates, low_is_first=True)
    n_p, n_h = max(len(def_r), 1), max(len(hit_r), 1)
    out: dict[str, dict[str, str]] = {}
    for abbr in teams:
        rec: dict[str, str] = {}
        if abbr in def_r:
            rec["pitch"] = def_tier_from_overall_rank(def_r[abbr], n_p)
        if abbr in hit_r:
            rec["hits"] = def_tier_from_overall_rank(hit_r[abbr], n_h)
        if rec:
            out[abbr] = rec
    return out


def _stamp(row: dict, tiers: dict[str, dict[str, str]]) -> dict:
    out = dict(row)
    if not out.get("side") and out.get("direction"):
        out["side"] = out["direction"]
    team = mkg._abbr(out.get("team") or out.get("Team"))
    opp = mkg._abbr(out.get("opp_team") or out.get("Opp") or out.get("opp"))
    own = tiers.get(team) or {}
    opp_t = tiers.get(opp) or {}
    if own.get("pitch"):
        out["own_def_tier"] = own["pitch"]
        out["team_def_tier"] = own["pitch"]
    if own.get("hits"):
        out["team_off_hits_tier"] = own["hits"]
        out["own_off_hits_tier"] = own["hits"]
    if opp_t.get("pitch"):
        out["opp_def_tier"] = opp_t["pitch"]
    if opp_t.get("hits"):
        out["opp_off_hits_tier"] = opp_t["hits"]
    return out


def _cell_key(row: dict) -> tuple | None:
    sport = norm_sport(str(row.get("sport") or ""))
    side = str(row.get("side") or row.get("direction") or "").strip().upper()
    pick = _pick_label(row.get("pick_type"))
    prop = canon_prop(sport, str(row.get("prop") or ""))
    if not sport or not prop or side not in {"OVER", "UNDER"} or not pick:
        return None
    return (sport, prop, side, pick)


def _fit(forward: list[dict], pitch_hist, hit_hist) -> None:
    templates = ROOT / "ui_runner" / "templates"
    cache: dict[str, dict] = {}

    def tiers(day: str) -> dict:
        if day not in cache:
            cache[day] = _tiers_for(day, pitch_hist, hit_hist)
        return cache[day]

    current: dict[tuple, list[int]] = defaultdict(lambda: [0, 0])
    asof: dict[tuple, list[int]] = defaultdict(lambda: [0, 0])
    asof_na: dict[tuple, int] = defaultdict(int)
    files = 0
    for path in sorted(templates.glob("graded_props_*.json")):
        day = path.stem.replace("graded_props_", "")[:10]
        if len(day) != 10 or day >= CUT:
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        props = payload.get("props") if isinstance(payload, dict) else None
        if not isinstance(props, list):
            continue
        files += 1
        day_tiers = tiers(day)
        for raw in props:
            if not isinstance(raw, dict) or is_grading_suspect(raw):
                continue
            res = str(raw.get("result") or "").strip().upper()
            row = dict(raw)
            if not row.get("side") and row.get("direction"):
                row["side"] = row["direction"]
            key = _cell_key(row)
            if key is None:
                continue
            try:
                ok_now = ticket_gate_passes(row)
            except Exception:
                ok_now = False
            stamped = _stamp(row, day_tiers)
            try:
                ok_asof = ticket_gate_passes(stamped)
            except Exception:
                ok_asof = False
            if res in {"HIT", "MISS"}:
                hit = 1 if res == "HIT" else 0
                if ok_now:
                    current[key][0] += hit
                    current[key][1] += 1
                if ok_asof:
                    asof[key][0] += hit
                    asof[key][1] += 1
            elif ok_asof and ("NO_ACTUAL" in res or "NO_ACTUAL" in str(raw.get("void_reason") or "").upper()):
                asof_na[key] += 1
        if files % 25 == 0:
            print(f"  fit files {files}", flush=True)

    def score(cells: dict[tuple, list[int]], label: str) -> None:
        used = []
        missing = 0
        for leg in forward:
            if leg.get("grade") not in {"HIT", "MISS"}:
                continue
            sport = norm_sport(leg.get("sport") or "")
            key = (
                sport,
                canon_prop(sport, leg.get("prop") or ""),
                str(leg.get("side") or "").upper(),
                _pick_label(leg.get("pick")),
            )
            cell = cells.get(key)
            if not cell or cell[1] < 1:
                missing += 1
                continue
            used.append((key, cell[0] / cell[1], 1 if leg["grade"] == "HIT" else 0))
        if not used:
            print(f"  {label}: no forward matches")
            return
        pred = sum(p for _, p, _ in used) / len(used)
        got = sum(h for _, _, h in used) / len(used)
        print(
            f"  {label}: forward legs {len(used)} missing {missing}  "
            f"pre-period rate {pred:.1%}  forward hit {got:.1%}"
        )

    print(f"\nGate refit on graded props before {CUT} ({files} files in this checkout)")
    score(current, "current defense file")
    score(asof, "as-of defense, games before the slate")
    by: dict[tuple, list] = defaultdict(list)
    for leg in forward:
        if leg.get("grade") not in {"HIT", "MISS"}:
            continue
        sport = norm_sport(leg.get("sport") or "")
        key = (
            sport,
            canon_prop(sport, leg.get("prop") or ""),
            str(leg.get("side") or "").upper(),
            _pick_label(leg.get("pick")),
        )
        cell = asof.get(key)
        if cell and cell[1] >= 1:
            by[key].append(1 if leg["grade"] == "HIT" else 0)
    print(f"  {'cell':<42} {'pre n':>6} {'as-of':>7} {'excluded':>8} {'fwd n':>6} {'fwd':>7}")
    for key, hits in sorted(by.items(), key=lambda kv: -len(kv[1])):
        if len(hits) < 5:
            continue
        g_hits, g_n = asof[key]
        label = f"{key[0]} {key[3]} {key[2]} {key[1]}"[:42]
        fwd = sum(hits) / len(hits)
        print(
            f"  {label:<42} {g_n:6d} {g_hits / g_n:7.1%} {asof_na[key]:8d} "
            f"{len(hits):6d} {fwd:7.1%}"
        )
    excluded = sum(asof_na.values())
    decided = sum(v[1] for v in asof.values())
    print(
        f"  as-of gate rows with a result: {decided}  "
        f"same gate but NO_ACTUAL, left out of the rate: {excluded}"
    )


def main() -> int:
    slips, forward, voids = _grade_goblin()
    _print_no_void_roi(slips)
    print("\nAttaching graded-prop and actuals-csv fields...", flush=True)
    _attach_archive(voids)
    print("Refetching MLB game logs for the void legs...", flush=True)
    _refetch_mlb(voids)
    _print_voids(voids)
    if "--voids-only" in sys.argv:
        return 0
    print("\nLoading 2026 team game logs for as-of defense...", flush=True)
    pitch, hit = _load_team_games()
    outs_n = sum(_num(st.get("outs")) for _, _, st in pitch)
    print(
        f"  pitching games {len(pitch)}  mean outs/game "
        f"{(outs_n / len(pitch) if pitch else 0):.1f}"
    )
    pitch_hist = _cumul(
        pitch,
        ("outs", "earnedRuns", "hits", "baseOnBalls", "hitByPitch", "atBats", "sacFlies"),
    )
    hit_hist = _cumul(hit, ("hits",))
    _fit(forward, pitch_hist, hit_hist)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
