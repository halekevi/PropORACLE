#!/usr/bin/env python3
"""Soccer game-line battery under data/reports/game_line_environment.md.

1. Multi-league closing-line backfill (pooled across leagues).
2. Name -> ESPN id crosswalk (club names on graded rows, not abbrs).
3. Pre-score per-bucket eligibility (200 treat + 200 ctrl). Refuse to score below floor.
4. Score the three named contrasts only when eligible (Bonferroni + pass rule).

Graded soccer rows often lack Team (em dash) but carry Opp. Join via opponent name
to the game, then take the player's side as the other club for team_win_prob.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))
sys.path.insert(0, str(_REPO / "scripts"))

from fetch_game_lines import (  # noqa: E402
    DB_PATH,
    _norm_team_name_key,
    fetch_core_odds_item,
    init_db,
    line_from_core_item,
    recompute_soccer_threeway_wp,
    refresh_crosswalk,
    run_fetch,
    stamp_team_ids_from_crosswalk,
)
from score_mlb_game_line_battery import (  # noqa: E402
    _pass_rule,
    _summarize,
)

GRADED = _REPO / "ui_runner" / "templates"
NOTE = _REPO / "data" / "reports" / "game_line_environment.md"
REPORT_JSON = _REPO / "data" / "reports" / "soccer_game_line_battery.json"

# Locked after per-game total histogram, before hits.
SOC_TOTAL_LOW_MAX: float | None = None
SOC_TOTAL_HIGH_MIN: float | None = None

# PrizePicks / grade club labels that differ from ESPN displayName keys.
SOCCER_NAME_ALIASES: dict[str, str] = {
    "RED BULL NEW YORK": "NEW YORK RED BULLS",
    "NY RED BULLS": "NEW YORK RED BULLS",
    "NYCFC": "NEW YORK CITY FC",
    "INTER MIAMI": "INTER MIAMI CF",
    "ATLETICO MADRID": "ATLETICO MADRID",
    "ATHLETIC BILBAO": "ATHLETIC CLUB",
    "SPORTING KC": "SPORTING KANSAS CITY",
    "EARTHQUAKES": "SAN JOSE EARTHQUAKES",
    "LAFC": "LAFC",
    "PSG": "PARIS SAINT GERMAIN",
    "PARIS SG": "PARIS SAINT GERMAIN",
    "MAN CITY": "MANCHESTER CITY",
    "MAN UTD": "MANCHESTER UNITED",
    "MAN UNITED": "MANCHESTER UNITED",
    "SPURS": "TOTTENHAM HOTSPUR",
    "TOTTENHAM": "TOTTENHAM HOTSPUR",
    "BAYERN": "BAYERN MUNICH",
    "DORTMUND": "BORUSSIA DORTMUND",
    "MGLADBACH": "BORUSSIA MONCHENGLADBACH",
    "INTER": "INTERNAZIONALE",
    "AC MILAN": "AC MILAN",
    "ATLANTICO DE SAN LUIS": "ATLETICO DE SAN LUIS",
}


def soccer_graded_dates() -> list[str]:
    out: list[str] = []
    for path in sorted(GRADED.glob("graded_props_*.json")):
        date = path.name[len("graded_props_") : -5]
        raw = path.read_text(encoding="utf-8", errors="ignore")
        if '"sport": "Soccer"' not in raw and '"sport":"Soccer"' not in raw:
            continue
        out.append(date)
    return out


def _soccer_game_count(date: str) -> int:
    if not DB_PATH.is_file():
        return 0
    conn = sqlite3.connect(str(DB_PATH))
    try:
        return int(
            conn.execute(
                "SELECT COUNT(*) FROM game_lines WHERE sport='Soccer' AND game_date=?",
                (date,),
            ).fetchone()[0]
        )
    finally:
        conn.close()


def backfill_soccer(dates: list[str], *, refresh: bool, sleep_s: float) -> dict[str, int]:
    conn = sqlite3.connect(str(DB_PATH))
    try:
        init_db(conn)
        refresh_crosswalk(conn, ["Soccer"])
        stamp_team_ids_from_crosswalk(conn)
    finally:
        conn.close()
    fetched = skipped = games = 0
    for i, d in enumerate(dates):
        before = _soccer_game_count(d)
        # Always re-fetch when --refresh: MLS-only remnants look "present" but lack
        # multi-league rows. Without refresh, skip only dates that already look full.
        need = refresh or before < 8
        if before and not need:
            skipped += 1
            games += before
            continue
        result = run_fetch(d, ["Soccer"], refresh=True)
        n = len(result.get("Soccer") or [])
        fetched += 1
        games += n
        print(f"  [{i + 1}/{len(dates)}] {d}: {n} Soccer games")
        if sleep_s > 0:
            time.sleep(sleep_s)
    return {"dates_fetched": fetched, "dates_skipped": skipped, "games": games}


def lock_total_cuts() -> tuple[float, float, dict]:
    """Fixed low/high from per-game Soccer totals (pooled leagues)."""
    global SOC_TOTAL_LOW_MAX, SOC_TOTAL_HIGH_MIN
    conn = sqlite3.connect(str(DB_PATH))
    try:
        totals = [
            float(r[0])
            for r in conn.execute(
                "SELECT total FROM game_lines WHERE sport='Soccer' "
                "AND total IS NOT NULL AND is_closing=1"
            )
        ]
        if len(totals) < 50:
            totals = [
                float(r[0])
                for r in conn.execute(
                    "SELECT total FROM game_lines WHERE sport='Soccer' AND total IS NOT NULL"
                )
            ]
    finally:
        conn.close()
    if not totals:
        raise SystemExit("No Soccer totals in game_lines — fetch first")
    ctr = Counter(round(t, 1) for t in totals)
    # Soccer O/U bunches at 2.5 / 3.5. Lock those fixed values (same spirit as MLB 7.5/9.0).
    # Do not widen after seeing hits; thin high band -> ineligible, not a new cut.
    low_max, high_min = 2.5, 3.5
    SOC_TOTAL_LOW_MAX = low_max
    SOC_TOTAL_HIGH_MIN = high_min
    meta = {
        "n_games_with_total": len(totals),
        "histogram": dict(sorted(ctr.items())),
        "low_max": low_max,
        "high_min": high_min,
        "n_low": sum(1 for t in totals if t <= low_max),
        "n_high": sum(1 for t in totals if t >= high_min),
        "n_mid": sum(1 for t in totals if low_max < t < high_min),
    }
    return low_max, high_min, meta


def load_name_crosswalk() -> dict[str, str]:
    """Unique name/abbr key -> espn_team_id. Ambiguous keys dropped."""
    conn = sqlite3.connect(str(DB_PATH))
    try:
        rows = conn.execute(
            "SELECT abbr, espn_team_id, display_name FROM team_crosswalk WHERE sport='Soccer'"
        ).fetchall()
    finally:
        conn.close()
    out: dict[str, str] = {}
    amb: set[str] = set()

    def add(key: str, tid: str) -> None:
        k = _norm_team_name_key(key) if " " in key or len(key) > 4 else str(key or "").upper()
        if not k:
            return
        # Always normalize long/name-like keys; keep short abbrs upper.
        if len(k) > 3 or " " in k:
            k = _norm_team_name_key(key)
        else:
            k = str(key or "").strip().upper()
        if not k:
            return
        if k in out and out[k] != tid:
            amb.add(k)
        out[k] = tid

    for abbr, tid, display in rows:
        tid_s = str(tid)
        add(str(abbr or ""), tid_s)
        if display:
            add(str(display), tid_s)
            add(_norm_team_name_key(str(display)), tid_s)
    for alias, target in SOCCER_NAME_ALIASES.items():
        # Resolve alias through existing name map after first pass.
        tgt = _norm_team_name_key(target)
        tid = out.get(tgt) or out.get(target.upper())
        if tid:
            add(alias, tid)
    for k in amb:
        out.pop(k, None)
    return out


def load_soccer_games() -> dict[tuple[str, str], dict]:
    """(date, id:TID | abbr:AB) -> game rec."""
    conn = sqlite3.connect(str(DB_PATH))
    try:
        rows = conn.execute(
            """
            SELECT game_date, home_team, away_team, home_team_id, away_team_id,
                   spread, total, home_win_prob, away_win_prob, event_id,
                   provider, is_closing, captured_at
            FROM game_lines WHERE sport='Soccer'
            """
        ).fetchall()
    finally:
        conn.close()
    by: dict[tuple[str, str], dict] = {}
    for gdate, home, away, hid, aid, spread, total, hwp, awp, eid, prov, closing, cap in rows:
        rec = {
            "date": gdate,
            "home": str(home or "").upper(),
            "away": str(away or "").upper(),
            "hid": str(hid or ""),
            "aid": str(aid or ""),
            "spread": None if spread is None else float(spread),
            "total": None if total is None else float(total),
            "hwp": None if hwp is None else float(hwp),
            "awp": None if awp is None else float(awp),
            "event_id": str(eid or ""),
            "provider": str(prov or ""),
            "is_closing": closing,
            "captured_at": cap,
        }
        if rec["hid"]:
            by[(gdate, "id:" + rec["hid"])] = rec
        if rec["aid"]:
            by[(gdate, "id:" + rec["aid"])] = rec
        by[(gdate, "abbr:" + rec["home"])] = rec
        by[(gdate, "abbr:" + rec["away"])] = rec
    return by


def resolve_opp(
    games: dict,
    name_to_id: dict[str, str],
    date: str,
    opp: str,
) -> tuple[dict | None, bool | None]:
    """Match opponent club name to a game; player is the other side."""
    raw = str(opp or "").strip()
    if not raw or "/" in raw:
        return None, None
    alias = SOCCER_NAME_ALIASES.get(_norm_team_name_key(raw)) or SOCCER_NAME_ALIASES.get(raw.upper())
    keys = [_norm_team_name_key(raw), raw.upper()]
    if alias:
        keys.append(_norm_team_name_key(alias))
        keys.append(alias.upper())
    tid = None
    for k in keys:
        if k in name_to_id:
            tid = name_to_id[k]
            break
    rec = None
    if tid:
        rec = games.get((date, "id:" + tid))
    if rec is None:
        for k in keys:
            rec = games.get((date, "abbr:" + k))
            if rec:
                break
    if rec is None:
        return None, None
    # Opp matched home => player is away; opp matched away => player is home.
    if tid and tid == rec["hid"]:
        return rec, False
    if tid and tid == rec["aid"]:
        return rec, True
    opp_keys = {_norm_team_name_key(raw), raw.upper()}
    if alias:
        opp_keys.add(_norm_team_name_key(alias))
    home_n = _norm_team_name_key(rec["home"])
    away_n = _norm_team_name_key(rec["away"])
    if rec["home"] in opp_keys or home_n in opp_keys:
        return rec, False
    if rec["away"] in opp_keys or away_n in opp_keys:
        return rec, True
    # id matched but side unclear — if tid set, already returned above.
    return rec, None


def pre_score_bucket_counts() -> dict[str, dict]:
    """Count unique closing-line games per locked bucket before joining hits."""
    assert SOC_TOTAL_LOW_MAX is not None and SOC_TOTAL_HIGH_MIN is not None
    conn = sqlite3.connect(str(DB_PATH))
    try:
        rows = conn.execute(
            """
            SELECT event_id, game_date, home_team, away_team,
                   home_win_prob, away_win_prob, total, is_closing
            FROM game_lines WHERE sport='Soccer'
            """
        ).fetchall()
    finally:
        conn.close()

    heavy: set[str] = set()
    pickem: set[str] = set()
    low_tot: set[str] = set()
    high_tot: set[str] = set()
    for eid, gdate, home, away, hwp, awp, total, closing in rows:
        if closing != 1:
            continue
        key = str(eid or f"{gdate}|{home}|{away}")
        for wp in (hwp, awp):
            if wp is None:
                continue
            w = float(wp)
            if w >= 0.70:
                heavy.add(key)
            if 0.42 <= w <= 0.58:
                pickem.add(key)
        if total is None:
            continue
        t = float(total)
        if t <= SOC_TOTAL_LOW_MAX:
            low_tot.add(key)
        if t >= SOC_TOTAL_HIGH_MIN:
            high_tot.add(key)

    def pack(treat_n: int, ctrl_n: int) -> dict:
        ok = treat_n >= 200 and ctrl_n >= 200
        return {
            "treat_games": treat_n,
            "ctrl_games": ctrl_n,
            "eligible": ok,
            "status": "eligible" if ok else "ineligible (below floor)",
        }

    return {
        "over_heavy_fav_vs_pickem": pack(len(heavy), len(pickem)),
        "under_heavy_fav_vs_pickem": pack(len(heavy), len(pickem)),
        "over_high_vs_low_total": pack(len(high_tot), len(low_tot)),
        "note": (
            "Counts are unique closing-line games pooled across soccer leagues. "
            "Floor is 200 per bucket. After three-way WP, the >= 0.70 band can fall "
            "below the floor the same way MLB's heavy-favorite band did."
        ),
    }


def collect_rows(
    games: dict,
    name_to_id: dict[str, str],
    *,
    closing_only: bool,
) -> list[dict]:
    rows: list[dict] = []
    seen: set[tuple] = set()
    for path in sorted(GRADED.glob("graded_props_*.json")):
        date = path.name[len("graded_props_") : -5]
        data = json.loads(path.read_text(encoding="utf-8"))
        for row in data.get("props") or []:
            if str(row.get("sport") or "") != "Soccer":
                continue
            res = str(row.get("result") or "").upper()
            if res not in ("HIT", "MISS"):
                continue
            direction = str(row.get("direction") or row.get("over_under") or "").upper()
            if direction not in ("OVER", "UNDER"):
                continue
            opp = str(row.get("opp_team") or row.get("opp") or "").strip()
            if not opp or "/" in opp:
                continue
            prop = str(row.get("prop") or "").strip().lower().replace(" ", "_")
            key = (date, row.get("player"), prop, row.get("line"), direction, opp.upper())
            if key in seen:
                continue
            seen.add(key)
            rec, player_is_home = resolve_opp(games, name_to_id, date, opp)
            if rec is None or player_is_home is None:
                continue
            if closing_only and rec.get("is_closing") != 1:
                continue
            wp = rec["hwp"] if player_is_home else rec["awp"]
            if wp is None or rec.get("total") is None:
                continue
            game = rec["event_id"] or f"{date}|{rec['hid']}|{rec['aid']}"
            rows.append(
                {
                    "hit": 1 if res == "HIT" else 0,
                    "direction": direction,
                    "prop": prop,
                    "player": str(row.get("player") or ""),
                    "opp": opp.upper(),
                    "date": date,
                    "game": game,
                    "wp": float(wp),
                    "total": float(rec["total"]),
                    "heavy_fav": float(wp) >= 0.70,
                    "pickem": 0.42 <= float(wp) <= 0.58,
                }
            )
    for r in rows:
        assert SOC_TOTAL_LOW_MAX is not None and SOC_TOTAL_HIGH_MIN is not None
        r["low_total"] = r["total"] <= SOC_TOTAL_LOW_MAX
        r["high_total"] = r["total"] >= SOC_TOTAL_HIGH_MIN
    return rows


def collapse_player_game(rows: list[dict]) -> list[dict]:
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        groups[(r["date"], r["player"], r["opp"], r["direction"], r["game"])].append(r)
    out = []
    for _key, grp in groups.items():
        base = dict(grp[0])
        base["hit"] = float(np.mean([g["hit"] for g in grp]))
        base["prop"] = "collapsed"
        out.append(base)
    return out


def score_contrasts(
    rows: list[dict],
    median_date: str,
    eligibility: dict[str, dict],
) -> dict:
    results = {}

    def run(name: str, pool: list[dict], treat_pred, ctrl_pred) -> dict:
        elig = eligibility.get(name) or {}
        eligible = bool(elig.get("eligible"))
        tagged = []
        for r in pool:
            t = treat_pred(r)
            c = ctrl_pred(r)
            if not t and not c:
                continue
            tagged.append({**r, "_treat": bool(t)})
        if not eligible:
            return {
                "scored": False,
                "eligibility": elig,
                "pooled": {"empty": True},
                "pre_median": {"empty": True},
                "post_median": {"empty": True},
                "decision": {
                    "status": "ineligible (below floor)",
                    "pass": False,
                    "reason": "ineligible (below floor)",
                },
            }
        pooled = _summarize(tagged, "_treat")
        rs = _summarize([r for r in tagged if r["date"] <= median_date], "_treat")
        late = _summarize([r for r in tagged if r["date"] > median_date], "_treat")
        decision = _pass_rule(pooled, rs, eligible=True)
        return {
            "scored": True,
            "eligibility": elig,
            "pooled": pooled,
            "pre_median": rs,
            "post_median": late,
            "decision": decision,
        }

    overs = [r for r in rows if r["direction"] == "OVER"]
    unders = [r for r in rows if r["direction"] == "UNDER"]

    results["over_heavy_fav_vs_pickem"] = run(
        "over_heavy_fav_vs_pickem",
        overs,
        lambda r: r["heavy_fav"],
        lambda r: r["pickem"],
    )
    results["over_high_vs_low_total"] = run(
        "over_high_vs_low_total",
        overs,
        lambda r: r["high_total"],
        lambda r: r["low_total"],
    )
    results["under_heavy_fav_vs_pickem"] = run(
        "under_heavy_fav_vs_pickem",
        unders,
        lambda r: r["heavy_fav"],
        lambda r: r["pickem"],
    )
    return results


def spot_check_closes(n: int = 5) -> list[dict]:
    conn = sqlite3.connect(str(DB_PATH))
    try:
        rows = conn.execute(
            """
            SELECT game_date, home_team, away_team, event_id, spread, total, is_closing
            FROM game_lines
            WHERE sport='Soccer' AND event_id IS NOT NULL AND event_id != ''
              AND is_closing = 1
            ORDER BY game_date DESC
            LIMIT 40
            """
        ).fetchall()
    finally:
        conn.close()
    out: list[dict] = []
    for gdate, home, away, eid, spread, total, closing in rows:
        if len(out) >= n:
            break
        # Try each soccer league path until core odds respond.
        from fetch_game_lines import SOCCER_LEAGUE_PATHS

        parsed = None
        for path in SOCCER_LEAGUE_PATHS:
            core = fetch_core_odds_item("Soccer", str(eid), league_path=path)
            if core:
                parsed = line_from_core_item(core)
                break
        if not parsed:
            continue
        out.append(
            {
                "date": gdate,
                "matchup": f"{away}@{home}",
                "event_id": eid,
                "stored_spread": spread,
                "stored_total": total,
                "refetch_spread": parsed["spread"],
                "refetch_total": parsed["total"],
                "spread_match": spread == parsed["spread"]
                or (
                    spread is not None
                    and parsed["spread"] is not None
                    and abs(float(spread) - float(parsed["spread"])) < 1e-9
                ),
                "total_match": total == parsed["total"]
                or (
                    total is not None
                    and parsed["total"] is not None
                    and abs(float(total) - float(parsed["total"])) < 1e-9
                ),
                "provider": parsed.get("provider"),
            }
        )
        time.sleep(0.2)
    return out


def _fmt_block(name: str, block: dict) -> str:
    lines = [f"#### {name}"]
    elig = block.get("eligibility") or {}
    if elig:
        lines.append(
            f"- pre-score bucket games: treat {elig.get('treat_games')} / "
            f"ctrl {elig.get('ctrl_games')} -> {elig.get('status')}"
        )
    d = block.get("decision") or {}
    status = str(d.get("status") or ("pass" if d.get("pass") else "fail")).upper()
    if not block.get("scored", True):
        lines.append(
            f"- **decision: {status}** ({d.get('reason')}) — not scored; no lift reported."
        )
        return "\n".join(lines)
    for split in ("pooled", "pre_median", "post_median"):
        s = block.get(split) or {}
        if s.get("empty"):
            lines.append(f"- {split}: empty")
            continue
        lines.append(
            f"- {split}: treat {s['treat_hits']}/{s['treat_n']} = {s['treat_rate']:.1%} "
            f"({s['treat_games']}g) vs ctrl {s['ctrl_hits']}/{s['ctrl_n']} = {s['ctrl_rate']:.1%} "
            f"({s['ctrl_games']}g); lift {s['lift']:+.1%} SE {s['se']:.1%} "
            f"Bonf CI [{s['ci_lo']:+.1%}, {s['ci_hi']:+.1%}]; "
            f"excludes0={s['excludes_zero']}"
        )
    lines.append(f"- **decision: {status}** ({d.get('reason')})")
    if status == "PASS" and block.get("pre_median") and block.get("post_median"):
        pre = block["pre_median"]
        post = block["post_median"]
        if not pre.get("empty") and not post.get("empty"):
            lines.append(
                f"- **Both halves:** pre-median lift {pre['lift']:+.1%} "
                f"(Bonf CI [{pre['ci_lo']:+.1%}, {pre['ci_hi']:+.1%}]); "
                f"post-median lift {post['lift']:+.1%} "
                f"(Bonf CI [{post['ci_lo']:+.1%}, {post['ci_hi']:+.1%}]). "
                "Post-median near zero or with a wide interval means the effect may be "
                "decaying across the season; that does not reverse a pass but belongs here."
            )
    return "\n".join(lines)


def append_note(payload: dict) -> None:
    eligibility = payload["eligibility"]
    cuts = payload["total_cuts"]
    primary = payload["primary"]
    lock = payload.get("lock_provenance") or {}
    wp_fix = payload.get("wp_fix") or {}
    lines = [
        "## Soccer contrast lock (before hit rates)",
        "",
        str(lock.get("summary") or ""),
        "",
        "## Soccer three-way win probability",
        "",
        str(wp_fix.get("note") or ""),
        "",
        "## Soccer locked total cuts",
        "",
        f"Set from {cuts.get('n_games_with_total')} closing (or available) per-game totals "
        "before joining hits (pooled leagues).",
        f"Histogram (rounded): `{json.dumps({str(k): v for k, v in (cuts.get('histogram') or {}).items()})}`.",
        f"**Low <= {cuts.get('low_max')}** (n={cuts.get('n_low')} games). "
        f"**High >= {cuts.get('high_min')}** (n={cuts.get('n_high')} games). "
        f"Mid excluded (n={cuts.get('n_mid')}).",
        "",
        "## Soccer pre-score bucket eligibility",
        "",
        "Floor is **200 games per treatment bucket and 200 per comparison**, counted from "
        "`game_lines` before hits. Counts below use **three-way** outright WP when drawOdds "
        "were present.",
        "",
        "| Contrast | Treat games | Ctrl games | Status |",
        "|---|---:|---:|---|",
    ]
    for name in (
        "over_heavy_fav_vs_pickem",
        "over_high_vs_low_total",
        "under_heavy_fav_vs_pickem",
    ):
        e = eligibility.get(name) or {}
        lines.append(
            f"| {name} | {e.get('treat_games')} | {e.get('ctrl_games')} | {e.get('status')} |"
        )
    lines.extend(
        [
            "",
            str(eligibility.get("note") or ""),
            "",
            "WP >= 0.70 stays fixed. Do not substitute a lower band after seeing these counts; "
            "any replacement band is only testable on future games.",
            "",
            "## Soccer battery results",
            "",
            f"Closing-line join via opponent club name. Median split date: **{payload['median_date']}**.",
            f"Matched decided props: {payload['n_rows']}. Unique games in rows: {payload['n_games']}.",
            "",
            "### Primary (one row per prop; cluster by game)",
            "",
        ]
    )
    for name, block in primary.items():
        lines.append(_fmt_block(name, block))
        lines.append("")
    collapsed = payload.get("collapsed") or {}
    if collapsed:
        lines.extend(["### Player-game collapse (diagnostic)", ""])
        for name, block in collapsed.items():
            lines.append(_fmt_block(name, block))
            lines.append("")
    lines.extend(
        [
            "A PASS here is review-only. Soccer game-line environment does not become a ticket gate "
            "from this battery.",
            "",
        ]
    )

    text = NOTE.read_text(encoding="utf-8")
    # Preserve sections that live after Soccer (e.g. MLB held-out model test).
    heldout_tail = ""
    heldout_marker = "## MLB game_total held-out model test"
    if heldout_marker in text:
        heldout_tail = "\n" + heldout_marker + text.split(heldout_marker, 1)[1]
        text = text.split(heldout_marker, 1)[0].rstrip() + "\n"
    # Replace prior Soccer battery sections if re-run; keep MLB sections.
    for marker in (
        "## Soccer contrast lock (before hit rates)",
        "## Soccer locked total cuts",
        "## Soccer pre-score bucket eligibility",
        "## Soccer battery results",
    ):
        if marker in text:
            idx = text.find(marker)
            if idx >= 0:
                text = text[:idx].rstrip() + "\n"
            break
    # Drop the old "Soccer next" stub under MLB if still present.
    stub = "### Soccer next"
    if stub in text:
        text = text.split(stub)[0].rstrip() + "\n"
    NOTE.write_text(text + "\n".join(lines) + "\n" + heldout_tail, encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="Soccer game-line battery")
    ap.add_argument("--skip-fetch", action="store_true")
    ap.add_argument("--refresh", action="store_true")
    ap.add_argument("--sleep", type=float, default=0.2)
    ap.add_argument("--max-dates", type=int, default=0, help="0 = all graded Soccer dates")
    ap.add_argument(
        "--skip-wp-recompute",
        action="store_true",
        help="Skip three-way WP rewrite (debug only)",
    )
    ap.add_argument(
        "--eligibility-only",
        action="store_true",
        help="Stop after pre-score bucket counts (no hit join)",
    )
    args = ap.parse_args()

    dates = soccer_graded_dates()
    if args.max_dates > 0:
        dates = dates[: args.max_dates]
    print(
        f"Soccer graded dates: {len(dates)} "
        f"({dates[0] if dates else '-'} ... {dates[-1] if dates else '-'})"
    )

    if not args.skip_fetch:
        print("Backfilling Soccer game lines (multi-league)...")
        stats = backfill_soccer(dates, refresh=args.refresh, sleep_s=args.sleep)
        print(stats)
    else:
        conn = sqlite3.connect(str(DB_PATH))
        try:
            init_db(conn)
            refresh_crosswalk(conn, ["Soccer"])
            stamp_team_ids_from_crosswalk(conn)
        finally:
            conn.close()

    wp_fix: dict = {
        "note": (
            "Prior Soccer WP used two-way home/(home+away), which ignores the draw and "
            "inflates outright win probability (e.g. a -270 favorite near 0.85). "
            "Corrected: when drawOdds is present, implied probs are normalized by "
            "home+draw+away. WP is the player's team **winning outright**, not "
            "'not losing'. Two-way sports without drawOdds are unchanged. "
            "WP contrasts were re-scored after this rewrite; the total contrast is "
            "unaffected. This is a fix to the locked rule, not a new contrast."
        ),
        "recompute": None,
    }
    if not args.skip_wp_recompute:
        print("Recomputing Soccer WP with three-way (home+draw+away) normalization...")
        wp_stats = recompute_soccer_threeway_wp(sleep_s=min(args.sleep, 0.08))
        wp_fix["recompute"] = wp_stats
        print(wp_stats)

    low, high, cuts = lock_total_cuts()
    print(f"Locked total cuts: low <= {low}, high >= {high}  meta={cuts}")

    lock_provenance = {
        "summary": (
            "The three Soccer contrasts are the same named contrasts locked for all "
            "team sports in this note (heavy-fav WP >= 0.70 vs pick'em [0.42, 0.58] "
            "overs and unders; high vs low total overs), written under "
            "**Named contrasts (MLB first; same shape for other team sports)** before "
            "any Soccer multi-league hit join. The note file is still untracked in git "
            "(no commit hash); the shared names and WP bands were set with the MLB "
            "pre-registration on 24 Sep 2026, after MLB results were known but before "
            "Soccer hit rates. Soccer total edges **low <= 2.5 / high >= 3.5** were "
            "locked from the per-game total histogram after the multi-league line "
            "fetch and before joining hits (`lock_total_cuts` in "
            "`scripts/score_soccer_game_line_battery.py`) — same process as MLB "
            "7.5/9.0. The 343 vs 1,235 split is that fixed cut on the closing-line "
            "pool (effectively 3.5+ vs <=2.5; mid empty because soccer O/U bunches "
            "at halves)."
        ),
        "named_contrasts_locked": "2026-09-24 (shared three-contrast rule; note untracked)",
        "total_cuts_locked": "after multi-league fetch, before hit join (low<=2.5, high>=3.5)",
    }

    eligibility = pre_score_bucket_counts()
    print("Pre-score bucket eligibility (three-way WP):")
    for name, e in eligibility.items():
        if name == "note":
            continue
        print(
            f"  {name}: treat={e['treat_games']} ctrl={e['ctrl_games']} -> {e['status']}"
        )

    if args.eligibility_only:
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_cuts": cuts,
            "eligibility": eligibility,
            "lock_provenance": lock_provenance,
            "wp_fix": wp_fix,
            "median_date": "",
            "n_rows": 0,
            "n_games": 0,
            "primary": {
                name: {
                    "scored": False,
                    "eligibility": eligibility[name],
                    "pooled": {"empty": True},
                    "pre_median": {"empty": True},
                    "post_median": {"empty": True},
                    "decision": {
                        "status": eligibility[name]["status"],
                        "pass": False,
                        "reason": eligibility[name]["status"],
                    },
                }
                for name in (
                    "over_heavy_fav_vs_pickem",
                    "over_high_vs_low_total",
                    "under_heavy_fav_vs_pickem",
                )
            },
            "collapsed": {},
            "spot_check": [],
        }
        REPORT_JSON.parent.mkdir(parents=True, exist_ok=True)
        REPORT_JSON.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        append_note(payload)
        print(f"Wrote eligibility-only {REPORT_JSON}")
        return

    print("Spot-checking post-final core odds...")
    spot = spot_check_closes(5)
    for s in spot:
        print(
            f"  {s['date']} {s['matchup']}: total {s['stored_total']}->{s['refetch_total']} "
            f"({'Y' if s['total_match'] else 'N'})"
        )

    name_to_id = load_name_crosswalk()
    games = load_soccer_games()
    conn = sqlite3.connect(str(DB_PATH))
    try:
        gdates = sorted(
            {
                r[0]
                for r in conn.execute(
                    "SELECT DISTINCT game_date FROM game_lines "
                    "WHERE sport='Soccer' AND is_closing=1"
                )
            }
        )
        if not gdates:
            gdates = sorted(
                {
                    r[0]
                    for r in conn.execute(
                        "SELECT DISTINCT game_date FROM game_lines WHERE sport='Soccer'"
                    )
                }
            )
    finally:
        conn.close()
    if not gdates:
        raise SystemExit("No Soccer game_dates after fetch")
    median_date = gdates[len(gdates) // 2]
    print(f"Median split date (from game_lines, before hits): {median_date}")

    any_eligible = any(
        (eligibility.get(n) or {}).get("eligible")
        for n in (
            "over_heavy_fav_vs_pickem",
            "over_high_vs_low_total",
            "under_heavy_fav_vs_pickem",
        )
    )
    rows: list[dict] = []
    if any_eligible:
        rows = collect_rows(games, name_to_id, closing_only=True)
        if len(rows) < 500:
            print(f"Closing-only rows thin ({len(rows)}); retrying without closing-only")
            rows = collect_rows(games, name_to_id, closing_only=False)
        print(f"Matched decided props: {len(rows)}")
    else:
        print(
            "All contrasts ineligible (below floor) — refusing to score hit rates."
        )

    n_games = len({r["game"] for r in rows}) if rows else 0
    primary = score_contrasts(rows, median_date, eligibility) if rows else {
        name: {
            "scored": False,
            "eligibility": eligibility[name],
            "pooled": {"empty": True},
            "pre_median": {"empty": True},
            "post_median": {"empty": True},
            "decision": {
                "status": "ineligible (below floor)",
                "pass": False,
                "reason": "ineligible (below floor)",
            },
        }
        for name in (
            "over_heavy_fav_vs_pickem",
            "over_high_vs_low_total",
            "under_heavy_fav_vs_pickem",
        )
    }
    collapsed = (
        score_contrasts(collapse_player_game(rows), median_date, eligibility)
        if rows
        else {}
    )

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_cuts": cuts,
        "eligibility": eligibility,
        "lock_provenance": lock_provenance,
        "wp_fix": wp_fix,
        "spot_check": spot,
        "median_date": median_date,
        "n_rows": len(rows),
        "n_games": n_games,
        "primary": primary,
        "collapsed": collapsed,
    }
    REPORT_JSON.parent.mkdir(parents=True, exist_ok=True)
    REPORT_JSON.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    append_note(payload)
    print(f"Wrote {REPORT_JSON}")
    print(f"Updated {NOTE}")
    for name, block in primary.items():
        d = block["decision"]
        print(f"  {name}: {d.get('status', '?').upper()} ({d['reason']})")


if __name__ == "__main__":
    main()
