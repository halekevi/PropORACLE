#!/usr/bin/env python3
"""Observe-only shadows for two playbook paths. Neither is a ticket rule.

Promotion criteria are frozen in data/reports/playbook_promotion_criteria.json
before this script scores anything. The 2026 discovery sample does not count
toward the 100-leg bar.

  py -3.14 scripts/observe_playbook_shadows.py
"""
from __future__ import annotations

import json
import math
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from scan_gate_thresholds import (  # noqa: E402
    _chronological_split,
    _load_graded_json,
    _wilson_lower,
)
from utils.prop_norm import canon_prop  # noqa: E402

CRITERIA_PATH = ROOT / "data" / "reports" / "playbook_promotion_criteria.json"
LEDGER_PATH = ROOT / "data" / "reports" / "playbook_observe_ledger.json"
OUT_PATH = ROOT / "data" / "reports" / "playbook_observe_latest.json"
GRADED = ROOT / "ui_runner" / "templates"
EXTRA_GRADED = Path(r"H:\PropORACLE") / "ui_runner" / "templates"
CDP_PATH = ROOT / "ui_runner" / "data" / "payout_ladder_live_cdp.json"

# Thursday of NFL week 1, 2026 (the Thursday after Labor Day).
NFL_WEEK1 = date(2026, 9, 10)
OBSERVE_AFTER = "2026-10-11"
FLEX_BAR = 1.0 / math.sqrt(3.0)

NFL_PROPS = ("receptions", "sacks", "receiving_yards", "longest_reception")

# Added 2026-10-24 (forward counter still 0, thresholds untouched). Live NFL step1
# did not persist pick_type before 2026-10-23 and step7 defaulted blanks to
# "Standard", so every graded row this file reads as Standard UNDER on
# 09-09..09-17 and 09-27..10-19 may be a Goblin or Demon rung. nfl_cells()
# filters by the label only. Real-face dates in the sample: 09-20 / 09-21 / 09-24.
# Pre-registered replacement that requires a persisted Standard face:
# ml/prereg/nfl_standard_under_2026-10-24.md (utils.nfl_std_under_shadow).
FACE_LABEL_CAVEAT = (
    "Label-based. The 10-11 Standard UNDER cells and the rule-4-for-NFL ruling were "
    "computed on pick_type that step1 never stored (defaulted to Standard downstream); "
    "Demon/Goblin rungs are mixed into the 'Standard' rows. Only slate dates with "
    "step1 pick_type persisted (2026-10-23+, plus 09-20/09-21/09-24) are real faces. "
    "Forward counting must use utils.face_ladder status == verified. "
    "See data/reports/face_contamination_2026-10-24.md."
)
FACE_VERIFIED_DATES_IN_SAMPLE = ("2026-09-20", "2026-09-21", "2026-09-24")

# Frozen before any diagnostic in this file is treated as evidence.
CRITERIA: dict[str, Any] = {
    "written": "2026-10-11",
    "status": "observe_only",
    "ticket_rules": "unchanged",
    "observe_after": OBSERVE_AFTER,
    "discovery_sample_counts": False,
    "paths": {
        "nfl_standard_under_3leg_flex": {
            "shape": "3-leg Flex",
            "sport": "NFL",
            "pick_type": "Standard",
            "direction": "UNDER",
            "props": list(NFL_PROPS),
            "gate": "L5=5, L10>=8, directional D (UNDER vs Elite or Above Avg)",
            "published_ladder_until_a_scrape_exists": {"3": 3.0, "2": 1.0},
            "breakeven_per_leg_on_that_card": round(FLEX_BAR, 4),
            "breakeven_uses": (
                "The N-correct Flex quote scraped on slips that contain these legs. "
                "The published 3x for 3/3 and 1x for 2/3 stays the bar only until that scrape exists."
            ),
            "promotion_requires_all": [
                "At least 100 decided legs whose slate date is after 2026-10-11.",
                "Those legs come from several NFL weeks and several players.",
                "Wilson 95% lower bound at or above the per-leg breakeven of the real scraped Flex quote.",
                "The four cells found by scanning every gated cell do not count toward the 100.",
                "Rule 4 applies to NFL as written, both directions. Lines 0.5, 2.5, and 4.5 are out when role_tier is SUPPORT. PRIMARY and SECONDARY on those lines may count. A fragile line with no role_tier does not count until role is known.",
            ],
            "face_label_caveat": FACE_LABEL_CAVEAT,
            "rule4": {
                "decision": "apply_to_nfl_as_written",
                "set_on": "2026-10-11",
                "face_label_caveat": (
                    "The sacks UNDER 0.5 fragility example and the ungated hit rate this "
                    "ruling cites were read from label-only Standard rows (see "
                    "face_label_caveat). Re-read on verified faces before relying on either."
                ),
                "lines": [0.5, 2.5, 4.5],
                "role_player": "SUPPORT",
                "directions": "OVER and UNDER",
                "why": (
                    "Rule 4 caps miss-by-1 tail risk. A sacks UNDER 0.5 loses on one sack, "
                    "the same fragility the rule was written for on OVERs. The ungated hit rate does not waive it."
                ),
            },
        },
        "goblin70_on_2x_plus": {
            "shape": "Goblin Power quoted at 2x or more",
            "promotion_requires_all": [
                "At least 100 decided legs whose slate date is after 2026-10-11.",
                "Those legs come from several weeks and several players.",
                "Each quote is stored at scrape time with standard_line, the Goblin-to-Standard gap, and Goblin-70 status.",
                "The hit rate is graded on those 2x-plus slips' own legs, not the pooled Goblin-70 sleeve.",
                "Wilson 95% lower bound at or above the per-leg breakeven of that slip's own quote.",
            ],
        },
    },
}

ANECDOTE = (
    ("2026-07-12", "kelsey mitchell", "assists"),
    ("2026-07-13", "allisha gray", "points"),
    ("2026-07-12", "jade melbourne", "assists"),
    ("2026-07-12", "jonquel jones", "assists"),
    ("2026-07-12", "natisha hiedeman", "rebounds"),
)


def nfl_week(day: date) -> int | None:
    delta = (day - NFL_WEEK1).days
    if delta < 0:
        return None
    return delta // 7 + 1


def _fold(raw: object) -> str:
    return "".join(ch for ch in str(raw or "").lower() if ch.isalnum())


def _cell_rows(frame, *, gated: bool) -> list[dict[str, Any]]:
    rows = []
    for rec in frame.to_dict(orient="records"):
        l5 = rec.get("l5")
        l10 = rec.get("l10")
        tier = str(rec.get("def_tier") or "")
        is_gated = (
            l5 == 5
            and l10 is not None
            and l10 >= 8
            and tier in ("Elite", "Above Avg")
        )
        if gated and not is_gated:
            continue
        day = rec["date"]
        if hasattr(day, "date"):
            day = day.date()
        rows.append(
            {
                "date": day.isoformat() if hasattr(day, "isoformat") else str(day)[:10],
                "player": str(rec.get("player") or ""),
                "line": rec.get("line"),
                "role_tier": str(rec.get("role_tier") or ""),
                "l5": l5 if l5 == l5 else None,
                "l10": l10 if l10 == l10 else None,
                "def_tier": "" if tier in ("", "nan", "None") else tier,
                "is_hit": int(rec.get("is_hit") or 0),
                "week": nfl_week(day if isinstance(day, date) else date.fromisoformat(str(day)[:10])),
            }
        )
    return rows


def _summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    in_season = [r for r in rows if r["week"] is not None]
    prior = [r for r in rows if r["week"] is None]
    n = len(in_season)
    hits = sum(r["is_hit"] for r in in_season)
    players = sorted({_fold(r["player"]) for r in in_season if _fold(r["player"])})
    weeks = sorted({r["week"] for r in in_season})
    by_week: dict[str, dict[str, int]] = {}
    for week in weeks:
        chunk = [r for r in in_season if r["week"] == week]
        by_week[str(week)] = {
            "hits": sum(r["is_hit"] for r in chunk),
            "n": len(chunk),
            "players": len({_fold(r["player"]) for r in chunk}),
        }
    lines = Counter()
    for r in in_season:
        line = r.get("line")
        if line in (0.5, 2.5):
            lines[f"{line:g}"] += 1
        else:
            lines["other"] += 1
    roles = Counter(r["role_tier"] or "(blank)" for r in in_season)
    lo = _wilson_lower(hits, n) if n else None
    prior_hits = sum(r["is_hit"] for r in prior)
    quotable = [
        r
        for r in in_season
        if nfl_promotion_disposition(r.get("line"), r.get("role_tier")) == "allow"
    ]
    q_hits = sum(r["is_hit"] for r in quotable)
    q_n = len(quotable)
    return {
        "hits": hits,
        "n": n,
        "rate": None if n == 0 else round(hits / n, 4),
        "wilson_lo": None if lo is None else round(lo, 4),
        "unique_players": len(players),
        "unique_weeks": weeks,
        "by_week": by_week,
        "lines_0_5_or_2_5": int(lines.get("0.5", 0) + lines.get("2.5", 0)),
        "line_counts": dict(lines),
        "role_counts": dict(roles),
        "prior_season_hits": prior_hits,
        "prior_season_n": len(prior),
        "quotable": {
            "hits": q_hits,
            "n": q_n,
            "rate": None if q_n == 0 else round(q_hits / q_n, 4),
            "wilson_lo": round(_wilson_lower(q_hits, q_n), 4) if q_n else None,
        },
    }


def zero_week_reasons(ungated_rows: list[dict[str, Any]], gated_weeks: list[int]) -> list[dict[str, Any]]:
    """Why a week with decided legs produced no gated leg.

    A run of these is a matchup week, not a blank pipeline, when the reason says the defense gate.
    """
    held = set(gated_weeks)
    by_week: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in ungated_rows:
        if row.get("week") is None:
            continue
        by_week[int(row["week"])].append(row)
    out = []
    for week, chunk in sorted(by_week.items()):
        if week in held:
            continue
        blank = 0
        clear = []
        for row in chunk:
            l5, l10 = row.get("l5"), row.get("l10")
            if l5 is None or l10 is None:
                blank += 1
                continue
            if l5 == 5 and l10 >= 8:
                clear.append(row)
        if blank == len(chunk):
            reason = "L5/L10 window missing"
        elif not clear:
            reason = "no leg cleared L5=5 and L10>=8"
        else:
            tiers = Counter(str(row.get("def_tier") or "(blank)") or "(blank)" for row in clear)
            shown = ", ".join(f"{name} {n}" for name, n in tiers.most_common())
            reason = f"defense gate failed ({shown}); none Elite or Above Avg"
        out.append(
            {
                "week": week,
                "ungated_n": len(chunk),
                "l5_l10_clear": len(clear),
                "reason": reason,
            }
        )
    return out


def _oos_fixed_gate(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Same chronological date split as scan_gate_thresholds. The gate is not re-chosen."""
    import pandas as pd

    if not rows:
        return {"status": "empty"}
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    dates = sorted(df["date"].dropna().unique())
    if len(dates) < 4:
        return {
            "status": "too_few_dates",
            "n_dates": len(dates),
            "note": "scan_gate_thresholds requires at least 4 slate dates. Week table is the split.",
        }
    find, hold, date_labels = _chronological_split(df, 0.5)
    def pack(part) -> dict[str, Any]:
        hits = int(part["is_hit"].sum())
        n = len(part)
        return {
            "hits": hits,
            "n": n,
            "rate": None if n == 0 else round(hits / n, 4),
            "wilson_lo": round(_wilson_lower(hits, n), 4) if n else None,
            "dates": sorted({str(pd.Timestamp(d).date()) for d in part["date"]}),
        }
    return {
        "status": "split",
        "note": (
            "Holdout is the later half of slate dates. These cells were chosen on the "
            "full sample, so this half was inside the original search. Legs after "
            f"{OBSERVE_AFTER} are the untouched sample."
        ),
        "all_dates": date_labels,
        "find": pack(find),
        "oos": pack(hold),
    }


def nfl_cells() -> list[dict[str, Any]]:
    raw = _load_graded_json(
        GRADED,
        sport="NFL",
        pick_type="Standard",
        direction="UNDER",
        props=None,
    )
    out = []
    if raw.empty:
        return out
    raw = raw.copy()
    raw["canon"] = raw["prop"].map(lambda p: canon_prop("NFL", p) or str(p))
    for prop in NFL_PROPS:
        want = canon_prop("NFL", prop)
        pool = raw[raw["canon"].astype(str) == want]
        ungated = _cell_rows(pool, gated=False)
        gated_frame = pool[
            pool["l5"].notna()
            & (pool["l5"] == 5)
            & pool["l10"].notna()
            & (pool["l10"] >= 8)
            & pool["def_tier"].isin(["Elite", "Above Avg"])
        ]
        gated = _cell_rows(gated_frame, gated=False)
        season = [r for r in gated if r["week"] is not None]
        gated_summary = _summary(gated)
        out.append(
            {
                "prop": prop,
                "ungated": _summary(ungated),
                "gated": gated_summary,
                "zero_weeks": zero_week_reasons(ungated, gated_summary["unique_weeks"]),
                "oos": _oos_fixed_gate(season),
            }
        )
    return out


def _load_props(path: Path) -> list[dict]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    props = payload.get("props") or []
    return props if isinstance(props, list) else []


def anecdote_results() -> list[dict[str, Any]]:
    files: dict[str, Path] = {}
    for folder in (GRADED, EXTRA_GRADED):
        if not folder.is_dir():
            continue
        for path in folder.glob("graded_props_20*.json"):
            day = path.stem.replace("graded_props_", "")[:10]
            files.setdefault(day, path)
    found = []
    for day, player, prop in ANECDOTE:
        path = files.get(day)
        hits = []
        if path is not None:
            for row in _load_props(path):
                if _fold(row.get("player")) != _fold(player):
                    continue
                if _fold(row.get("prop")) != _fold(prop):
                    continue
                if "gob" not in str(row.get("pick_type") or "").lower():
                    continue
                side = str(row.get("direction") or "").upper()
                if side not in ("OVER", "O"):
                    continue
                hits.append(
                    {
                        "result": str(row.get("result") or "").upper(),
                        "line": row.get("line"),
                        "actual": row.get("actual_value"),
                        "l5": row.get("l5_over"),
                        "l10": row.get("l10_over"),
                        "def_tier": row.get("def_tier"),
                    }
                )
        found.append(
            {
                "date": day,
                "player": player,
                "prop": prop,
                "graded_file": None if path is None else str(path),
                "rows": hits,
                "counts_toward_promotion": False,
            }
        )
    return found


def flex_quote_status() -> dict[str, Any]:
    n_flex = 0
    n_rows = 0
    if CDP_PATH.is_file():
        try:
            payload = json.loads(CDP_PATH.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = {}
        for row in payload.get("rows") or []:
            if not isinstance(row, dict):
                continue
            n_rows += 1
            ladder = row.get("flex_n_correct")
            if isinstance(ladder, dict) and ladder:
                n_flex += 1
    return {
        "cdp_rows": n_rows,
        "rows_with_flex_ladder": n_flex,
        "bar": (
            "Published 3x for 3/3 and 1x for 2/3, breakeven 57.7% a leg. "
            "No scraped Flex ladder is on file, so a lower PrizePicks quote would move this bar up."
        ),
    }


def _game_key(row: dict) -> tuple[str, str]:
    team = _fold(row.get("team"))
    opp = _fold(row.get("opp") or row.get("opp_team") or row.get("opponent"))
    if team and opp:
        pair = tuple(sorted((team, opp)))
        return pair  # type: ignore[return-value]
    return ("player", _fold(row.get("player")))


def _posted_line(row: dict) -> float | None:
    try:
        return float(row.get("line"))
    except (TypeError, ValueError):
        return None


def pick_sacks_under_legs(rows: list[dict], n: int = 3) -> list[dict]:
    """Sacks UNDER 1.5 legs a 3-leg Flex would be allowed to play.

    One player per game. 0.5, 0.25, 2.5, and 4.5 stay off this quote.
    """
    pool: list[dict] = []
    seen_players: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        prop = _fold(row.get("prop") or row.get("prop_type"))
        if "sack" not in prop:
            continue
        side = str(row.get("direction") or row.get("dir") or "").strip().upper()
        if side not in ("UNDER", "U"):
            continue
        pick = str(row.get("pick_type") or row.get("pick") or "Standard").lower()
        if "demon" in pick:
            continue
        line = _posted_line(row)
        if line != 1.5:
            continue
        player = _fold(row.get("player"))
        if not player or player in seen_players:
            continue
        seen_players.add(player)
        pool.append(row)
    pool.sort(key=lambda row: str(row.get("player") or ""))
    chosen: list[dict] = []
    seen_games: set[tuple[str, str]] = set()
    for row in pool:
        game = _game_key(row)
        if game in seen_games:
            continue
        seen_games.add(game)
        chosen.append(row)
        if len(chosen) >= n:
            break
    return chosen


def nfl_promotion_disposition(line: object, role_tier: object) -> str:
    """How a post-cutoff NFL leg meets rule 4.

    allow: counts toward the 100.
    exclude: SUPPORT on 0.5, 2.5, or 4.5.
    unresolved: fragile line and role_tier is still blank.
    """
    try:
        posted = float(line)
    except (TypeError, ValueError):
        posted = None
    fragile = posted in (0.5, 2.5, 4.5)
    if not fragile:
        return "allow"
    role = str(role_tier or "").strip().upper()
    if role == "SUPPORT":
        return "exclude"
    if role in ("PRIMARY", "SECONDARY"):
        return "allow"
    return "unresolved"


def write_criteria() -> None:
    """Rewrite the frozen file only while the forward counter is still empty."""
    CRITERIA_PATH.parent.mkdir(parents=True, exist_ok=True)
    if LEDGER_PATH.is_file():
        try:
            ledger = json.loads(LEDGER_PATH.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            ledger = {}
        forward = ledger.get("forward_legs") or {}
        if any(int(v or 0) > 0 for v in forward.values() if not isinstance(v, dict)):
            return
    CRITERIA_PATH.write_text(json.dumps(CRITERIA, indent=2), encoding="utf-8")


def main() -> int:
    write_criteria()
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "criteria": CRITERIA,
        "counts_toward_promotion": False,
        "face_label_caveat": FACE_LABEL_CAVEAT,
        "face_verified_dates_in_sample": list(FACE_VERIFIED_DATES_IN_SAMPLE),
        "nfl_standard_under": nfl_cells(),
        "goblin70_2x_anecdote": anecdote_results(),
        "flex_quotes": flex_quote_status(),
        "week4_gate": {
            "finding": "real defense miss, not a blank L5 or L10",
            "decided_standard_under_rows": 85,
            "l5_eq5_and_l10_ge8": 56,
            "directional_d_pass": 0,
            "def_tiers": {"Weak": 78, "Avg": 7},
            "opponents": ["IND Weak", "ATL Weak", "WSH Weak", "DET Weak", "NO Avg"],
        },
        "forward_capture": (
            "New CDP ladder rows store each leg's standard_line, gap, and Goblin-70 "
            "status from the ticket at scrape time. Older rows are not backfilled."
        ),
    }
    OUT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")
    ledger = {
        "criteria": str(CRITERIA_PATH),
        "observe_after": OBSERVE_AFTER,
        "forward_legs": {"nfl_standard_under_3leg_flex": 0, "goblin70_on_2x_plus": 0},
        "last_diagnostic": str(OUT_PATH),
        "face_label_caveat": FACE_LABEL_CAVEAT,
        "forward_legs_require": "utils.face_ladder status == verified for the leg's sport-date",
    }
    LEDGER_PATH.write_text(json.dumps(ledger, indent=2), encoding="utf-8")
    _print(report)
    print(f"Wrote {OUT_PATH}")
    return 0


def _pct(rate: float | None) -> str:
    if rate is None:
        return "n/a"
    return f"{100.0 * rate:.1f}%"


def _print(report: dict[str, Any]) -> None:
    print("Observe only. These paths are not ticket rules.")
    print(f"FACE CAVEAT: {report.get('face_label_caveat')}")
    print(f"Promotion counts only slate dates after {OBSERVE_AFTER}. This sample does not count.")
    print()
    for cell in report["nfl_standard_under"]:
        g = cell["gated"]
        u = cell["ungated"]
        print(f"NFL Standard UNDER {cell['prop']} (2026 weeks only; week 1 opens {NFL_WEEK1.isoformat()})")
        if g["prior_season_n"] or u["prior_season_n"]:
            print(
                f"  prior-season rows kept out of these counts: "
                f"ungated {u['prior_season_hits']}/{u['prior_season_n']}, "
                f"gated {g['prior_season_hits']}/{g['prior_season_n']}"
            )
        print(
            f"  ungated {_pct(u['rate'])} ({u['hits']}/{u['n']}), "
            f"players {u['unique_players']}, weeks {u['unique_weeks']}"
        )
        print(
            f"  gated   {_pct(g['rate'])} ({g['hits']}/{g['n']}), "
            f"Wilson lower {_pct(g['wilson_lo'])}, "
            f"players {g['unique_players']}, weeks {g['unique_weeks']}"
        )
        print(
            f"  gated lines at 0.5 or 2.5: {g['lines_0_5_or_2_5']} of {g['n']} "
            f"{g['line_counts']}"
        )
        print(f"  gated role_tier: {g['role_counts']}")
        quote = g.get("quotable") or {}
        if quote.get("n") != g["n"]:
            print(
                f"  backtest quotes on legs rule 4 can count today: "
                f"{_pct(quote.get('rate'))} ({quote.get('hits')}/{quote.get('n')}). "
                "Fragile lines with no role are out, so this is not the full gated n."
            )
        for gap in cell.get("zero_weeks") or []:
            print(
                f"  week {gap['week']} gated legs 0: {gap['reason']} "
                f"(ungated n={gap['ungated_n']}, L5=5 and L10>=8: {gap['l5_l10_clear']})"
            )
        if g["by_week"]:
            bits = [
                f"W{wk} {info['hits']}/{info['n']} ({info['players']} players)"
                for wk, info in g["by_week"].items()
            ]
            missing = [w for w in u["unique_weeks"] if w not in g["unique_weeks"]]
            extra = f" Ungated also has week(s) {missing} with no gated leg." if missing else ""
            print("  gated by week: " + "; ".join(bits) + extra)
        oos = cell["oos"]
        if oos.get("status") == "split":
            print(
                f"  date-split find {_pct(oos['find']['rate'])} "
                f"({oos['find']['hits']}/{oos['find']['n']}), "
                f"later half {_pct(oos['oos']['rate'])} "
                f"({oos['oos']['hits']}/{oos['oos']['n']}), "
                f"later Wilson {_pct(oos['oos']['wilson_lo'])}"
            )
        else:
            print(f"  date-split: {oos.get('status')} {oos.get('note', '')}")
        print()
    print("Goblin-70 legs that sat on a 2x-plus slip. One result each, not a rate.")
    for leg in report["goblin70_2x_anecdote"]:
        rows = leg["rows"]
        if not rows:
            print(f"  {leg['date']} {leg['player']} {leg['prop']}: no graded row")
            continue
        results = Counter(r["result"] for r in rows)
        shown = rows[0]
        print(
            f"  {leg['date']} {leg['player']} {leg['prop']}: "
            f"{dict(results)} line {shown.get('line')} actual {shown.get('actual')}"
        )
    flex = report["flex_quotes"]
    print()
    print(
        f"Flex ladders in the CDP log: {flex['rows_with_flex_ladder']} "
        f"of {flex['cdp_rows']} rows. {flex['bar']}"
    )


if __name__ == "__main__":
    raise SystemExit(main())
