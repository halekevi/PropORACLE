"""Log Fri 10-23 + Sat 10-24 L5=5 legs as paper trades (source=paper_l5eq5).

Placed=false. Off /tickets. Grades later with strict DNP-zero (positive
evidence only). Prints each gate cell with n + Wilson 95% CI next to the
~84.5% all-Goblin Power break-even bar from the shadow study.

  py -3.14 scripts/log_paper_l5eq5_fri_sat.py
"""
from __future__ import annotations

import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from proporacle.data.table_io import read_table, write_excel_sheets  # noqa: E402
from utils.category_hit_rate import load_category_hr_index, lookup_category_hr  # noqa: E402
from utils.comparison_tracks import wilson_band  # noqa: E402
from utils.prop_norm import canon_prop  # noqa: E402

ET = ZoneInfo("America/New_York")
FRI = "2026-10-23"
SAT = "2026-10-24"
TRACK = "paper_l5eq5"
SOURCE = "paper_l5eq5"
# Shadow study: all-Goblin Power needs ~84.5%/leg at real PP quotes.
GOBLIN_POWER_BAR = 0.845
_LOG = _REPO / "data" / "reports" / "track_comparison.jsonl"
_REPORT = _REPO / "outputs" / "L5eq5_paper_fri_sat_2026-10-23_24.json"
_REPORT_TXT = _REPO / "outputs" / "L5eq5_paper_fri_sat_2026-10-23_24.txt"
_NBA_FLAG = _REPO / "outputs" / "2026-10-23" / "nba" / "BOARD_INCOMPLETE.json"

# Keep / reference cells used on the Fri-Sat list (hits, n, label).
GATE_CELLS: dict[str, tuple[int, int, str]] = {
    "CFB Rush Goblin OVER L5=5+D": (16, 19, "CFB Rush Yards Goblin OVER L5=5+D"),
    "CFB Pass Goblin OVER L5=5+cover": (0, 0, "CFB Pass Yards Goblin OVER ~74% ungated (n not locked in keep file)"),
    "NBA Goblin Assists OVER": (0, 0, "NBA Assists Goblin category"),
    "NBA Goblin Points OVER": (0, 0, "NBA Points Goblin category"),
    "MLB H+R+RBI Goblin OVER category": (32, 44, "keep missed; category ungated"),
    "MLB H+R+RBI Goblin OVER keep": (84, 104, "BA>=.275+L5=5+Opp Weak|Below (keep — not cleared)"),
    "Soccer Shots Goblin OVER category": (55, 82, "keep L10 miss; category"),
    "Soccer Saves ~60%": (205, 343, "Goblin Saves 1.5 ungated — OFF tickets"),
}


def _f(x):
    try:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return None
        return float(x)
    except Exception:
        return None


def _s(x) -> str:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return ""
    return str(x).strip()


def _wilson_str(hits: int, n: int) -> str:
    if n <= 0:
        return "n=0 (no CI)"
    band = wilson_band(hits, n)
    if not band:
        return f"n={n}"
    lo, hi = band
    hr = hits / n
    clears = "YES" if lo >= GOBLIN_POWER_BAR else "NO"
    return (
        f"{hr * 100:.1f}% ({hits}/{n})  Wilson95%[{lo * 100:.1f}–{hi * 100:.1f}]  "
        f"clears {GOBLIN_POWER_BAR * 100:.1f}% bar? {clears}"
    )


def _load_fri_sat_xlsx() -> pd.DataFrame:
    path = _REPO / "outputs" / "L5eq5_Fri-Sat_2026-10-23_24.xlsx"
    if not path.exists():
        raise SystemExit(f"missing {path} — run scripts/export_l5eq5_fri_sat.py first")
    return read_table(path, sheet="L5eq5")


def _nba_fri_l5eq5() -> list[dict]:
    path = _REPO / "outputs" / "2026-10-23" / "nba" / "step8_all_direction.csv"
    if not path.exists():
        return []
    df = read_table(path)
    idx = load_category_hr_index()
    out = []
    for _, r in df.iterrows():
        pt = _s(r.get("pick_type"))
        lo, lu = _f(r.get("l5_over")), _f(r.get("l5_under"))
        side = _s(r.get("direction") or r.get("final_bet_direction")).upper()
        if "goblin" in pt.lower():
            if lo != 5:
                continue
            side, bucket = "OVER", "Goblin"
        elif "demon" in pt.lower():
            if lo != 5:
                continue
            side, bucket = "OVER", "Demon"
        elif "standard" in pt.lower():
            if side == "OVER" and lo == 5:
                bucket = "Standard"
            elif side == "UNDER" and lu == 5:
                bucket = "Standard"
            else:
                continue
        else:
            continue
        avg = _f(r.get("stat_last5_avg"))
        line = _f(r.get("line"))
        gap = None
        if avg is not None and line is not None:
            gap = (avg - line) if side == "OVER" else (line - avg)
        cat = lookup_category_hr(
            idx, sport="NBA", prop=r.get("prop_type"), pick_type=bucket, direction=side
        )
        out.append(
            {
                "Day": "Fri 10-23",
                "Sport": "NBA",
                "pick_type": bucket,
                "side": side,
                "player": _s(r.get("player")),
                "team": _s(r.get("team")),
                "opp": _s(r.get("opponent") or r.get("opp")),
                "prop": _s(r.get("prop_type")),
                "line": line,
                "l5_avg": avg,
                "gap": gap,
                "season_l5_flag": "",
                "opp_def_tier": _s(r.get("def_tier") or r.get("opp_def_tier")),
                "tip": FRI,
                "board_status": "INCOMPLETE_SOFT_WRITE",
                "p": float(cat["hr"]) if cat else None,
                "p_n": float(cat["n"]) if cat else None,
                "p_hit": float(cat["hit"]) if cat else None,
                "p_source": "category" if cat else "none",
            }
        )
    return out


def _gate_meta(row: dict) -> dict:
    sport = _s(row.get("Sport") or row.get("sport")).upper()
    prop = _s(row.get("prop"))
    pt = _s(row.get("pick_type"))
    side = _s(row.get("side")).upper()
    prop_n = canon_prop("CFB" if sport == "CFB1H" else sport, prop)

    if sport == "CFB" and pt == "Goblin" and side == "OVER" and prop_n == "rush_yds":
        hits, n, label = 16, 19, "CFB Rush Goblin OVER L5=5+D"
        return {
            "gate_key": label,
            "gate_hr": hits / n,
            "gate_hits": hits,
            "gate_n": n,
            "gate_label": label,
            "clears_goblin_power_bar": False,
        }
    if sport == "CFB" and pt == "Goblin" and side == "OVER" and prop_n == "pass_yds":
        # Documented ~74% ungated; n not locked — use category if present else 74/100 proxy note
        return {
            "gate_key": "CFB Pass Goblin OVER ~74%",
            "gate_hr": 0.74,
            "gate_hits": None,
            "gate_n": None,
            "gate_label": "CFB Pass Yards Goblin OVER ~74% ungated (keep file; n not locked)",
            "clears_goblin_power_bar": False,
        }
    if sport == "MLB" and pt == "Goblin" and "hits+runs+rbis" in prop_n.replace(" ", ""):
        return {
            "gate_key": "MLB H+R+RBI Goblin category",
            "gate_hr": 32 / 44,
            "gate_hits": 32,
            "gate_n": 44,
            "gate_label": "keep missed; category 32/44",
            "clears_goblin_power_bar": False,
        }
    if sport == "SOCCER" and "shot" in prop.lower() and "target" not in prop.lower():
        return {
            "gate_key": "Soccer Shots category",
            "gate_hr": 55 / 82,
            "gate_hits": 55,
            "gate_n": 82,
            "gate_label": "keep L10 miss; category 55/82",
            "clears_goblin_power_bar": False,
        }
    if sport == "SOCCER" and "save" in prop.lower():
        return {
            "gate_key": "Soccer Saves ungated",
            "gate_hr": 205 / 343,
            "gate_hits": 205,
            "gate_n": 343,
            "gate_label": "Goblin Saves 1.5 ungated — OFF tickets",
            "clears_goblin_power_bar": False,
        }
    if sport == "NBA" and pt == "Goblin":
        p, pn, ph = row.get("p"), row.get("p_n"), row.get("p_hit")
        if p is not None and pn:
            hits = int(ph) if ph is not None else int(round(float(p) * float(pn)))
            n = int(pn)
            return {
                "gate_key": f"NBA {prop} Goblin category",
                "gate_hr": float(p),
                "gate_hits": hits,
                "gate_n": n,
                "gate_label": f"NBA {prop} Goblin category",
                "clears_goblin_power_bar": False,
            }
    # fallback to row p fields
    if row.get("p") is not None and row.get("p_n"):
        n = int(row["p_n"])
        hits = int(row["p_hit"]) if row.get("p_hit") is not None else int(round(float(row["p"]) * n))
        return {
            "gate_key": f"{sport} {prop} {pt} category",
            "gate_hr": float(row["p"]),
            "gate_hits": hits,
            "gate_n": n,
            "gate_label": "category cell",
            "clears_goblin_power_bar": False,
        }
    return {
        "gate_key": f"{sport} {prop} {pt}",
        "gate_hr": None,
        "gate_hits": None,
        "gate_n": None,
        "gate_label": "no cell",
        "clears_goblin_power_bar": False,
    }


def _leg_record(row: dict, *, date: str, index: int) -> dict:
    meta = _gate_meta(row)
    pt = _s(row.get("pick_type"))
    side = _s(row.get("side")).upper()
    n_g = 1 if pt == "Goblin" else 0
    n_s = 1 if pt == "Standard" else 0
    n_d = 1 if pt == "Demon" else 0
    return {
        "date": date,
        "id": f"P-leg-{date}-{index}",
        "track": TRACK,
        "ticket_source": SOURCE,
        "source": SOURCE,
        "kind": "leg",
        "placed": False,
        "ticket_track": "",
        "postseason": 1 if _s(row.get("Sport")).upper() == "MLB" else 0,
        "composition": f"{n_s}S+{n_g}G+{n_d}D",
        "n_legs": 1,
        "n_standard": n_s,
        "n_goblin": n_g,
        "player": row.get("player"),
        "sport": _s(row.get("Sport") or row.get("sport")).upper(),
        "prop": row.get("prop"),
        "prop_canon": canon_prop(
            "CFB" if _s(row.get("Sport")).upper() == "CFB1H" else _s(row.get("Sport")).upper(),
            row.get("prop"),
        ),
        "pick_type": pt,
        "direction": side,
        "line": row.get("line"),
        "team": row.get("team"),
        "opponent": row.get("opp"),
        "l5_avg": row.get("l5_avg"),
        "gap": row.get("gap") if row.get("gap") is not None else row.get("line_div_l5avg"),
        "season_l5_flag": row.get("season_l5_flag"),
        "opp_def_tier": row.get("opp_def_tier"),
        "tip": row.get("tip"),
        "day": row.get("Day"),
        "board_status": row.get("board_status") or "ok",
        "p": meta["gate_hr"] if meta["gate_hr"] is not None else row.get("p"),
        "p_n": meta["gate_n"] if meta["gate_n"] is not None else row.get("p_n"),
        "p_hit": meta["gate_hits"] if meta["gate_hits"] is not None else row.get("p_hit"),
        "p_source": meta["gate_label"],
        "gate_key": meta["gate_key"],
        "clears_goblin_power_bar": False,
        "goblin_power_bar": GOBLIN_POWER_BAR,
        "p_win": meta["gate_hr"] if meta["gate_hr"] is not None else row.get("p"),
        "quote": None,
        "payout_source": None,
        "scraped_payout": None,
        "ev": None,
        "dollar_test": False,
        "ev_label": "paper",
        "stake": 20,
        "result": None,
        "grade_pending": True,
        "grade_rules": "strict_dnp_zero_positive_evidence_only",
        "exclude_from_book": True,
        "note": "Paper trade. Not placed. Grade after tip with strict DNP.",
    }


def _rewrite_paper_days(new_rows: list[dict], dates: set[str]) -> None:
    kept: list[str] = []
    if _LOG.is_file():
        for line in _LOG.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                kept.append(line)
                continue
            if (
                isinstance(rec, dict)
                and rec.get("track") == TRACK
                and str(rec.get("date") or "") in dates
            ):
                continue
            kept.append(json.dumps(rec, ensure_ascii=False))
    _LOG.parent.mkdir(parents=True, exist_ok=True)
    with _LOG.open("w", encoding="utf-8") as fh:
        for line in kept:
            fh.write(line + "\n")
        for rec in new_rows:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")


def flag_nba_incomplete() -> dict:
    """Mark soft-written Fri NBA board incomplete until attempt tabs post."""
    step1 = _REPO / "outputs" / "2026-10-23" / "nba" / "step1_pp_props_today.csv"
    attempt_n = 0
    if step1.exists():
        df = read_table(step1)
        attempt_n = int(
            df["prop_type"]
            .astype(str)
            .str.contains(
                "Attempt|FG Made|Free Throw|Two Pointer|Offensive Reb|Defensive Reb|Personal Foul",
                case=False,
                na=False,
            )
            .sum()
        )
    payload = {
        "slate": FRI,
        "sport": "NBA",
        "status": "INCOMPLETE" if attempt_n == 0 else "COMPLETE",
        "reason": (
            "Soft-written past step1 coverage miss: PrizePicks had not posted "
            "attempt/shooting tabs (FGA/FG Made/FTA/…). Counting board only. "
            "Rebuild via scripts/_run_nba_full_slate.ps1 -Date 2026-10-23 once "
            "partner dump includes those tabs — do not treat as final."
            if attempt_n == 0
            else "Attempt props present — board may be rebuilt as final."
        ),
        "attempt_prop_rows": attempt_n,
        "soft_write_at": "2026-10-22T23:40:00Z",
        "flagged_at": datetime.now(timezone.utc).isoformat(),
        "rebuild_command": (
            "py -3.14 Sports/NBA/scripts/step1_fetch_prizepicks_api.py "
            "--league_id 7 --date 2026-10-23 "
            "--output outputs/2026-10-23/nba/step1_pp_props_today.csv "
            "&& powershell -File scripts/_run_nba_full_slate.ps1 -Date 2026-10-23"
        ),
    }
    _NBA_FLAG.parent.mkdir(parents=True, exist_ok=True)
    _NBA_FLAG.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    # Also stamp step8 meta sidecar note
    note = _REPO / "outputs" / "2026-10-23" / "nba" / "README_BOARD_STATUS.txt"
    note.write_text(
        f"BOARD STATUS: {payload['status']}\n{payload['reason']}\n"
        f"attempt_prop_rows={attempt_n}\nflagged_at={payload['flagged_at']}\n",
        encoding="utf-8",
    )
    return payload


def main() -> int:
    fri_sat = _load_fri_sat_xlsx()
    legs: list[dict] = []

    for _, r in fri_sat.iterrows():
        day = _s(r.get("Day"))
        if "10-23" in day:
            date = FRI
        elif "10-24" in day:
            date = SAT
        else:
            continue
        row = r.to_dict()
        # directional gap if we have avg+line
        avg, line = _f(row.get("l5_avg")), _f(row.get("line"))
        side = _s(row.get("side")).upper()
        if avg is not None and line is not None and row.get("gap") is None:
            row["gap"] = (avg - line) if side == "OVER" else (line - avg)
        legs.append((date, row))

    for row in _nba_fri_l5eq5():
        legs.append((FRI, row))

    records = []
    for i, (date, row) in enumerate(legs, 1):
        records.append(_leg_record(row, date=date, index=i))

    _rewrite_paper_days(records, {FRI, SAT})

    nba_flag = flag_nba_incomplete()

    # Gate summary with Wilson CI
    gate_groups: dict[str, dict] = {}
    for rec in records:
        key = rec.get("gate_key") or "unknown"
        g = gate_groups.setdefault(
            key,
            {
                "gate_key": key,
                "label": rec.get("p_source"),
                "hits": rec.get("p_hit"),
                "n": rec.get("p_n"),
                "hr": rec.get("p"),
                "legs": 0,
                "sports": set(),
                "examples": [],
            },
        )
        g["legs"] += 1
        g["sports"].add(rec["sport"])
        if len(g["examples"]) < 3:
            g["examples"].append(
                f"{rec['player']} {rec['prop']} "
                f"{'O' if rec['direction'] == 'OVER' else 'U'}{rec['line']}"
            )

    lines = []
    lines.append("=" * 72)
    lines.append(f"PAPER L5=5  track={TRACK}  source={SOURCE}  placed=false")
    lines.append(f"Dates: {FRI} + {SAT}   legs logged: {len(records)}")
    lines.append(
        f"All-Goblin Power break-even bar (shadow study): {GOBLIN_POWER_BAR * 100:.1f}%/leg"
    )
    lines.append("Grade later with strict DNP-zero (positive evidence only).")
    lines.append("Pending: L5 leakage check; CFB 2026 logs (still PRIOR_SEASON_FILL).")
    lines.append("=" * 72)
    lines.append("")
    lines.append("GATE CELLS vs ~84.5% Goblin Power bar")
    lines.append("-" * 72)

    summary_rows = []
    # Dedupe by hits/n for display
    seen = set()
    for key, g in sorted(gate_groups.items(), key=lambda kv: -(kv[1]["hr"] or 0)):
        hits, n = g["hits"], g["n"]
        sig = (key, hits, n)
        if sig in seen:
            continue
        seen.add(sig)
        if hits is not None and n is not None and int(n) > 0:
            hits_i, n_i = int(hits), int(n)
            band = wilson_band(hits_i, n_i)
            lo = band[0] if band else None
            hi = band[1] if band else None
            hr = hits_i / n_i
            clears = bool(lo is not None and lo >= GOBLIN_POWER_BAR)
            wilson_s = _wilson_str(hits_i, n_i)
        else:
            hr = g["hr"]
            lo = hi = None
            clears = False
            if hr is not None:
                wilson_s = f"{hr * 100:.1f}% (n not locked)  clears {GOBLIN_POWER_BAR * 100:.1f}% bar? NO"
            else:
                wilson_s = "n/a  clears 84.5% bar? NO"
        lines.append(f"  {key}")
        lines.append(f"    {wilson_s}")
        lines.append(f"    paper legs on list: {g['legs']}  sports={sorted(g['sports'])}")
        lines.append(f"    e.g. {'; '.join(g['examples'])}")
        lines.append("")
        summary_rows.append(
            {
                "gate": key,
                "hr": round(hr, 4) if hr is not None else None,
                "hits": int(hits) if hits is not None else None,
                "n": int(n) if n is not None else None,
                "wilson_low": round(lo, 4) if lo is not None else None,
                "wilson_high": round(hi, 4) if hi is not None else None,
                "clears_84_5": clears,
                "paper_legs": g["legs"],
            }
        )

    lines.append("NBA BOARD")
    lines.append("-" * 72)
    lines.append(f"  status: {nba_flag['status']}")
    lines.append(f"  attempt_prop_rows: {nba_flag['attempt_prop_rows']}")
    lines.append(f"  {nba_flag['reason']}")
    lines.append(f"  flag file: {_NBA_FLAG}")
    lines.append("")
    lines.append("PLAY NOTE (if any): Markell Holman Rush O24.5 (WYO vs AFA) —")
    lines.append("  84% cell, opp rush D Below Avg, wide gap. Still fails 84.5% Wilson bar")
    lines.append("  (n=19 → Wilson low ~62%). Small Flex only. Prefer paper-trade whole slate.")
    lines.append("")
    lines.append(f"Logged -> {_LOG}  (track={TRACK}, dates rewritten)")
    lines.append(f"Report -> {_REPORT_TXT}")

    text = "\n".join(lines)
    _REPORT_TXT.write_text(text, encoding="utf-8")
    _REPORT.write_text(
        json.dumps(
            {
                "track": TRACK,
                "source": SOURCE,
                "goblin_power_bar": GOBLIN_POWER_BAR,
                "n_legs": len(records),
                "dates": [FRI, SAT],
                "gates": summary_rows,
                "nba_board": nba_flag,
                "pending": [
                    "L5 leakage check",
                    "CFB 2026 game logs (drop PRIOR_SEASON_FILL)",
                    "NBA rebuild when attempt props post",
                    "Grade after tip with strict DNP-zero",
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    # Excel gate summary
    write_excel_sheets(
        _REPO / "outputs" / "L5eq5_paper_fri_sat_gates_2026-10-23_24.xlsx",
        {
            "gates": pd.DataFrame(summary_rows),
            "legs": pd.DataFrame(
                [
                    {
                        "date": r["date"],
                        "sport": r["sport"],
                        "pick_type": r["pick_type"],
                        "player": r["player"],
                        "prop": r["prop"],
                        "side": r["direction"],
                        "line": r["line"],
                        "gap": r.get("gap"),
                        "gate": r.get("gate_key"),
                        "gate_hr": r.get("p"),
                        "gate_n": r.get("p_n"),
                        "season_l5_flag": r.get("season_l5_flag"),
                        "board_status": r.get("board_status"),
                        "result": r.get("result"),
                    }
                    for r in records
                ]
            ),
        },
    )
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
