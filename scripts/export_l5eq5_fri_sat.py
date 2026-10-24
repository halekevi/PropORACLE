"""Export every directional L5=5 face for Fri 2026-10-23 + Sat 2026-10-24.

One workbook with Trust (Medium / Low / Skip) + Note. Medium rows sort first.
No bucket has a proven hit rate until the L5 leakage check runs — Medium is the
best rating in the file.

  py -3.14 scripts/_export_l5eq5_fri_sat.py
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from proporacle.data.table_io import read_table, write_excel_sheets  # noqa: E402

FRI = "2026-10-23"
SAT = "2026-10-24"
OUT = _REPO / "outputs" / f"L5eq5_Fri-Sat_{FRI[5:]}_{SAT[5:]}.xlsx".replace(
    "10-23", "2026-10-23"
).replace("10-24", "24")
# Prefer the name the user already uses:
OUT = _REPO / "outputs" / "L5eq5_Fri-Sat_2026-10-23_24.xlsx"

TRUST_ORDER = {"Medium": 0, "Low": 1, "Skip": 2}


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


def _pick_bucket(pt: object) -> str:
    s = _s(pt).upper()
    if "DEMON" in s:
        return "Demon"
    if "GOBLIN" in s:
        return "Goblin"
    if "STANDARD" in s or s in ("", "STD"):
        return "Standard"
    return s or "Unknown"


def _day_label(ymd: str) -> str:
    if ymd == FRI:
        return "Fri 10-23"
    if ymd == SAT:
        return "Sat 10-24"
    return ymd


def _ymd(val) -> str:
    s = _s(val)
    if not s:
        return ""
    return s[:10]


def _col(df: pd.DataFrame, *cands: str) -> str | None:
    lower = {c.lower(): c for c in df.columns}
    for c in cands:
        if c.lower() in lower:
            return lower[c.lower()]
    return None


def _stat_cols(df: pd.DataFrame) -> list[str]:
    cols = []
    for i in range(1, 6):
        c = _col(df, f"stat_g{i}", f"Stat G{i}", f"last5_{i}")
        if c:
            cols.append(c)
    return cols


def _l5_avg_from_stats(vals: list[float | None]) -> float | None:
    nums = [v for v in vals if v is not None]
    if len(nums) < 5:
        return None
    return sum(nums) / 5.0


def _base_row(
    *,
    day: str,
    sport: str,
    pick_type: str,
    side: str,
    player: str,
    team: str,
    opp: str,
    prop: str,
    line,
    l5_avg,
    stats: list,
    season_flag: str,
    opp_def: str,
    tip: str,
    l5_over,
    l5_under,
    source: str,
) -> dict:
    line_f = _f(line)
    avg_f = _f(l5_avg)
    ratio = None
    if line_f is not None and avg_f is not None and avg_f != 0:
        ratio = round(line_f / avg_f, 4)
    pad = list(stats) + [None] * 5
    return {
        "Day": _day_label(day),
        "Sport": sport,
        "Trust": "",
        "Note": "",
        "pick_type": pick_type,
        "side": side,
        "player": player,
        "team": team,
        "opp": opp,
        "prop": prop,
        "line": line_f if line_f is not None else line,
        "l5_avg": round(avg_f, 2) if avg_f is not None else None,
        "line_div_l5avg": ratio,
        "stat_g1": pad[0],
        "stat_g2": pad[1],
        "stat_g3": pad[2],
        "stat_g4": pad[3],
        "stat_g5": pad[4],
        "season_l5_flag": season_flag or None,
        "opp_def_tier": opp_def or None,
        "tip": tip,
        "l5_over": l5_over,
        "l5_under": l5_under,
        "source": source,
        "_day_ymd": day,
    }


def _directional_hit(side: str, l5o, l5u) -> bool:
    if side == "OVER":
        return _f(l5o) == 5.0
    if side == "UNDER":
        return _f(l5u) == 5.0
    return False


def extract_generic(
    path: Path,
    *,
    sport: str,
    day_default: str,
    source: str,
) -> list[dict]:
    if not path.exists():
        return []
    df = read_table(path)
    c_player = _col(df, "player", "Player", "player_name")
    c_prop = _col(df, "prop", "Prop", "prop_type", "prop_display")
    c_line = _col(df, "line", "Line", "line_score")
    c_pt = _col(df, "pick_type", "Pick Type")
    c_dir = _col(df, "direction", "Direction", "side", "final_bet_direction")
    c_l5o = _col(df, "l5_over", "L5 Over", "last5_over")
    c_l5u = _col(df, "l5_under", "L5 Under", "last5_under")
    c_team = _col(df, "team", "Team", "team_abbr", "pp_team")
    c_opp = _col(df, "opponent", "Opponent", "opp", "Opp", "opp_team_abbr", "pp_opp_team", "opp_team")
    c_tip = _col(df, "start_time", "Game Time", "tip", "game_start", "game_date", "Game Date")
    c_flag = _col(df, "season_l5_flag", "Season L5 Flag")
    c_def = _col(df, "opp_def_tier", "DEF_TIER", "def_tier", "Opp Def Tier", "Opp Defense Tier")
    c_avg = _col(df, "l5_avg", "L5 Avg", "avg_L5", "last5_avg", "Intel L5 Avg")
    scols = _stat_cols(df)
    out: list[dict] = []
    for _, r in df.iterrows():
        pt = _pick_bucket(r.get(c_pt) if c_pt else "")
        if pt not in ("Goblin", "Standard", "Demon"):
            continue
        tip_raw = r.get(c_tip) if c_tip else ""
        day = _ymd(tip_raw) or day_default
        if day not in (FRI, SAT):
            continue
        direction = _s(r.get(c_dir) if c_dir else "").upper()
        l5o = _f(r.get(c_l5o) if c_l5o else None)
        l5u = _f(r.get(c_l5u) if c_l5u else None)
        if not direction or direction in ("NO_EDGE", "PASS", "NAN"):
            if pt in ("Goblin", "Demon"):
                direction = "OVER"
            elif l5o == 5 and l5u != 5:
                direction = "OVER"
            elif l5u == 5 and l5o != 5:
                direction = "UNDER"
            else:
                continue
        if pt == "Goblin" and direction != "OVER":
            continue
        if pt == "Demon" and direction != "OVER":
            continue
        if not _directional_hit(direction, l5o, l5u):
            continue
        stats = [_f(r.get(c)) for c in scols] if scols else []
        avg = _f(r.get(c_avg) if c_avg else None)
        if avg is None:
            avg = _l5_avg_from_stats(stats)
        tip_s = _s(tip_raw)
        if tip_s and len(tip_s) >= 16:
            tip_s = tip_s[:16].replace("T", " ")
        elif tip_s and len(tip_s) >= 10:
            tip_s = tip_s[:10]
        out.append(
            _base_row(
                day=day,
                sport=sport,
                pick_type=pt,
                side=direction,
                player=_s(r.get(c_player) if c_player else ""),
                team=_s(r.get(c_team) if c_team else ""),
                opp=_s(r.get(c_opp) if c_opp else ""),
                prop=_s(r.get(c_prop) if c_prop else ""),
                line=r.get(c_line) if c_line else None,
                l5_avg=avg,
                stats=stats,
                season_flag=_s(r.get(c_flag) if c_flag else ""),
                opp_def=_s(r.get(c_def) if c_def else ""),
                tip=tip_s,
                l5_over=l5o,
                l5_under=l5u,
                source=source,
            )
        )
    return out


def assign_trust(row: dict) -> tuple[str, str]:
    sport = row["Sport"]
    pt = row["pick_type"]
    side = row["side"]
    prop = _s(row["prop"]).lower()
    line = _f(row["line"])
    flag = _s(row.get("season_l5_flag"))
    ratio = _f(row.get("line_div_l5avg"))

    if pt == "Demon":
        return "Skip", "Demon — off best-props / Goblin-70 / mixer paths"

    # --- MLB ---
    if sport == "MLB":
        if pt == "Standard" and line is not None and line <= 0.5:
            return "Skip", "Rule 4: Standard 0.5 line — too thin / role trap"
        if pt == "Goblin":
            return "Medium", "MLB Goblin L5=5 (keep props 1–10 still apply for tickets); Medium is best until leakage check"
        return "Low", "MLB Standard L5=5 — Standard stays off G70; observe only"

    # --- Soccer (corrected step8) ---
    if sport == "Soccer":
        if "save" in prop:
            return (
                "Medium",
                "Corrected L5=5 Saves — list OK; Saves stay OFF official tickets",
            )
        if "shot" in prop and "target" not in prop:
            return "Medium", "Corrected L5=5 Shots — ticket still needs L10≥8 + keep"
        return "Medium", "Corrected soccer L5=5 after alias/gap/null→0 fix"

    # --- CFB ---
    if sport in ("CFB", "CFB1H"):
        if pt == "Standard" and side == "UNDER" and ratio is not None and ratio >= 1.5:
            return (
                "Skip",
                f"Role-change UNDER risk: line/L5avg={ratio:.2f} on prior-season tape",
            )
        if flag in ("PRIOR_SEASON_FILL", "PRIOR_SEASON_L5"):
            return "Low", f"L5 uses last season's boxes ({flag})"
        if not flag or flag.upper() in ("NAN", "NONE", ""):
            # Fri CFB often has no step8 — mark Low when source is step5b-only Fri
            if row.get("source", "").startswith("step5b") and row["_day_ymd"] == FRI:
                return "Low", "Fri CFB from step5b only — no step8 keep gates on this cut"
            return "Medium", "This season's L5 window (no PRIOR_SEASON flag)"
        if flag.startswith("THIN"):
            return "Low", f"Thin current-season sample ({flag})"
        return "Low", f"CFB flag={flag or 'unknown'}"

    # --- NFL ---
    if sport == "NFL":
        if "fantasy" in prop:
            return "Skip", "Fantasy prop — off tickets for every sport"
        if flag in ("PRIOR_SEASON_FILL", "PRIOR_SEASON_L5"):
            return "Low", f"L5 padded with prior season ({flag})"
        if pt == "Standard" and line is not None and line <= 0.5:
            return "Skip", "Rule 4: Standard 0.5 line"
        if not flag:
            return "Medium", "NFL L5=5 current-window face; Medium best until leakage check"
        return "Low", f"NFL flag={flag}"

    # --- Tennis ---
    if sport == "Tennis":
        if "ace" in prop or "double fault" in prop or "double_fault" in prop:
            if pt == "Standard" and side == "UNDER":
                return "Skip", "Tennis Standard UNDER Aces/DF faded after serve-actual backfill"
            return "Low", "Tennis Aces/DF — not on Goblin keep path"
        if "1st set" in prop or "first set" in prop:
            return "Low", "1st-set prop — not on tennis keep / G70 path"
        if "break point" in prop:
            return "Low", "Break Points — not a tennis keep prop"
        if "total games" in prop or "games won" in prop:
            return "Medium", "Tennis Games / Total Games L5=5 — keep gates still apply for tickets"
        return "Low", "Tennis L5=5 — prop outside Games Won / Total Games keep set"

    # --- NHL ---
    if sport == "NHL":
        return "Medium", "NHL L5=5 — Medium best until leakage check"

    # --- NBA / others ---
    if sport.startswith("NBA"):
        return "Medium", "NBA L5=5 card-window face; Medium best until leakage check"

    return "Low", f"{sport} L5=5 — no sport-specific trust upgrade"


def dedupe(rows: list[dict]) -> list[dict]:
    seen: set[tuple] = set()
    out: list[dict] = []
    for r in rows:
        key = (
            r["Day"],
            r["Sport"],
            r["pick_type"],
            r["side"],
            r["player"],
            r["prop"],
            r["line"],
            r["team"],
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def main() -> int:
    rows: list[dict] = []

    # Soccer — corrected step8 (post L5 fix rebuild)
    rows += extract_generic(
        _REPO / "outputs/2026-10-23/soccer/step8_soccer_direction_clean.xlsx",
        sport="Soccer",
        day_default=FRI,
        source="soccer step8 corrected",
    )

    # MLB WS G1
    rows += extract_generic(
        _REPO / "outputs/2026-10-23/mlb/step8_mlb_direction.csv",
        sport="MLB",
        day_default=FRI,
        source="mlb step8",
    )

    # NHL
    rows += extract_generic(
        _REPO / "outputs/2026-10-23/nhl/step8_nhl_direction_clean.xlsx",
        sport="NHL",
        day_default=FRI,
        source="nhl step8",
    )

    # NFL — only Fri/Sat tips (board is mostly Sun; may be empty)
    rows += extract_generic(
        _REPO / "outputs/2026-10-23/nfl/step8_nfl_direction_clean.xlsx",
        sport="NFL",
        day_default=FRI,
        source="nfl step8",
    )

    # Tennis — use CSV start_time (ISO). Clean xlsx Game Time is "10/22 7:00 AM".
    rows += extract_generic(
        _REPO / "Sports/Tennis/outputs/step8_tennis_direction.csv",
        sport="Tennis",
        day_default=FRI,
        source="tennis step8 csv",
    )

    # CFB Sat — curated L5eq5 workbook (already directional L5=5).
    sat_xlsx = _REPO / "outputs/2026-10-24/cfb/l5eq5_all_picks_2026-10-24.xlsx"
    if sat_xlsx.exists():
        cfb_sat = read_table(sat_xlsx)
        for _, r in cfb_sat.iterrows():
            tip = _s(r.get("tip"))
            day = _ymd(tip) or SAT
            if day != SAT:
                continue
            stats = [_f(r.get(f"stat_g{i}")) for i in range(1, 6)]
            rows.append(
                _base_row(
                    day=day,
                    sport="CFB",
                    pick_type=_pick_bucket(r.get("pick_type")),
                    side=_s(r.get("side")).upper() or "OVER",
                    player=_s(r.get("player")),
                    team=_s(r.get("team")),
                    opp=_s(r.get("opp")),
                    prop=_s(r.get("prop")),
                    line=r.get("line"),
                    l5_avg=r.get("l5_avg"),
                    stats=stats,
                    season_flag=_s(r.get("season_l5_flag")),
                    opp_def=_s(r.get("opp_def_tier")),
                    tip=tip,
                    l5_over=_f(r.get("l5_over")),
                    l5_under=_f(r.get("l5_under")),
                    source="cfb l5eq5_all_picks sat",
                )
            )
    else:
        sat_only = extract_generic(
            _REPO / "outputs/2026-10-24/cfb/step5b_cfb.csv",
            sport="CFB",
            day_default=SAT,
            source="step5b cfb sat",
        )
        rows += [r for r in sat_only if r["_day_ymd"] == SAT]

    # Fri CFB only (do not pull Sat tips out of the 10-23 step5b board).
    fri_cfb = extract_generic(
        _REPO / "outputs/2026-10-23/cfb/step5b_cfb.csv",
        sport="CFB",
        day_default=FRI,
        source="step5b cfb fri",
    )
    rows += [r for r in fri_cfb if r["_day_ymd"] == FRI]

    # CFB1H — Fri/Sat tips only
    cfb1h = extract_generic(
        _REPO / "outputs/2026-10-23/cfb1h/step5b_cfb1h.csv",
        sport="CFB1H",
        day_default=FRI,
        source="step5b cfb1h",
    )
    rows += [r for r in cfb1h if r["_day_ymd"] in (FRI, SAT)]

    rows = dedupe(rows)
    for r in rows:
        trust, note = assign_trust(r)
        r["Trust"] = trust
        r["Note"] = note

    df = pd.DataFrame(rows)
    if df.empty:
        print("No L5=5 rows found for Fri/Sat.")
        return 1

    df["_trust_ord"] = df["Trust"].map(TRUST_ORDER).fillna(9)
    df["_day_ord"] = df["_day_ymd"].map({FRI: 0, SAT: 1}).fillna(9)
    df = df.sort_values(
        ["_trust_ord", "_day_ord", "Sport", "pick_type", "player", "prop", "line"],
        kind="mergesort",
    ).reset_index(drop=True)
    df.insert(0, "rank", range(1, len(df) + 1))
    df = df.drop(columns=["_trust_ord", "_day_ord", "_day_ymd"])

    summary = (
        df.groupby(["Day", "Sport", "Trust"], dropna=False)
        .size()
        .unstack(fill_value=0)
        .reindex(columns=["Medium", "Low", "Skip"], fill_value=0)
        .reset_index()
    )
    empty_notes = pd.DataFrame(
        [
            {
                "Day": "Fri 10-23",
                "Sport": "NFL",
                "Medium": 0,
                "Low": 0,
                "Skip": 0,
                "Note": "Board tips are Thu 10-22 / Sun 10-25 — no Fri/Sat L5=5 rows",
            },
            {
                "Day": "Fri 10-23",
                "Sport": "NHL",
                "Medium": 0,
                "Low": 0,
                "Skip": 0,
                "Note": "step8 present; no directional L5=5 faces",
            },
            {
                "Day": "Fri-Sat",
                "Sport": "WNBA",
                "Medium": 0,
                "Low": 0,
                "Skip": 0,
                "Note": "No step8 on board",
            },
            {
                "Day": "Fri-Sat",
                "Sport": "NBA",
                "Medium": 0,
                "Low": 0,
                "Skip": 0,
                "Note": "Last board was Thu 10-22 — outside Fri/Sat window",
            },
            {
                "Day": "Sat 10-24",
                "Sport": "NFL/MLB/Soccer/Tennis/NHL",
                "Medium": 0,
                "Low": 0,
                "Skip": 0,
                "Note": "No Sat board for these sports in this export",
            },
        ]
    )
    readme = pd.DataFrame(
        [
            {
                "note": (
                    "Directional L5=5 only for tips on Fri 2026-10-23 or Sat 2026-10-24. "
                    "Trust Medium = best rating here — no bucket has a proven hit rate until "
                    "the L5 leakage check runs. Demons and rule-4 / role-change Unders are Skip. "
                    "Soccer uses the corrected step8 after the L5 window fix."
                )
            }
        ]
    )

    OUT.parent.mkdir(parents=True, exist_ok=True)
    write_excel_sheets(
        OUT,
        {
            "L5eq5": df,
            "Summary": summary,
            "EmptySports": empty_notes,
            "README": readme,
        },
    )

    print(f"Wrote {OUT}  rows={len(df)}")
    print(summary.to_string(index=False))
    print("\nBy pick_type:")
    print(df.groupby(["Sport", "pick_type", "Trust"]).size().to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
