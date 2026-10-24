"""Tomorrow (2026-10-23) L5=5 faces by sport × Goblin/Standard/Demon,
ranked by highest keep/category gate hit rate, with avg→line gap.
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from proporacle.data.table_io import read_table  # noqa: E402
from utils.category_hit_rate import load_category_hr_index, lookup_category_hr  # noqa: E402
from utils.mlb_keep_gates import mlb_goblin_keep_eligible  # noqa: E402
from utils.prop_norm import canon_prop  # noqa: E402
from utils.soccer_keep_gates import soccer_list_eligible  # noqa: E402
from utils.cfb_keep_gates import cfb_list_eligible  # noqa: E402
from utils.tennis_keep_gates import tennis_list_eligible  # noqa: E402

TOMORROW = "2026-10-23"

# Documented keep-gate cells (highest rated gate we price / ticket on).
# (sport, prop_norm, pick, side) -> (hr, n, label)
KEEP_CELL: dict[tuple[str, str, str, str], tuple[float, int, str]] = {
    ("MLB", "hits+runs+rbis", "Goblin", "OVER"): (
        0.808,
        104,
        "BA>=.275 + L5=5 + Opp pitch Weak|Below",
    ),
    ("MLB", "hits", "Goblin", "OVER"): (0.71, 170, "BA>=.275 + L5=5 + Opp pitch Weak|Below"),
    ("MLB", "total_bases", "Goblin", "OVER"): (
        0.71,
        170,
        "BA>=.275 + L5=5 + Opp pitch Weak|Below",
    ),
    ("MLB", "hitter_ks", "Goblin", "OVER"): (
        0.766,
        325,
        "K%>=28 + L10>=8 + Opp pitch Elite|Above",
    ),
    ("SOCCER", "shots", "Goblin", "OVER"): (0.762, 21, "L5=5 + L10>=8"),
    ("SOCCER", "shots", "Standard", "OVER"): (0.762, 21, "L5=5 + L10>=8"),
    ("SOCCER", "shots", "Standard", "UNDER"): (0.762, 21, "L5=5 + L10>=8 (Shots keep)"),
    ("SOCCER", "sog", "Goblin", "OVER"): (0.676, 68, "L5>=4 + Off (list); ticket L5=5"),
    ("SOCCER", "saves", "Goblin", "OVER"): (
        0.598,
        343,
        "Goblin Saves 1.5 ungated (OFF tickets; L5>=4 cell 80.8% n=26 is not a price)",
    ),
    ("SOCCER", "saves", "Standard", "UNDER"): (
        0.598,
        343,
        "Saves OFF tickets (Goblin 1.5 band ~60%)",
    ),
    ("CFB", "pass_yds", "Goblin", "OVER"): (0.74, 0, "L5=5+cover, no D (ungated)"),
    ("CFB", "rush_yds", "Goblin", "OVER"): (0.84, 19, "L5=5 + D (16/19)"),
    ("CFB", "rush_yds", "Standard", "UNDER"): (0.71, 2, "L5=5+D+cover (thin 2/2)"),
    ("TENNIS", "games_won", "Goblin", "OVER"): (
        0.727,
        99,
        "Std−Goblin>=4 discount (cap price 0.62)",
    ),
    ("TENNIS", "match_total_games", "Goblin", "OVER"): (
        0.702,
        208,
        "L5>=4 + L10>=8",
    ),
    ("TENNIS", "games_won", "Standard", "OVER"): (
        0.75,
        40,
        "L10>=8 + (L5gap>=5 | L5 2nd-won%>=45.7)",
    ),
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


def _pick(pt) -> str:
    s = _s(pt).upper()
    if "DEMON" in s:
        return "Demon"
    if "GOBLIN" in s:
        return "Goblin"
    if "STANDARD" in s or s in ("", "STD"):
        return "Standard"
    return s or "Unknown"


def _col(df, *cands):
    lower = {c.lower(): c for c in df.columns}
    for c in cands:
        if c.lower() in lower:
            return lower[c.lower()]
    return None


def _ymd(v) -> str:
    return _s(v)[:10]


def extract(path: Path, sport: str) -> list[dict]:
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
    c_team = _col(df, "team", "Team", "team_abbr")
    c_opp = _col(df, "opponent", "Opponent", "opp", "Opp", "opp_team_abbr", "opp_team")
    c_tip = _col(df, "start_time", "Game Time", "tip", "game_date", "Game Date")
    c_avg = _col(df, "l5_avg", "L5 Avg", "avg_L5", "last5_avg", "Intel L5 Avg")
    c_def = _col(df, "opp_def_tier", "DEF_TIER", "def_tier", "Opp Def Tier")
    c_flag = _col(df, "season_l5_flag", "Season L5 Flag")
    c_l10o = _col(df, "l10_over", "L10 Over", "last10_over")
    c_l10u = _col(df, "l10_under", "L10 Under", "last10_under")
    scols = [c for i in range(1, 6) if (c := _col(df, f"stat_g{i}", f"Stat G{i}"))]
    out = []
    for _, r in df.iterrows():
        day = _ymd(r.get(c_tip) if c_tip else "") or TOMORROW
        if day != TOMORROW:
            continue
        pt = _pick(r.get(c_pt) if c_pt else "")
        if pt not in ("Goblin", "Standard", "Demon"):
            continue
        side = _s(r.get(c_dir) if c_dir else "").upper()
        l5o = _f(r.get(c_l5o) if c_l5o else None)
        l5u = _f(r.get(c_l5u) if c_l5u else None)
        if not side or side in ("NO_EDGE", "PASS"):
            if pt in ("Goblin", "Demon"):
                side = "OVER"
            elif l5o == 5 and l5u != 5:
                side = "OVER"
            elif l5u == 5 and l5o != 5:
                side = "UNDER"
            else:
                continue
        if pt == "Goblin" and side != "OVER":
            continue
        if pt == "Demon" and side != "OVER":
            continue
        hit = l5o if side == "OVER" else l5u
        if hit != 5:
            continue
        stats = [_f(r.get(c)) for c in scols] if scols else []
        avg = _f(r.get(c_avg) if c_avg else None)
        if avg is None and len([v for v in stats if v is not None]) == 5:
            avg = sum(v for v in stats if v is not None) / 5.0
        line = _f(r.get(c_line) if c_line else None)
        gap = None
        if avg is not None and line is not None:
            gap = (avg - line) if side == "OVER" else (line - avg)
        row = {
            "sport": sport,
            "pick_type": pt,
            "side": side,
            "player": _s(r.get(c_player) if c_player else ""),
            "team": _s(r.get(c_team) if c_team else ""),
            "opp": _s(r.get(c_opp) if c_opp else ""),
            "prop": _s(r.get(c_prop) if c_prop else ""),
            "line": line,
            "l5_avg": round(avg, 2) if avg is not None else None,
            "gap": round(gap, 2) if gap is not None else None,
            "l5_over": l5o,
            "l5_under": l5u,
            "l10_over": _f(r.get(c_l10o) if c_l10o else None),
            "l10_under": _f(r.get(c_l10u) if c_l10u else None),
            "def_tier": _s(r.get(c_def) if c_def else ""),
            "season_l5_flag": _s(r.get(c_flag) if c_flag else ""),
            "tip": _s(r.get(c_tip) if c_tip else "")[:16].replace("T", " "),
            "_raw": dict(r),
        }
        out.append(row)
    return out


def _gate_for(row: dict, idx) -> tuple[float | None, str, str]:
    """Return (hr, gate_label, source). Prefer keep cell when eligible."""
    sport = row["sport"].upper()
    if sport == "SOCCER":
        sport_key = "SOCCER"
    elif sport == "TENNIS":
        sport_key = "TENNIS"
    else:
        sport_key = sport
    prop_n = canon_prop(sport_key if sport_key != "CFB1H" else "CFB", row["prop"])
    pick = row["pick_type"]
    side = row["side"]

    # Build eligibility probe row
    probe = {
        "sport": sport,
        "prop": row["prop"],
        "prop_type": row["prop"],
        "pick_type": pick,
        "side": side,
        "direction": side,
        "l5_over": row["l5_over"],
        "l5_under": row["l5_under"],
        "l10_over": row.get("l10_over"),
        "l10_under": row.get("l10_under"),
        "def_tier": row.get("def_tier"),
        "DEF_TIER": row.get("def_tier"),
        "line": row["line"],
        **{k: v for k, v in (row.get("_raw") or {}).items() if not str(k).startswith("_")},
    }

    keep_ok = None
    if sport == "MLB" and pick == "Goblin":
        try:
            keep_ok = mlb_goblin_keep_eligible(probe)
        except Exception:
            keep_ok = False
    elif sport == "SOCCER":
        try:
            keep_ok = soccer_list_eligible(probe)
        except Exception:
            keep_ok = False
    elif sport in ("CFB", "CFB1H"):
        try:
            keep_ok = cfb_list_eligible(probe)
        except Exception:
            keep_ok = True
    elif sport == "TENNIS":
        try:
            keep_ok = tennis_list_eligible(probe)
        except Exception:
            keep_ok = False

    key = (sport_key if sport != "CFB1H" else "CFB", prop_n, pick, side)
    # CFB1H same fold
    if sport == "CFB1H":
        key = ("CFB", prop_n, pick, side)

    if keep_ok and key in KEEP_CELL:
        hr, n, label = KEEP_CELL[key]
        return hr, f"KEEP {label}" + (f" n={n}" if n else ""), "keep_gate"
    if keep_ok is False and key in KEEP_CELL:
        # Still show the keep cell as reference but mark miss
        hr, n, label = KEEP_CELL[key]
        cat = lookup_category_hr(
            idx, sport=sport_key, prop=row["prop"], pick_type=pick, direction=side
        )
        if cat:
            return (
                float(cat["hr"]),
                f"keep-MISS ({label}); category {cat['hr']:.1%} n={int(cat['n'])}",
                "category_after_keep_miss",
            )
        return hr, f"keep-MISS ref {label} — using keep rate as ceiling only", "keep_ref_miss"

    if key in KEEP_CELL and keep_ok is None:
        hr, n, label = KEEP_CELL[key]
        return hr, f"KEEP {label}" + (f" n={n}" if n else ""), "keep_gate"

    cat = lookup_category_hr(
        idx, sport=sport_key, prop=row["prop"], pick_type=pick, direction=side
    )
    if cat:
        return float(cat["hr"]), f"category ungated {cat['hr']:.1%} n={int(cat['n'])}", "category"

    # Demon / unknown: no cell
    if pick == "Demon":
        return None, "Demon — no keep cell (faded)", "none"
    return None, "no gate/category cell", "none"


def dedupe(rows: list[dict]) -> list[dict]:
    seen = set()
    out = []
    for r in rows:
        k = (r["sport"], r["pick_type"], r["side"], r["player"], r["prop"], r["line"], r["team"])
        if k in seen:
            continue
        seen.add(k)
        out.append(r)
    return out


def main() -> int:
    idx = load_category_hr_index()
    rows: list[dict] = []
    rows += extract(_REPO / "outputs/2026-10-23/soccer/step8_soccer_direction_clean.xlsx", "Soccer")
    rows += extract(_REPO / "outputs/2026-10-23/mlb/step8_mlb_direction.csv", "MLB")
    rows += extract(_REPO / "outputs/2026-10-23/nhl/step8_nhl_direction_clean.xlsx", "NHL")
    rows += extract(_REPO / "outputs/2026-10-23/nfl/step8_nfl_direction_clean.xlsx", "NFL")
    rows += extract(_REPO / "Sports/Tennis/outputs/step8_tennis_direction.csv", "Tennis")
    # CFB Fri tips from step5b
    rows += extract(_REPO / "outputs/2026-10-23/cfb/step5b_cfb.csv", "CFB")
    rows += extract(_REPO / "outputs/2026-10-23/cfb1h/step5b_cfb1h.csv", "CFB1H")
    rows = dedupe(rows)

    for r in rows:
        hr, label, src = _gate_for(r, idx)
        r["gate_hr"] = hr
        r["gate_label"] = label
        r["gate_src"] = src
        del r["_raw"]

    # Sort within sport × pick_type by gate_hr desc, gap desc
    sports_order = ["MLB", "Soccer", "Tennis", "CFB", "CFB1H", "NHL", "NFL"]
    pick_order = ["Goblin", "Standard", "Demon"]

    lines = []
    lines.append(f"TOMORROW L5=5  slate={TOMORROW}  n={len(rows)}")
    lines.append(
        "Ranked by highest keep/category gate hit rate, then avg→line gap "
        "(OVER: L5avg−line; UNDER: line−L5avg)."
    )
    lines.append("Demons listed for inventory only — we do not play them.")
    lines.append("")

    by_sport = {s: [r for r in rows if r["sport"] == s] for s in sports_order}
    # any leftover sports
    for r in rows:
        if r["sport"] not in by_sport:
            by_sport.setdefault(r["sport"], []).append(r)

    for sport in list(by_sport.keys()):
        sport_rows = by_sport[sport]
        lines.append(f"===== {sport} =====")
        if not sport_rows:
            lines.append("  (none)")
            lines.append("")
            continue
        for pt in pick_order:
            bucket = [r for r in sport_rows if r["pick_type"] == pt]
            if not bucket:
                lines.append(f"--- {pt} (n=0) ---")
                lines.append("  (none)")
                continue
            bucket.sort(
                key=lambda r: (
                    -(r["gate_hr"] if r["gate_hr"] is not None else -1),
                    -(r["gap"] if r["gap"] is not None else -999),
                    r["player"],
                    r["prop"],
                )
            )
            lines.append(f"--- {pt} (n={len(bucket)}) ---")
            for i, r in enumerate(bucket, 1):
                hr_s = f"{r['gate_hr']*100:.1f}%" if r["gate_hr"] is not None else "n/a"
                gap_s = f"{r['gap']:+.2f}" if r["gap"] is not None else "n/a"
                avg_s = f"{r['l5_avg']:.2f}" if r["l5_avg"] is not None else "?"
                side_c = "O" if r["side"] == "OVER" else "U"
                match = f"{r['team']}" + (f" vs {r['opp']}" if r["opp"] else "")
                flag = f"  [{r['season_l5_flag']}]" if r.get("season_l5_flag") else ""
                lines.append(
                    f"  {i:>2}. {r['player']}  {r['prop']}  {side_c}{r['line']}  "
                    f"({match})  L5avg={avg_s}  gap={gap_s}  "
                    f"gate={hr_s}  {r['gate_label']}{flag}"
                )
        lines.append("")

    text = "\n".join(lines)
    out = _REPO / "outputs" / "2026-10-23" / "l5eq5_tomorrow_ranked_by_gate.txt"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text, encoding="utf-8")
    print(text)
    print(f"\nWrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
