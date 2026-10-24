#!/usr/bin/env python3
"""Build NFL O/D rankings markdown from live unit / defense CSVs.

Always writes:
  Sports/NFL/data/nfl_od_rankings_current.md   ← agents + tickets should read this
  Sports/NFL/data/nfl_od_rankings_meta.json

With --week N also archives:
  Sports/NFL/data/nfl_od_rankings_weekN.md

Does not overwrite sealed prior-week archives unless --force-archive.

Refresh::

  py -3.14 Sports/NFL/scripts/build_nfl_od_rankings_md.py --season 2026 --week 3
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

_SCRIPT = Path(__file__).resolve().parent
_NFL = _SCRIPT.parent
_REPO = _SCRIPT.parents[2]
_DATA = _NFL / "data"
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from utils.nfl_espn_context import NFL_SCOREBOARD, _get_json, canon_nfl_abbr  # noqa: E402

GATE_NOTES = """## Gate integration notes

- **Rec yards / receptions / longest rec UNDER**: key off Opp Pass D Rk + Opp Sec Rk. Stack with Low WOPR player for highest confidence.
- **Rush yards UNDER**: key off Opp Rush D Rk + Opp Box Rk. Both Elite = strongest signal.
- **Rush yards OVER**: own Rush Off Tier Elite/Above + Opp Rush D Weak/Below.
- **Sacks UNDER**: own OL pass tier × opp DL tier. Elite OL vs Weak DL = highest confidence.
- **Pass yards**: gated out both sides until Gate70 sample builds (post Week 4+).
- **Next week**: O/D tier columns carry forward. Swap matchup pairing sheet with new schedule only.
"""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _mtime_iso(path: Path) -> str | None:
    if not path.is_file():
        return None
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat()


def _latest_season(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "season" not in df.columns:
        return df
    s = pd.to_numeric(df["season"], errors="coerce")
    mx = s.max()
    if pd.isna(mx):
        return df
    return df[s == mx].copy()


def _read_csv(name: str) -> pd.DataFrame:
    p = _DATA / name
    if not p.is_file():
        return pd.DataFrame()
    df = pd.read_csv(p, encoding="utf-8-sig")
    if "team" in df.columns:
        df = df.copy()
        df["team"] = df["team"].map(lambda x: canon_nfl_abbr(x) or str(x).upper().strip())
    return _latest_season(df)


def tier_from_rank(r: Any, n: int = 32) -> str:
    if r is None or (isinstance(r, float) and pd.isna(r)):
        return ""
    try:
        v = float(r)
    except (TypeError, ValueError):
        return ""
    if v <= 8:
        return "Elite"
    if v <= 14:
        return "Above"
    if v <= 20:
        return "Avg"
    if v <= 26:
        return "Below"
    return "Weak"


def _fmt_num(v: Any, digits: int = 0) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    try:
        x = float(v)
    except (TypeError, ValueError):
        return ""
    if digits == 0:
        return str(int(round(x)))
    return f"{x:.{digits}f}"


def _top_shares() -> pd.DataFrame:
    p = _DATA / "nfl_player_prop_share.csv"
    if not p.is_file():
        return pd.DataFrame(columns=["team", "top_rec_share", "top_rush_share"])
    df = pd.read_csv(p, encoding="utf-8-sig")
    df["team"] = df["team"].map(lambda x: canon_nfl_abbr(x) or str(x).upper().strip())
    df["prop"] = df["prop"].astype(str).str.lower()
    df["share_pct"] = pd.to_numeric(df["share_pct"], errors="coerce")
    rows = []
    for team, g in df.groupby("team"):
        rec = g[g["prop"].str.contains("receiving_yards|receptions", regex=True)]
        rush = g[g["prop"].str.contains("rushing_yards", regex=True) & ~g["prop"].str.contains("rec")]
        rows.append(
            {
                "team": team,
                "top_rec_share": float(rec["share_pct"].max()) if rec["share_pct"].notna().any() else None,
                "top_rush_share": float(rush["share_pct"].max()) if rush["share_pct"].notna().any() else None,
            }
        )
    return pd.DataFrame(rows)


def _md_table(headers: list[str], rows: list[list[str]]) -> str:
    sep = ["-" * max(3, len(h)) for h in headers]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(sep) + " |",
    ]
    for row in rows:
        padded = list(row) + [""] * (len(headers) - len(row))
        lines.append("| " + " | ".join(str(c) for c in padded[: len(headers)]) + " |")
    return "\n".join(lines)


def fetch_week_pairings(season: int, week: int) -> list[tuple[str, str]]:
    """Return (team, opp) both directions for ESPN regular-season week."""
    params = {"seasontype": 2, "week": int(week), "dates": int(season)}
    try:
        payload = _get_json(NFL_SCOREBOARD, params=params, timeout=45.0)
    except Exception as exc:
        print(f"[od-md] ESPN week pairings failed: {exc}")
        return []
    pairs: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for ev in payload.get("events") or []:
        comps = (ev.get("competitions") or [{}])[0].get("competitors") or []
        home = away = ""
        for c in comps:
            abbr = canon_nfl_abbr((c.get("team") or {}).get("abbreviation"))
            if not abbr:
                continue
            if c.get("homeAway") == "home":
                home = abbr
            else:
                away = abbr
        if not home or not away:
            continue
        for a, b in ((home, away), (away, home)):
            key = (a, b)
            if key not in seen:
                seen.add(key)
                pairs.append(key)
    return pairs


def _pairings_from_games_csv(season: int, week: int) -> list[tuple[str, str]]:
    p = _DATA / "nfl_game_results.csv"
    if not p.is_file():
        return []
    df = pd.read_csv(p, encoding="utf-8-sig")
    if df.empty:
        return []
    s = pd.to_numeric(df["season"], errors="coerce")
    w = pd.to_numeric(df["week"], errors="coerce")
    sub = df[(s == season) & (w == week)].copy()
    if sub.empty:
        return []
    pairs: list[tuple[str, str]] = []
    for _, row in sub.iterrows():
        home = canon_nfl_abbr(row.get("home"))
        away = canon_nfl_abbr(row.get("away"))
        if home and away:
            pairs.extend([(home, away), (away, home)])
    return pairs


def build_boards() -> dict[str, Any]:
    units = _read_csv("nfl_team_unit_ranks.csv")
    defense = _read_csv("defense_rankings.csv")
    ol = _read_csv("nfl_ol_ranks.csv")
    dl = _read_csv("nfl_dl_ranks.csv")
    sec = _read_csv("nfl_secondary_ranks.csv")
    box = _read_csv("nfl_box_ranks.csv")
    shares = _top_shares()

    sources = {
        "nfl_team_unit_ranks.csv": {
            "mtime": _mtime_iso(_DATA / "nfl_team_unit_ranks.csv"),
            "season": int(units["season"].iloc[0]) if not units.empty and "season" in units.columns else None,
            "n": int(len(units)),
        },
        "defense_rankings.csv": {
            "mtime": _mtime_iso(_DATA / "defense_rankings.csv"),
            "season": int(defense["season"].iloc[0]) if not defense.empty and "season" in defense.columns else None,
            "n": int(len(defense)),
        },
        "nfl_ol_ranks.csv": {
            "mtime": _mtime_iso(_DATA / "nfl_ol_ranks.csv"),
            "season": int(ol["season"].iloc[0]) if not ol.empty and "season" in ol.columns else None,
            "n": int(len(ol)),
        },
        "nfl_dl_ranks.csv": {
            "mtime": _mtime_iso(_DATA / "nfl_dl_ranks.csv"),
            "season": int(dl["season"].iloc[0]) if not dl.empty and "season" in dl.columns else None,
            "n": int(len(dl)),
        },
        "nfl_secondary_ranks.csv": {
            "mtime": _mtime_iso(_DATA / "nfl_secondary_ranks.csv"),
            "season": int(sec["season"].iloc[0]) if not sec.empty and "season" in sec.columns else None,
            "n": int(len(sec)),
        },
        "nfl_box_ranks.csv": {
            "mtime": _mtime_iso(_DATA / "nfl_box_ranks.csv"),
            "season": int(box["season"].iloc[0]) if not box.empty and "season" in box.columns else None,
            "n": int(len(box)),
        },
    }

    if units.empty:
        raise RuntimeError("nfl_team_unit_ranks.csv missing/empty — run build_nfl_division_ranks.py")

    off = units[
        [
            c
            for c in (
                "team",
                "pass_off_rank",
                "rush_off_rank",
                "pass_off_ypg",
                "rush_off_ypg",
                "pass_def_rank",
                "rush_def_rank",
                "opp_pass_ypg",
                "opp_rush_ypg",
            )
            if c in units.columns
        ]
    ].copy()
    if not shares.empty:
        off = off.merge(shares, on="team", how="left")
    else:
        off["top_rec_share"] = None
        off["top_rush_share"] = None

    # Prefer defense_rankings YPG when present (same axis as step4).
    if not defense.empty:
        dcols = defense[["team"] + [c for c in ("pass_yards_allowed_pg", "rush_yards_allowed_pg", "pass_def_rank", "rush_def_rank") if c in defense.columns]].copy()
        off = off.merge(dcols, on="team", how="left", suffixes=("", "_def"))
        if "pass_yards_allowed_pg" in off.columns:
            off["opp_pass_ypg"] = off["pass_yards_allowed_pg"].fillna(off.get("opp_pass_ypg"))
        if "rush_yards_allowed_pg" in off.columns:
            off["opp_rush_ypg"] = off["rush_yards_allowed_pg"].fillna(off.get("opp_rush_ypg"))
        if "pass_def_rank_def" in off.columns:
            off["pass_def_rank"] = off["pass_def_rank"].fillna(off["pass_def_rank_def"])
        if "rush_def_rank_def" in off.columns:
            off["rush_def_rank"] = off["rush_def_rank"].fillna(off["rush_def_rank_def"])

    if not sec.empty:
        off = off.merge(
            sec[["team", "secondary_rank", "tier"]].rename(columns={"tier": "secondary_tier"}),
            on="team",
            how="left",
        )
    else:
        off["secondary_rank"] = None
        off["secondary_tier"] = ""
    if not box.empty:
        off = off.merge(
            box[["team", "box_rank", "tier"]].rename(columns={"tier": "box_tier"}),
            on="team",
            how="left",
        )
    else:
        off["box_rank"] = None
        off["box_tier"] = ""
    if not dl.empty:
        off = off.merge(
            dl[["team", "dl_rank", "sacks_pg", "tier"]].rename(columns={"tier": "dl_tier"}),
            on="team",
            how="left",
        )
    else:
        off["dl_rank"] = None
        off["sacks_pg"] = None
        off["dl_tier"] = ""
    if not ol.empty:
        off = off.merge(
            ol[["team", "ol_rank", "tier"]].rename(columns={"tier": "ol_tier"}),
            on="team",
            how="left",
        )

    for col in ("pass_off_rank", "rush_off_rank", "pass_def_rank", "rush_def_rank"):
        off[f"{col}_tier"] = off[col].map(tier_from_rank)

    return {"off": off, "sources": sources}


def _lean_tables(off: pd.DataFrame, pairs: list[tuple[str, str]], edge_min: float = 8.0) -> dict[str, list[list[str]]]:
    by_team = off.set_index("team")
    pass_over: list[tuple[float, list[str]]] = []
    pass_under: list[tuple[float, list[str]]] = []
    rush_over: list[tuple[float, list[str]]] = []
    rush_under: list[tuple[float, list[str]]] = []

    for team, opp in pairs:
        if team not in by_team.index or opp not in by_team.index:
            continue
        t = by_team.loc[team]
        o = by_team.loc[opp]
        try:
            po = float(t["pass_off_rank"])
            pd_ = float(o["pass_def_rank"])
            ro = float(t["rush_off_rank"])
            rd = float(o["rush_def_rank"])
        except (TypeError, ValueError, KeyError):
            continue
        pass_edge = pd_ - po
        rush_edge = rd - ro
        prow = [team, opp, _fmt_num(po), _fmt_num(pd_), f"{int(pass_edge):+d}"]
        rrow = [team, opp, _fmt_num(ro), _fmt_num(rd), f"{int(rush_edge):+d}"]
        if pass_edge >= edge_min:
            pass_over.append((pass_edge, prow))
        if pass_edge <= -edge_min:
            pass_under.append((pass_edge, prow))
        if rush_edge >= edge_min:
            rush_over.append((rush_edge, rrow))
        if rush_edge <= -edge_min:
            rush_under.append((rush_edge, rrow))

    def _sort(rows: list[tuple[float, list[str]]], reverse: bool) -> list[list[str]]:
        rows = sorted(rows, key=lambda x: x[0], reverse=reverse)
        return [r for _, r in rows]

    return {
        "pass_over": _sort(pass_over, True),
        "pass_under": _sort(pass_under, False),
        "rush_over": _sort(rush_over, True),
        "rush_under": _sort(rush_under, False),
    }


def render_md(
    *,
    season: int,
    week: int | None,
    off: pd.DataFrame,
    sources: dict[str, Any],
    leans: dict[str, list[list[str]]] | None,
    built_at: str,
) -> str:
    title_week = f"Week {week} {season}" if week else f"Current ({season})"
    src_bits = []
    for name, meta in sources.items():
        if meta.get("season") is not None:
            src_bits.append(f"{name} s{meta['season']}")
    src_line = ", ".join(src_bits) if src_bits else "unit/defense CSVs"

    lines = [
        f"# NFL O/D Rankings by Prop — {title_week}",
        "",
        f"Source: {src_line}",
        f"Built: `{built_at}` (UTC) · **canonical live file:** `nfl_od_rankings_current.md`",
        "Tier: 1–8 Elite · 9–14 Above · 15–20 Avg · 21–26 Below · 27–32 Weak",
        "Edge = opp_def_rank − own_off_rank (≥+8 OVER lean · ≤−8 UNDER lean)",
        "",
        "---",
        "",
        "## Pass Offense (rec yards / pass yards OVER context)",
        "",
    ]

    pass_off = off.sort_values("pass_off_rank", na_position="last")
    rows = []
    for _, r in pass_off.iterrows():
        rows.append(
            [
                _fmt_num(r["pass_off_rank"]),
                r["team"],
                tier_from_rank(r["pass_off_rank"]),
                _fmt_num(r.get("pass_off_ypg")),
                _fmt_num(r.get("top_rec_share")),
            ]
        )
    lines.append(_md_table(["Rk", "Team", "Tier", "YPG", "Top Rec%"], rows))
    lines.extend(["", "---", "", "## Rush Offense (rush yards OVER context)", ""])

    rush_off = off.sort_values("rush_off_rank", na_position="last")
    rows = []
    for _, r in rush_off.iterrows():
        rows.append(
            [
                _fmt_num(r["rush_off_rank"]),
                r["team"],
                tier_from_rank(r["rush_off_rank"]),
                _fmt_num(r.get("rush_off_ypg")),
                _fmt_num(r.get("top_rush_share")),
            ]
        )
    lines.append(_md_table(["Rk", "Team", "Tier", "YPG", "Top Rush%"], rows))
    lines.extend(
        [
            "",
            "---",
            "",
            "## Pass Defense (rec yards / pass yards UNDER context)",
            "Rank 1 = stingiest. UNDER targets → Elite/Above. OVER context → Weak/Below.",
            "",
        ]
    )
    pass_def = off.sort_values("pass_def_rank", na_position="last")
    rows = []
    for _, r in pass_def.iterrows():
        rows.append(
            [
                _fmt_num(r["pass_def_rank"]),
                r["team"],
                tier_from_rank(r["pass_def_rank"]),
                _fmt_num(r.get("opp_pass_ypg")),
                _fmt_num(r.get("secondary_rank")),
            ]
        )
    lines.append(_md_table(["Rk", "Team", "Tier", "Yds/G", "Sec Rk"], rows))
    lines.extend(
        [
            "",
            "---",
            "",
            "## Rush Defense (rush yards UNDER context)",
            "Rank 1 = stingiest.",
            "",
        ]
    )
    rush_def = off.sort_values("rush_def_rank", na_position="last")
    rows = []
    for _, r in rush_def.iterrows():
        rows.append(
            [
                _fmt_num(r["rush_def_rank"]),
                r["team"],
                tier_from_rank(r["rush_def_rank"]),
                _fmt_num(r.get("opp_rush_ypg")),
                _fmt_num(r.get("box_rank")),
            ]
        )
    lines.append(_md_table(["Rk", "Team", "Tier", "Yds/G", "Box Rk"], rows))
    lines.extend(
        [
            "",
            "---",
            "",
            "## Pass Rush / Sacks (sacks UNDER gate: own OL × opp DL)",
            "",
        ]
    )
    sack = off.copy()
    sack["_s"] = pd.to_numeric(sack.get("sacks_pg"), errors="coerce")
    sack = sack.sort_values(["_s", "dl_rank"], ascending=[False, True], na_position="last")
    # dense rank by sacks_pg
    sack["_rk"] = sack["_s"].rank(method="min", ascending=False)
    rows = []
    for _, r in sack.iterrows():
        rows.append(
            [
                _fmt_num(r["_rk"]),
                r["team"],
                str(r.get("dl_tier") or tier_from_rank(r.get("dl_rank"))),
                _fmt_num(r.get("sacks_pg")),
            ]
        )
    lines.append(_md_table(["Rk", "Team", "DL Tier", "Sacks/G"], rows))
    lines.extend(
        [
            "",
            "---",
            "",
            "## Secondary Rankings (rec yards / receptions / longest reception UNDER)",
            "Rank 1 = best coverage. Elite secondary + Low WOPR = highest UNDER confidence stack.",
            "",
        ]
    )
    sec = off.sort_values("secondary_rank", na_position="last")
    rows = []
    for _, r in sec.iterrows():
        rows.append(
            [
                _fmt_num(r.get("secondary_rank")),
                r["team"],
                str(r.get("secondary_tier") or ""),
                str(r.get("box_tier") or ""),
            ]
        )
    lines.append(_md_table(["Rk", "Team", "Sec Tier", "Box Tier"], rows))

    if week is not None and leans is not None:
        lines.extend(["", "---", "", f"## Week {week} Matchup Leans", ""])
        for title, key, headers in (
            ("### Pass / Rec OVER leans", "pass_over", ["Team", "Opp", "Pass Off Rk", "Opp Pass D Rk", "Edge"]),
            ("### Pass / Rec UNDER leans", "pass_under", ["Team", "Opp", "Pass Off Rk", "Opp Pass D Rk", "Edge"]),
            ("### Rush OVER leans", "rush_over", ["Team", "Opp", "Rush Off Rk", "Opp Rush D Rk", "Edge"]),
            ("### Rush UNDER leans", "rush_under", ["Team", "Opp", "Rush Off Rk", "Opp Rush D Rk", "Edge"]),
        ):
            lines.append(title)
            body = leans.get(key) or []
            if body:
                lines.append(_md_table(headers, body))
            else:
                lines.append("_No leans at |edge| ≥ 8._")
            lines.append("")

    lines.extend(["---", "", GATE_NOTES.strip(), ""])
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description="Build NFL O/D rankings current.md (+ week archive).")
    ap.add_argument("--season", type=int, default=2026)
    ap.add_argument("--week", type=int, default=0, help="Upcoming slate week for matchup leans / archive (0 = ranks only).")
    ap.add_argument("--force-archive", action="store_true", help="Overwrite nfl_od_rankings_weekN.md if it exists.")
    ap.add_argument("--skip-espn", action="store_true", help="Only use nfl_game_results.csv for pairings.")
    args = ap.parse_args()

    built_at = _now_iso()
    boards = build_boards()
    off = boards["off"]
    sources = boards["sources"]

    week = int(args.week) or None
    leans = None
    pair_src = None
    if week:
        pairs = []
        if not args.skip_espn:
            pairs = fetch_week_pairings(int(args.season), week)
            if pairs:
                pair_src = "espn_scoreboard"
        if not pairs:
            pairs = _pairings_from_games_csv(int(args.season), week)
            if pairs:
                pair_src = "nfl_game_results.csv"
        leans = _lean_tables(off, pairs) if pairs else {
            "pass_over": [],
            "pass_under": [],
            "rush_over": [],
            "rush_under": [],
        }
        print(f"[od-md] week={week} pairings={len(pairs)//2} games src={pair_src or 'none'}")

    md = render_md(
        season=int(args.season),
        week=week,
        off=off,
        sources=sources,
        leans=leans,
        built_at=built_at,
    )

    current_path = _DATA / "nfl_od_rankings_current.md"
    current_path.write_text(md, encoding="utf-8")
    print(f"[od-md] wrote {current_path}")

    archive_path = None
    if week:
        archive_path = _DATA / f"nfl_od_rankings_week{week}.md"
        if archive_path.is_file() and not args.force_archive:
            print(f"[od-md] archive exists (sealed): {archive_path} — skip (pass --force-archive to overwrite)")
            archive_path = None
        else:
            archive_path.write_text(md, encoding="utf-8")
            print(f"[od-md] wrote archive {archive_path}")

    meta = {
        "built_at": built_at,
        "season": int(args.season),
        "week": week,
        "current_md": str(current_path.relative_to(_REPO)).replace("\\", "/"),
        "archive_md": (
            str(archive_path.relative_to(_REPO)).replace("\\", "/") if archive_path else None
        ),
        "pairing_source": pair_src,
        "sources": sources,
        "canonical": "nfl_od_rankings_current.md",
        "note": "Prefer nfl_od_rankings_current.md over sealed weekN archives for live tickets.",
    }
    meta_path = _DATA / "nfl_od_rankings_meta.json"
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"[od-md] wrote {meta_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
