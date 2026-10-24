#!/usr/bin/env python3
"""Rank existing CFB line-secondary z + ESPN unit YPG into NFL-shaped CSVs + markdown.

Does **not** recompute OL/DL/secondary z-scores. Reads
``Sports/CFB/data/reference/cfb_line_secondary.csv`` (or invokes the existing
builder if empty and a CFBD key is present). Box ranks are an alias of
``dl_index``.

Writes::

  Sports/CFB/data/cfb_ol_ranks.csv
  Sports/CFB/data/cfb_dl_ranks.csv
  Sports/CFB/data/cfb_secondary_ranks.csv
  Sports/CFB/data/cfb_box_ranks.csv
  Sports/CFB/data/cfb_od_rankings_week{N}.md

Refresh::

  py -3.14 Sports/CFB/scripts/build_cfb_unit_line_ranks.py
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

_SCRIPT = Path(__file__).resolve().parent
_CFB = _SCRIPT.parent
_REPO = _SCRIPT.parents[2]
_DATA = _CFB / "data"
_REF = _DATA / "reference"
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from utils.cfb_line_secondary import (  # noqa: E402
    build_team_lookup,
    index_rows,
    load_line_secondary_table,
    write_outputs,
)
from utils.cfb_unit_line_ranks import (  # noqa: E402
    cfb_week_from_date,
    clean_cfb_abbr,
    now_iso,
    od_board_authority_note,
    percentile_tier,
    rank_higher_better,
)
from utils.cfbd_client import cfbd_get, infer_default_year, resolve_cfbd_key  # noqa: E402

OUT_OL = _DATA / "cfb_ol_ranks.csv"
OUT_DL = _DATA / "cfb_dl_ranks.csv"
OUT_SEC = _DATA / "cfb_secondary_ranks.csv"
OUT_BOX = _DATA / "cfb_box_ranks.csv"
OUT_META = _DATA / "cfb_unit_line_ranks_meta.json"
RANK_CSV = _REF / "cfb_team_unit_rankings.csv"
VOL_CSV = _REF / "cfb_player_volume.csv"
LINE_CSV = _REF / "cfb_line_secondary.csv"

GATE_NOTES = """## Gate integration notes

- **Rec yards / receptions / longest rec UNDER**: Opp Pass D Rk + Opp Sec (`secondary_index`). Rec Yards Standard UNDER is already off the cover-stack ticket path.
- **Rush yards UNDER**: Opp Rush D Rk + Opp front-seven (`dl_index`). Standard Rush UNDER **keeps** the full ticket stack — do not lump with Rec U.
- **Rush yards OVER**: own Rush Off Tier Elite/Above + Opp Rush D Weak/Below + own `ol_index` (run-block). Goblin OVER **keeps D**.
- **Sacks UNDER**: own ESPN `off_sacks_taken_pg` rank × opp ESPN `def_sacks_pg`. **Not** `ol_index` (run-block) and **not** `dl_index` (run-stop + mixed frontSeven). DL tier (`dl_index`) and Sacks UNDER (ESPN sack rates) are overlapping-but-distinct by construction; do not assume one explains the other. `havoc.frontSeven` blends TFLs and forced fumbles with sacks.
- **Pass yards Goblin OVER**: D is skipped on tickets; this board is context only.
- Soft context only — do not promote into Gate70.
"""


def _to_date(s: object) -> date | None:
    t = str(s or "").strip()[:10]
    if len(t) < 10:
        return None
    try:
        return date.fromisoformat(t)
    except ValueError:
        return None


def find_latest_step8() -> Path | None:
    hits: list[Path] = []
    for root in (_REPO, _REPO.parent / "PropORACLE_main_cp"):
        base = root / "outputs"
        if not base.is_dir():
            continue
        hits.extend(base.glob("*/cfb/step8_cfb_direction_clean.xlsx"))
    if not hits:
        return None

    def _key(p: Path) -> str:
        return p.parent.parent.name

    return max(hits, key=_key)


def load_slate_pairs(step8: Path | None) -> tuple[list[tuple[str, str]], Path | None, date | None]:
    if step8 is None or not step8.is_file():
        return [], None, None
    df = pd.read_excel(step8, sheet_name=0)
    if "Team" not in df.columns:
        return [], step8, _to_date(step8.parent.parent.name)
    pairs: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    opp_col = "Opp" if "Opp" in df.columns else ""
    for _, row in df.iterrows():
        team = clean_cfb_abbr(row.get("Team"))
        opp = clean_cfb_abbr(row.get(opp_col)) if opp_col else ""
        if not team:
            continue
        key = (team, opp)
        if key not in seen:
            seen.add(key)
            pairs.append(key)
    return pairs, step8, _to_date(step8.parent.parent.name)


def ensure_line_secondary(*, year: int) -> tuple[pd.DataFrame, str]:
    """Reuse existing z-score table. Fetch only via the existing builder path."""
    tab = load_line_secondary_table()
    if not tab.empty:
        return tab, str(LINE_CSV.relative_to(_REPO)).replace("\\", "/")
    key = resolve_cfbd_key()
    if not key:
        print(
            "[CFB unit-line] CANNOT-OBTAIN: no CFBD_API_KEY. "
            "OL/DL/sec/box columns stay blank; ESPN YPG markdown still emits."
        )
        return pd.DataFrame(), "CANNOT-OBTAIN"
    print(f"[CFB unit-line] line-secondary CSV empty — invoking existing CFBD fetch year={year}")
    payload = cfbd_get("/stats/season/advanced", {"year": year}, key)
    if not isinstance(payload, list):
        print("[CFB unit-line] unexpected CFBD payload; leaving line-secondary empty")
        return pd.DataFrame(), "CANNOT-OBTAIN"
    rk = pd.read_csv(RANK_CSV) if RANK_CSV.is_file() else pd.DataFrame()
    lookup = build_team_lookup(rk)
    games: dict[str, float] = {}
    if not rk.empty and "team_abbr" in rk.columns:
        for _, r in rk.iterrows():
            abbr = str(r.get("team_abbr") or "").strip().upper()
            try:
                g = float(r.get("games") or "")
            except (TypeError, ValueError):
                continue
            if abbr and g == g:
                games[abbr] = g
    df, meta = index_rows(payload, lookup=lookup, games_by_abbr=games, season=year)
    write_outputs(df, meta)
    return df, str(LINE_CSV.relative_to(_REPO)).replace("\\", "/")


def load_espn() -> pd.DataFrame:
    if not RANK_CSV.is_file():
        raise RuntimeError(f"missing {RANK_CSV} — run Sports/CFB/scripts/build_cfb_unit_rankings.py")
    df = pd.read_csv(RANK_CSV).copy()
    df["team"] = df["team_abbr"].map(clean_cfb_abbr)
    df["conference"] = df.get("conference_name", pd.Series("", index=df.index)).fillna("").astype(str)
    return df


def top_shares(espn: pd.DataFrame) -> pd.DataFrame:
    if not VOL_CSV.is_file():
        return pd.DataFrame(columns=["team", "top_rec_share", "top_rush_share"])
    vol = pd.read_csv(VOL_CSV)
    id_map = {}
    if "team_id" in espn.columns:
        for _, r in espn.iterrows():
            try:
                tid = str(int(float(r.get("team_id"))))
            except (TypeError, ValueError):
                continue
            if tid:
                id_map[tid] = r["team"]
    vol = vol.copy()
    names = vol.get("player_norm", pd.Series("", index=vol.index)).astype(str).str.strip().str.lower()
    vol = vol[~names.isin(("", "team", "nan", "none"))].copy()
    vol["_tid"] = pd.to_numeric(vol.get("team_id"), errors="coerce")
    vol["_tid"] = vol["_tid"].map(lambda x: "" if pd.isna(x) else str(int(x)))
    vol["team"] = vol.get("team_abbr", pd.Series("", index=vol.index)).map(clean_cfb_abbr)
    vol["team"] = [
        t if t else id_map.get(tid, "")
        for t, tid in zip(vol["team"].tolist(), vol["_tid"].tolist())
    ]
    games = pd.to_numeric(vol.get("volume_games"), errors="coerce").fillna(0)
    vol = vol[games >= 5].copy()
    vol["rec_tgt_share"] = pd.to_numeric(vol.get("rec_tgt_share"), errors="coerce")
    vol["rush_att_share"] = pd.to_numeric(vol.get("rush_att_share"), errors="coerce")
    # 1.0 on a thin target sample is junk in this CSV — do not print as 100%.
    for col in ("rec_tgt_share", "rush_att_share"):
        s = vol[col]
        vol.loc[s.notna() & (s >= 0.99), col] = pd.NA
    rows = []
    for team, g in vol.groupby("team"):
        if not team:
            continue
        rec = g["rec_tgt_share"].dropna()
        rush = g["rush_att_share"].dropna()
        rows.append(
            {
                "team": team,
                "top_rec_share": float(rec.max()) if not rec.empty else None,
                "top_rush_share": float(rush.max()) if not rush.empty else None,
            }
        )
    return pd.DataFrame(rows)


def _join_line(espn: pd.DataFrame, line: pd.DataFrame) -> pd.DataFrame:
    out = espn.copy()
    n = max(int(len(out)), 1)
    if line.empty:
        out["ol_index"] = pd.NA
        out["dl_index"] = pd.NA
        out["secondary_index"] = pd.NA
        out["ol_rank"] = pd.NA
        out["dl_rank"] = pd.NA
        out["secondary_rank"] = pd.NA
        out["box_rank"] = pd.NA
        out["ol_tier"] = ""
        out["dl_tier"] = ""
        out["secondary_tier"] = ""
        out["box_tier"] = ""
        out["line_games"] = pd.NA
        out["early_season"] = pd.NA
        return out
    ls = line.copy()
    ls["team"] = ls["team_abbr"].map(clean_cfb_abbr)
    keep = [c for c in ("team", "ol_index", "dl_index", "secondary_index", "games", "early_season") if c in ls.columns]
    ls = ls[keep].drop_duplicates(subset=["team"], keep="last")
    out = out.merge(ls, on="team", how="left", suffixes=("", "_line"))
    if "games_line" in out.columns:
        out["line_games"] = out["games_line"]
    else:
        out["line_games"] = out.get("games")
    out["ol_rank"] = rank_higher_better(out["ol_index"])
    out["dl_rank"] = rank_higher_better(out["dl_index"])
    out["secondary_rank"] = rank_higher_better(out["secondary_index"])
    out["box_rank"] = out["dl_rank"]
    out["ol_tier"] = out["ol_rank"].map(lambda r: percentile_tier(r, n))
    out["dl_tier"] = out["dl_rank"].map(lambda r: percentile_tier(r, n))
    out["secondary_tier"] = out["secondary_rank"].map(lambda r: percentile_tier(r, n))
    out["box_tier"] = out["dl_tier"]
    return out


def write_rank_csvs(board: pd.DataFrame, *, season: int, n: int, line_src: str) -> None:
    _DATA.mkdir(parents=True, exist_ok=True)
    ol = board[
        [
            c
            for c in (
                "team",
                "conference",
                "ol_index",
                "ol_rank",
                "ol_tier",
                "off_sacks_taken_rank",
                "off_sacks_taken_pg",
                "line_games",
                "early_season",
            )
            if c in board.columns
        ]
    ].copy()
    ol = ol.rename(
        columns={
            "ol_tier": "tier",
            "off_sacks_taken_rank": "sack_rate_allowed_rank",
            "off_sacks_taken_pg": "sacks_taken_pg",
            "line_games": "games",
        }
    )
    ol["season"] = season
    ol["ol_meaning"] = "run-block ol_index only"
    ol.to_csv(OUT_OL, index=False)

    dl = board[
        [
            c
            for c in (
                "team",
                "conference",
                "dl_index",
                "dl_rank",
                "dl_tier",
                "def_sacks_pg",
                "def_sacks_rank",
                "line_games",
                "early_season",
            )
            if c in board.columns
        ]
    ].copy()
    dl = dl.rename(
        columns={
            "dl_tier": "tier",
            "def_sacks_pg": "espn_sacks_pg",
            "def_sacks_rank": "espn_sacks_rank",
            "line_games": "games",
        }
    )
    dl["season"] = season
    dl["dl_meaning"] = "run-stop + havoc.frontSeven; not sack rate"
    dl.to_csv(OUT_DL, index=False)

    sec = board[
        [
            c
            for c in (
                "team",
                "conference",
                "secondary_index",
                "secondary_rank",
                "secondary_tier",
                "line_games",
                "early_season",
            )
            if c in board.columns
        ]
    ].copy()
    sec = sec.rename(columns={"secondary_tier": "tier", "line_games": "games"})
    sec["season"] = season
    sec.to_csv(OUT_SEC, index=False)

    box = dl.copy()
    if "dl_rank" in box.columns:
        box["box_rank"] = box["dl_rank"]
    box["source"] = "alias_of_dl_index"
    box.to_csv(OUT_BOX, index=False)

    meta = {
        "built_at": now_iso(),
        "season": season,
        "n_fbs": n,
        "line_secondary_source": line_src,
        "note": (
            "OL/DL/secondary ranks are percentiles of existing ol_index/dl_index/"
            "secondary_index. Box is alias of dl_index. ESPN sacks ranks are a "
            "separate axis from dl_index."
        ),
        "paths": {
            "ol": str(OUT_OL.relative_to(_REPO)).replace("\\", "/"),
            "dl": str(OUT_DL.relative_to(_REPO)).replace("\\", "/"),
            "secondary": str(OUT_SEC.relative_to(_REPO)).replace("\\", "/"),
            "box": str(OUT_BOX.relative_to(_REPO)).replace("\\", "/"),
        },
    }
    OUT_META.write_text(json.dumps(meta, indent=2), encoding="utf-8")


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


def _fmt_share(v: Any) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    try:
        x = float(v)
    except (TypeError, ValueError):
        return ""
    if abs(x) <= 1.5:
        x *= 100.0
    return str(int(round(x)))


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


def _filter_slate(board: pd.DataFrame, pairs: list[tuple[str, str]]) -> pd.DataFrame:
    if not pairs:
        return board
    teams = {t for t, _ in pairs} | {o for _, o in pairs if o}
    return board[board["team"].isin(teams)].copy()


def _lean_tables(board: pd.DataFrame, pairs: list[tuple[str, str]], edge_min: float = 8.0) -> dict[str, list[list[str]]]:
    by = board.drop_duplicates(subset=["team"]).set_index("team")
    buckets: dict[str, list[tuple[float, list[str]]]] = {
        "pass_over": [],
        "pass_under": [],
        "rush_over": [],
        "rush_under": [],
    }
    for team, opp in pairs:
        if not team or not opp or team not in by.index or opp not in by.index:
            continue
        t = by.loc[team]
        o = by.loc[opp]
        try:
            po = float(t["off_pass_rank"])
            pd_ = float(o["def_pass_rank"])
            ro = float(t["off_rush_rank"])
            rd = float(o["def_rush_rank"])
        except (TypeError, ValueError, KeyError):
            continue
        pass_edge = pd_ - po
        rush_edge = rd - ro
        conf = str(t.get("conference") or "")
        prow = [team, conf, opp, _fmt_num(po), _fmt_num(pd_), f"{int(pass_edge):+d}"]
        rrow = [team, conf, opp, _fmt_num(ro), _fmt_num(rd), f"{int(rush_edge):+d}"]
        if pass_edge >= edge_min:
            buckets["pass_over"].append((pass_edge, prow))
        if pass_edge <= -edge_min:
            buckets["pass_under"].append((pass_edge, prow))
        if rush_edge >= edge_min:
            buckets["rush_over"].append((rush_edge, rrow))
        if rush_edge <= -edge_min:
            buckets["rush_under"].append((rush_edge, rrow))

    def _sort(rows: list[tuple[float, list[str]]], reverse: bool) -> list[list[str]]:
        return [r for _, r in sorted(rows, key=lambda x: x[0], reverse=reverse)]

    return {
        "pass_over": _sort(buckets["pass_over"], True),
        "pass_under": _sort(buckets["pass_under"], False),
        "rush_over": _sort(buckets["rush_over"], True),
        "rush_under": _sort(buckets["rush_under"], False),
    }


def render_md(
    *,
    week: int,
    n: int,
    board: pd.DataFrame,
    print_df: pd.DataFrame,
    leans: dict[str, list[list[str]]] | None,
    line_src: str,
    step8_path: str,
    built_at: str,
    slate_only: bool,
    slate_date: date | None = None,
) -> str:
    elite_n = math.ceil(n * 0.15)
    lines = [
        f"# CFB O/D Rankings by Prop — Week {week}",
        "",
        f"Source: `{step8_path}` + `{RANK_CSV.name}` + `{line_src}`",
        f"Built: `{built_at}` (UTC)",
        f"Tier: percentile on full FBS (n={n}) · Elite top 15% (≈{elite_n}) · Above 16–30% · Avg 31–65% · Below 66–85% · Weak bottom 15%",
        "Display: slate teams only" if slate_only else "Display: all FBS (no step8 slate found)",
        "Edge = opp_def_rank − own_off_rank (≥+8 OVER lean · ≤−8 UNDER lean) using ESPN YPG ranks",
        f"`ol_index` weighting = equal_mean_placeholder (run-block only). Line secondary: `{line_src}`.",
        od_board_authority_note(slate_date),
        "",
        "---",
        "",
        "## Pass Offense (rec yards / pass yards OVER context)",
        "",
    ]
    pass_off = print_df.sort_values("off_pass_rank", na_position="last")
    rows = []
    for _, r in pass_off.iterrows():
        rows.append(
            [
                _fmt_num(r["off_pass_rank"]),
                r["team"],
                str(r.get("conference") or ""),
                percentile_tier(r["off_pass_rank"], n),
                _fmt_num(r.get("off_pass_ypg")),
                _fmt_share(r.get("top_rec_share")),
            ]
        )
    lines.append(_md_table(["Rk", "Team", "Conference", "Tier", "YPG", "Top Rec%"], rows))
    lines.extend(["", "---", "", "## Rush Offense (rush yards OVER context)", ""])
    rush_off = print_df.sort_values("off_rush_rank", na_position="last")
    rows = []
    for _, r in rush_off.iterrows():
        rows.append(
            [
                _fmt_num(r["off_rush_rank"]),
                r["team"],
                str(r.get("conference") or ""),
                percentile_tier(r["off_rush_rank"], n),
                _fmt_num(r.get("off_rush_ypg")),
                _fmt_share(r.get("top_rush_share")),
                str(r.get("ol_tier") or ""),
            ]
        )
    lines.append(_md_table(["Rk", "Team", "Conference", "Tier", "YPG", "Top Rush%", "OL run-block"], rows))
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
    pass_def = print_df.sort_values("def_pass_rank", na_position="last")
    rows = []
    for _, r in pass_def.iterrows():
        rows.append(
            [
                _fmt_num(r["def_pass_rank"]),
                r["team"],
                str(r.get("conference") or ""),
                percentile_tier(r["def_pass_rank"], n),
                _fmt_num(r.get("def_pass_ypg_allowed")),
                _fmt_num(r.get("secondary_rank")),
            ]
        )
    lines.append(_md_table(["Rk", "Team", "Conference", "Tier", "Yds/G", "Sec Rk"], rows))
    lines.extend(
        [
            "",
            "---",
            "",
            "## Rush Defense (rush yards UNDER context)",
            "Rank 1 = stingiest. Front-seven / box column is `dl_index` (same number, not a second z).",
            "",
        ]
    )
    rush_def = print_df.sort_values("def_rush_rank", na_position="last")
    rows = []
    for _, r in rush_def.iterrows():
        rows.append(
            [
                _fmt_num(r["def_rush_rank"]),
                r["team"],
                str(r.get("conference") or ""),
                percentile_tier(r["def_rush_rank"], n),
                _fmt_num(r.get("def_rush_ypg_allowed")),
                _fmt_num(r.get("box_rank")),
            ]
        )
    lines.append(_md_table(["Rk", "Team", "Conference", "Tier", "Yds/G", "Front-seven Rk"], rows))
    lines.extend(
        [
            "",
            "---",
            "",
            "## Pass Rush / Sacks (ESPN sacks/G — not `dl_index`)",
            "A team can be Elite on `dl_index` and Avg on sacks/G (or the reverse); print both, do not reconcile them.",
            "",
        ]
    )
    sack = print_df.sort_values("def_sacks_rank", na_position="last")
    rows = []
    for _, r in sack.iterrows():
        rows.append(
            [
                _fmt_num(r.get("def_sacks_rank")),
                r["team"],
                str(r.get("conference") or ""),
                percentile_tier(r.get("def_sacks_rank"), n),
                _fmt_num(r.get("def_sacks_pg"), 2),
                str(r.get("dl_tier") or ""),
            ]
        )
    lines.append(_md_table(["Rk", "Team", "Conference", "Sacks Tier", "Sacks/G", "DL tier (distinct)"], rows))
    lines.extend(
        [
            "",
            "---",
            "",
            "## Secondary Rankings (rec yards / receptions / longest reception UNDER)",
            "Rank 1 = best `secondary_index`. Empty when line-secondary CSV is CANNOT-OBTAIN.",
            "",
        ]
    )
    sec = print_df.sort_values("secondary_rank", na_position="last")
    rows = []
    for _, r in sec.iterrows():
        rows.append(
            [
                _fmt_num(r.get("secondary_rank")),
                r["team"],
                str(r.get("conference") or ""),
                str(r.get("secondary_tier") or ""),
            ]
        )
    lines.append(_md_table(["Rk", "Team", "Conference", "Tier"], rows))

    if leans is not None:
        lines.extend(["", "---", "", f"## Week {week} Matchup Leans", ""])
        for title, key, headers in (
            ("### Pass / Rec OVER leans", "pass_over", ["Team", "Conference", "Opp", "Pass Off Rk", "Opp Pass D Rk", "Edge"]),
            ("### Pass / Rec UNDER leans", "pass_under", ["Team", "Conference", "Opp", "Pass Off Rk", "Opp Pass D Rk", "Edge"]),
            ("### Rush OVER leans", "rush_over", ["Team", "Conference", "Opp", "Rush Off Rk", "Opp Rush D Rk", "Edge"]),
            ("### Rush UNDER leans", "rush_under", ["Team", "Conference", "Opp", "Rush Off Rk", "Opp Rush D Rk", "Edge"]),
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
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--season", type=int, default=0)
    ap.add_argument("--week", type=int, default=0, help="Override CFB week label (0 = from step8 date).")
    ap.add_argument("--step8", default="", help="Path to step8_cfb_direction_clean.xlsx")
    args = ap.parse_args()
    season = int(args.season) or infer_default_year()

    espn = load_espn()
    n = int(len(espn))
    line, line_src = ensure_line_secondary(year=season)
    board = _join_line(espn, line)
    shares = top_shares(espn)
    if not shares.empty:
        board = board.merge(shares, on="team", how="left")
    else:
        board["top_rec_share"] = None
        board["top_rush_share"] = None

    write_rank_csvs(board, season=season, n=n, line_src=line_src)

    step8 = Path(args.step8) if args.step8 else find_latest_step8()
    pairs, step8_used, slate_d = load_slate_pairs(step8)
    week = int(args.week) or (cfb_week_from_date(slate_d) if slate_d else cfb_week_from_date(date.today()))
    print_df = _filter_slate(board, pairs)
    slate_only = bool(pairs)
    leans = _lean_tables(board, pairs) if pairs else {
        "pass_over": [],
        "pass_under": [],
        "rush_over": [],
        "rush_under": [],
    }
    built_at = now_iso()
    step8_label = (
        str(step8_used).replace("\\", "/") if step8_used else "CANNOT-OBTAIN (no step8)"
    )
    md = render_md(
        week=week,
        n=n,
        board=board,
        print_df=print_df,
        leans=leans,
        line_src=line_src,
        step8_path=step8_label,
        built_at=built_at,
        slate_only=slate_only,
        slate_date=slate_d,
    )
    out_md = _DATA / f"cfb_od_rankings_week{week}.md"
    current = _DATA / "cfb_od_rankings_current.md"
    out_md.write_text(md, encoding="utf-8")
    current.write_text(md, encoding="utf-8")
    print(f"[CFB unit-line] teams={n} line={line_src} step8={step8_label}")
    print(f"[CFB unit-line] wrote {OUT_OL.name} {OUT_DL.name} {OUT_SEC.name} {OUT_BOX.name}")
    print(f"[CFB unit-line] wrote {out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
