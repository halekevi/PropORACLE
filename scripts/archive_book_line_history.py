#!/usr/bin/env python3
"""
Normalize book ladder CSVs, append monthly history, and join to PP Standard/Goblin.

Standards join book **main** lines (exact line preferred, else nearest).
Goblins join the book's rung at the **exact same line** (alt/milestone ok).

Does not touch ticket gates or Odds API.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

_SCRIPT_DIR = Path(__file__).resolve().parent
_ROOT = _SCRIPT_DIR.parent
sys.path.insert(0, str(_SCRIPT_DIR))
sys.path.insert(0, str(_ROOT))

from combined_slate_tickets import (  # noqa: E402
    _canon_join_prop,
    _join_sport_key,
    _norm_player_join,
)
from utils.book_line_ladder import (  # noqa: E402
    PP_NEED,
    PP_SAME_LINE_COLUMNS,
    american_to_prob,
    append_history,
    dk_frame_to_ladder,
    normalize_ladder,
    ud_need_from_mult,
    utc_now_iso,
)


def _add_join_keys(df: pd.DataFrame, sport_col="sport", player_col="player", prop_col="prop") -> pd.DataFrame:
    out = df.copy()
    out["_js"] = out[sport_col].map(_join_sport_key)
    out["_jp"] = out[player_col].map(_norm_player_join)
    out["_jpr"] = [_canon_join_prop(a, b) for a, b in zip(out["_js"], out[prop_col])]
    out["line"] = pd.to_numeric(out["line"], errors="coerce")
    return out.dropna(subset=["line", "_jp", "_jpr"])


def _load_pp(slate_date: str, pp_root: Path) -> pd.DataFrame:
    rows = []
    for sport, rel, player_col in (
        ("NBA", Path("nba") / "step1_pp_props_today.csv", "player"),
        ("NFL", Path("nfl") / "step1_pp_props_today.csv", "player_name"),
        ("NHL", Path("nhl") / "step1_pp_props_today.csv", "player"),
        ("WNBA", Path("wnba") / "step1_pp_props_today.csv", "player"),
        ("CFB", Path("cfb") / "step1_pp_props_today.csv", "player_name"),
        ("MLB", Path("mlb") / "step1_pp_props_today.csv", "player"),
    ):
        path = pp_root / rel
        if not path.is_file():
            # also try sport nested under date already
            alt = pp_root / slate_date / rel
            path = alt if alt.is_file() else path
        if not path.is_file():
            continue
        df = pd.read_csv(path)
        pcol = player_col if player_col in df.columns else "player"
        prop_col = "prop_type" if "prop_type" in df.columns else "prop"
        pick_col = "pick_type" if "pick_type" in df.columns else "pick"
        for _, r in df.iterrows():
            pick = str(r.get(pick_col) or "")
            if pick not in ("Standard", "Goblin"):
                continue
            rows.append(
                {
                    "sport": sport,
                    "player": r.get(pcol),
                    "prop": r.get(prop_col),
                    "line": r.get("line"),
                    "pp_pick": pick,
                }
            )
    if not rows:
        return pd.DataFrame(columns=["sport", "player", "prop", "line", "pp_pick"])
    return _add_join_keys(pd.DataFrame(rows))


def _join_same_line(pp: pd.DataFrame, book: pd.DataFrame, *, pick: str, exact_only: bool) -> pd.DataFrame:
    legs = pp[pp["pp_pick"] == pick].copy()
    if legs.empty or book.empty:
        return pd.DataFrame(columns=PP_SAME_LINE_COLUMNS)
    b = book.copy()
    if pick == "Standard" and "kind" in b.columns:
        mains = b[b["kind"].astype(str).str.lower().eq("main")]
        if not mains.empty:
            b = mains
    legs = legs.rename(columns={"line": "line_pp", "sport": "sport_pp", "player": "player_pp", "prop": "prop_pp"})
    b = b.rename(columns={"line": "line_book"})
    m = legs.merge(b, on=["_js", "_jp", "_jpr"])
    if m.empty:
        return pd.DataFrame(columns=PP_SAME_LINE_COLUMNS)
    m["line_pp"] = pd.to_numeric(m["line_pp"], errors="coerce")
    m["line_book"] = pd.to_numeric(m["line_book"], errors="coerce")
    m["abs_gap"] = (m["line_pp"] - m["line_book"]).abs()
    m = m[m["abs_gap"] < 1e-9]
    if m.empty:
        return pd.DataFrame(columns=PP_SAME_LINE_COLUMNS)
    # cheapest book chance when multiple rungs collide
    m["book_implied_p"] = m["american_odds"].map(american_to_prob)
    m = m.sort_values("book_implied_p", ascending=True).drop_duplicates(
        ["_js", "_jp", "_jpr", "line_pp", "book"], keep="first"
    )
    out = pd.DataFrame(
        {
            "fetched_at": m["fetched_at"] if "fetched_at" in m.columns else utc_now_iso(),
            "slate_date": m["slate_date"] if "slate_date" in m.columns else "",
            "book": m["book"],
            "sport": m["sport_pp"],
            "player": m["player_pp"],
            "prop": m["prop_pp"],
            "line": m["line_pp"],
            "pp_pick": pick,
            "book_kind": m["kind"] if "kind" in m.columns else "",
            "american_odds": m["american_odds"] if "american_odds" in m.columns else pd.NA,
            "payout_multiplier": m["payout_multiplier"] if "payout_multiplier" in m.columns else pd.NA,
            "book_implied_p": m["book_implied_p"],
        }
    )
    out["pp_need_p"] = PP_NEED[pick]
    out["ud_need_p"] = out["payout_multiplier"].map(ud_need_from_mult)
    out["book_below_pp_need"] = out["book_implied_p"] < out["pp_need_p"]
    return out[PP_SAME_LINE_COLUMNS]


def _ingest_ladder_csv(path: Path, book: str, slate_date: str, fetched_at: str) -> pd.DataFrame:
    if not path.is_file():
        return pd.DataFrame()
    raw = pd.read_csv(path)
    if book == "draftkings" and "dk_american_odds" in raw.columns:
        raw = dk_frame_to_ladder(raw)
    # legacy short schema sport,player,prop,line,odds,kind
    if "american_odds" not in raw.columns and "odds" in raw.columns:
        raw = raw.rename(columns={"odds": "american_odds"})
    return normalize_ladder(raw, book=book, slate_date=slate_date, fetched_at=fetched_at)


def main() -> None:
    ap = argparse.ArgumentParser(description="Archive FD/DK/UD ladders + PP same-line join")
    ap.add_argument("--date", required=True, help="Slate date YYYY-MM-DD")
    ap.add_argument("--repo-root", default=str(_ROOT))
    ap.add_argument(
        "--pp-root",
        default="",
        help="Directory containing <sport>/step1_pp_props_today.csv (default: outputs/<date> then PropORACLE_main_cp)",
    )
    ap.add_argument("--fanduel", default="", help="Override FD ladder CSV")
    ap.add_argument("--draftkings", default="", help="Override DK ladder or step1 CSV")
    ap.add_argument("--underdog", default="", help="Override UD ladder CSV")
    ap.add_argument("--seed-only", action="store_true", help="Archive existing CSVs without requiring fresh fetches")
    args = ap.parse_args()

    root = Path(args.repo_root)
    slate = args.date
    fetched_at = utc_now_iso()
    books_dir = root / "outputs" / slate / "books"
    books_dir.mkdir(parents=True, exist_ok=True)

    fd_path = Path(args.fanduel) if args.fanduel else books_dir / "fanduel_props_ladder.csv"
    dk_path = Path(args.draftkings) if args.draftkings else books_dir / "draftkings_props_ladder.csv"
    ud_path = Path(args.underdog) if args.underdog else books_dir / "underdog_props_ladder.csv"

    # Fallbacks for first-night seed CSVs living outside books/
    if not fd_path.is_file():
        alt = root / "outputs" / slate / "fanduel_props_ladder.csv"
        if alt.is_file():
            fd_path = alt
    if not ud_path.is_file():
        alt = root / "outputs" / slate / "underdog_props_ladder.csv"
        if alt.is_file():
            ud_path = alt
    if not dk_path.is_file():
        for cand in (
            root / "outputs" / slate / "draftkings_props_all.csv",
            root / "outputs" / slate / "draftkings_props_nfl.csv",
            root / "outputs" / "2026-11-22" / "dk_props_nfl.csv",
        ):
            if cand.is_file():
                dk_path = cand
                break

    ladders = []
    for book, path in (("fanduel", fd_path), ("draftkings", dk_path), ("underdog", ud_path)):
        part = _ingest_ladder_csv(path, book, slate, fetched_at)
        if part.empty:
            print(f"[archive] {book}: missing/empty ({path})")
            continue
        # ensure books/ copy for stable layout
        dest = books_dir / f"{book}_props_ladder.csv"
        if Path(path).resolve() != dest.resolve():
            part.to_csv(dest, index=False, encoding="utf-8-sig")
        else:
            part.to_csv(dest, index=False, encoding="utf-8-sig")
        print(f"[archive] {book}: {len(part)} rungs -> {dest}")
        ladders.append(part)

    if not ladders:
        print("[archive] nothing to archive")
        sys.exit(0 if args.seed_only else 1)

    all_rungs = pd.concat(ladders, ignore_index=True)
    rpath = append_history(root, "rungs", slate, all_rungs)
    print(f"[archive] rungs history -> {rpath}")

    pp_root = Path(args.pp_root) if args.pp_root else root / "outputs" / slate
    if not any((pp_root / s / "step1_pp_props_today.csv").is_file() for s in ("nba", "nfl", "nhl")):
        main_cp = Path(r"H:\PropORACLE_main_cp\outputs") / slate
        if main_cp.is_dir():
            pp_root = main_cp
    pp = _load_pp(slate, pp_root)
    print(f"[archive] PP Standard/Goblin legs: {len(pp)} from {pp_root}")

    join_parts = []
    for part in ladders:
        keyed = _add_join_keys(part)
        for pick, exact in (("Standard", True), ("Goblin", True)):
            j = _join_same_line(pp, keyed, pick=pick, exact_only=exact)
            if not j.empty:
                join_parts.append(j)
    if join_parts:
        joins = pd.concat(join_parts, ignore_index=True)
        jpath = append_history(root, "pp_same_line", slate, joins)
        print(f"[archive] PP same-line joins: {len(joins)} -> {jpath}")
    else:
        print("[archive] no PP same-line joins")


if __name__ == "__main__":
    main()
