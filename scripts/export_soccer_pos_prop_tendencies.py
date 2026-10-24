#!/usr/bin/env python3
"""Soccer position / player tendencies by prop (tackles, shots, SOT, …).

Joins graded soccer props to a player→position map built from step6 boards.
Writes data/reports/soccer_pos_prop_tendencies.xlsx and adds sheets to
soccer_hit_rates_gates_latest.xlsx when that file is writable.

  py -3.14 scripts/export_soccer_pos_prop_tendencies.py
"""
from __future__ import annotations

import sys
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO / "scripts"))
sys.path.insert(0, str(_REPO / "Sports" / "Soccer" / "scripts"))

from export_soccer_hit_rates_excel import (  # noqa: E402
    _OUT_XLSX,
    _style_sheet,
    _write_df,
    load_graded,
    load_live_board,
)
from step6_team_role_context_soccer import norm_position  # noqa: E402

_OUT = _REPO / "data" / "reports" / "soccer_pos_prop_tendencies.xlsx"
FOCUS = {
    "shots",
    "shots on target",
    "tackles",
    "goalie saves",
    "passes attempted",
    "clearances",
    "attempted dribbles",
    "goals",
    "assists",
    "fouls",
}
FOCUS_DETAIL = {"shots", "shots on target", "tackles"}


def _name_key(v: object) -> str:
    s = unicodedata.normalize("NFKD", str(v or ""))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return "".join(ch for ch in s.lower() if ch.isalnum())


def _iter_step6() -> list[Path]:
    seen: set[str] = set()
    out: list[Path] = []
    for root in (_REPO, Path(r"H:\PropORACLE_main_cp")):
        if not root.exists():
            continue
        for p in root.rglob("step6_soccer_role_context.csv"):
            key = str(p.resolve()).lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(p)
    return out


def build_pos_map() -> pd.DataFrame:
    votes: dict[str, Counter] = defaultdict(Counter)
    display: dict[str, str] = {}
    raw_pos: dict[str, Counter] = defaultdict(Counter)
    for path in _iter_step6():
        try:
            df = pd.read_csv(path, usecols=lambda c: str(c).lower() in {"player", "pos", "position_group"}, low_memory=False)
        except Exception:
            continue
        cols = {c.lower(): c for c in df.columns}
        if "player" not in cols:
            continue
        for _, row in df.iterrows():
            name = str(row[cols["player"]] or "").strip()
            key = _name_key(name)
            if not key:
                continue
            display.setdefault(key, name)
            pos_raw = str(row[cols["pos"]],) if "pos" in cols else ""
            grp = str(row[cols["position_group"]],) if "position_group" in cols else ""
            grp = str(grp or "").strip().upper()
            if grp not in {"GK", "DEF", "MID", "FWD"}:
                grp = norm_position(pos_raw)
            if grp not in {"GK", "DEF", "MID", "FWD"}:
                continue
            votes[key][grp] += 1
            if pos_raw and str(pos_raw).lower() not in {"nan", "none", ""}:
                raw_pos[key][str(pos_raw).strip()] += 1
    rows = []
    for key, ctr in votes.items():
        pos_g, n = ctr.most_common(1)[0]
        raw = raw_pos[key].most_common(1)[0][0] if raw_pos[key] else ""
        rows.append(
            {
                "player_key": key,
                "player": display.get(key, key),
                "pos_group": pos_g,
                "pos_raw": raw,
                "pos_votes": int(n),
                "pos_sources": int(sum(ctr.values())),
            }
        )
    return pd.DataFrame(rows)


def _hr(s: pd.DataFrame) -> dict:
    n = int(len(s))
    hits = int(s["hit"].sum()) if n else 0
    return {"hits": hits, "n": n, "hit_rate": (hits / n) if n else None}


def main() -> int:
    print("Building player -> position map from step6 boards…")
    pos_map = build_pos_map()
    print(f"  {len(pos_map):,} players  {pos_map['pos_group'].value_counts().to_dict()}")

    print("Loading graded soccer…")
    graded = load_graded()
    graded["player_key"] = graded["player"].map(_name_key)
    g = graded.merge(pos_map[["player_key", "pos_group", "pos_raw"]], on="player_key", how="left")
    g["pos_group"] = g["pos_group"].fillna("UNMAPPED")
    mapped = float((g["pos_group"] != "UNMAPPED").mean())
    print(f"  graded {len(g):,}  position matched {mapped:.0%}")

    focus = g[g["prop_key"].isin(FOCUS)].copy()
    list_shape = focus[focus["list_shape"]].copy()

    # Position x prop — all list-shape, then L5>=4
    pos_prop_rows = []
    for (prop, pos), sub in list_shape.groupby(["prop", "pos_group"]):
        ge4 = sub[sub["list_eligible"]]
        over = sub[sub["direction"] == "OVER"]
        under = sub[sub["direction"] == "UNDER"]
        gob = sub[sub["pick_type"] == "Goblin"]
        std = sub[sub["pick_type"] == "Standard"]
        row = {
            "prop": prop,
            "pos_group": pos,
            "n": int(len(sub)),
            "hr": sub["hit"].mean() if len(sub) else None,
            "n_l5_ge4": int(len(ge4)),
            "hr_l5_ge4": ge4["hit"].mean() if len(ge4) else None,
            "n_over": int(len(over)),
            "hr_over": over["hit"].mean() if len(over) else None,
            "n_under": int(len(under)),
            "hr_under": under["hit"].mean() if len(under) else None,
            "n_goblin": int(len(gob)),
            "hr_goblin": gob["hit"].mean() if len(gob) else None,
            "n_standard": int(len(std)),
            "hr_standard": std["hit"].mean() if len(std) else None,
        }
        pos_prop_rows.append(row)
    pos_prop = pd.DataFrame(pos_prop_rows).sort_values(["prop", "n"], ascending=[True, False])

    detail_rows = []
    det = list_shape[list_shape["prop_key"].isin(FOCUS_DETAIL)]
    for (prop, pos, pick, direction), sub in det.groupby(["prop", "pos_group", "pick_type", "direction"]):
        ge4 = sub[sub["list_eligible"]]
        if len(sub) < 20 and len(ge4) < 8:
            continue
        detail_rows.append(
            {
                "prop": prop,
                "pos_group": pos,
                "pick_type": pick,
                "direction": direction,
                "n": int(len(sub)),
                "hr": sub["hit"].mean(),
                "n_l5_ge4": int(len(ge4)),
                "hr_l5_ge4": ge4["hit"].mean() if len(ge4) else None,
            }
        )
    pos_prop_detail = pd.DataFrame(detail_rows).sort_values(["prop", "pos_group", "n"], ascending=[True, True, False])

    # Player leaders on focus props (graded)
    player_rows = []
    play = list_shape[list_shape["prop_key"].isin(FOCUS_DETAIL)]
    for (player, prop, pos, pick), sub in play.groupby(["player", "prop", "pos_group", "pick_type"]):
        if len(sub) < 8:
            continue
        ge4 = sub[sub["list_eligible"]]
        player_rows.append(
            {
                "player": player,
                "pos_group": pos,
                "prop": prop,
                "pick_type": pick,
                "n": int(len(sub)),
                "hr": sub["hit"].mean(),
                "n_over": int((sub["direction"] == "OVER").sum()),
                "hr_over": sub.loc[sub["direction"] == "OVER", "hit"].mean() if (sub["direction"] == "OVER").any() else None,
                "n_l5_ge4": int(len(ge4)),
                "hr_l5_ge4": ge4["hit"].mean() if len(ge4) else None,
                "mean_l5": pd.to_numeric(sub["l5"], errors="coerce").mean(),
            }
        )
    players = pd.DataFrame(player_rows)
    if not players.empty:
        players = players.sort_values(["prop", "hr", "n"], ascending=[True, False, False])

    # Live 9/3 board: volume + L5-clear rate by pos x prop
    today = datetime.now().strftime("%Y-%m-%d")
    live_path = Path(r"H:\PropORACLE_main_cp") / "outputs" / today / "soccer" / "step8_soccer_direction_clean.xlsx"
    s6_path = Path(r"H:\PropORACLE_main_cp") / "outputs" / today / "soccer" / "step6_soccer_role_context.csv"
    live_pos = pd.DataFrame()
    live_players = pd.DataFrame()
    live_note = str(live_path)
    if s6_path.exists():
        s6 = pd.read_csv(s6_path, low_memory=False)
        s6["prop"] = s6.get("prop_type", s6.get("prop", "")).map(lambda x: str(x or "").strip())
        s6["prop_key"] = s6["prop"].str.casefold()
        s6["pos_group"] = s6.get("position_group", pd.Series("", index=s6.index)).astype(str).str.upper()
        miss = ~s6["pos_group"].isin(["GK", "DEF", "MID", "FWD"])
        if "pos" in s6.columns:
            s6.loc[miss, "pos_group"] = s6.loc[miss, "pos"].map(norm_position)
        s6["pick_type"] = s6.get("pick_type", "").astype(str)
        s6["l5_over"] = pd.to_numeric(s6.get("last5_over"), errors="coerce")
        s6["l5_under"] = pd.to_numeric(s6.get("last5_under"), errors="coerce")
        s6["avg_l5"] = pd.to_numeric(s6.get("stat_last5_avg"), errors="coerce")
        s6["line"] = pd.to_numeric(s6.get("line"), errors="coerce")
        live_rows = []
        subf = s6[s6["prop_key"].isin(FOCUS)]
        for (prop, pos, pick), sub in subf.groupby(["prop", "pos_group", "pick_type"]):
            live_rows.append(
                {
                    "prop": prop,
                    "pos_group": pos,
                    "pick_type": pick,
                    "n": int(len(sub)),
                    "mean_line": float(sub["line"].mean()) if sub["line"].notna().any() else None,
                    "mean_l5_avg": float(sub["avg_l5"].mean()) if sub["avg_l5"].notna().any() else None,
                    "median_l5_avg": float(sub["avg_l5"].median()) if sub["avg_l5"].notna().any() else None,
                    "mean_l5_over": float(sub["l5_over"].mean()) if sub["l5_over"].notna().any() else None,
                    "pct_l5_over_ge4": float((sub["l5_over"] >= 4).mean()) if sub["l5_over"].notna().any() else None,
                    "n_l5_over_ge4": int((sub["l5_over"] >= 4).sum()),
                    "n_with_tape": int(sub["avg_l5"].notna().sum()),
                }
            )
        live_pos = pd.DataFrame(live_rows).sort_values(["prop", "pos_group", "n"], ascending=[True, True, False])

        lp = []
        hot = s6[s6["prop_key"].isin(FOCUS_DETAIL) & s6["l5_over"].ge(4)]
        for _, r in hot.iterrows():
            lp.append(
                {
                    "player": r.get("player"),
                    "pos_group": r.get("pos_group"),
                    "team": r.get("team"),
                    "opp": r.get("opp_team"),
                    "prop": r.get("prop"),
                    "pick_type": r.get("pick_type"),
                    "line": r.get("line"),
                    "l5_over": r.get("l5_over"),
                    "l5_under": r.get("l5_under"),
                    "avg_l5": r.get("avg_l5"),
                    "def_tier": r.get("DEF_TIER") or r.get("def_tier"),
                }
            )
        live_players = pd.DataFrame(lp)
        if not live_players.empty:
            live_players = live_players.sort_values(["prop", "pos_group", "l5_over"], ascending=[True, True, False])
        live_note = str(s6_path)
    elif live_path.exists():
        live_note = str(live_path)

    how = pd.DataFrame(
        [
            ["Question", "Do some positions or players clear tackles / shots / SOT more than others?"],
            ["Position source", "Mode of pos/position_group on soccer step6 boards (not on graded JSON). Wingers map to MID, strikers to FWD."],
            ["Graded join", f"{mapped:.0%} of decided soccer rows matched a position. Unmapped stay in UNMAPPED."],
            ["Volume vs hit", "Live board mean last-5 average is raw production. Graded hit_rate is whether the posted line cashed."],
            ["Read L5>=4 first", "Ungated HR is pulled down by 0.5 Demon/Goblin volume. List-gate (L5>=4) is the playable number."],
            ["Step7 void", "Shots/SOT/goals already voided for DEF/GK. Tackles voided for FWD/GK. That is why DEF shots n is thin."],
            ["Live board", live_note],
        ],
        columns=["topic", "detail"],
    )

    headline = []
    for prop in ["Shots", "Shots On Target", "Tackles"]:
        sub = pos_prop[pos_prop["prop"].str.casefold() == prop.casefold()]
        if sub.empty:
            continue
        best = sub[sub["n_l5_ge4"] >= 8].sort_values("hr_l5_ge4", ascending=False).head(1)
        vol = sub.sort_values("n", ascending=False).head(1)
        headline.append(
            {
                "prop": prop,
                "highest_volume_pos": (vol["pos_group"].iloc[0] if len(vol) else None),
                "volume_n": int(vol["n"].iloc[0]) if len(vol) else 0,
                "volume_ungated_hr": float(vol["hr"].iloc[0]) if len(vol) else None,
                "best_l5ge4_pos": (best["pos_group"].iloc[0] if len(best) else None),
                "best_l5ge4_n": int(best["n_l5_ge4"].iloc[0]) if len(best) else 0,
                "best_l5ge4_hr": float(best["hr_l5_ge4"].iloc[0]) if len(best) else None,
            }
        )
    headline_df = pd.DataFrame(headline)

    dated = _REPO / "outputs" / datetime.now().strftime("%Y-%m-%d") / "soccer_pos_prop_tendencies.xlsx"
    paths = [_OUT, dated]
    main_cp = Path(r"H:\PropORACLE_main_cp") / "outputs" / datetime.now().strftime("%Y-%m-%d") / "soccer" / "soccer_pos_prop_tendencies.xlsx"
    paths.append(main_cp)

    first = paths[0]
    first.parent.mkdir(parents=True, exist_ok=True)
    print(f"Writing {first}")
    with pd.ExcelWriter(first, engine="openpyxl") as writer:
        _write_df(writer, "How to read", how)
        _write_df(writer, "Headline", headline_df)
        _write_df(writer, "Pos x prop graded", pos_prop)
        _write_df(writer, "Pos x pick x dir", pos_prop_detail)
        _write_df(writer, "Player leaders graded", players)
        _write_df(writer, "Live pos volume L5", live_pos)
        _write_df(writer, "Live L5ge4 players", live_players)
        _write_df(writer, "Position map", pos_map.sort_values(["pos_group", "player"]))

    blob = first.read_bytes()
    for extra in paths[1:]:
        extra.parent.mkdir(parents=True, exist_ok=True)
        extra.write_bytes(blob)
        print(f"Copied {extra}")

    # Try to append key sheets onto the gates workbook the user already has.
    gates = _OUT_XLSX
    if gates.exists():
        try:
            with pd.ExcelWriter(gates, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                _write_df(writer, "Pos x prop graded", pos_prop)
                _write_df(writer, "Player leaders graded", players)
                _write_df(writer, "Live pos volume L5", live_pos)
                _write_df(writer, "Live L5ge4 players", live_players)
            print(f"Updated sheets on {gates}")
        except PermissionError:
            print(f"Could not update {gates} (file open). Standalone workbook is enough.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
