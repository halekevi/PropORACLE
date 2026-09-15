#!/usr/bin/env python3
"""Measure Saves keep with own-D polarity vs legacy opp D (graded + step6)."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO / "scripts"))
sys.path.insert(0, str(_REPO))

from export_soccer_combo_gates import _iter_step6, _name_key, _prop_key  # noqa: E402
from export_soccer_hit_rates_excel import _d_pass, _norm_def, load_graded  # noqa: E402

WEAK = {"Weak", "Below Avg"}
STINGY = {"Elite", "Above Avg"}
STRONG = STINGY


def _mode_tier(s: pd.Series) -> str:
    s = s.astype(str).map(_norm_def)
    s = s[s.ne("") & s.notna()]
    if s.empty:
        return ""
    return str(s.mode().iloc[0])


def main() -> int:
    def_path = _REPO / "Sports" / "Soccer" / "cache" / "soccer_defense_summary.csv"
    d = pd.read_csv(def_path, encoding="utf-8-sig")
    d["pp"] = d["pp_name"].astype(str).str.strip().str.upper()
    d["off"] = d.get("OFF_TIER", pd.Series("", index=d.index)).map(_norm_def)
    dmap_off = d.drop_duplicates("pp").set_index("pp")["off"]

    frames = []
    for path in _iter_step6():
        try:
            df = pd.read_csv(
                path,
                usecols=lambda c: str(c).strip().lower()
                in {
                    "player",
                    "prop_type",
                    "prop",
                    "pick_type",
                    "game_date",
                    "team",
                    "opp_team",
                    "def_tier",
                },
                low_memory=False,
            )
        except Exception:
            continue
        df.columns = [str(c).strip().lower() for c in df.columns]
        df = df.loc[:, ~df.columns.duplicated()].copy()
        frames.append(df)
    s6 = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    s6["player_key"] = s6.get("player", "").map(_name_key)
    s6["prop_key"] = s6.get("prop_type", s6.get("prop", "")).map(_prop_key)
    s6["pick_type"] = (
        s6.get("pick_type", pd.Series("", index=s6.index))
        .astype(str)
        .str.strip()
        .str.lower()
        .replace({"goblin": "Goblin", "standard": "Standard", "demon": "Demon"})
    )
    s6["join_date"] = s6.get("game_date", "").astype(str).str.slice(0, 10)
    s6["team_u"] = s6.get("team", "").astype(str).str.strip().str.upper()
    s6["opp_u"] = s6.get("opp_team", "").astype(str).str.strip().str.upper()
    s6["opp_d_board"] = s6.get("def_tier", pd.Series("", index=s6.index)).map(_norm_def)
    s6 = s6.drop_duplicates(["player_key", "prop_key", "pick_type", "join_date"], keep="last")

    # When opp_team=X carries DEF_TIER, that is X's defense (usable as own_d for X).
    prof = (
        s6[s6["opp_u"].astype(str).str.len() > 0]
        .groupby("opp_u", dropna=False)["opp_d_board"]
        .agg(_mode_tier)
        .rename("own_d")
        .reset_index()
        .rename(columns={"opp_u": "team_u"})
    )

    g = load_graded()
    g = g[g["list_shape"] & g["prop_key"].eq("goalie saves")].copy()
    g["player_key"] = g["player"].map(_name_key)
    g["join_date"] = g["file_date"]
    m = g.merge(
        s6[["player_key", "prop_key", "pick_type", "join_date", "team_u", "opp_u"]],
        on=["player_key", "prop_key", "pick_type", "join_date"],
        how="left",
    )
    m = m.merge(prof, on="team_u", how="left")
    m["opp_off"] = m["opp_u"].map(dmap_off)
    m["own_d"] = m["own_d"].fillna("")
    m["opp_off"] = m["opp_off"].fillna("")

    def pack(label, s):
        n = len(s)
        hits = int(s["hit"].sum()) if n else 0
        hr = hits / n if n else None
        return label, hits, n, hr

    rows = []
    for direction in ("OVER", "UNDER"):
        sub = m[m["direction"] == direction]
        l5 = pd.to_numeric(sub["l5"], errors="coerce").ge(4)
        own = sub["own_d"].fillna("")
        opp_off = sub["opp_off"].fillna("")
        opp_d = sub["def_tier"].map(_norm_def).fillna("")
        if direction == "OVER":
            own_ok = own.isin(WEAK)
            off_ok = opp_off.isin(STRONG) | opp_off.eq("")
            legacy = pd.Series([_d_pass("OVER", t) for t in opp_d], index=sub.index)
        else:
            own_ok = own.isin(STINGY)
            off_ok = opp_off.isin(WEAK) | opp_off.eq("")
            legacy = pd.Series([_d_pass("UNDER", t) for t in opp_d], index=sub.index)
        for label, mask in [
            ("all", pd.Series(True, index=sub.index)),
            ("L5>=4", l5),
            ("L5>=4 + legacy opp D", l5 & legacy),
            ("L5>=4 + OWN D", l5 & own_ok),
            ("L5>=4 + OWN D + OPP_OFF", l5 & own_ok & off_ok & opp_off.ne("")),
            ("own_d filled among L5>=4", l5 & own.ne("")),
        ]:
            rows.append(
                {
                    "direction": direction,
                    **dict(zip(("gate", "hits", "n", "hr"), pack(label, sub.loc[mask]))),
                }
            )

    out = pd.DataFrame(rows)
    print(out.to_string(index=False))
    print(f"\nown_d fill on saves join: {(m['own_d'].astype(str).str.len()>0).mean():.0%}")
    paths = [
        _REPO / "data" / "reports" / "soccer_combo_gates_latest.xlsx",
        _REPO / "data" / "reports" / "soccer_hit_rates_gates_latest.xlsx",
    ]
    for path in paths:
        if not path.exists():
            continue
        try:
            with pd.ExcelWriter(path, engine="openpyxl", mode="a", if_sheet_exists="replace") as w:
                out.to_excel(w, sheet_name="Saves own-D polarity", index=False)
            print(f"wrote {path}")
        except PermissionError:
            print(f"locked {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
