#!/usr/bin/env python3
"""Soccer prop-category combo gates: L5, L10, position, opponent D, player role.

Graded JSON has L5 + opponent D but not L10. L10 / shot_volume / starter_tier
are joined from historical step6 boards on player + prop + pick + date.

  py -3.14 scripts/export_soccer_combo_gates.py
"""
from __future__ import annotations

import sys
import unicodedata
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO / "scripts"))
sys.path.insert(0, str(_REPO / "Sports" / "Soccer" / "scripts"))

from export_soccer_hit_rates_excel import (  # noqa: E402
    _OUT_XLSX,
    _d_pass,
    _norm_def,
    _write_df,
    load_graded,
)
from step6_team_role_context_soccer import norm_position  # noqa: E402

_OUT = _REPO / "data" / "reports" / "soccer_combo_gates_latest.xlsx"
_TOP3 = _REPO / "Sports" / "Soccer" / "data" / "soccer_top3_vs_defense_slate.csv"
_TOP3_FB = _REPO / "Sports" / "Soccer" / "data" / "soccer_top3_vs_defense.csv"

POS_OK = {
    "shots": {"FWD", "MID"},
    "shots on target": {"FWD", "MID"},
    "shots assisted": {"FWD", "MID"},
    "goals": {"FWD", "MID"},
    "assists": {"FWD", "MID"},
    "goal + assist": {"FWD", "MID"},
    "tackles": {"DEF", "MID"},
    "fouls": {"DEF", "MID"},
    "fouls drawn": {"FWD", "MID"},
    "attempted dribbles": {"FWD", "MID"},
    "clearances": {"DEF", "GK"},
    "goalie saves": {"GK"},
    "goals allowed": {"GK"},
    "cards": {"DEF", "MID", "FWD"},
    "goals allowed in first 30 minutes": {"GK"},
    "crosses": {"MID", "DEF", "FWD"},
}


def _name_key(v: object) -> str:
    s = unicodedata.normalize("NFKD", str(v or ""))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return "".join(ch for ch in s.lower() if ch.isalnum())


def _prop_key(v: object) -> str:
    return " ".join(str(v or "").strip().split()).casefold()


def _pos_ok(prop_key: str, pos: str) -> bool:
    allowed = POS_OK.get(prop_key)
    if not allowed:
        return bool(pos) and pos != "UNMAPPED"
    return pos in allowed


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


_S6_KEEP = {
    "player",
    "pos",
    "position_group",
    "prop_type",
    "prop",
    "pick_type",
    "game_date",
    "last5_over",
    "last5_under",
    "l10_over",
    "l10_under",
    "l10_games_played",
    "def_tier",
    "shots_def_tier",
    "starter_tier",
    "shot_volume",
    "pass_role",
    "team",
    "opp_team",
    "line",
    "stat_last5_avg",
}


def load_step6() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in _iter_step6():
        try:
            df = pd.read_csv(
                path,
                usecols=lambda c: str(c).strip().lower() in _S6_KEEP,
                low_memory=False,
            )
        except Exception:
            continue
        if df.empty:
            continue
        df.columns = [str(c).strip().lower() for c in df.columns]
        df = df.loc[:, ~df.columns.duplicated()].copy()
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    s6 = pd.concat(frames, ignore_index=True)
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
    pos = s6.get("position_group", pd.Series("", index=s6.index)).astype(str).str.upper()
    miss = ~pos.isin(["GK", "DEF", "MID", "FWD"])
    if "pos" in s6.columns:
        pos = pos.where(~miss, s6["pos"].map(norm_position))
    s6["pos_group"] = pos.where(pos.isin(["GK", "DEF", "MID", "FWD"]), "UNMAPPED")
    s6["l5_over_s6"] = pd.to_numeric(s6.get("last5_over"), errors="coerce")
    s6["l5_under_s6"] = pd.to_numeric(s6.get("last5_under"), errors="coerce")
    s6["l10_over_s6"] = pd.to_numeric(s6.get("l10_over"), errors="coerce")
    s6["l10_under_s6"] = pd.to_numeric(s6.get("l10_under"), errors="coerce")
    s6["l10_n"] = pd.to_numeric(s6.get("l10_games_played"), errors="coerce")
    def_shots = s6["shots_def_tier"].map(_norm_def) if "shots_def_tier" in s6.columns else pd.Series("", index=s6.index)
    def_all = s6["def_tier"].map(_norm_def) if "def_tier" in s6.columns else pd.Series("", index=s6.index)
    s6["def_s6"] = np.where(def_shots.astype(str).str.len() > 0, def_shots, def_all)
    s6["starter_tier"] = s6.get("starter_tier", pd.Series("", index=s6.index)).astype(str).str.upper()
    s6["shot_volume"] = s6.get("shot_volume", pd.Series("", index=s6.index)).astype(str).str.upper()
    s6["pass_role"] = s6.get("pass_role", pd.Series("", index=s6.index)).astype(str).str.upper()
    keep = [
        "player_key",
        "prop_key",
        "pick_type",
        "join_date",
        "pos_group",
        "l5_over_s6",
        "l5_under_s6",
        "l10_over_s6",
        "l10_under_s6",
        "l10_n",
        "def_s6",
        "starter_tier",
        "shot_volume",
        "pass_role",
        "player",
        "team",
        "opp_team",
        "line",
        "stat_last5_avg",
    ]
    keep = [c for c in keep if c in s6.columns]
    s6 = s6[keep].drop_duplicates(["player_key", "prop_key", "pick_type", "join_date"], keep="last")
    return s6


def load_top3() -> set[tuple[str, str]]:
    path = _TOP3 if _TOP3.exists() else _TOP3_FB
    if not path.exists():
        return set()
    df = pd.read_csv(path, low_memory=False)
    cols = {c.lower(): c for c in df.columns}
    name_c = cols.get("player") or cols.get("player_name")
    prop_c = cols.get("prop_norm") or cols.get("category")
    rank_c = cols.get("rank_on_team")
    side_c = cols.get("leader_side")
    if not name_c or not prop_c or not rank_c:
        return set()
    rank = pd.to_numeric(df[rank_c], errors="coerce")
    side = df[side_c].astype(str).str.lower() if side_c else "top"
    m = rank.le(3) & side.eq("top")
    out = set()
    for _, r in df.loc[m, [name_c, prop_c]].iterrows():
        pk = _prop_key(r[prop_c]).replace("_", " ")
        if pk == "saves":
            pk = "goalie saves"
        if pk == "passes":
            pk = "passes attempted"
        out.add((_name_key(r[name_c]), pk))
    return out


def _dir_count(over, under, direction: pd.Series) -> pd.Series:
    over = pd.to_numeric(over, errors="coerce")
    under = pd.to_numeric(under, errors="coerce")
    return pd.Series(np.where(direction.eq("OVER"), over, np.where(direction.eq("UNDER"), under, np.nan)), index=direction.index)


def _off_ok(df: pd.DataFrame) -> pd.Series:
    starter = df.get("starter_tier", pd.Series("", index=df.index)).astype(str).eq("STARTER")
    high_vol = df.get("shot_volume", pd.Series("", index=df.index)).astype(str).eq("HIGH_VOL")
    primary = df.get("pass_role", pd.Series("", index=df.index)).astype(str).eq("PRIMARY")
    shotish = df["prop_key"].isin({"shots", "shots on target", "shots assisted"})
    passish = df["prop_key"].eq("passes attempted")
    vol = (shotish & high_vol) | (passish & primary) | (~shotish & ~passish & starter)
    return starter | vol | df["off_top3"].fillna(False)


def _pack(name: str, s: pd.DataFrame) -> dict:
    n = int(len(s))
    hits = int(s["hit"].sum()) if n else 0
    return {f"{name}_n": n, f"{name}_hits": hits, f"{name}_hr": (hits / n) if n else None}


def combo_table(df: pd.DataFrame, min_n: int = 1) -> pd.DataFrame:
    rows = []
    for prop, sub in df.groupby("prop"):
        gates = {
            "all": sub.index,
            "L5>=4": sub.index[sub["g_l5"]],
            "L5=5": sub.index[sub["g_l5eq5"]],
            "L10>=8": sub.index[sub["g_l10"]],
            "Pos": sub.index[sub["g_pos"]],
            "D": sub.index[sub["g_d"]],
            "Off": sub.index[sub["g_off"]],
            "L5+L10": sub.index[sub["g_l5"] & sub["g_l10"]],
            "L5=5+L10": sub.index[sub["g_l5eq5"] & sub["g_l10"]],
            "L5+Pos": sub.index[sub["g_l5"] & sub["g_pos"]],
            "L5=5+Pos": sub.index[sub["g_l5eq5"] & sub["g_pos"]],
            "L5+D": sub.index[sub["g_l5"] & sub["g_d"]],
            "L5=5+D": sub.index[sub["g_l5eq5"] & sub["g_d"]],
            "L5+Off": sub.index[sub["g_l5"] & sub["g_off"]],
            "L5=5+Off": sub.index[sub["g_l5eq5"] & sub["g_off"]],
            "L5+Pos+D": sub.index[sub["g_l5"] & sub["g_pos"] & sub["g_d"]],
            "L5=5+Pos+D": sub.index[sub["g_l5eq5"] & sub["g_pos"] & sub["g_d"]],
            "L5+L10+D": sub.index[sub["g_l5"] & sub["g_l10"] & sub["g_d"]],
            "L5=5+L10+D": sub.index[sub["g_l5eq5"] & sub["g_l10"] & sub["g_d"]],
            "L5=5+L10+D+Off": sub.index[sub["g_l5eq5"] & sub["g_l10"] & sub["g_d"] & sub["g_off"]],
            "L5+L10+Pos": sub.index[sub["g_l5"] & sub["g_l10"] & sub["g_pos"]],
            "L5=5+L10+Pos": sub.index[sub["g_l5eq5"] & sub["g_l10"] & sub["g_pos"]],
            "L5+L10+Pos+D": sub.index[sub["g_l5"] & sub["g_l10"] & sub["g_pos"] & sub["g_d"]],
            "L5=5+L10+Pos+D": sub.index[sub["g_l5eq5"] & sub["g_l10"] & sub["g_pos"] & sub["g_d"]],
            "Full L5+L10+Pos+D+Off": sub.index[sub["g_l5"] & sub["g_l10"] & sub["g_pos"] & sub["g_d"] & sub["g_off"]],
            "Full L5=5+L10+Pos+D+Off": sub.index[sub["g_l5eq5"] & sub["g_l10"] & sub["g_pos"] & sub["g_d"] & sub["g_off"]],
        }
        row = {"prop": prop, "n_board": int(len(sub))}
        for gname, idx in gates.items():
            piece = _pack(gname, sub.loc[idx])
            row.update(piece)
        rows.append(row)
    out = pd.DataFrame(rows).sort_values("n_board", ascending=False)
    return out


def l5eq5_l10_focus(df: pd.DataFrame, slice_name: str) -> pd.DataFrame:
    """Narrow view: L5=5+L10, then +D, then +D+Off."""
    stacks = [
        ("L5=5+L10", lambda s: s["g_l5eq5"] & s["g_l10"]),
        ("L5=5+L10+D", lambda s: s["g_l5eq5"] & s["g_l10"] & s["g_d"]),
        ("L5=5+L10+D+Off", lambda s: s["g_l5eq5"] & s["g_l10"] & s["g_d"] & s["g_off"]),
    ]
    rows = []

    def _one(prop: str, sub: pd.DataFrame) -> dict:
        row: dict = {"slice": slice_name, "prop": prop, "n_pool": int(len(sub))}
        for name, fn in stacks:
            m = sub[fn(sub)]
            n = int(len(m))
            hits = int(m["hit"].sum()) if n else 0
            row[f"{name} hits"] = hits
            row[f"{name} n"] = n
            row[f"{name} hr"] = (hits / n) if n else None
        return row

    rows.append(_one("ALL", df))
    for prop, sub in df.groupby("prop"):
        rows.append(_one(str(prop), sub))
    out = pd.DataFrame(rows)
    out["_ord"] = out["prop"].map(lambda p: -1 if p == "ALL" else 0)
    return out.sort_values(["_ord", "L5=5+L10 n"], ascending=[True, False]).drop(columns="_ord")


def pulled_inventory(df: pd.DataFrame) -> pd.DataFrame:
    """Every decided soccer prop this year vs the kept L5=5+L10 / +D / +D+Off gates."""
    prop_col = "prop_raw" if "prop_raw" in df.columns else "prop"
    rows = []
    for prop, sub in df.groupby(prop_col):
        std = sub[sub["pick_type"] == "Standard"]
        gob = sub[sub["pick_type"] == "Goblin"]
        dem = sub[sub["pick_type"] == "Demon"]
        gate = sub[sub["list_shape"]] if "list_shape" in sub.columns else sub
        l55 = gate["g_l5eq5"] & gate["g_l10"] if "g_l5eq5" in gate.columns else pd.Series(False, index=gate.index)
        l55d = l55 & gate["g_d"] if "g_d" in gate.columns else l55
        l55do = l55d & gate["g_off"] if "g_off" in gate.columns else l55d
        notes = []
        if int(len(dem)) and not int(len(std) + len(gob)):
            notes.append("Demon-only — list/ticket gates never apply")
        if "combo" in str(prop).lower():
            notes.append("Combo board; folded into parent name on some HR sheets")
        if int(len(gate)) == 0:
            notes.append("Not in Std+Goblin gate pool")
        elif int(l55.sum()) == 0:
            notes.append("Pulled/graded, but no L5=5+L10 sample yet")
        rows.append(
            {
                "prop": prop,
                "decided": int(len(sub)),
                "hr_all": float(sub["hit"].mean()) if len(sub) else None,
                "n_standard": int(len(std)),
                "n_goblin": int(len(gob)),
                "n_demon": int(len(dem)),
                "n_gate_pool": int(len(gate)),
                "L5=5+L10 n": int(l55.sum()),
                "L5=5+L10 hr": float(gate.loc[l55, "hit"].mean()) if int(l55.sum()) else None,
                "L5=5+L10+D n": int(l55d.sum()),
                "L5=5+L10+D hr": float(gate.loc[l55d, "hit"].mean()) if int(l55d.sum()) else None,
                "L5=5+L10+D+Off n": int(l55do.sum()),
                "L5=5+L10+D+Off hr": float(gate.loc[l55do, "hit"].mean()) if int(l55do.sum()) else None,
                "in_gate_tables": int(len(gate)) > 0,
                "notes": "; ".join(notes),
            }
        )
    extra = pd.DataFrame(
        [
            {
                "prop": "SOCCER1H (1H Shots / 1H Saves / etc.)",
                "decided": 0,
                "hr_all": None,
                "n_standard": 0,
                "n_goblin": 0,
                "n_demon": 0,
                "n_gate_pool": 0,
                "L5=5+L10 n": 0,
                "L5=5+L10 hr": None,
                "L5=5+L10+D n": 0,
                "L5=5+L10+D hr": None,
                "L5=5+L10+D+Off n": 0,
                "L5=5+L10+D+Off hr": None,
                "in_gate_tables": False,
                "notes": "Separate PrizePicks board (league 242). Not fetched unless --include_halves. Not in this year's tape.",
            },
            {
                "prop": "Outfield / Goalie Fantasy Score",
                "decided": 0,
                "hr_all": None,
                "n_standard": 0,
                "n_goblin": 0,
                "n_demon": 0,
                "n_gate_pool": 0,
                "L5=5+L10 n": 0,
                "L5=5+L10 hr": None,
                "L5=5+L10+D n": 0,
                "L5=5+L10+D hr": None,
                "L5=5+L10+D+Off n": 0,
                "L5=5+L10+D+Off hr": None,
                "in_gate_tables": False,
                "notes": "Pulled on step1 some days; dropped in step2. Zero decided grades.",
            },
        ]
    )
    out = pd.concat([pd.DataFrame(rows), extra], ignore_index=True)
    return out.sort_values(["in_gate_tables", "decided"], ascending=[False, False])


def live_combo(s6: pd.DataFrame, top3: set[tuple[str, str]]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    today = datetime.now().strftime("%Y-%m-%d")
    path = Path(r"H:\PropORACLE_main_cp") / "outputs" / today / "soccer" / "step6_soccer_role_context.csv"
    s8_path = Path(r"H:\PropORACLE_main_cp") / "outputs" / today / "soccer" / "step8_soccer_direction_clean.xlsx"
    if not path.exists():
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    df = pd.read_csv(path, low_memory=False)
    df.columns = [str(c).strip() for c in df.columns]
    low = {c.lower(): c for c in df.columns}

    def c(*names):
        for n in names:
            if n.lower() in low:
                return low[n.lower()]
        return None

    out = pd.DataFrame()
    out["player"] = df[c("player")]
    out["player_key"] = out["player"].map(_name_key)
    out["prop"] = df[c("prop_type", "prop")].map(lambda x: str(x or "").strip())
    out["prop_key"] = out["prop"].map(_prop_key)
    out["pick_type"] = df[c("pick_type")].astype(str).str.strip()
    pt = out["pick_type"].str.lower()
    out.loc[pt.str.contains("goblin"), "pick_type"] = "Goblin"
    out.loc[pt.str.contains("standard"), "pick_type"] = "Standard"
    out.loc[pt.str.contains("demon"), "pick_type"] = "Demon"
    out["line"] = pd.to_numeric(df[c("line")], errors="coerce")
    pos = df[c("position_group")].astype(str).str.upper() if c("position_group") else "MID"
    out["pos_group"] = [p if p in {"GK", "DEF", "MID", "FWD"} else norm_position(p) for p in pos]
    l5o = pd.to_numeric(df[c("last5_over")], errors="coerce")
    l5u = pd.to_numeric(df[c("last5_under")], errors="coerce")
    l10o = pd.to_numeric(df[c("l10_over")], errors="coerce")
    l10u = pd.to_numeric(df[c("l10_under")], errors="coerce")
    out["l5_over"] = l5o
    out["l10_over"] = l10o
    out["l5_under"] = l5u
    out["l10_under"] = l10u
    shots_t = df[c("SHOTS_DEF_TIER")].map(_norm_def) if c("SHOTS_DEF_TIER") else ""
    def_t = df[c("DEF_TIER")].map(_norm_def) if c("DEF_TIER") else ""
    out["def_tier"] = np.where(pd.Series(shots_t).astype(str).str.len() > 0, shots_t, def_t)
    out["starter_tier"] = df[c("starter_tier")].astype(str).str.upper() if c("starter_tier") else ""
    out["shot_volume"] = df[c("shot_volume")].astype(str).str.upper() if c("shot_volume") else ""
    out["pass_role"] = df[c("pass_role")].astype(str).str.upper() if c("pass_role") else ""
    out["off_top3"] = [((k, p) in top3) for k, p in zip(out["player_key"], out["prop_key"])]
    # Standard: use step8 direction when present, else the side with more L5 hits.
    if s8_path.exists():
        s8 = pd.read_excel(s8_path, sheet_name=0)
        s8c = {str(c).strip().lower(): c for c in s8.columns}
        pk_col = s8c.get("pick type") or s8c.get("pick_type")
        if "player" in s8c and "prop" in s8c and "direction" in s8c:
            s8k = pd.DataFrame(
                {
                    "player_key": s8[s8c["player"]].map(_name_key),
                    "prop_key": s8[s8c["prop"]].map(_prop_key),
                    "pick_type": s8[pk_col].astype(str).str.strip() if pk_col else "Standard",
                    "direction_s8": s8[s8c["direction"]].astype(str).str.upper(),
                }
            )
            s8k["pick_type"] = s8k["pick_type"].str.lower().replace(
                {"goblin": "Goblin", "standard": "Standard", "demon": "Demon"}
            )
            out = out.merge(s8k.drop_duplicates(["player_key", "prop_key", "pick_type"]), how="left")
    gob = out["pick_type"].eq("Goblin")
    direction_s8 = out["direction_s8"] if "direction_s8" in out.columns else pd.Series("", index=out.index)
    l5o_m = pd.to_numeric(out["l5_over"], errors="coerce")
    l5u_m = pd.to_numeric(out["l5_under"], errors="coerce")
    l10o_m = pd.to_numeric(out["l10_over"], errors="coerce")
    l10u_m = pd.to_numeric(out["l10_under"], errors="coerce")
    std_side = np.where(l5u_m.fillna(0) > l5o_m.fillna(0), "UNDER", "OVER")
    direction = pd.Series(
        np.where(gob, "OVER", np.where(direction_s8.isin(["OVER", "UNDER"]), direction_s8, std_side)),
        index=out.index,
    )
    out["direction"] = direction
    l5_use = pd.Series(np.where(direction.eq("OVER"), l5o_m, l5u_m), index=out.index)
    l10_use = pd.Series(np.where(direction.eq("OVER"), l10o_m, l10u_m), index=out.index)
    out["l5_use"] = l5_use
    out["l10_use"] = l10_use
    out["list_shape"] = out["pick_type"].isin(["Standard", "Goblin"]) & ~(gob & direction.ne("OVER"))
    out["g_l5"] = pd.to_numeric(l5_use, errors="coerce").ge(4)
    out["g_l5eq5"] = pd.to_numeric(l5_use, errors="coerce").eq(5)
    out["g_l10"] = pd.to_numeric(l10_use, errors="coerce").ge(8)
    out["g_pos"] = [_pos_ok(pk, pos) for pk, pos in zip(out["prop_key"], out["pos_group"])]
    out["g_d"] = [_d_pass(d, t) for d, t in zip(direction, out["def_tier"])]
    out["g_off"] = _off_ok(out)
    shaped = out[out["list_shape"]].copy()
    counts = []
    for prop, sub in shaped.groupby("prop"):
        counts.append(
            {
                "prop": prop,
                "n": int(len(sub)),
                "L5>=4": int(sub["g_l5"].sum()),
                "L5=5": int(sub["g_l5eq5"].sum()),
                "L10>=8": int(sub["g_l10"].sum()),
                "Pos": int(sub["g_pos"].sum()),
                "D": int(sub["g_d"].sum()),
                "Off": int(sub["g_off"].sum()),
                "L5+L10": int((sub["g_l5"] & sub["g_l10"]).sum()),
                "L5=5+L10": int((sub["g_l5eq5"] & sub["g_l10"]).sum()),
                "L5+Pos": int((sub["g_l5"] & sub["g_pos"]).sum()),
                "L5=5+Pos": int((sub["g_l5eq5"] & sub["g_pos"]).sum()),
                "L5+D": int((sub["g_l5"] & sub["g_d"]).sum()),
                "L5=5+D": int((sub["g_l5eq5"] & sub["g_d"]).sum()),
                "L5+Off": int((sub["g_l5"] & sub["g_off"]).sum()),
                "L5=5+Off": int((sub["g_l5eq5"] & sub["g_off"]).sum()),
                "L5+Pos+D": int((sub["g_l5"] & sub["g_pos"] & sub["g_d"]).sum()),
                "L5=5+Pos+D": int((sub["g_l5eq5"] & sub["g_pos"] & sub["g_d"]).sum()),
                "L5+L10+D": int((sub["g_l5"] & sub["g_l10"] & sub["g_d"]).sum()),
                "L5=5+L10+D": int((sub["g_l5eq5"] & sub["g_l10"] & sub["g_d"]).sum()),
                "L5=5+L10+D+Off": int((sub["g_l5eq5"] & sub["g_l10"] & sub["g_d"] & sub["g_off"]).sum()),
                "L5+L10+Pos+D": int((sub["g_l5"] & sub["g_l10"] & sub["g_pos"] & sub["g_d"]).sum()),
                "L5=5+L10+Pos+D": int((sub["g_l5eq5"] & sub["g_l10"] & sub["g_pos"] & sub["g_d"]).sum()),
                "Full L5ge4": int((sub["g_l5"] & sub["g_l10"] & sub["g_pos"] & sub["g_d"] & sub["g_off"]).sum()),
                "Full L5=5": int((sub["g_l5eq5"] & sub["g_l10"] & sub["g_pos"] & sub["g_d"] & sub["g_off"]).sum()),
            }
        )
    live_tbl = pd.DataFrame(counts).sort_values("n", ascending=False)
    keep = ["player", "pos_group", "prop", "pick_type", "direction", "line", "l5_use", "l10_use", "def_tier", "starter_tier", "shot_volume"]
    full = shaped[shaped["g_l5"] & shaped["g_l10"] & shaped["g_pos"] & shaped["g_d"] & shaped["g_off"]]
    full5 = shaped[shaped["g_l5eq5"] & shaped["g_l10"] & shaped["g_pos"] & shaped["g_d"] & shaped["g_off"]]
    live_full = full[keep].sort_values(["prop", "l5_use"], ascending=[True, False]) if len(full) else pd.DataFrame()
    live_full5 = full5[keep].sort_values(["prop", "l5_use"], ascending=[True, False]) if len(full5) else pd.DataFrame()
    ladder_parts = []
    for label, mask in [
        ("L5=5+L10", shaped["g_l5eq5"] & shaped["g_l10"]),
        ("L5=5+L10+D", shaped["g_l5eq5"] & shaped["g_l10"] & shaped["g_d"]),
        ("L5=5+L10+D+Off", shaped["g_l5eq5"] & shaped["g_l10"] & shaped["g_d"] & shaped["g_off"]),
    ]:
        part = shaped.loc[mask, keep].copy()
        if part.empty:
            continue
        part.insert(0, "stack", label)
        ladder_parts.append(part)
    live_ladder = pd.concat(ladder_parts, ignore_index=True) if ladder_parts else pd.DataFrame()
    return live_tbl, live_full, live_full5, live_ladder


def main() -> int:
    print("Loading step6 boards for L10 / role…")
    s6 = load_step6()
    print(f"  step6 join keys {len(s6):,}")
    top3 = load_top3()
    print(f"  top3 usage keys {len(top3):,}")

    print("Loading graded soccer…")
    g = load_graded()
    g["player_key"] = g["player"].map(_name_key)
    g["join_date"] = g["file_date"]
    before = len(g)
    merged = g.merge(
        s6.drop(columns=["player"], errors="ignore"),
        on=["player_key", "prop_key", "pick_type", "join_date"],
        how="left",
    )
    print(f"  graded {before:,}  s6 matched {(merged['l10_over_s6'].notna() | merged['l5_over_s6'].notna()).mean():.0%}")

    # Prefer graded L5; fall back to step6 directional counts.
    l5_s6 = _dir_count(merged.get("l5_over_s6"), merged.get("l5_under_s6"), merged["direction"])
    merged["l5_use"] = merged["l5"].where(merged["l5"].notna(), l5_s6)
    l10 = _dir_count(merged.get("l10_over_s6"), merged.get("l10_under_s6"), merged["direction"])
    merged["l10_use"] = l10

    pos = merged.get("pos_group", pd.Series("UNMAPPED", index=merged.index)).fillna("UNMAPPED")
    merged["pos_group"] = pos
    def_s6 = merged.get("def_s6", pd.Series("", index=merged.index)).fillna("")
    def_use = np.where(def_s6.astype(str).str.len() > 1, def_s6, merged["def_tier"].replace("—", ""))
    merged["g_d"] = [_d_pass(d, t) for d, t in zip(merged["direction"], def_use)]
    # If step6 D available for shot props, already in def_s6.

    merged["off_top3"] = [((k, p) in top3) for k, p in zip(merged["player_key"], merged["prop_key"])]
    merged["g_off"] = _off_ok(merged)
    merged["g_pos"] = [_pos_ok(pk, p) for pk, p in zip(merged["prop_key"], merged["pos_group"])]
    merged["g_l5"] = pd.to_numeric(merged["l5_use"], errors="coerce").ge(4)
    merged["g_l5eq5"] = pd.to_numeric(merged["l5_use"], errors="coerce").eq(5)
    merged["g_l10"] = pd.to_numeric(merged["l10_use"], errors="coerce").ge(8)

    base = merged[merged["list_shape"]].copy()
    gob = merged[(merged["pick_type"] == "Goblin") & (merged["direction"] == "OVER")].copy()
    std = merged[merged["pick_type"] == "Standard"].copy()

    print("Building combo tables…")
    all_tbl = combo_table(base)
    gob_tbl = combo_table(gob)
    std_tbl = combo_table(std)

    live_tbl, live_full, live_full5, live_ladder = live_combo(s6, top3)
    focus_all = l5eq5_l10_focus(base, "Std+Goblin")
    focus_gob = l5eq5_l10_focus(gob, "Goblin OVER")
    focus_std = l5eq5_l10_focus(std, "Standard")
    focus = pd.concat([focus_all, focus_gob, focus_std], ignore_index=True)
    inventory = pulled_inventory(merged)

    how = pd.DataFrame(
        [
            ["L5", "Directional last-5 hits vs today's line. List gate = >=4. Perfect tape = 5/5. Both are stacked with L10/Pos/D/Off in this file."],
            ["L10", "Directional last-10 hits. Pass = >=8. Not stored on graded JSON — joined from step6 boards."],
            ["Pos", "Step7 role fit. Shots/SOT/goals/assists = FWD|MID; tackles/fouls = DEF|MID; saves = GK; clearances = DEF|GK; dribbles = FWD|MID."],
            ["Opp D", "OVER Weak|Below Avg; UNDER Elite|Above Avg; Avg fails. Shot props prefer SHOTS_DEF_TIER when joined."],
            ["Off (player/team usage)", "STARTER, or HIGH_VOL (shots/SOT), or PRIMARY passer, or top-3 on team for that stat. We do not have own-team attack rankings."],
            ["Full stack L5>=4", "L5>=4 AND L10>=8 AND Pos AND D AND Off."],
            ["Kept gates", "L5=5+L10, then + opponent D, then + D + Offense. List gate is still L5>=4; this ladder is the tighten screen."],
            ["Coverage", "Pulled this year sheet lists every decided soccer prop Mar–Aug 2026 plus 1H/fantasy boards we did not grade."],
            ["1H board", "PrizePicks SOCCER1H (e.g. Isak 1H Shots 1.5) is a separate league id 242. Today's fetch is full-game SOCCER only unless --include_halves."],
            ["Fantasy", "Outfield/GK fantasy is dropped in step2 and is not in these HR tables."],
        ],
        columns=["gate", "rule"],
    )

    coverage = pd.DataFrame(
        [
            {"field": "graded decided list-shape", "n": int(len(base))},
            {"field": "has L5", "n": int(base["l5_use"].notna().sum())},
            {"field": "L5>=4", "n": int(base["g_l5"].sum())},
            {"field": "L5=5", "n": int(base["g_l5eq5"].sum())},
            {"field": "has L10 from step6", "n": int(base["l10_use"].notna().sum())},
            {"field": "has Pos", "n": int((base["pos_group"] != "UNMAPPED").sum())},
            {"field": "D pass", "n": int(base["g_d"].sum())},
            {"field": "Off pass", "n": int(base["g_off"].sum())},
            {"field": "step6 matched (any L5/L10)", "n": int((merged["l10_over_s6"].notna() | merged["l5_over_s6"].notna()).sum())},
        ]
    )

    dated = _REPO / "outputs" / datetime.now().strftime("%Y-%m-%d") / "soccer_combo_gates.xlsx"
    main_cp = Path(r"H:\PropORACLE_main_cp") / "outputs" / datetime.now().strftime("%Y-%m-%d") / "soccer" / "soccer_combo_gates.xlsx"
    first = _OUT
    first.parent.mkdir(parents=True, exist_ok=True)
    print(f"Writing {first}")
    with pd.ExcelWriter(first, engine="openpyxl") as writer:
        _write_df(writer, "How to read", how)
        _write_df(writer, "Coverage", coverage)
        _write_df(writer, "Combo HR Std+Goblin", all_tbl)
        _write_df(writer, "Combo HR Goblin OVER", gob_tbl)
        _write_df(writer, "Combo HR Standard", std_tbl)
        _write_df(writer, "L5=5 L10 +D +Off", focus)
        _write_df(writer, "Pulled this year", inventory)
        _write_df(writer, "Live 9-3 stack counts", live_tbl)
        _write_df(writer, "Live full-stack names", live_full)
        _write_df(writer, "Live full L5=5 names", live_full5)
        _write_df(writer, "Live L5=5 L10 ladder", live_ladder)

    blob = first.read_bytes()
    for extra in (dated, main_cp):
        extra.parent.mkdir(parents=True, exist_ok=True)
        extra.write_bytes(blob)
        print(f"Copied {extra}")

    if _OUT_XLSX.exists():
        try:
            with pd.ExcelWriter(_OUT_XLSX, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                _write_df(writer, "L5=5 L10 +D +Off", focus)
                _write_df(writer, "Pulled this year", inventory)
                _write_df(writer, "Live combo counts", live_tbl)
            print(f"Updated {_OUT_XLSX}")
        except PermissionError:
            print("Gates workbook is open; standalone combo file is complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
