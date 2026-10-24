"""NFL prop×gate ledger helpers.

Rates are **unique player-games by default**. Ladder boards (same QB with
stacked lines, Goblin distance ladders) inflate raw n/HR — always dedupe
before reading keep/allowlist signals.

Dedup key: (slate_date, player, prop, side, pick_type).
Representative line: closest to the median line for that group (stable
primary-board proxy), ties broken by |actual−line|.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from utils.defense_tiers import normalize_def_tier_label

WEAK = {"Weak", "Below Avg"}
ELITE = {"Elite", "Above Avg"}

# Blank / defaulted face labels land here, never in "Standard".
UNVERIFIED_PICK = "Unverified"
FACE_MIN_ROWS = 100

# Active monitors (not allowlisted yet). Thresholds are unique-game n.
WATCH_SACKS_TAKEN_UNDER_N = 15
WATCH_FANTASY_SKILL_UNDER_N = 40


def d_ok(side: str, tier: object) -> bool:
    t = normalize_def_tier_label(tier) or str(tier or "").strip()
    if side == "OVER":
        return t in WEAK
    if side == "UNDER":
        return t in ELITE
    return False


def rate(hits: int, n: int) -> float | None:
    return round(100.0 * hits / n, 1) if n else None


def _norm_prop(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.lower()
        .str.replace(r"[\s\-]+", "_", regex=True)
        .str.replace(r"_+", "_", regex=True)
        .str.strip("_")
    )


def prepare_decided(boxes: list[pd.DataFrame]) -> pd.DataFrame:
    """Stack graded Box Raw frames and keep HIT/MISS with gate columns."""
    frames: list[pd.DataFrame] = []
    for df in boxes:
        if df is None or df.empty or "result" not in df.columns:
            continue
        out = df.copy()
        if "slate" not in out.columns:
            out["slate"] = ""
        frames.append(out)
    if not frames:
        return pd.DataFrame()

    all_df = pd.concat(frames, ignore_index=True)
    all_df["result"] = all_df["result"].astype(str).str.upper()
    dec = all_df[all_df["result"].isin(["HIT", "MISS"])].copy()
    if dec.empty:
        return dec

    prop_col = "prop_type_norm" if "prop_type_norm" in dec.columns else "prop_type"
    dec["prop"] = _norm_prop(dec[prop_col])
    dec["side"] = dec["bet_direction"].astype(str).str.upper().str.strip()
    # Face labels fail CLOSED (2026-10-24). A missing or blank pick_type used to
    # default to "Standard", which is how every NFL Goblin / Demon rung on the
    # 09-09..10-19 boards was counted as a Standard face (receptions UNDER
    # 78% default-label vs 59.5% real faces; sacks UNDER 89.7% was Demon
    # rungs). Blank labels become UNVERIFIED_PICK and never enter a Standard
    # cell; a date with >= FACE_MIN_ROWS decided rows and zero Goblin/Demon is
    # treated as defaulted upstream and goes to UNVERIFIED_PICK as a block.
    pick_raw = dec["pick_type"] if "pick_type" in dec.columns else ""
    dec["pick"] = pd.Series(pick_raw, index=dec.index).astype(str).str.strip().str.title()
    blank = dec["pick"].str.lower().isin(("", "nan", "none"))
    dec.loc[blank, "pick"] = UNVERIFIED_PICK
    dec["face_verified"] = ~blank
    date_col = next(
        (c for c in ("slate", "game_date", "board_date", "date", "file_date") if c in dec.columns and dec[c].astype(str).str.strip().ne("").any()),
        None,
    )
    grp_key = dec[date_col].astype(str).str[:10] if date_col else pd.Series("", index=dec.index)
    for _, idx in dec.groupby(grp_key).groups.items():
        g = dec.loc[idx]
        if len(g) >= FACE_MIN_ROWS and not g["pick"].isin(("Goblin", "Demon")).any():
            dec.loc[idx, "pick"] = UNVERIFIED_PICK
            dec.loc[idx, "face_verified"] = False

    if "player" not in dec.columns:
        dec["player"] = ""
    dec["player"] = dec["player"].astype(str)

    def _dir_l5(row: pd.Series) -> float:
        col = "l5_under" if row["side"] == "UNDER" else "l5_over"
        return pd.to_numeric(row.get(col), errors="coerce")

    def _dir_l10(row: pd.Series) -> float:
        col = "l10_under" if row["side"] == "UNDER" else "l10_over"
        return pd.to_numeric(row.get(col), errors="coerce")

    dec["l5"] = dec.apply(_dir_l5, axis=1)
    dec["l10"] = dec.apply(_dir_l10, axis=1)
    dec["dok"] = [d_ok(s, t) for s, t in zip(dec["side"], dec.get("def_tier", ""))]
    dec["gate70"] = (dec["l5"] == 5) & (dec["l10"] >= 8) & dec["dok"]
    dec["line_f"] = pd.to_numeric(dec.get("line"), errors="coerce")
    dec["actual_f"] = pd.to_numeric(dec.get("actual"), errors="coerce")
    return dec


def dedupe_unique_player_games(dec: pd.DataFrame) -> pd.DataFrame:
    """One row per (slate, player, prop, side, pick_type).

    Picks the line closest to the group median (primary-board proxy); ties
    break toward the line nearest actual. Use this frame for keep/allowlist
    rates — never raw ladder n.
    """
    if dec is None or dec.empty:
        return pd.DataFrame() if dec is None else dec

    need = {"slate", "player", "prop", "side", "pick"}
    missing = need - set(dec.columns)
    if missing:
        raise KeyError(f"dedupe_unique_player_games missing columns: {sorted(missing)}")

    rows: list[pd.Series] = []
    keys = ["slate", "player", "prop", "side", "pick"]
    for _, g in dec.groupby(keys, dropna=False):
        if g.empty:
            continue
        g = g.copy()
        med = g["line_f"].median()
        g["dm"] = (g["line_f"] - med).abs()
        g["gap"] = (g["actual_f"] - g["line_f"]).abs()
        g = g.sort_values(["dm", "gap"], kind="mergesort")
        rows.append(g.iloc[0])
    if not rows:
        return dec.iloc[0:0].copy()
    out = pd.DataFrame(rows)
    out["unique_game"] = True
    return out.reset_index(drop=True)


def _agg_group(g: pd.DataFrame) -> dict[str, Any]:
    n = len(g)
    h = int((g["result"] == "HIT").sum())
    g4 = g[g["l5"] >= 4]
    g5 = g[g["l5"] == 5]
    g70 = g[g["gate70"]]
    return {
        "n": n,
        "hits": h,
        "hr": rate(h, n),
        "l5_ge4_n": len(g4),
        "l5_ge4_hr": rate(int((g4["result"] == "HIT").sum()), len(g4)),
        "l5_eq5_n": len(g5),
        "l5_eq5_hr": rate(int((g5["result"] == "HIT").sum()), len(g5)),
        "gate70_n": len(g70),
        "gate70_hr": rate(int((g70["result"] == "HIT").sum()), len(g70)),
        "players": int(g["player"].nunique()),
    }


def build_prop_gate_ledger(boxes: list[pd.DataFrame]) -> pd.DataFrame:
    """Prop×side×pick_type ledger with unique-game primary rates.

    Columns:
      n / hits / hr / gate70_*  — unique player-games (canonical)
      ladder_n / ladder_hr      — raw decided legs (transparency only)
    """
    dec = prepare_decided(boxes)
    if dec.empty:
        return pd.DataFrame()

    uniq = dedupe_unique_player_games(dec)
    rows: list[dict[str, Any]] = []
    for (prop, side, pick), g_u in uniq.groupby(["prop", "side", "pick"]):
        g_l = dec[
            (dec["prop"] == prop) & (dec["side"] == side) & (dec["pick"] == pick)
        ]
        u = _agg_group(g_u)
        l_n = len(g_l)
        l_h = int((g_l["result"] == "HIT").sum())
        rows.append(
            {
                "prop": prop,
                "side": side,
                "pick_type": pick,
                "n": u["n"],
                "hits": u["hits"],
                "hr": u["hr"],
                "players": u["players"],
                "ladder_n": l_n,
                "ladder_hr": rate(l_h, l_n),
                "l5_ge4_n": u["l5_ge4_n"],
                "l5_ge4_hr": u["l5_ge4_hr"],
                "l5_eq5_n": u["l5_eq5_n"],
                "l5_eq5_hr": u["l5_eq5_hr"],
                "gate70_n": u["gate70_n"],
                "gate70_hr": u["gate70_hr"],
            }
        )
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(
        ["pick_type", "side", "n"], ascending=[True, True, False]
    ).reset_index(drop=True)


def ledger_methodology_note() -> str:
    return (
        "Canonical n/hr/gate70 are unique player-games "
        "(dedupe slate|player|prop|side|pick_type; median-line representative). "
        "ladder_n/ladder_hr are raw decided legs — do not use for allowlist. "
        "Gate70 = L5=5 + L10>=8 + directional D. "
        f"Watch: sacks_taken UNDER until unique n>={WATCH_SACKS_TAKEN_UNDER_N}; "
        f"fantasy_score skill UNDER until unique n>={WATCH_FANTASY_SKILL_UNDER_N}. "
        "Kicker fantasy is a kick-pts duplicate — stay off allowlist. "
        "NFL Goblin tickets stay unissued until goblin_diagnostic_report answers "
        "OVER/UNDER skew, extreme-low min-line floor, L10+D vs line, and cover%%."
    )


def goblin_diagnostic_report(boxes: list[pd.DataFrame]) -> dict[str, Any]:
    """Week 2+ Goblin ledger questions (unique-game). Empty when no Goblin rows.

    Answers:
      1. OVER vs UNDER by chip
      2. Extreme-low (line<=0.5) vs rest — does L5=5 alone inflate?
      3. Gate attrition: raw → L5=5 → L5=5+L10>=8 → full Gate70
      4. Cover as % of line vs absolute cover — compare HR by cover_pct buckets
    """
    from utils.nfl_keep_gates import (
        NFL_GOBLIN_EXTREME_LOW_LINE,
        NFL_GOBLIN_EXTREME_LOW_PROPS,
    )

    dec = prepare_decided(boxes)
    if dec.empty:
        return {"goblin_n": 0, "note": "no decided rows"}
    gob = dec[dec["pick"].astype(str).str.lower().eq("goblin")].copy()
    if gob.empty:
        return {"goblin_n": 0, "note": "no Goblin rows yet — Standard UNDER priority holds"}

    uniq = dedupe_unique_player_games(gob)
    uniq["hit"] = uniq["result"].eq("HIT")
    # cover fields if present on graded box
    cover = pd.to_numeric(uniq.get("cover", uniq.get("dist_l5")), errors="coerce")
    uniq["cover_f"] = cover
    uniq["cover_pct"] = uniq["cover_f"] / uniq["line_f"].replace(0, pd.NA)

    by_chip: list[dict[str, Any]] = []
    for (prop, side), g in uniq.groupby(["prop", "side"]):
        by_chip.append(
            {
                "prop": prop,
                "side": side,
                "n": len(g),
                "hr": rate(int(g["hit"].sum()), len(g)),
                "l5_eq5_n": int((g["l5"] == 5).sum()),
                "l5_eq5_hr": rate(int(g.loc[g["l5"] == 5, "hit"].sum()), int((g["l5"] == 5).sum())),
                "gate70_n": int(g["gate70"].sum()),
                "gate70_hr": rate(int(g.loc[g["gate70"], "hit"].sum()), int(g["gate70"].sum())),
            }
        )

    extreme_mask = uniq["prop"].isin(NFL_GOBLIN_EXTREME_LOW_PROPS) & (
        uniq["line_f"] <= NFL_GOBLIN_EXTREME_LOW_LINE + 1e-9
    )
    ext = uniq[extreme_mask]
    rest = uniq[~extreme_mask & uniq["side"].eq("OVER")]

    def _attrition(g: pd.DataFrame) -> dict[str, Any]:
        if g.empty:
            return {"n": 0}
        l5 = g[g["l5"] == 5]
        l5l10 = l5[l5["l10"] >= 8]
        g70 = g[g["gate70"]]
        return {
            "n": len(g),
            "hr": rate(int(g["hit"].sum()), len(g)),
            "l5_eq5_n": len(l5),
            "l5_eq5_hr": rate(int(l5["hit"].sum()), len(l5)),
            "l5_l10_n": len(l5l10),
            "l5_l10_hr": rate(int(l5l10["hit"].sum()), len(l5l10)),
            "gate70_n": len(g70),
            "gate70_hr": rate(int(g70["hit"].sum()), len(g70)),
        }

    # Cover % buckets on Goblin OVER with L5=5
    over_l5 = uniq[(uniq["side"] == "OVER") & (uniq["l5"] == 5)].copy()
    cover_pct_buckets: list[dict[str, Any]] = []
    if len(over_l5) and over_l5["cover_pct"].notna().any():
        bins = [-0.01, 0.25, 0.50, 0.75, 1.01, 10.0]
        labels = ["<25%", "25-50%", "50-75%", "75-100%", ">100%"]
        over_l5["cp_bin"] = pd.cut(
            over_l5["cover_pct"], bins=bins, labels=labels, include_lowest=True
        )
        for lab, g in over_l5.groupby("cp_bin", observed=True):
            cover_pct_buckets.append(
                {
                    "cover_pct": str(lab),
                    "n": len(g),
                    "hr": rate(int(g["hit"].sum()), len(g)),
                }
            )

    return {
        "goblin_n": len(uniq),
        "goblin_ladder_n": len(gob),
        "by_chip": sorted(by_chip, key=lambda r: (-int(r["n"]), r["prop"], r["side"])),
        "over_under": {
            "OVER": _attrition(uniq[uniq["side"] == "OVER"]),
            "UNDER": _attrition(uniq[uniq["side"] == "UNDER"]),
        },
        "extreme_low_over": _attrition(ext[ext["side"] == "OVER"] if len(ext) else ext),
        "non_extreme_over": _attrition(rest),
        "gate_attrition_over": _attrition(uniq[uniq["side"] == "OVER"]),
        "cover_pct_buckets_over_l5eq5": cover_pct_buckets,
        "questions": [
            "OVER vs UNDER skew by chip",
            "extreme-low 0.5 lines need min-line floor?",
            "L10+D filtering vs low line doing the work?",
            "cover as % of line vs absolute Standard floors?",
        ],
    }


def print_goblin_diagnostics(report: dict[str, Any]) -> None:
    n = int(report.get("goblin_n") or 0)
    if n <= 0:
        print(f"\nGoblin diagnostics: {report.get('note', 'no Goblin rows')}")
        return
    print(f"\n=== Goblin unique-game diagnostics (n={n}, ladder={report.get('goblin_ladder_n')}) ===")
    ou = report.get("over_under") or {}
    for side in ("OVER", "UNDER"):
        a = ou.get(side) or {}
        print(
            f"  {side}: n={a.get('n')} hr={a.get('hr')} "
            f"L5=5 {a.get('l5_eq5_n')}/{a.get('l5_eq5_hr')} "
            f"L5+L10 {a.get('l5_l10_n')}/{a.get('l5_l10_hr')} "
            f"G70 {a.get('gate70_n')}/{a.get('gate70_hr')}"
        )
    ext = report.get("extreme_low_over") or {}
    rest = report.get("non_extreme_over") or {}
    print(
        f"  extreme-low OVER (<=0.5): n={ext.get('n')} hr={ext.get('hr')} "
        f"L5=5 {ext.get('l5_eq5_n')}/{ext.get('l5_eq5_hr')}"
    )
    print(
        f"  other OVER: n={rest.get('n')} hr={rest.get('hr')} "
        f"L5=5 {rest.get('l5_eq5_n')}/{rest.get('l5_eq5_hr')}"
    )
    buckets = report.get("cover_pct_buckets_over_l5eq5") or []
    if buckets:
        print("  cover% of line (OVER L5=5):")
        for b in buckets:
            print(f"    {b['cover_pct']}: n={b['n']} hr={b['hr']}")
    chips = report.get("by_chip") or []
    if chips:
        print("  by chip (top):")
        for c in chips[:12]:
            print(
                f"    {c['prop']} {c['side']}: n={c['n']} hr={c['hr']} "
                f"g70={c['gate70_n']}/{c['gate70_hr']}"
            )
