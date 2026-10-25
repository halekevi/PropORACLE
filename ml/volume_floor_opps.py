#!/usr/bin/env python3
"""Count how often a Standard or Demon volume line sits under the L5 floor.

Opportunity count only. Does not log, place, or edit the three frozen tracks.

  py -3.14 -m ml.volume_floor_opps

Uses face-labeled NFL step8 boards plus nflverse 2026 weeks already on disk
(through week 7). A line counts when:

  pick_type in {Standard, Demon}
  prop is rush attempts / receptions / targets
  side is OVER (Demon UNDER is invalid)
  player is top-2 on his team for that volume share (as-of)
  five prior games exist and min(those) > line
"""
from __future__ import annotations

import json
import math
import sys
from pathlib import Path
from typing import Any

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from utils.nfl_demon_volume_shadow import volume_prop  # noqa: E402
from utils.nfl_player_names import norm_nfl_player_name  # noqa: E402

MAIN = _REPO.parent / "PropORACLE_main_cp"
CACHE = _REPO / "Sports" / "NFL" / "data" / "cache"
OUT = _REPO / "data" / "reports" / "volume_floor_opps_latest.json"
OUT_MD = _REPO / "data" / "reports" / "volume_floor_opps_latest.md"
SEASON = 2026
# First step1 dates that store pick_type. Earlier boards defaulted Standard.
FACE_VERIFIED_FROM = "2026-10-23"
SLATE_WEEK = 8  # 2026-10-22 TNF through 10-26; nflverse on disk stops at 7
RANK_MAX = 2
L5_GAMES = 5
VOLUME_FAMILIES = {
    "rush_attempts": "rush_attempts",
    "receptions": "receptions",
    "rec_tgt": "rec_tgt",
    "rec_targets": "rec_tgt",
    "targets": "rec_tgt",
}
BOOKS = ("Standard", "Demon")


def _f(raw: object) -> float | None:
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def _pick(raw: object) -> str:
    k = str(raw or "").strip().lower()
    if "gob" in k:
        return "Goblin"
    if "dem" in k:
        return "Demon"
    if "std" in k or k == "standard":
        return "Standard"
    return ""


def _family(prop: object) -> str | None:
    folded = volume_prop(prop)
    if folded in VOLUME_FAMILIES:
        return VOLUME_FAMILIES[folded]
    key = str(folded or "").strip().lower()
    return VOLUME_FAMILIES.get(key)


def step8_paths() -> list[tuple[str, Path]]:
    found: list[tuple[str, Path]] = []
    for root in (MAIN, _REPO):
        outputs = root / "outputs"
        if not outputs.is_dir():
            continue
        for day in sorted(outputs.iterdir()):
            if not day.is_dir() or not day.name.startswith("2026-"):
                continue
            for name in (
                f"step8_nfl_direction_clean_{day.name}.xlsx",
                "step8_nfl_direction_clean.xlsx",
            ):
                path = day / "nfl" / name
                if path.is_file():
                    found.append((day.name, path))
                    break
    # One board per date; prefer main_cp (listed first).
    by_date: dict[str, Path] = {}
    for date, path in found:
        if date < FACE_VERIFIED_FROM:
            continue
        by_date.setdefault(date, path)
    return sorted(by_date.items())


def _col(df: pd.DataFrame, *names: str) -> str | None:
    cmap = {str(c).strip().lower(): c for c in df.columns}
    for n in names:
        if n.lower() in cmap:
            return cmap[n.lower()]
    return None


def load_board(path: Path, date: str) -> list[dict[str, Any]]:
    df = pd.read_excel(path)
    c_player = _col(df, "Player", "player")
    c_team = _col(df, "Team", "team")
    c_prop = _col(df, "Prop", "prop", "prop_type")
    c_pick = _col(df, "Pick Type", "pick_type")
    c_line = _col(df, "Line", "line")
    c_dir = _col(df, "Direction", "direction")
    c_l5 = _col(df, "L5 Over", "l5_over")
    if not all((c_player, c_team, c_prop, c_pick, c_line)):
        return []
    rows = []
    for rec in df.to_dict("records"):
        player = str(rec.get(c_player) or "").strip()
        if not player or "+" in player:
            continue
        family = _family(rec.get(c_prop))
        if not family:
            continue
        side = str(rec.get(c_dir) or "OVER").strip().upper()
        if side in {"U", "UNDER", "LESS"}:
            continue
        pick = _pick(rec.get(c_pick))
        if pick not in {"Standard", "Demon", "Goblin"}:
            continue
        line = _f(rec.get(c_line))
        if line is None:
            continue
        rows.append(
            {
                "date": date,
                "player": player,
                "player_norm": norm_nfl_player_name(player),
                "team": str(rec.get(c_team) or "").strip().upper(),
                "prop": family,
                "pick": pick,
                "line": line,
                "l5_over": _f(rec.get(c_l5) if c_l5 else None),
            }
        )
    # One face per player-prop-pick.
    best: dict[tuple, dict[str, Any]] = {}
    for r in rows:
        key = (r["player_norm"], r["prop"], r["pick"], r["line"])
        best[key] = r
    return list(best.values())


def load_nflverse_2026() -> pd.DataFrame:
    path = CACHE / f"nflverse_stats_player_week_{SEASON}.csv"
    df = pd.read_csv(path)
    if "season_type" in df.columns:
        reg = df[df["season_type"].astype(str).str.upper().isin(("REG", "REGULAR"))]
        if not reg.empty:
            df = reg
    df["week"] = pd.to_numeric(df["week"], errors="coerce")
    df = df[df["week"] < SLATE_WEEK]
    keep = df[
        [
            "player_display_name",
            "position",
            "week",
            "team",
            "carries",
            "receptions",
            "targets",
            "target_share",
        ]
    ].copy()
    keep["player_norm"] = keep["player_display_name"].map(norm_nfl_player_name)
    keep["team"] = keep["team"].astype(str).str.upper()
    keep["position"] = keep["position"].astype(str).str.upper()
    keep["carries"] = pd.to_numeric(keep["carries"], errors="coerce")
    keep["receptions"] = pd.to_numeric(keep["receptions"], errors="coerce")
    keep["targets"] = pd.to_numeric(keep["targets"], errors="coerce")
    share = pd.to_numeric(keep["target_share"], errors="coerce")
    keep["target_share_pp"] = share.where(share.isna() | (share > 1.5), share * 100.0)
    return keep


def player_floors(df: pd.DataFrame) -> dict[str, dict[str, float]]:
    """player_norm -> L5 min for each family, last 5 games before slate week."""
    out: dict[str, dict[str, float]] = {}
    for player, sub in df.sort_values("week").groupby("player_norm"):
        if not player:
            continue
        slot: dict[str, float] = {}
        for fam, col in (
            ("rush_attempts", "carries"),
            ("receptions", "receptions"),
            ("rec_tgt", "targets"),
        ):
            vals = [v for v in sub[col].tolist() if pd.notna(v)]
            if len(vals) >= L5_GAMES:
                slot[fam] = float(min(vals[-L5_GAMES:]))
        if slot:
            out[str(player)] = slot
    return out


def team_ranks(df: pd.DataFrame) -> dict[str, dict[str, dict[str, int]]]:
    """family -> team -> player_norm -> rank (1 = highest L3 share)."""
    last3 = df[df["week"] >= (SLATE_WEEK - 3)]
    team_carries = last3.groupby(["week", "team"])["carries"].transform("sum")
    work = last3.copy()
    work["carry_share"] = work["carries"] / team_carries.replace(0, pd.NA)
    pos = work.groupby(["player_norm", "team"])["position"].agg(
        lambda s: s.dropna().astype(str).iloc[-1] if len(s.dropna()) else ""
    )
    agg = (
        work.groupby(["player_norm", "team"])
        .agg(
            carry_share=("carry_share", "mean"),
            tgt_share=("target_share_pp", "mean"),
        )
        .reset_index()
    )
    agg["position"] = agg.apply(lambda r: pos.get((r.player_norm, r.team), ""), axis=1)
    ranks: dict[str, dict[str, dict[str, int]]] = {
        "rush_attempts": {},
        "receptions": {},
        "rec_tgt": {},
        "receptions_wrte": {},
    }
    for team, sub in agg.groupby("team"):
        rush = sub.dropna(subset=["carry_share"]).sort_values("carry_share", ascending=False)
        rec = sub.dropna(subset=["tgt_share"]).sort_values("tgt_share", ascending=False)
        ranks["rush_attempts"][str(team)] = {
            str(r.player_norm): i for i, r in enumerate(rush.itertuples(index=False), 1)
        }
        rec_map = {str(r.player_norm): i for i, r in enumerate(rec.itertuples(index=False), 1)}
        wrte = sub[sub["position"].isin(("WR", "TE"))].dropna(subset=["tgt_share"])
        wrte = wrte.sort_values("tgt_share", ascending=False)
        wr_map = {str(r.player_norm): i for i, r in enumerate(wrte.itertuples(index=False), 1)}
        ranks["receptions"][str(team)] = rec_map
        ranks["rec_tgt"][str(team)] = rec_map
        ranks.setdefault("receptions_wrte", {})[str(team)] = wr_map
    return ranks


def classify(
    row: dict[str, Any],
    floors: dict[str, dict[str, float]],
    ranks: dict[str, dict[str, dict[str, int]]],
) -> dict[str, Any]:
    fam = row["prop"]
    floor = (floors.get(row["player_norm"]) or {}).get(fam)
    rank = (ranks.get(fam) or {}).get(row["team"], {}).get(row["player_norm"])
    wr_rank = (ranks.get("receptions_wrte") or {}).get(row["team"], {}).get(row["player_norm"])
    rec = {
        **row,
        "l5_min": floor,
        "rank": rank,
        "wrte_rank": wr_rank,
        "top2": rank is not None and rank <= RANK_MAX,
        "below_floor": floor is not None and floor > row["line"],
    }
    rec["opp"] = bool(rec["top2"] and rec["below_floor"] and rec["pick"] in BOOKS)
    rec["goblin_floor"] = bool(rec["top2"] and rec["below_floor"] and rec["pick"] == "Goblin")
    return rec


def run() -> dict[str, Any]:
    boards = step8_paths()
    weeks = load_nflverse_2026()
    floors = player_floors(weeks)
    ranks = team_ranks(weeks)
    slates = []
    all_opps = []
    all_goblin = []
    for date, path in boards:
        rows = [classify(r, floors, ranks) for r in load_board(path, date)]
        if not rows:
            continue
        # Face-labeled board only (has at least one Demon).
        if not any(r["pick"] == "Demon" for r in rows):
            continue
        opps = [r for r in rows if r["opp"]]
        goblins = [r for r in rows if r["goblin_floor"]]
        vol = [r for r in rows if r["pick"] in BOOKS]
        slates.append(
            {
                "date": date,
                "path": str(path),
                "volume_std_demon_n": len(vol),
                "top2_std_demon_n": sum(1 for r in vol if r["top2"]),
                "opp_n": len(opps),
                "opp_standard": sum(1 for r in opps if r["pick"] == "Standard"),
                "opp_demon": sum(1 for r in opps if r["pick"] == "Demon"),
                "goblin_floor_n": len(goblins),
                "opps": [
                    {
                        "player": r["player"],
                        "team": r["team"],
                        "prop": r["prop"],
                        "pick": r["pick"],
                        "line": r["line"],
                        "l5_min": r["l5_min"],
                        "rank": r["rank"],
                    }
                    for r in sorted(opps, key=lambda x: (x["pick"], x["player"]))
                ],
            }
        )
        all_opps.extend(opps)
        all_goblin.extend(goblins)
    unique = {}
    for r in all_opps:
        unique[(r["player_norm"], r["prop"], r["pick"], r["line"])] = r
    return {
        "purpose": "opportunity count; not a track; frozen gates untouched",
        "slate_week": SLATE_WEEK,
        "nflverse_prior_weeks": int(weeks["week"].max()) if not weeks.empty else None,
        "slates": slates,
        "total_opp": len(all_opps),
        "total_standard": sum(1 for r in all_opps if r["pick"] == "Standard"),
        "total_demon": sum(1 for r in all_opps if r["pick"] == "Demon"),
        "total_goblin_floor": len(all_goblin),
        "per_slate": {s["date"]: s["opp_n"] for s in slates},
        "mean_per_slate": (
            round(sum(s["opp_n"] for s in slates) / len(slates), 2) if slates else None
        ),
        "unique_player_prop_line": len(unique),
        "face_verified_from": FACE_VERIFIED_FROM,
        "tucker_note": (
            "Tre Tucker Demon receptions 3.5 is below his L5 min (4) but "
            "L3 target-share rank is 3 (Jeanty is 2). Rec-floor would skip him "
            "on this as-of share. He is #2 among LV WR/TE."
        ),
    }


def _md(payload: dict[str, Any]) -> str:
    lines = [
        "# NFL volume-floor opportunities (Standard / Demon)",
        "",
        "Count only. The three frozen tracks were not opened. "
        "A row is a top-2 rush-attempt / reception / target player whose "
        "Standard or Demon OVER line sits strictly below his nflverse L5 min.",
        "",
        f"Slates: {', '.join(payload['per_slate']) or '(none)'}. "
        f"Mean per slate: {payload['mean_per_slate']}. "
        f"Goblin lines on the same floor: {payload['total_goblin_floor']}. "
        f"Unique player-prop-line across these fetches: {payload.get('unique_player_prop_line')}.",
        "",
        payload.get("tucker_note") or "",
        "",
        "| Date | Std+Demon volume | Top-2 | Opp | Standard | Demon | Goblin on floor |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for s in payload.get("slates") or []:
        lines.append(
            f"| {s['date']} | {s['volume_std_demon_n']} | {s['top2_std_demon_n']} | "
            f"{s['opp_n']} | {s['opp_standard']} | {s['opp_demon']} | {s['goblin_floor_n']} |"
        )
    lines += ["", "## Legs", ""]
    for s in payload.get("slates") or []:
        lines.append(f"### {s['date']}")
        if not s["opps"]:
            lines.append("None.")
            continue
        for r in s["opps"]:
            lines.append(
                f"- {r['pick']} {r['player']} ({r['team']}) {r['prop']} "
                f"{r['line']} < L5 min {r['l5_min']} (rank {r['rank']})"
            )
        lines.append("")
    lines += [
        "## How to read",
        "",
        "- A few per week: the Demon tracks can reach 100 legs in a reasonable time.",
        "- Near zero: the shape is real but rare. Check every board; do not expect it daily.",
        "- Goblin-on-floor is the efficient price. Those are not the edge.",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    payload = run()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    OUT_MD.write_text(_md(payload), encoding="utf-8")
    print(_md(payload))
    print(f"wrote {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
