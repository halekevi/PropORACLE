"""Goblin legs on today's board after the regular-season block and a cell check."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT.parent / "PropORACLE_main_cp"
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

from utils.category_hit_rate import load_category_hr_index, lookup_category_hr  # noqa: E402
from utils.postseason_slate import season_blocks_ticket, season_class  # noqa: E402
from utils.prop_norm import canon_prop  # noqa: E402
from utils.ticket_70_pool import goblin_70_eligible  # noqa: E402

DATE = "2026-10-08"
INDEX = load_category_hr_index()


def _files() -> list[tuple[str, Path]]:
    day = MAIN / "outputs" / DATE
    found = []
    named = {
        "MLB": day / "mlb" / "step8_mlb_direction.csv",
        "NFL": day / "nfl" / "step8_nfl_direction_clean.xlsx",
        "SOCCER": day / "soccer" / "step8_soccer_direction.csv",
        "TENNIS": day / "step8_tennis_direction.csv",
        "GOLF": day / "golf" / "step8_golf_direction.csv",
        "NHL": day / "nhl" / "step1_nhl_props.csv",
        "NBA": day / "nba" / "step1_pp_props_today.csv",
        "WNBA": day / "wnba" / "step1_wnba_props.csv",
        "CFB": day / "cfb" / "step8_cfb_direction.csv",
    }
    for sport, path in named.items():
        if path.is_file():
            found.append((sport, path))
    return found


def _load(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in {".xlsx", ".xls"}:
        return pd.read_excel(path, dtype=str)
    return pd.read_csv(path, dtype=str, encoding_errors="replace")


def _row(sport: str, raw: dict) -> dict:
    row = dict(raw)
    row["sport"] = sport
    row["prop"] = (
        row.get("prop")
        or row.get("prop_type")
        or row.get("Prop")
        or row.get("stat_type")
    )
    row["side"] = str(row.get("side") or row.get("direction") or row.get("Direction") or "OVER").upper()
    row["pick_type"] = row.get("pick_type") or row.get("Pick Type") or row.get("odds_type") or ""
    if "goblin" in str(row.get("pick_type")).lower():
        row["pick_type"] = "Goblin"
    row["team"] = row.get("team") or row.get("Team")
    row["opp_team"] = row.get("opp_team") or row.get("Opp") or row.get("opponent")
    if row.get("l5_over") in (None, "", "nan") and row.get("L5 Over") not in (None, "", "nan"):
        row["l5_over"] = row.get("L5 Over")
    if row.get("l10_over") in (None, "", "nan") and row.get("L10 Over") not in (None, "", "nan"):
        row["l10_over"] = row.get("L10 Over")
    if not str(row.get("start_time") or row.get("game_date") or "").strip():
        row["game_date"] = DATE
    return row


def _has_cell(row: dict) -> bool:
    prop = canon_prop(row["sport"], str(row.get("prop") or ""))
    if row["sport"] == "TENNIS" and prop in {"games_won", "match_total_games"}:
        return True
    found = lookup_category_hr(
        INDEX,
        sport=row["sport"],
        prop=row.get("prop"),
        pick_type="Goblin",
        direction=row.get("side") or "OVER",
    )
    return bool(found and found.get("n"))


def main() -> None:
    print(f"slate {DATE}")
    for sport, path in _files():
        df = _load(path)
        goblin = 0
        regular = 0
        eligible = 0
        with_cell = 0
        classes: dict[str, int] = {}
        for _, raw in df.iterrows():
            row = _row(sport, raw.to_dict())
            pick = str(row.get("pick_type") or "")
            if "Goblin" not in pick and "goblin" not in pick.lower():
                continue
            goblin += 1
            kind = season_class(row) or "unconfirmed"
            classes[kind] = classes.get(kind, 0) + 1
            if season_blocks_ticket(row):
                continue
            regular += 1
            if not goblin_70_eligible(row):
                continue
            eligible += 1
            if _has_cell(row):
                with_cell += 1
        print(
            f"{sport:7} file={path.name} goblin={goblin} "
            f"past_season_block={regular} eligible={eligible} "
            f"eligible_with_cell={with_cell} season={classes}"
        )


if __name__ == "__main__":
    main()
