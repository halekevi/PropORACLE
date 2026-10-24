"""Label Total Games and Games Won rates by attach path."""
from __future__ import annotations

import json
import math
import sys
from collections import Counter
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT.parent / "PropORACLE_main_cp"
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

from utils.category_hit_rate import load_category_hr_index, lookup_category_hr  # noqa: E402
from utils.prop_norm import canon_prop  # noqa: E402
from _tg_corrected_cell import _norm, _step8_maps  # noqa: E402
from _wrong_match_and_cap_effect import _board_matches, _load_board_opp_map, _opp_match  # noqa: E402

OUT = ROOT / "data" / "reports" / "wo4_tennis_paths.json"
UNGATED_GW = (819, 1224)


def rate(hits: int, n: int) -> dict:
    if not n:
        return {"hits": 0, "n": 0, "hr": None, "band": None}
    p = hits / n
    se = math.sqrt(p * (1 - p) / n)
    return {
        "hits": hits,
        "n": n,
        "hr": round(p, 4),
        "band": [round(p - 1.96 * se, 4), round(p + 1.96 * se, 4)],
    }


def on_row() -> dict:
    """Stored grade, no board-opponent check. This is the 44/58 path."""
    counts = Counter()
    seen = set()
    for base in (MAIN, ROOT):
        folder = base / "ui_runner" / "templates"
        if not folder.is_dir():
            continue
        for path in sorted(folder.glob("graded_props_20*.json")):
            if path.name in seen or ".bak_" in path.name:
                continue
            seen.add(path.name)
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            for row in raw.get("props") or []:
                if not isinstance(row, dict):
                    continue
                if not str(row.get("sport") or "").upper().startswith("TENNIS"):
                    continue
                if "goblin" not in str(row.get("pick_type") or "").lower():
                    continue
                if str(row.get("direction") or "").upper() != "OVER":
                    continue
                prop = canon_prop("TENNIS", str(row.get("prop") or ""))
                if prop != "match_total_games":
                    continue
                result = str(row.get("result") or "").upper()
                if result not in {"HIT", "MISS"}:
                    continue
                try:
                    l5 = float(row.get("l5_over") if row.get("l5_over") not in (None, "") else row.get("l5"))
                    l10 = float(row.get("l10_over") if row.get("l10_over") not in (None, "") else row.get("l10"))
                except (TypeError, ValueError):
                    continue
                if l5 >= 4 and l10 >= 8:
                    counts[result] += 1
    hits = counts["HIT"]
    return rate(hits, hits + counts["MISS"])


def main() -> None:
    index = load_category_hr_index()
    catalog = lookup_category_hr(
        index, sport="TENNIS", prop="Total Games", pick_type="Goblin", direction="OVER"
    )
    catalog_pack = None
    if catalog:
        catalog_pack = rate(int(catalog["hit"]), int(catalog["n"]))

    pairs: set[tuple[str, str, str]] = set()
    seen = set()
    for base in (MAIN / "outputs", ROOT / "outputs"):
        if not base.is_dir():
            continue
        for path in sorted(base.glob("**/actuals_tennis_*.csv")):
            if path.name in seen:
                continue
            seen.add(path.name)
            day = path.stem.replace("actuals_tennis_", "")[:10]
            try:
                import pandas as pd

                df = pd.read_csv(path, dtype=str)
            except Exception:
                continue
            if df.empty or "player" not in df.columns:
                continue
            for _, rec in df.iterrows():
                prop = canon_prop("TENNIS", str(rec.get("prop_type") or ""))
                if prop not in {"games_won", "match_total_games"}:
                    continue
                pairs.add((_norm(rec.get("player")), day, prop))
    pull = set()
    for _player, day, _prop in pairs:
        dd = date.fromisoformat(day)
        for off in (-1, 0, 1):
            pull.add((dd + timedelta(days=off)).isoformat())
    boards = {}
    maps = {}
    for i, day in enumerate(sorted(pull)):
        boards[day] = _board_matches(day)
        maps[day] = _load_board_opp_map(day)
        if (i + 1) % 25 == 0 or i + 1 == len(pull):
            print(f"loaded {i + 1}/{len(pull)}", flush=True)

    correct: dict[tuple[str, str], dict] = {}
    for player, day, _prop in pairs:
        if (player, day) in correct:
            continue
        board = ""
        for d in (
            day,
            (date.fromisoformat(day) - timedelta(days=1)).isoformat(),
            (date.fromisoformat(day) + timedelta(days=1)).isoformat(),
        ):
            found = (maps.get(d) or {}).get(player, "")
            if found and "UNKNOWN" not in found.upper():
                board = found
                break
        if not board:
            continue
        cands = []
        for d in (
            day,
            (date.fromisoformat(day) + timedelta(days=1)).isoformat(),
            (date.fromisoformat(day) - timedelta(days=1)).isoformat(),
        ):
            cands.extend(boards.get(d, {}).get(player) or [])
        matched = [c for c in cands if _opp_match(board, c["opp"]) and not c["retired"]]
        if not matched:
            continue
        matched.sort(key=lambda c: 0 if c["ymd"] == day else 1)
        correct[(player, day)] = matched[0]

    step8 = _step8_maps()
    tallies = {
        "games_won_ungated": Counter(),
        "match_total_ungated": Counter(),
        "match_total_l5ge4_l10ge8": Counter(),
    }
    templates = []
    for base in (MAIN, ROOT):
        folder = base / "ui_runner" / "templates"
        if folder.is_dir():
            templates.extend(sorted(folder.glob("graded_props_2026-*.json")))
    used_names = set()
    for path in templates:
        if path.name in used_names or ".bak_" in path.name:
            continue
        used_names.add(path.name)
        day = path.stem.replace("graded_props_", "")[:10]
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        for row in raw.get("props") or []:
            if not isinstance(row, dict):
                continue
            if not str(row.get("sport") or "").upper().startswith("TENNIS"):
                continue
            if "goblin" not in str(row.get("pick_type") or "").lower():
                continue
            if str(row.get("direction") or "").upper() != "OVER":
                continue
            prop = canon_prop("TENNIS", str(row.get("prop") or ""))
            if prop not in {"games_won", "match_total_games"}:
                continue
            key = (_norm(row.get("player")), day)
            box = correct.get(key)
            if not box:
                continue
            try:
                line = float(row.get("line"))
            except (TypeError, ValueError):
                continue
            actual = float(box["games"] if prop == "games_won" else box["match_total"])
            if actual == line:
                continue
            label = "HIT" if actual > line else "MISS"
            if prop == "games_won":
                tallies["games_won_ungated"][label] += 1
                continue
            tallies["match_total_ungated"][label] += 1
            extra = (step8.get(day) or {}).get((key[0], round(line, 3))) or {}
            try:
                l5 = float(extra.get("l5_over") if extra.get("l5_over") not in (None, "") else row.get("l5_over"))
                l10 = float(extra.get("l10_over") if extra.get("l10_over") not in (None, "") else row.get("l10_over"))
            except (TypeError, ValueError):
                continue
            if l5 >= 4 and l10 >= 8:
                tallies["match_total_l5ge4_l10ge8"][label] += 1

    def from_counter(c: Counter) -> dict:
        return rate(c["HIT"], c["HIT"] + c["MISS"])

    gw = from_counter(tallies["games_won_ungated"])
    shift = None
    if gw["hr"] is not None and UNGATED_GW[1]:
        shift = round(gw["hr"] - (UNGATED_GW[0] / UNGATED_GW[1]), 4)
    payload = {
        "paths": {
            "37/47": {
                "attach": "catalog file, uncorrected",
                "cell": catalog_pack,
            },
            "44/58": {
                "attach": "on-row graded L5>=4+L10>=8, stored result, no board-opponent check",
                "cell": on_row(),
            },
            "146/208": {
                "attach": "corrected board-opponent, non-retired, Goblin OVER Total Games, L5>=4+L10>=8",
                "cell": from_counter(tallies["match_total_l5ge4_l10ge8"]),
                "on_file": "146/208 (70.2%)",
            },
        },
        "corrected_ungated_total_games": from_counter(tallies["match_total_ungated"]),
        "corrected_ungated_games_won": gw,
        "games_won_vs_819_1224": {
            "on_file": "819/1224 (66.9%)",
            "shift": shift,
        },
    }
    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
