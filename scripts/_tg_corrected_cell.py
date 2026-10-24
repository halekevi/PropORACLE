"""Total Games Goblin OVER on board-opponent attaches only."""
from __future__ import annotations

import json
import math
import sys
from collections import Counter
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT.parent / "PropORACLE_main_cp"
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

from utils.category_hit_rate import load_category_hr_index, lookup_category_hr  # noqa: E402
from utils.prop_norm import canon_prop  # noqa: E402
from utils.tennis_keep_gates import tennis_goblin_keep_eligible  # noqa: E402
from _wrong_match_and_cap_effect import (  # noqa: E402
    _board_matches,
    _load_board_opp_map,
    _norm,
    _opp_match,
)


def _p(*a):
    print(*a, flush=True)


def _rate(hits: int, n: int) -> str:
    if not n:
        return "n=0"
    p = hits / n
    se = math.sqrt(p * (1 - p) / n)
    return f"{hits}/{n} ({p:.1%}, band {p - 1.96 * se:.1%}–{p + 1.96 * se:.1%})"


def _step8_maps() -> dict[str, dict[tuple[str, float], dict]]:
    out: dict[str, dict[tuple[str, float], dict]] = {}
    root = MAIN / "outputs"
    if not root.is_dir():
        return out
    for day_dir in root.iterdir():
        if not day_dir.is_dir() or len(day_dir.name) != 10:
            continue
        files = list(day_dir.glob("**/step8*tennis*clean*.parquet")) + list(
            day_dir.glob("**/step8*tennis*clean*.xlsx")
        )
        if not files:
            continue
        path = files[0]
        try:
            df = (
                pd.read_parquet(path)
                if path.suffix == ".parquet"
                else pd.read_excel(path)
            )
        except Exception:
            continue
        cols = {str(c).lower(): c for c in df.columns}
        pc = cols.get("player") or cols.get("player_name")
        lc = cols.get("line")
        pr = cols.get("prop_type") or cols.get("prop")
        if not (pc and lc):
            continue
        bucket: dict[tuple[str, float], dict] = {}
        for _, r in df.iterrows():
            if pr and canon_prop("TENNIS", str(r.get(pr) or "")) != "match_total_games":
                continue
            try:
                line = float(r.get(lc))
            except (TypeError, ValueError):
                continue
            rec = {str(c): r.get(c) for c in df.columns}
            rec["sport"] = "TENNIS"
            rec["side"] = "OVER"
            rec["pick_type"] = "Goblin"
            rec["prop"] = "match_total_games"
            rec["line"] = line
            bucket[(_norm(r.get(pc)), round(line, 3))] = rec
        if bucket:
            out[day_dir.name] = bucket
    return out


def main() -> None:
    index = load_category_hr_index()
    _p("=== PRICING CELLS (catalog file, uncorrected) ===")
    for sport, prop, label in (
        ("WNBA", "3-PT Made", "Bonner 3-PT Made"),
        ("WNBA", "Pts+Asts", "Canada Pts+Asts"),
        ("TENNIS", "Total Games", "Total Games"),
    ):
        cell = lookup_category_hr(
            index,
            sport=sport,
            prop=prop,
            pick_type="Goblin",
            direction="OVER",
        )
        if not cell:
            _p(f"  {label}: no cell")
            continue
        hits = int(cell["hit"])
        n = int(cell["n"])
        _p(f"  {label}: {_rate(hits, n)}")

    _p("\n=== TOTAL GAMES, board-opponent attach only ===")
    pairs: set[tuple[str, str]] = set()
    seen = set()
    for root in (MAIN / "outputs", ROOT / "outputs"):
        if not root.is_dir():
            continue
        for path in sorted(root.glob("**/actuals_tennis_*.csv")):
            if path.name in seen:
                continue
            seen.add(path.name)
            day = path.stem.replace("actuals_tennis_", "")[:10]
            try:
                df = pd.read_csv(path, dtype=str)
            except Exception:
                continue
            if df.empty or "player" not in df.columns:
                continue
            for _, r in df.iterrows():
                prop = canon_prop("TENNIS", str(r.get("prop_type") or ""))
                if prop not in {"games_won", "match_total_games"}:
                    continue
                pairs.add((_norm(r.get("player")), day))
    days = sorted({d for _, d in pairs})
    pull = set()
    for d in days:
        dd = date.fromisoformat(d)
        for off in (-1, 0, 1):
            pull.add((dd + timedelta(days=off)).isoformat())
    boards = {}
    maps = {}
    for i, d in enumerate(sorted(pull)):
        boards[d] = _board_matches(d)
        maps[d] = _load_board_opp_map(d)
        if (i + 1) % 25 == 0 or i + 1 == len(pull):
            _p(f"  loaded {i + 1}/{len(pull)}")

    correct: dict[tuple[str, str], float] = {}
    for player, day in pairs:
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
            matched = [c for c in cands if _opp_match(board, c["opp"])]
        if not matched:
            continue
        matched.sort(key=lambda c: (c["retired"], 0 if c["ymd"] == day else 1))
        correct[(player, day)] = float(matched[0]["match_total"])
    _p(f"player-days with a board-opponent match: {len(correct)}")

    step8 = _step8_maps()
    templates = MAIN / "ui_runner" / "templates"
    if not templates.is_dir():
        templates = ROOT / "ui_runner" / "templates"
    ungated = Counter()
    gated = Counter()
    no_keep_inputs = 0
    used = 0
    for path in sorted(templates.glob("graded_props_2026-*.json")):
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
            if canon_prop("TENNIS", str(row.get("prop") or "")) != "match_total_games":
                continue
            key = (_norm(row.get("player")), day)
            if key not in correct:
                continue
            try:
                line = float(row.get("line"))
            except (TypeError, ValueError):
                continue
            total = correct[key]
            if total == line:
                continue
            hit = total > line
            ungated["HIT" if hit else "MISS"] += 1
            used += 1
            extra = (step8.get(day) or {}).get((key[0], round(line, 3))) or {}
            merged = dict(extra)
            merged.update({k: v for k, v in row.items() if v not in (None, "")})
            merged["sport"] = "TENNIS"
            merged["side"] = "OVER"
            merged["pick_type"] = "Goblin"
            merged["prop"] = "match_total_games"
            merged["line"] = line
            if tennis_goblin_keep_eligible(merged):
                gated["HIT" if hit else "MISS"] += 1
            else:
                no_keep_inputs += 1
    uh, un = ungated["HIT"], ungated["HIT"] + ungated["MISS"]
    gh, gn = gated["HIT"], gated["HIT"] + gated["MISS"]
    _p(f"verified ungated Goblin OVER Total Games: {_rate(uh, un)}")
    _p(f"verified keep (L5>=4+L10>=8 or L10>=8+lefty): {_rate(gh, gn)}")
    _p(f"rows used {used}; rows outside the keep gate {no_keep_inputs}")
    _p("pricing cell on file: 37/47 (78.7%); keep validation was 48/61 (78.7%)")
    if gn and un:
        _p(f"keep minus ungated on this set: {gh / gn - uh / un:+.1%}")


if __name__ == "__main__":
    main()
