"""Hit rates on Sep 1–27 Goblin Saves, split by own-defense and opp attack."""
from __future__ import annotations

import math
import sys
import unicodedata
from collections import Counter
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT.parent / "PropORACLE_main_cp"
sys.path.insert(0, str(ROOT))

from utils.prop_norm import canon_prop  # noqa: E402
from utils.soccer_keep_gates import (  # noqa: E402
    _opp_off_tier,
    _own_def_tier,
    _saves_opp_off_ok,
    _saves_own_d_ok,
)


def _p(*a):
    print(*a, flush=True)


def _norm(name: object) -> str:
    raw = unicodedata.normalize("NFKD", str(name or ""))
    raw = "".join(ch for ch in raw if not unicodedata.combining(ch))
    return " ".join(raw.casefold().split())


def _rate(hits: int, n: int) -> str:
    if not n:
        return "n=0"
    p = hits / n
    se = math.sqrt(p * (1 - p) / n)
    return f"{hits}/{n} ({p:.1%}, band {p - 1.96 * se:.1%}–{p + 1.96 * se:.1%})"


def _actuals() -> dict[tuple[str, str], tuple[float, str]]:
    out: dict[tuple[str, str], tuple[float, str]] = {}
    root = MAIN / "outputs"
    for path in sorted(root.glob("2026-09-*/actuals_soccer_2026-09-*.csv")):
        day = path.stem.replace("actuals_soccer_", "")[:10]
        try:
            df = pd.read_csv(path, dtype=str)
        except Exception:
            continue
        if "prop_type" not in df.columns:
            continue
        sub = df[df["prop_type"].astype(str).str.contains("Save", case=False)]
        for _, r in sub.iterrows():
            try:
                actual = float(r.get("actual"))
            except (TypeError, ValueError):
                continue
            key = (_norm(r.get("player")), day)
            out[key] = (actual, str(r.get("league") or ""))
    return out


def _opp_bucket(row: dict) -> str:
    tier = _opp_off_tier(row)
    if not tier or tier in {"N/A", "Avg"}:
        return "not_applied"
    if _saves_opp_off_ok(row):
        return "filled_pass"
    return "filled_fail"


def _own_bucket(row: dict) -> str:
    tier = _own_def_tier(row)
    if not tier or tier in {"N/A"}:
        return "missing"
    if _saves_own_d_ok(row):
        return "pass"
    return "fail"


def main() -> None:
    actuals = _actuals()
    _p(f"goalkeeper-saves actuals player-days in Sep files: {len(actuals)}")
    start = date(2026, 9, 1)
    end = date(2026, 9, 27)
    day = start
    seen = set()
    first_days: set[tuple[str, str]] = set()
    graded = []
    no_actual = 0
    pushes = 0
    while day <= end:
        stamp = day.isoformat()
        path = MAIN / "outputs" / stamp / "soccer" / "step8_soccer_direction.csv"
        if not path.is_file():
            path = ROOT / "outputs" / stamp / "soccer" / "step8_soccer_direction.csv"
        day += timedelta(days=1)
        if not path.is_file():
            continue
        df = pd.read_csv(path, dtype=str, encoding_errors="replace")
        for _, r in df.iterrows():
            prop = canon_prop("SOCCER", str(r.get("prop_type") or ""))
            if prop != "saves":
                continue
            if "goblin" not in str(r.get("pick_type") or "").lower():
                continue
            side = str(r.get("side") or r.get("direction") or "").upper()
            if side not in {"", "OVER"}:
                continue
            player = str(r.get("player") or "").strip()
            if not player:
                continue
            try:
                line = float(r.get("line"))
            except (TypeError, ValueError):
                continue
            key = (stamp, _norm(player), round(line, 3))
            if key in seen:
                continue
            seen.add(key)
            player_day = (stamp, _norm(player))
            row = {str(c): r.get(c) for c in df.columns}
            row["sport"] = "SOCCER"
            row["side"] = "OVER"
            row["pick_type"] = "Goblin"
            row["prop"] = "Goalie Saves"
            got = actuals.get((_norm(player), stamp))
            src = "same_day"
            if got is None:
                nxt = (date.fromisoformat(stamp) + timedelta(days=1)).isoformat()
                prv = (date.fromisoformat(stamp) - timedelta(days=1)).isoformat()
                got = actuals.get((_norm(player), nxt))
                src = "next_day"
                if got is None:
                    got = actuals.get((_norm(player), prv))
                    src = "prev_day"
            if got is None:
                no_actual += 1
                continue
            actual, league = got
            if actual == line:
                pushes += 1
                continue
            graded.append(
                {
                    "hit": actual > line,
                    "own": _own_bucket(row),
                    "opp": _opp_bucket(row),
                    "own_tier": _own_def_tier(row) or "missing",
                    "opp_tier": _opp_off_tier(row) or "missing",
                    "src": src,
                    "league": league,
                    "player_day": player_day,
                    "first": player_day not in first_days,
                }
            )
            first_days.add(player_day)
    _p(f"unique goblin saves lines: {len(seen)}")
    _p(f"no box actual: {no_actual}  pushes excluded: {pushes}  decided: {len(graded)}")
    hits = sum(1 for g in graded if g["hit"])
    _p(f"all decided: {_rate(hits, len(graded))}")
    _p(f"actual source: {dict(Counter(g['src'] for g in graded))}")
    _p(f"actual league: {dict(Counter(g['league'] for g in graded).most_common(8))}")

    def _split(label: str, key: str) -> None:
        _p(f"\n{label}")
        buckets = sorted({g[key] for g in graded})
        for b in buckets:
            rows = [g for g in graded if g[key] == b]
            h = sum(1 for g in rows if g["hit"])
            _p(f"  {b}: {_rate(h, len(rows))}")

    _split("own-defense", "own")
    _split("own-defense tier", "own_tier")
    _split("opponent attack", "opp")
    _split("opponent attack tier", "opp_tier")

    first = [g for g in graded if g["first"]]
    fh = sum(1 for g in first if g["hit"])
    _p(f"\nplayer-days (one line each, the 807 set): decided {_rate(fh, len(first))}")
    for label, key in (("own-defense", "own"), ("opponent attack", "opp")):
        _p(label)
        for b in sorted({g[key] for g in first}):
            rows = [g for g in first if g[key] == b]
            h = sum(1 for g in rows if g["hit"])
            _p(f"  {b}: {_rate(h, len(rows))}")

    _p("\nown-defense x opponent attack")
    for own in ("pass", "fail", "missing"):
        for opp in ("filled_pass", "filled_fail", "not_applied"):
            rows = [g for g in graded if g["own"] == own and g["opp"] == opp]
            if not rows:
                continue
            h = sum(1 for g in rows if g["hit"])
            _p(f"  own {own} / opp {opp}: {_rate(h, len(rows))}")


if __name__ == "__main__":
    main()
