"""Paired Goblin/Standard Saves lines, and own-defense vs shots on target faced."""
from __future__ import annotations

import sys
import unicodedata
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT.parent / "PropORACLE_main_cp"
sys.path.insert(0, str(ROOT))

from utils.prop_norm import canon_prop  # noqa: E402
from utils.soccer_keep_gates import _own_def_tier  # noqa: E402


def _p(*a):
    print(*a, flush=True)


def _norm(name: object) -> str:
    raw = unicodedata.normalize("NFKD", str(name or ""))
    raw = "".join(ch for ch in raw if not unicodedata.combining(ch))
    return " ".join(raw.casefold().split())


def _load_actuals():
    """(player, day) -> (actual saves or None, team code). (day, team) -> SOT sum."""
    saves: dict[tuple[str, str], float] = {}
    team_of: dict[tuple[str, str], str] = {}
    sot: dict[tuple[str, str], float] = defaultdict(float)
    root = MAIN / "outputs"
    for path in sorted(root.glob("2026-09-*/actuals_soccer_2026-09-*.csv")):
        day = path.stem.replace("actuals_soccer_", "")[:10]
        df = pd.read_csv(path, dtype=str)
        for _, r in df.iterrows():
            player = _norm(r.get("player"))
            team = str(r.get("team") or "").strip().upper()
            if player and team:
                team_of[(player, day)] = team
            prop = str(r.get("prop_type") or "")
            try:
                actual = float(r.get("actual"))
            except (TypeError, ValueError):
                continue
            canon = canon_prop("SOCCER", prop)
            if canon == "saves" or "save" in prop.lower():
                saves[(player, day)] = actual
            if canon in {"sog", "shots_on_target"} or "on target" in prop.lower():
                if team:
                    sot[(day, team)] += actual
    return saves, team_of, sot


def main() -> None:
    saves, team_of, sot = _load_actuals()
    start = date(2026, 9, 1)
    end = date(2026, 9, 27)
    day = start
    # player-day -> list of (pick, line, own, team name, opp name)
    legs: dict[tuple[str, str], list] = defaultdict(list)
    name_to_code: dict[tuple[str, str, str], str] = {}
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
            if canon_prop("SOCCER", str(r.get("prop_type") or "")) != "saves":
                continue
            player = _norm(r.get("player"))
            if not player:
                continue
            try:
                line = float(r.get("line"))
            except (TypeError, ValueError):
                continue
            pick = str(r.get("pick_type") or "")
            low = pick.lower()
            if "goblin" in low:
                kind = "Goblin"
            elif "standard" in low:
                kind = "Standard"
            elif "demon" in low:
                kind = "Demon"
            else:
                continue
            team = str(r.get("team") or "").strip().upper()
            opp = str(r.get("opp_team") or "").strip().upper()
            code = team_of.get((player, stamp), "")
            if code and team:
                name_to_code[(stamp, team)] = code
            row = {str(c): r.get(c) for c in df.columns}
            legs[(player, stamp)].append(
                {
                    "kind": kind,
                    "line": line,
                    "own": _own_def_tier(row) or "missing",
                    "team": team,
                    "opp": opp,
                }
            )

    # Paired check: one goblin line and one standard line per keeper-game.
    both = 0
    order_ok = 0
    goblin_lower = 0
    impossible = 0
    samples = []
    decided_pairs = 0
    for (player, stamp), items in legs.items():
        actual = saves.get((player, stamp))
        g_lines = sorted({it["line"] for it in items if it["kind"] == "Goblin"})
        s_lines = sorted({it["line"] for it in items if it["kind"] == "Standard"})
        if not g_lines or not s_lines:
            continue
        both += 1
        g = min(g_lines)
        s = min(s_lines)
        if g < s:
            goblin_lower += 1
        elif g == s:
            order_ok += 1
        if actual is None:
            continue
        decided_pairs += 1
        g_hit = actual > g
        s_hit = actual > s
        # Goblin line below Standard: a Goblin miss cannot be a Standard hit.
        if g < s and (not g_hit) and s_hit:
            impossible += 1
            if len(samples) < 8:
                samples.append((stamp, player, g, s, actual))
        if g > s and g_hit and (not s_hit):
            impossible += 1
            if len(samples) < 8:
                samples.append((stamp, player, g, s, actual, "goblin_higher"))

    _p(f"keeper-games with both a Goblin and a Standard saves line: {both}")
    _p(f"  Goblin line lower: {goblin_lower}")
    _p(f"  lines equal: {order_ok}")
    _p(f"  Goblin line higher: {both - goblin_lower - order_ok}")
    _p(f"decided pairs (box actual): {decided_pairs}")
    _p(f"ordering breaks (miss on the lower line, hit on the higher): {impossible}")
    for s in samples:
        _p("  ", s)

    # SOT faced by own-defense tier, on goblin keeper-days with a saves actual.
    by_tier = defaultdict(list)
    missing_opp = 0
    used = 0
    for (player, stamp), items in legs.items():
        if (player, stamp) not in saves:
            continue
        gob = [it for it in items if it["kind"] == "Goblin"]
        if not gob:
            continue
        it = gob[0]
        own_code = team_of.get((player, stamp), "")
        opp_code = name_to_code.get((stamp, it["opp"]), "")
        if not opp_code or opp_code == own_code:
            missing_opp += 1
            continue
        faced = sot.get((stamp, opp_code))
        if faced is None:
            missing_opp += 1
            continue
        by_tier[it["own"]].append(faced)
        used += 1
    _p(f"\nSOT faced attached for {used} goblin keeper-days; unmatched opponent {missing_opp}")
    for tier in ("Elite", "Above Avg", "Avg", "Below Avg", "Weak", "missing"):
        vals = by_tier.get(tier) or []
        if not vals:
            continue
        _p(f"  {tier}: n={len(vals)} mean SOT faced={sum(vals)/len(vals):.2f}")


if __name__ == "__main__":
    main()
