"""Saves hit rate by line and pick, and which team OWN_DEF_TIER follows."""
from __future__ import annotations

import math
import sys
import unicodedata
from collections import Counter, defaultdict
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


def _rate(hits: int, n: int) -> str:
    if not n:
        return "n=0"
    p = hits / n
    se = math.sqrt(p * (1 - p) / n)
    lo = max(0.0, p - 1.96 * se)
    hi = min(1.0, p + 1.96 * se)
    return f"{hits}/{n} ({p:.1%}, band {lo:.1%}–{hi:.1%})"


def _actuals() -> dict[tuple[str, str], float]:
    out: dict[tuple[str, str], float] = {}
    root = MAIN / "outputs"
    for path in sorted(root.glob("2026-09-*/actuals_soccer_2026-09-*.csv")):
        day = path.stem.replace("actuals_soccer_", "")[:10]
        df = pd.read_csv(path, dtype=str)
        sub = df[df["prop_type"].astype(str).str.contains("Save", case=False)]
        for _, r in sub.iterrows():
            try:
                actual = float(r.get("actual"))
            except (TypeError, ValueError):
                continue
            out[(_norm(r.get("player")), day)] = actual
    return out


def _pick(raw: object) -> str:
    low = str(raw or "").lower()
    if "goblin" in low:
        return "Goblin"
    if "demon" in low:
        return "Demon"
    if "standard" in low:
        return "Standard"
    return str(raw or "")


def main() -> None:
    actuals = _actuals()
    start = date(2026, 9, 1)
    end = date(2026, 9, 27)
    day = start
    seen_lines: set[tuple] = set()
    first_goblin: set[tuple[str, str]] = set()
    rows = []
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
            player = str(r.get("player") or "").strip()
            if not player:
                continue
            try:
                line = float(r.get("line"))
            except (TypeError, ValueError):
                continue
            pick = _pick(r.get("pick_type"))
            key = (stamp, _norm(player), pick, round(line, 3))
            if key in seen_lines:
                continue
            seen_lines.add(key)
            actual = actuals.get((_norm(player), stamp))
            if actual is None or actual == line:
                continue
            own = _own_def_tier({str(c): r.get(c) for c in df.columns}) or "missing"
            def_tier = str(r.get("DEF_TIER") or r.get("def_tier") or "").strip() or "missing"
            team = str(r.get("team") or "").strip().upper()
            opp = str(r.get("opp_team") or "").strip().upper()
            pd_key = (stamp, _norm(player))
            is_first_gob = pick == "Goblin" and pd_key not in first_goblin
            if pick == "Goblin":
                first_goblin.add(pd_key)
            rows.append(
                {
                    "hit": actual > line,
                    "line": line,
                    "pick": pick,
                    "own": own,
                    "def_tier": def_tier,
                    "team": team,
                    "opp": opp,
                    "first_goblin": is_first_gob,
                    "ga": r.get("OWN_GA_PG") or r.get("goals_conceded_pg"),
                }
            )

    gob = [r for r in rows if r["first_goblin"]]
    _p(f"decided goblin keeper-days (one line): {len(gob)}")
    _p(f"decided saves lines, all picks: {len(rows)}")

    def _by_line(label: str, subset: list[dict]) -> None:
        _p(f"\n{label}")
        lines = sorted({r["line"] for r in subset})
        for line in lines:
            got = [r for r in subset if r["line"] == line]
            h = sum(1 for r in got if r["hit"])
            _p(f"  {line}: {_rate(h, len(got))}")

    _by_line("Goblin keeper-days by posted line", gob)
    _p("\nAll decided lines by pick x line")
    for pick in ("Goblin", "Standard", "Demon"):
        sub = [r for r in rows if r["pick"] == pick]
        if not sub:
            continue
        h = sum(1 for r in sub if r["hit"])
        _p(f"  {pick} all lines: {_rate(h, len(sub))}")
        _by_line(f"  {pick}", sub)

    _p("\nGoblin keeper-days, own-defense tier by line")
    for line in (1.5, 2.5, 3.5):
        for tier in ("Weak", "Below Avg", "Avg", "Above Avg", "Elite", "missing"):
            got = [r for r in gob if r["line"] == line and r["own"] == tier]
            if not got:
                continue
            h = sum(1 for r in got if r["hit"])
            _p(f"  {line} {tier}: {_rate(h, len(got))}")

    _p("\n=== OWN_DEF attach ===")
    # Stability: a team-level attach is constant on `team` and moves with the keeper.
    # An opponent attach is constant on `opp` and moves when the opponent changes.
    by_team_own: dict[str, set[str]] = defaultdict(set)
    by_opp_own: dict[str, set[str]] = defaultdict(set)
    by_team_def: dict[str, set[str]] = defaultdict(set)
    by_opp_def: dict[str, set[str]] = defaultdict(set)
    same = 0
    both = 0
    for r in gob:
        if not r["team"] or not r["opp"]:
            continue
        if r["own"] != "missing":
            by_team_own[r["team"]].add(r["own"])
            by_opp_own[r["opp"]].add(r["own"])
        if r["def_tier"] != "missing":
            by_team_def[r["team"]].add(r["def_tier"])
            by_opp_def[r["opp"]].add(r["def_tier"])
        if r["own"] != "missing" and r["def_tier"] != "missing":
            both += 1
            if r["own"] == r["def_tier"]:
                same += 1

    def _stable(groups: dict[str, set[str]]) -> str:
        multi = {k: v for k, v in groups.items() if len(v) >= 1 and k}
        if not multi:
            return "n=0"
        one = sum(1 for v in multi.values() if len(v) == 1)
        return f"{one}/{len(multi)} clubs keep a single tier ({one / len(multi):.0%})"

    _p(f"OWN_DEF constant on the keeper's team: {_stable(by_team_own)}")
    _p(f"OWN_DEF constant on the opponent: {_stable(by_opp_own)}")
    _p(f"DEF_TIER constant on the keeper's team: {_stable(by_team_def)}")
    _p(f"DEF_TIER constant on the opponent: {_stable(by_opp_def)}")
    _p(f"OWN_DEF equals DEF_TIER on {same}/{both} keeper-days with both filled")

    # Same keeper-team across two opponents should keep OWN_DEF if attach is own.
    switched_opp = 0
    own_held = 0
    for team, tiers in by_team_own.items():
        opps = {r["opp"] for r in gob if r["team"] == team and r["opp"]}
        if len(opps) < 2 or not tiers:
            continue
        switched_opp += 1
        if len(tiers) == 1:
            own_held += 1
    _p(
        f"keeper clubs with 2+ opponents: {switched_opp}; "
        f"OWN_DEF stayed one tier: {own_held}"
    )

    ga_by_own: dict[str, list[float]] = defaultdict(list)
    for r in gob:
        try:
            ga = float(r["ga"])
        except (TypeError, ValueError):
            continue
        ga_by_own[r["own"]].append(ga)
    _p("mean OWN_GA_PG by OWN_DEF tier (goals allowed, own-team column)")
    for tier, vals in sorted(ga_by_own.items()):
        _p(f"  {tier}: n={len(vals)} mean={sum(vals)/len(vals):.2f}")


if __name__ == "__main__":
    main()
