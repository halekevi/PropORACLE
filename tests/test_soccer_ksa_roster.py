"""Saudi / Andean leagues on the PrizePicks SOCCER tab must stay in ESPN coverage."""
from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _assigned_str_list(path: Path, name: str) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign):
            targets = [t.id for t in node.targets if isinstance(t, ast.Name)]
            if name in targets and isinstance(node.value, ast.List):
                out = []
                for elt in node.value.elts:
                    if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                        out.append(elt.value)
                    elif isinstance(elt, ast.Tuple) and elt.elts:
                        first = elt.elts[0]
                        if isinstance(first, ast.Constant) and isinstance(first.value, str):
                            out.append(first.value)
                return out
    raise AssertionError(f"{name} not found in {path}")


def test_roster_cache_rebuilds_when_ksa_missing_from_fingerprint(tmp_path):
    import sys

    sys.path.insert(0, str(ROOT / "Sports" / "Soccer" / "scripts"))
    from step2_attach_picktypes_soccer import (  # noqa: E402
        _cached_roster_leagues,
        _save_roster_leagues,
    )

    cache = tmp_path / "soccer_roster_cache.csv"
    cache.write_text("player_norm,espn_athlete_id,team_norm\n", encoding="utf-8")
    assert _cached_roster_leagues(str(cache)) == set()
    _save_roster_leagues(str(cache), ["esp.1", "eng.1"])
    assert _cached_roster_leagues(str(cache)) == {"esp.1", "eng.1"}
    assert "ksa.1" not in _cached_roster_leagues(str(cache))
    text = (ROOT / "Sports" / "Soccer" / "scripts" / "step2_attach_picktypes_soccer.py").read_text(
        encoding="utf-8"
    )
    assert "PropOracle/1.0" in text
    assert "Chrome/122" not in text.split("LEAGUE_SLUGS_FOR_ROSTER")[0]
    slugs = _assigned_str_list(
        ROOT / "Sports" / "Soccer" / "scripts" / "step2_attach_picktypes_soccer.py",
        "LEAGUE_SLUGS_FOR_ROSTER",
    )
    for slug in ("ksa.1", "col.1", "ecu.1"):
        assert slug in slugs, f"{slug} missing from soccer step2 roster leagues"


def test_fetch_actuals_includes_ksa1_not_sau1():
    slugs = _assigned_str_list(ROOT / "scripts" / "fetch_actuals.py", "SOCCER_LEAGUES")
    assert "ksa.1" in slugs
    assert "sau.1" not in slugs
    assert "col.1" in slugs
    assert "ecu.1" in slugs


def test_boxscore_builders_include_ksa1():
    for rel in (
        Path("scripts") / "build_boxscore_ref.py",
        Path("Sports") / "NBA" / "scripts" / "build_boxscore_ref.py",
    ):
        slugs = _assigned_str_list(ROOT / rel, "SOCCER_LEAGUES")
        assert "ksa.1" in slugs, f"{rel} missing ksa.1"
        assert "col.1" in slugs, f"{rel} missing col.1"
        assert "ecu.1" in slugs, f"{rel} missing ecu.1"
