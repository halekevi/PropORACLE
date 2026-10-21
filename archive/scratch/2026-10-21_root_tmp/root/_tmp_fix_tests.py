"""Update tests for hitter_ks fade, PRA Off+S/A, MAIN demote."""
from __future__ import annotations

from pathlib import Path

REPO = Path(r"H:\PropORACLE")

# test_build_goblin70_tickets.py — PRA needs Off+S/A; B-tier fails; hitter_ks blocked
p = REPO / "tests" / "test_build_goblin70_tickets.py"
text = p.read_text(encoding="utf-8")
old = '''    # PRA cover floor is 3.7 and Off (high usage or HIGH minutes) is required.
    assert not goblin_70_eligible(
        _gob(prop="Pts+Rebs+Asts", cover=4.0, l5_over=5, l10_over=8)
    )
    assert goblin_70_eligible(
        _gob(
            prop="Pts+Rebs+Asts",
            cover=4.0,
            l5_over=5,
            l10_over=8,
            usage_tier="high",
        )
    )
    assert goblin_70_eligible(
        _gob(
            prop="Pts+Rebs+Asts",
            cover=4.0,
            l5_over=5,
            l10_over=8,
            minutes_tier="HIGH",
        )
    )
    assert not goblin_70_eligible(
        _gob(
            prop="Pts+Rebs+Asts",
            cover=4.0,
            l5_over=5,
            l10_over=8,
            usage_tier="medium",
            minutes_tier="MEDIUM",
        )
    )
'''
new = '''    # PRA cover floor is 3.7; Off AND prop_tier S/A are required (_gob stamps A).
    assert not goblin_70_eligible(
        _gob(prop="Pts+Rebs+Asts", cover=4.0, l5_over=5, l10_over=8)
    )
    assert goblin_70_eligible(
        _gob(
            prop="Pts+Rebs+Asts",
            cover=4.0,
            l5_over=5,
            l10_over=8,
            usage_tier="high",
        )
    )
    assert goblin_70_eligible(
        _gob(
            prop="Pts+Rebs+Asts",
            cover=4.0,
            l5_over=5,
            l10_over=8,
            minutes_tier="HIGH",
        )
    )
    # Catalog PRA is B — Off alone is not enough when tier is stamped B.
    assert not goblin_70_eligible(
        _gob(
            prop="Pts+Rebs+Asts",
            cover=4.0,
            l5_over=5,
            l10_over=8,
            usage_tier="high",
            prop_tier="B",
        )
    )
    assert not goblin_70_eligible(
        _gob(
            prop="Pts+Rebs+Asts",
            cover=4.0,
            l5_over=5,
            l10_over=8,
            usage_tier="medium",
            minutes_tier="MEDIUM",
        )
    )
'''
if old not in text:
    raise SystemExit("PRA block not found in test_build_goblin70_tickets")
text = text.replace(old, new, 1)

old_hk = '''    hitter_k["def"] = "Elite"
    assert goblin_70_eligible(hitter_k)
'''
new_hk = '''    hitter_k["def"] = "Elite"
    # Keep-gate may pass; Goblin-70 tickets hard-fade hitter_ks.
    assert not goblin_70_eligible(hitter_k)
'''
if old_hk not in text:
    raise SystemExit("hitter_k assert not found")
text = text.replace(old_hk, new_hk, 1)
p.write_text(text, encoding="utf-8")
print("patched test_build_goblin70_tickets")

# test_mlb_keep_gates.py
p = REPO / "tests" / "test_mlb_keep_gates.py"
text = p.read_text(encoding="utf-8")
old = '''    assert mlb_goblin_keep_eligible(hk)
    assert goblin_70_eligible(hk)
'''
new = '''    assert mlb_goblin_keep_eligible(hk)
    assert not goblin_70_eligible(hk)  # ticket hard-fade; list keep-gate still on
'''
if old not in text:
    raise SystemExit("mlb hk assert not found")
text = text.replace(old, new, 1)
p.write_text(text, encoding="utf-8")
print("patched test_mlb_keep_gates")

# test_cell_hr_priority.py
p = REPO / "tests" / "test_cell_hr_priority.py"
text = p.read_text(encoding="utf-8")
old = '''def test_jul22_priority_and_weak_sets_load():
    priority, weak = load_jul22_cell_sets()
    assert ("MLB", "pitcherstrikeouts", "Goblin", "OVER") in priority
    assert ("WNBA", "points", "Goblin", "OVER") in priority
    assert ("SOCCER", "shots", "Goblin", "OVER") in weak
    assert ("TENNIS", "aces", "Goblin", "OVER") in weak
    assert ("TENNIS", "doublefaults", "Goblin", "OVER") in weak
'''
new = '''def test_jul22_priority_and_weak_sets_load():
    priority, weak = load_jul22_cell_sets()
    assert ("MLB", "pitcherstrikeouts", "Goblin", "OVER") in priority
    assert ("WNBA", "points", "Goblin", "OVER") in priority
    assert ("SOCCER", "shots", "Goblin", "OVER") in weak
    assert ("TENNIS", "aces", "Goblin", "OVER") in weak
    assert ("TENNIS", "doublefaults", "Goblin", "OVER") in weak
    # Bundle demote: PRA / hitter_ks / games_won Goblin OVER
    assert ("WNBA", "ptsrebsasts", "Goblin", "OVER") in weak
    assert ("WNBA", "ptsrebsasts", "Goblin", "OVER") not in priority
    assert ("MLB", "hitterstrikeouts", "Goblin", "OVER") in weak
    assert ("TENNIS", "totalgameswon", "Goblin", "OVER") in weak
    assert ("TENNIS", "totalgameswon", "Goblin", "OVER") not in priority
'''
if old not in text:
    raise SystemExit("cell_hr load test not found")
text = text.replace(old, new, 1)

old = '''    assert float(boost.iloc[4]) > 0  # Tennis totals priority
'''
new = '''    assert float(boost.iloc[4]) <= -0.20  # Tennis games_won Goblin demoted
'''
if old not in text:
    raise SystemExit("boost assert not found")
text = text.replace(old, new, 1)

# Add explicit PRA/hitter_ks penalty test at end if missing
if "test_bundle_weak_penalty" not in text:
    text += '''

def test_bundle_weak_penalty():
    df = pd.DataFrame(
        [
            {
                "sport": "WNBA",
                "prop_type": "Pts+Rebs+Asts",
                "pick_type": "Goblin",
                "direction": "OVER",
            },
            {
                "sport": "MLB",
                "prop_type": "Hitter Strikeouts",
                "pick_type": "Goblin",
                "direction": "OVER",
            },
        ]
    )
    boost = cell_hr_priority_boost_series(df)
    assert float(boost.iloc[0]) <= -0.20
    assert float(boost.iloc[1]) <= -0.20
'''
p.write_text(text, encoding="utf-8")
print("patched test_cell_hr_priority")
