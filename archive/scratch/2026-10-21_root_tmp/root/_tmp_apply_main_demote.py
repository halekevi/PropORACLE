"""Patch MAIN soft demote: Jul-22 JSON + cell_hr_priority.py."""
from __future__ import annotations

import json
from pathlib import Path

REPO = Path(r"H:\PropORACLE")

# --- cell_hr_priority.py ---
p = REPO / "utils" / "cell_hr_priority.py"
text = p.read_text(encoding="utf-8")

old_doc = '''Soft-downranks known weak lanes (Soccer OVER Shots Goblin, Tennis Ace/DF Goblin, …).
'''
new_doc = '''Soft-downranks known weak lanes (Soccer OVER Shots Goblin, Tennis Ace/DF Goblin,
WNBA PRA Goblin, MLB hitter_ks Goblin, Tennis games_won Goblin, …).
'''
if old_doc in text:
    text = text.replace(old_doc, new_doc, 1)

old_const = '''WEAK_PENALTY = float(os.getenv("PROPORACLE_CELL_HR_WEAK_PENALTY", "0.14"))
# Tennis Ace/DF Goblin: soft −0.14 was not enough for slate ranking — hard downrank.
TENNIS_SERVE_WEAK_PENALTY = float(
    os.getenv("PROPORACLE_CELL_HR_TENNIS_SERVE_WEAK_PENALTY", "0.35")
)
ROLLING_HR_FLOOR = float(os.getenv("PROPORACLE_CELL_HR_ROLLING_FLOOR", "0.60"))
ROLLING_HR_MIN_N = int(os.getenv("PROPORACLE_CELL_HR_ROLLING_MIN_N", "10"))

_TENNIS_SERVE_WEAK_PROPS = frozenset({"aces", "ace", "doublefaults", "doublefault"})


def _weak_penalty_for_key(key: tuple[str, str, prop, str]) -> float:
'''

# Fix - I had a typo in old_const. Use exact text.
old_const = '''WEAK_PENALTY = float(os.getenv("PROPORACLE_CELL_HR_WEAK_PENALTY", "0.14"))
# Tennis Ace/DF Goblin: soft −0.14 was not enough for slate ranking — hard downrank.
TENNIS_SERVE_WEAK_PENALTY = float(
    os.getenv("PROPORACLE_CELL_HR_TENNIS_SERVE_WEAK_PENALTY", "0.35")
)
ROLLING_HR_FLOOR = float(os.getenv("PROPORACLE_CELL_HR_ROLLING_FLOOR", "0.60"))
ROLLING_HR_MIN_N = int(os.getenv("PROPORACLE_CELL_HR_ROLLING_MIN_N", "10"))

_TENNIS_SERVE_WEAK_PROPS = frozenset({"aces", "ace", "doublefaults", "doublefault"})


def _weak_penalty_for_key(key: tuple[str, str, str, str]) -> float:
    sport, prop_norm, pick, direction = key
    if (
        sport == "TENNIS"
        and pick == "Goblin"
        and direction == "OVER"
        and prop_norm in _TENNIS_SERVE_WEAK_PROPS
    ):
        return float(TENNIS_SERVE_WEAK_PENALTY)
    return float(WEAK_PENALTY)
'''

new_const = '''WEAK_PENALTY = float(os.getenv("PROPORACLE_CELL_HR_WEAK_PENALTY", "0.14"))
# Bundle fade (PRA / hitter_ks / games_won): −0.20 on MAIN rank_score.
BUNDLE_WEAK_PENALTY = float(os.getenv("PROPORACLE_CELL_HR_BUNDLE_WEAK_PENALTY", "0.20"))
# Tennis Ace/DF Goblin: soft −0.14 was not enough for slate ranking — hard downrank.
TENNIS_SERVE_WEAK_PENALTY = float(
    os.getenv("PROPORACLE_CELL_HR_TENNIS_SERVE_WEAK_PENALTY", "0.35")
)
ROLLING_HR_FLOOR = float(os.getenv("PROPORACLE_CELL_HR_ROLLING_FLOOR", "0.60"))
ROLLING_HR_MIN_N = int(os.getenv("PROPORACLE_CELL_HR_ROLLING_MIN_N", "10"))

_TENNIS_SERVE_WEAK_PROPS = frozenset({"aces", "ace", "doublefaults", "doublefault"})
# Goblin OVER lanes demoted from Jul-22 priority after strict-gate backtest.
_BUNDLE_WEAK_PROP_NORMS = frozenset(
    {
        "pra",
        "ptsrebsasts",
        "pointsreboundsassists",
        "hitterks",
        "hitterstrikeouts",
        "gameswon",
        "totalgameswon",
    }
)


def _is_bundle_weak(key: tuple[str, str, str, str]) -> bool:
    sport, prop_norm, pick, direction = key
    if pick != "Goblin" or direction != "OVER":
        return False
    if prop_norm not in _BUNDLE_WEAK_PROP_NORMS:
        return False
    if sport == "WNBA" and prop_norm in {"pra", "ptsrebsasts", "pointsreboundsassists"}:
        return True
    if sport == "MLB" and prop_norm in {"hitterks", "hitterstrikeouts"}:
        return True
    if sport == "TENNIS" and prop_norm in {"gameswon", "totalgameswon"}:
        return True
    return False


def _weak_penalty_for_key(key: tuple[str, str, str, str]) -> float:
    sport, prop_norm, pick, direction = key
    if (
        sport == "TENNIS"
        and pick == "Goblin"
        and direction == "OVER"
        and prop_norm in _TENNIS_SERVE_WEAK_PROPS
    ):
        return float(TENNIS_SERVE_WEAK_PENALTY)
    if _is_bundle_weak(key):
        return float(BUNDLE_WEAK_PENALTY)
    return float(WEAK_PENALTY)
'''

if old_const not in text:
    if "BUNDLE_WEAK_PENALTY" in text:
        print("cell_hr_priority constants already patched")
    else:
        raise SystemExit("cell_hr_priority const block not found")
else:
    text = text.replace(old_const, new_const, 1)
    print("cell_hr_priority constants patched")

# Update load_jul22 fallbacks: remove games_won Goblin from priority; add bundle to weak
old_fb = '''    if not weak:
        weak.update(
            {
                ("SOCCER", "shots", "Goblin", "OVER"),
                ("SOCCER", "goalassist", "Goblin", "OVER"),
                ("TENNIS", "aces", "Goblin", "OVER"),
                ("TENNIS", "doublefaults", "Goblin", "OVER"),
            }
        )
    if not priority:
        priority.update(
            {
                ("TENNIS", "totalgames", "Goblin", "OVER"),
                ("TENNIS", "totalgameswon", "Goblin", "OVER"),
                ("TENNIS", "totalgameswon", "Standard", "OVER"),
                ("TENNIS", "totalgames", "Standard", "OVER"),
            }
        )
    return frozenset(priority), frozenset(weak)
'''
new_fb = '''    if not weak:
        weak.update(
            {
                ("SOCCER", "shots", "Goblin", "OVER"),
                ("SOCCER", "goalassist", "Goblin", "OVER"),
                ("TENNIS", "aces", "Goblin", "OVER"),
                ("TENNIS", "doublefaults", "Goblin", "OVER"),
                ("WNBA", "ptsrebsasts", "Goblin", "OVER"),
                ("MLB", "hitterstrikeouts", "Goblin", "OVER"),
                ("TENNIS", "totalgameswon", "Goblin", "OVER"),
            }
        )
    if not priority:
        priority.update(
            {
                ("TENNIS", "totalgames", "Goblin", "OVER"),
                ("TENNIS", "totalgameswon", "Standard", "OVER"),
                ("TENNIS", "totalgames", "Standard", "OVER"),
            }
        )
    # Always demote bundle lanes even if Jul-22 JSON still lists them as priority.
    for key in (
        ("WNBA", "ptsrebsasts", "Goblin", "OVER"),
        ("WNBA", "pra", "Goblin", "OVER"),
        ("MLB", "hitterstrikeouts", "Goblin", "OVER"),
        ("MLB", "hitterks", "Goblin", "OVER"),
        ("TENNIS", "totalgameswon", "Goblin", "OVER"),
        ("TENNIS", "gameswon", "Goblin", "OVER"),
    ):
        priority.discard(key)
        weak.add(key)
    return frozenset(priority), frozenset(weak)
'''
if old_fb not in text:
    if "Always demote bundle lanes" in text:
        print("fallbacks already patched")
    else:
        raise SystemExit("fallback block not found")
else:
    text = text.replace(old_fb, new_fb, 1)
    print("fallbacks patched")

# Docstring on boost series
text = text.replace(
    "    -WEAK_PENALTY when weak-cell match (overrides priority for that leg)\n",
    "    -WEAK_PENALTY / -BUNDLE_WEAK_PENALTY (-0.20) when weak-cell match\n"
    "    (overrides priority for that leg)\n",
    1,
)

p.write_text(text, encoding="utf-8")
print("wrote", p)

# --- Jul-22 JSON ---
jp = REPO / "data" / "reports" / "jul22_priority_hr_cells.json"
data = json.loads(jp.read_text(encoding="utf-8"))

def is_bundle_priority(row: dict) -> bool:
    sport = str(row.get("sport_key") or row.get("sport") or "").upper()
    prop = str(row.get("prop") or "")
    pick = str(row.get("pick_type") or "")
    direction = str(row.get("direction") or "").upper()
    if pick != "Goblin" or direction != "OVER":
        return False
    if sport == "WNBA" and prop == "Pts+Rebs+Asts":
        return True
    if sport == "MLB" and prop == "Hitter Strikeouts":
        return True
    if sport == "TENNIS" and prop == "Total Games Won":
        return True
    return False

pri = data.get("priority_cells") or []
weak = data.get("weak_cells") or []
moved = [r for r in pri if is_bundle_priority(r)]
pri2 = [r for r in pri if not is_bundle_priority(r)]
# Avoid dupes in weak
weak_keys = {(r.get("sport_key"), r.get("prop"), r.get("pick_type"), r.get("direction")) for r in weak}
for r in moved:
    r = dict(r)
    r["note"] = (
        str(r.get("note") or "")
        + " | demoted 2026-09-15: strict-gate bundle fade (−0.20 MAIN rank_score)"
    ).strip(" |")
    key = (r.get("sport_key"), r.get("prop"), r.get("pick_type"), r.get("direction"))
    if key not in weak_keys:
        weak.append(r)
        weak_keys.add(key)

data["priority_cells"] = pri2
data["weak_cells"] = weak
crit = data.get("criteria") or {}
crit["weak"] = (
    "seeded weak lanes: Soccer OVER Shots/Goal+Assist Goblin; Tennis OVER Aces/"
    "Double Faults Goblin (hard ticket ban + -0.35 rank); WNBA PRA Goblin / MLB "
    "hitter_ks Goblin / Tennis games_won Goblin (−0.20 MAIN rank_score, 2026-09-15)"
)
crit["tennis_prefer"] = (
    "Total Games Goblin+Standard OVER; Total Games Won Standard OVER "
    "(Goblin games_won demoted)"
)
data["criteria"] = crit
jp.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
print(f"jul22: moved {len(moved)} cells to weak; priority now {len(pri2)}")
for r in moved:
    print(" ", r.get("sport"), r.get("prop"), r.get("pick_type"), r.get("hr"))
