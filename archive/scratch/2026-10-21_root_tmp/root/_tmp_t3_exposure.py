from utils.manual_slip_exposure import check_slip_exposure

def L(player, prop, line):
    return {
        "sport": "MLB",
        "player": player,
        "prop_type": prop,
        "direction": "OVER",
        "line": line,
        "pick_type": "goblin",
    }

cands = {
    "skenes_rocker_espino": (
        "Paul Skenes",
        [
            L("Paul Skenes", "Walks Allowed", 0.5),
            L("Kumar Rocker", "Walks Allowed", 0.5),
            L("Daniel Espino", "Hits Allowed", 0.5),
        ],
    ),
    "skenes_rocker_thornton": (
        "Paul Skenes",
        [
            L("Paul Skenes", "Walks Allowed", 0.5),
            L("Kumar Rocker", "Walks Allowed", 0.5),
            L("Zac Thornton", "Earned Runs Allowed", 0.5),
        ],
    ),
    "skenes_singer_espino": (
        "Paul Skenes",
        [
            L("Paul Skenes", "Walks Allowed", 0.5),
            L("Brady Singer", "Walks Allowed", 0.5),
            L("Daniel Espino", "Hits Allowed", 0.5),
        ],
    ),
    "rocker_espino_dobnak": (
        "Kumar Rocker",
        [
            L("Kumar Rocker", "Walks Allowed", 0.5),
            L("Daniel Espino", "Hits Allowed", 0.5),
            L("Randy Dobnak", "Walks Allowed", 0.5),
        ],
    ),
    "schlittler_glasnow_painter": (
        "Cam Schlittler",
        [
            L("Cam Schlittler", "Pitcher Strikeouts", 3.5),
            L("Tyler Glasnow", "Pitcher Strikeouts", 4.5),
            L("Andrew Painter", "Pitcher Strikeouts", 2.5),
        ],
    ),
}
for name, (anchor, legs) in cands.items():
    ok, reason = check_slip_exposure(
        legs, slate_date="2026-09-24", explicit_anchor_player=anchor
    )
    print(f"{name}: {'OK' if ok else 'BLOCKED'} {reason} anchor={anchor}")
