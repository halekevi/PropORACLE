from utils.manual_slip_exposure import check_slip_exposure

t1 = [
    {"player": "Peter Lambert", "prop_type": "Hits Allowed", "direction": "OVER", "pick_type": "Goblin", "sport": "MLB", "line": 3.5},
    {"player": "Jackie Young", "prop_type": "Assists", "direction": "OVER", "pick_type": "Goblin", "sport": "WNBA", "line": 3.5},
    {"player": "Tyler Mahle", "prop_type": "Pitching Outs", "direction": "OVER", "pick_type": "Goblin", "sport": "MLB", "line": 14.5},
]
t2 = [
    {"player": "A'ja Wilson", "prop_type": "Rebs+Asts", "direction": "OVER", "pick_type": "Goblin", "sport": "WNBA", "line": 9.5},
    {"player": "Cam Schlittler", "prop_type": "Pitcher Strikeouts", "direction": "OVER", "pick_type": "Goblin", "sport": "MLB", "line": 3.5},
    {"player": "Tyler Glasnow", "prop_type": "Pitcher Strikeouts", "direction": "OVER", "pick_type": "Goblin", "sport": "MLB", "line": 4.5},
]
t3 = [
    {"player": "Kyle Leahy", "prop_type": "Hits Allowed", "direction": "OVER", "pick_type": "Goblin", "sport": "MLB", "line": 1.5},
    {"player": "Andrew Painter", "prop_type": "Pitcher Strikeouts", "direction": "OVER", "pick_type": "Goblin", "sport": "MLB", "line": 2.5},
    {"player": "Tyler Phillips", "prop_type": "Pitcher Strikeouts", "direction": "OVER", "pick_type": "Goblin", "sport": "MLB", "line": 2.5},
]
print("T1", check_slip_exposure(t1, slate_date="2026-09-24", explicit_anchor_player="Peter Lambert"))
print("T2", check_slip_exposure(t2, slate_date="2026-09-24", explicit_anchor_player="A'ja Wilson"))
print("T3", check_slip_exposure(t3, slate_date="2026-09-24", explicit_anchor_player="Kyle Leahy"))
