"""Count distinct players/teams across everything played / on-card 2026-09-19."""
from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(r"H:\PropORACLE")
MAIN = Path(r"H:\PropORACLE_main_cp")


def norm(n: str) -> str:
    return re.sub(r"\s+", " ", (n or "").strip().lower())


def walk_tickets(path: Path) -> list[dict]:
    if not path.exists():
        return []
    d = json.loads(path.read_text(encoding="utf-8"))
    out = []
    for g in d.get("groups") or []:
        gname = g.get("group_name") or ""
        for t in g.get("tickets") or []:
            legs = t.get("legs") or []
            out.append(
                {
                    "source": path.name,
                    "group": gname,
                    "ticket_id": t.get("ticket_id") or t.get("web_group_name"),
                    "track": t.get("ticket_track"),
                    "legs": legs,
                }
            )
    return out


# Manual / CDP-submitted slips reconstructed from today's session scripts
# (not all landed on tickets_latest). User-named: Power 3 #1, Flex 4, 4-Pick Power Play.
MANUAL = [
    {
        "ticket": "Power 3 #1",
        "legs": [
            ("MLB", "Cam Schlittler", None),
            ("WNBA", "Arike Ogunbowale", None),
            ("CFB", "Devon Dampier", "UTAH"),
        ],
    },
    {
        "ticket": "Flex 4 (Jones/Overton/Pendergrass/Hiedeman)",
        "legs": [
            ("CFB", "Tavorus Jones", None),
            ("CFB", "Jeffrey Overton Jr.", "VT"),
            ("CFB", "Pendergrass", None),  # first name TBD from scripts
            ("WNBA", "Natasha Hiedeman", None),
        ],
    },
    {
        "ticket": "4-Pick Power Play (Dampier+Overton stack)",
        "legs": [
            ("CFB", "Devon Dampier", "UTAH"),
            ("CFB", "Jeffrey Overton Jr.", "VT"),
            # other two unknown without slip — leave as placeholders if found
        ],
    },
]


def load_manual_from_scripts() -> list[dict]:
    """Pull concrete LEGS lists from today's submit/build scripts."""
    tickets = []
    # Flex 4 rebuild submit
    tickets.append(
        {
            "ticket": "Flex4 submit Jones/Overton/Pendergrass/Hiedeman",
            "legs": [
                {"sport": "CFB", "player": "Tavorus Jones", "team": ""},
                {"sport": "CFB", "player": "Jeffrey Overton Jr.", "team": "VT"},
                {"sport": "CFB", "player": "Pendergrass", "team": ""},
                {"sport": "WNBA", "player": "Natasha Hiedeman", "team": ""},
            ],
        }
    )
    tickets.append(
        {
            "ticket": "Power3 #1 Schlittler/Arike/Dampier",
            "legs": [
                {"sport": "MLB", "player": "Cam Schlittler", "team": ""},
                {"sport": "WNBA", "player": "Arike Ogunbowale", "team": ""},
                {"sport": "CFB", "player": "Devon Dampier", "team": "UTAH"},
            ],
        }
    )
    # Goblin diamond eight from check_played (may span multiple tickets)
    eight = [
        ("DeSean Bishop", "Rush"),
        ("Marcellous Hawkins Jr.", "Rush"),
        ("Nate Frazier", "Rush"),
        ("Wayshawn Parker", "Rush"),
        ("Waymond Jordan", "Rush"),
        ("Devon Dampier", "Pass"),
        ("Jayden Maiava", "Pass"),
        ("Josiah Freeman", "Rec"),  # or Gavin — both in file
    ]
    tickets.append(
        {
            "ticket": "Goblin Diamonds cluster (played lines audit)",
            "legs": [{"sport": "CFB", "player": p, "team": ""} for p, _ in eight],
        }
    )
    # Flex6 intended
    tickets.append(
        {
            "ticket": "Flex6 intended Schlittler/Gasser/Dampier/Bishop/Malen/Arike",
            "legs": [
                {"sport": "MLB", "player": "Cam Schlittler", "team": ""},
                {"sport": "MLB", "player": "Gasser", "team": ""},
                {"sport": "CFB", "player": "Devon Dampier", "team": "UTAH"},
                {"sport": "CFB", "player": "DeSean Bishop", "team": ""},
                {"sport": "SOCCER", "player": "Malen", "team": ""},
                {"sport": "WNBA", "player": "Arike Ogunbowale", "team": ""},
            ],
        }
    )
    return tickets


def analyze(tickets: list[dict], label: str) -> None:
    player_tix: dict[str, list[str]] = defaultdict(list)
    team_tix: dict[str, list[str]] = defaultdict(list)
    players = set()
    teams = set()
    sports = Counter()
    cfb_players = set()
    cfb_teams = set()

    print(f"\n======== {label} ({len(tickets)} tickets) ========")
    for t in tickets:
        tid = t.get("ticket") or t.get("ticket_id") or t.get("group")
        print(f"\n-- {tid}")
        for leg in t.get("legs") or []:
            if not isinstance(leg, dict):
                continue
            pl = (leg.get("player") or "").strip()
            tm = (leg.get("team") or "").strip()
            sp = (leg.get("sport") or "").strip().upper()
            prop = leg.get("prop_type") or leg.get("prop") or ""
            if not pl:
                continue
            print(f"   {sp:7} {pl:28} {prop} {leg.get('line','')}")
            players.add(norm(pl))
            player_tix[norm(pl)].append(str(tid))
            sports[sp or "?"] += 1
            if sp in ("CFB", "CFB1H"):
                cfb_players.add(norm(pl))
            if tm:
                teams.add(tm.upper())
                team_tix[tm.upper()].append(str(tid))
                if sp in ("CFB", "CFB1H"):
                    cfb_teams.add(tm.upper())

    print(f"\nDISTINCT players: {len(players)}")
    print(f"DISTINCT teams:   {len(teams)}")
    print(f"CFB players:      {len(cfb_players)}")
    print(f"CFB teams:        {len(cfb_teams)}")
    print(f"Legs by sport:    {dict(sports)}")

    multi = [(len(v), k, v) for k, v in player_tix.items() if len(v) >= 2]
    print("\nMULTI-TICKET PLAYERS:")
    for n, k, v in sorted(multi, reverse=True):
        print(f"  {n}x  {k:30} -> {v}")

    multi_t = [(len(v), k, v) for k, v in team_tix.items() if len(v) >= 2]
    print("\nMULTI-TICKET TEAMS:")
    for n, k, v in sorted(multi_t, reverse=True)[:20]:
        print(f"  {n}x  {k:8} -> {v}")


def main() -> None:
    # Published /tickets card
    published = walk_tickets(MAIN / "ui_runner" / "runtime" / "tickets_latest.json")
    analyze(published, "Published tickets_latest (main_cp 9/19)")

    # Also goblin70 report
    g70 = walk_tickets(MAIN / "data" / "reports" / "goblin70_tickets_2026-09-19.json")
    analyze(g70, "Goblin70 report 9/19")

    manual = load_manual_from_scripts()
    analyze(manual, "Manual/CDP session slips (reconstructed)")

    # CFB names on published card
    cfb = []
    for t in published:
        for leg in t.get("legs") or []:
            if str(leg.get("sport") or "").upper() in ("CFB", "CFB1H"):
                cfb.append(leg)
    print(f"\nPublished CFB legs total: {len(cfb)}")
    pc = Counter((leg.get("player") or "") for leg in cfb)
    print("Published CFB player frequency:")
    for pl, n in pc.most_common(30):
        print(f"  {n}  {pl}")


if __name__ == "__main__":
    main()
