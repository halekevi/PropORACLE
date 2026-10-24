"""Assemble playable tickets from today's S+A Gold+ Goblin pool."""
from __future__ import annotations

import json
from pathlib import Path

HOT = Path(r"H:\PropORACLE\data\reports\best_props_sa_2026-08-26.json")
OUT = Path(r"H:\PropORACLE\data\reports\sa_tickets_2026-08-26.json")

# N-correct / To Win (ignore 1st place). Medians from predicted_payout_tables_latest.
PAY = {
    3: {"power": 2.9, "flex": 2.125, "flex_partial": "2/3 = 0.5x"},
    4: {"power": 4.5, "flex": 3.0, "flex_partial": "3/4 = 0.75x"},
    5: {"power": 6.5, "flex": 4.5, "flex_partial": "4/5 = 0.75x"},
    6: {"power": 9.5, "flex": 5.0, "flex_partial": "5/6 = 0.5x"},
}

# One line per player, already the play we want on the ticket.
LEGS = {
    "malonga_pra": {
        "player": "Dominique Malonga",
        "prop": "Pts+Rebs+Asts",
        "line": 24.5,
        "tag": "A/Gold",
        "l5": "4/1",
        "cover": 4.9,
        "d": "Weak",
        "matchup": "SEA vs TOR",
        "sport": "WNBA",
    },
    "melbourne_3s": {
        "player": "Jade Melbourne",
        "prop": "3-PT Made",
        "line": 0.5,
        "tag": "A/Gold",
        "l5": "5/0",
        "cover": 2.1,
        "d": "Weak",
        "matchup": "SEA vs TOR",
        "sport": "WNBA",
    },
    "melbourne_pts": {
        "player": "Jade Melbourne",
        "prop": "Points",
        "line": 9.5,
        "tag": "A/Gold",
        "l5": "4/1",
        "cover": 4.5,
        "d": "Weak",
        "matchup": "SEA vs TOR",
        "sport": "WNBA",
    },
    "magbegor_reb": {
        "player": "Ezi Magbegor",
        "prop": "Rebounds",
        "line": 3.5,
        "tag": "A/Gold",
        "l5": "4/1",
        "cover": 1.1,
        "d": "Weak",
        "matchup": "SEA vs TOR",
        "sport": "WNBA",
    },
    "gore_k": {
        "player": "MacKenzie Gore",
        "prop": "Pitcher Strikeouts",
        "line": 3.5,
        "tag": "S/Platinum",
        "l5": "5/0",
        "cover": 2.9,
        "d": "Below Avg",
        "matchup": "TEX vs CWS",
        "sport": "MLB",
    },
    "woo_ha": {
        "player": "Bryan Woo",
        "prop": "Hits Allowed",
        "line": 2.5,
        "tag": "S/Platinum",
        "l5": "5/0",
        "cover": 2.6,
        "d": "Weak",
        "matchup": "SEA vs PHI",
        "sport": "MLB",
    },
    "ginn_k": {
        "player": "J.T. Ginn",
        "prop": "Pitcher Strikeouts",
        "line": 2.5,
        "tag": "S/Platinum",
        "l5": "4/1",
        "cover": 2.5,
        "d": "Below Avg",
        "matchup": "ATH vs MIN",
        "sport": "MLB",
    },
    "prielipp_k": {
        "player": "Connor Prielipp",
        "prop": "Pitcher Strikeouts",
        "line": 3.5,
        "tag": "S/Platinum",
        "l5": "5/0",
        "cover": 2.4,
        "d": "Weak",
        "matchup": "MIN vs ATH",
        "sport": "MLB",
    },
    "cantillo_k": {
        "player": "Joey Cantillo",
        "prop": "Pitcher Strikeouts",
        "line": 3.5,
        "tag": "S/Platinum",
        "l5": "4/1",
        "cover": 2.4,
        "d": "Below Avg",
        "matchup": "CLE vs LAA",
        "sport": "MLB",
    },
    "boyd_k": {
        "player": "Matthew Boyd",
        "prop": "Pitcher Strikeouts",
        "line": 1.5,
        "tag": "S/Platinum",
        "l5": "5/0",
        "cover": 2.1,
        "d": "Below Avg",
        "matchup": "CHC vs AZ",
        "sport": "MLB",
    },
    "bradish_k": {
        "player": "Kyle Bradish",
        "prop": "Pitcher Strikeouts",
        "line": 2.5,
        "tag": "S/Platinum",
        "l5": "5/0",
        "cover": 1.9,
        "d": "Below Avg",
        "matchup": "BAL vs STL",
        "sport": "MLB",
    },
    "mcgreevy_ha": {
        "player": "Michael McGreevy",
        "prop": "Hits Allowed",
        "line": 3.5,
        "tag": "S/Platinum",
        "l5": "4/1",
        "cover": 1.9,
        "d": "Weak",
        "matchup": "STL vs BAL",
        "sport": "MLB",
    },
    "fearnley_tg": {
        "player": "Jacob Fearnley",
        "prop": "Total Games",
        "line": 19.5,
        "tag": "A/Gold",
        "l5": "5/0",
        "cover": 7.1,
        "d": "Weak",
        "matchup": "Fearnley vs Pavlovic",
        "sport": "Tennis",
    },
    "fery_tg": {
        "player": "Arthur Fery",
        "prop": "Total Games",
        "line": 18.5,
        "tag": "A/Gold",
        "l5": "5/0",
        "cover": 7.0,
        "d": "Weak",
        "matchup": "Fery vs Rottgering",
        "sport": "Tennis",
    },
    "stoiana_tg": {
        "player": "Mary Stoiana",
        "prop": "Total Games",
        "line": 18.5,
        "tag": "A/Gold",
        "l5": "5/0",
        "cover": 6.1,
        "d": "Weak",
        "matchup": "Stoiana vs Swan",
        "sport": "Tennis",
    },
    "zheng_tg": {
        "player": "Qinwen Zheng",
        "prop": "Total Games",
        "line": 18.5,
        "tag": "A/Gold",
        "l5": "4/1",
        "cover": 5.7,
        "d": "Weak",
        "matchup": "Zheng vs Burel",
        "sport": "Tennis",
    },
    "tsitsipas_tg": {
        "player": "Stefanos Tsitsipas",
        "prop": "Total Games",
        "line": 19.5,
        "tag": "A/Gold",
        "l5": "5/0",
        "cover": 5.3,
        "d": "Below Avg",
        "matchup": "Tsitsipas vs Kovacevic",
        "sport": "Tennis",
    },
    "lee_gw": {
        "player": "Jordan Lee",
        "prop": "Total Games Won",
        "line": 9.5,
        "tag": "A/Gold",
        "l5": "5/0",
        "cover": 3.9,
        "d": "Weak",
        "matchup": "Lee vs Rincon",
        "sport": "Tennis",
    },
    "alonso_hr": {
        "player": "Pete Alonso",
        "prop": "Hits+Runs+RBIs",
        "line": 0.5,
        "tag": "A/Platinum*",
        "l5": "4/1",
        "cover": 3.2,
        "d": "Below Avg",
        "matchup": "BAL vs STL",
        "sport": "MLB",
    },
    "kwan_hr": {
        "player": "Steven Kwan",
        "prop": "Hits+Runs+RBIs",
        "line": 0.5,
        "tag": "A/Gold*",
        "l5": "4/1",
        "cover": 2.6,
        "d": "Below Avg",
        "matchup": "CLE vs LAA",
        "sport": "MLB",
    },
}

# Rules: unique player, no pitcher+hitter same MLB game, max 2 WNBA from SEA vs TOR.
TICKETS = [
    {
        "id": "T1",
        "name": "3-leg core",
        "keys": ["malonga_pra", "gore_k", "fery_tg"],
        "note": "One from each sport. Tightest Power that still clears 2x.",
    },
    {
        "id": "T2",
        "name": "4-leg SEA + Woo + Fearnley",
        "keys": ["malonga_pra", "melbourne_3s", "woo_ha", "fearnley_tg"],
        "note": "Two SEA Goblins (same game cap). Melbourne 3s is L5 5/0.",
    },
    {
        "id": "T3",
        "name": "4-leg pitcher stack",
        "keys": ["gore_k", "woo_ha", "prielipp_k", "cantillo_k"],
        "note": "Four S-tier pitcher Goblins, four different games. No Ginn (same ATH/MIN as Prielipp).",
    },
    {
        "id": "T4",
        "name": "4-leg tennis",
        "keys": ["fearnley_tg", "fery_tg", "stoiana_tg", "zheng_tg"],
        "note": "Four A/Gold total-games, four matches. No opponent overlap.",
    },
    {
        "id": "T5",
        "name": "5-leg mixed",
        "keys": ["malonga_pra", "melbourne_3s", "gore_k", "woo_ha", "fery_tg"],
        "note": "Main mixed book. Power ~6.5x.",
    },
    {
        "id": "T6",
        "name": "5-leg no-SEA-stack",
        "keys": ["malonga_pra", "boyd_k", "prielipp_k", "fearnley_tg", "stoiana_tg"],
        "note": "Only one WNBA. Adds Boyd O1.5 Ks (easiest pitcher Goblin line).",
    },
    {
        "id": "T7",
        "name": "6-leg high Power",
        "keys": ["malonga_pra", "melbourne_3s", "gore_k", "woo_ha", "prielipp_k", "fearnley_tg"],
        "note": "Highest N-correct from this list. Power median 9.5x (observed 4–15x).",
    },
    {
        "id": "T8",
        "name": "6-leg tennis-heavy",
        "keys": ["malonga_pra", "gore_k", "fearnley_tg", "fery_tg", "stoiana_tg", "lee_gw"],
        "note": "One WNBA, one pitcher, four tennis matches. Alternate 6-leg if you want less MLB.",
    },
]


def main():
    tickets = []
    for spec in TICKETS:
        legs = [dict(LEGS[k], book="Goblin OVER", side="OVER") for k in spec["keys"]]
        n = len(legs)
        players = [x["player"] for x in legs]
        assert len(players) == len(set(players)), spec["id"]
        pay = PAY[n]
        tickets.append(
            {
                **spec,
                "n_legs": n,
                "mix": f"{n} Goblin / 0 Standard",
                "power_n_correct_x": pay["power"],
                "flex_n_correct_x": pay["flex"],
                "flex_partial": pay["flex_partial"],
                "play": "Power",
                "legs": legs,
            }
        )
        del tickets[-1]["keys"]
    out = {
        "date": "2026-08-26",
        "source": "S+A Gold/Platinum Goblin pool, unique player, max 2 SEA vs TOR, no same-game pitcher+hitter",
        "payout_note": "N-correct / To Win only. Ignore 1st place. Medians from live mix grid.",
        "tickets": tickets,
    }
    OUT.write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")
    for t in tickets:
        print(f"\n{t['id']}  {t['name']}  {t['n_legs']}G Power {t['power_n_correct_x']}x  Flex {t['flex_n_correct_x']}x")
        print(f"  {t['note']}")
        for lg in t["legs"]:
            print(
                f"  {lg['tag']:12} {lg['player']:22} Goblin {lg['prop']:20} O{lg['line']}  "
                f"L5 {lg['l5']} cov {lg['cover']:+.1f}  {lg['matchup']}"
            )
    print("wrote", OUT)


if __name__ == "__main__":
    main()
