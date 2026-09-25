"""Unit tests for payout ticket signature + ambiguous-sig skip."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

from utils.payout_ticket_sig import (  # noqa: E402
    ambiguous_sig_keys,
    ticket_payout_sig,
    ticket_product_token,
)
import collect_payout_data as cpd  # noqa: E402


def _leg(player: str, prop: str, *, line: float = 1.5, pick: str = "Goblin") -> dict:
    return {
        "player": player,
        "prop_type": prop,
        "direction": "OVER",
        "line": line,
        "pick_type": pick,
    }


def test_sig_includes_pick_type_and_product():
    gob = {
        "play": "Power",
        "n_legs": 2,
        "legs": [_leg("A", "Assists"), _leg("B", "Rebounds")],
    }
    std = {
        "play": "Power",
        "n_legs": 2,
        "legs": [
            _leg("A", "Assists", pick="Standard"),
            _leg("B", "Rebounds", pick="Standard"),
        ],
    }
    flex = {
        "play": "Flex",
        "n_legs": 2,
        "legs": [_leg("A", "Assists"), _leg("B", "Rebounds")],
    }
    assert ticket_payout_sig(gob) != ticket_payout_sig(std)
    assert ticket_payout_sig(gob) != ticket_payout_sig(flex)
    assert ticket_product_token(flex) == "flex"
    assert "goblin" in ticket_payout_sig(gob)
    assert "standard" in ticket_payout_sig(std)


def test_sig_order_independent():
    a = {
        "play": "Power",
        "legs": [_leg("A", "Assists"), _leg("B", "Rebounds")],
    }
    b = {
        "play": "Power",
        "legs": [_leg("B", "Rebounds"), _leg("A", "Assists")],
    }
    assert ticket_payout_sig(a) == ticket_payout_sig(b)


def test_ambiguous_sig_skips_all(tmp_path, monkeypatch):
    monkeypatch.setattr(cpd, "ROOT", tmp_path)
    monkeypatch.setenv("PROPORACLE_REQUIRE_LIVE_PAYOUT", "1")
    (tmp_path / "data" / "reports").mkdir(parents=True)
    date = "2026-09-24"
    legs = [_leg("A", "Assists"), _leg("B", "Rebounds")]
    t1 = {
        "ticket_id": "d|G70|1",
        "play": "Power",
        "n_legs": 2,
        "legs": legs,
        "payout": {"payout_source": "pending_live", "min_payout_x": 5.0},
    }
    t2 = {
        "ticket_id": "d|G70|2",
        "play": "Power",
        "n_legs": 2,
        "legs": [dict(x) for x in legs],
        "payout": {"payout_source": "pending_live", "min_payout_x": 5.0},
    }
    assert ticket_payout_sig(t1) == ticket_payout_sig(t2)
    assert ticket_payout_sig(t1) in ambiguous_sig_keys([t1, t2])

    tickets_path = tmp_path / "tickets.json"
    payload = {"date": date, "groups": [{"name": "G70", "tickets": [t1, t2]}]}
    tickets_path.write_text(__import__("json").dumps(payload), encoding="utf-8")
    # Patch only has a floor under the shared sig (simulating prior scrape of one slip).
    sig = ticket_payout_sig(t1)
    patch = {
        "date": date,
        "by_ticket_id": {},
        "by_leg_sig": {
            sig: {"power_min_x": 2.9, "display_min_x": 2.9, "payout_source": "live_cdp"}
        },
    }
    (tmp_path / "data" / "reports" / f"payout_patch_{date}.json").write_text(
        __import__("json").dumps(patch), encoding="utf-8"
    )
    counts = cpd.apply_payout_patch_entries_to_payload(payload, patch)
    assert counts["n_sig_ambiguous"] == 2
    assert counts["n_patched"] == 0
    assert payload["groups"][0]["tickets"][0]["payout"]["payout_source"] == "pending_live"
    assert payload["groups"][0]["tickets"][1]["payout"]["payout_source"] == "pending_live"


def test_unique_sig_still_paints_after_id_change(tmp_path, monkeypatch):
    monkeypatch.setattr(cpd, "ROOT", tmp_path)
    monkeypatch.setenv("PROPORACLE_REQUIRE_LIVE_PAYOUT", "1")
    (tmp_path / "data" / "reports").mkdir(parents=True)
    date = "2026-09-24"
    legs = [_leg("A", "Assists"), _leg("B", "Rebounds")]
    old = {
        "ticket_id": "d|G70|OLD",
        "play": "Power",
        "n_legs": 2,
        "legs": legs,
        "ticket_type_captured": "power",
        "status": "ok",
        "power_min_x": 3.2,
    }
    # Rebuild regenerated the positional ticket_id.
    new = {
        "ticket_id": "d|G70|NEW",
        "play": "Power",
        "n_legs": 2,
        "legs": [dict(x) for x in legs],
        "payout": {"payout_source": "pending_live", "min_payout_x": 5.0},
    }
    tickets_path = tmp_path / "tickets.json"
    tickets_path.write_text(
        __import__("json").dumps({"date": date, "groups": [{"tickets": [new]}]}),
        encoding="utf-8",
    )
    cpd.write_payout_patch_and_apply_to_tickets(
        tickets_path=tickets_path,
        captured=[old],
        date_str=date,
    )
    data = __import__("json").loads(tickets_path.read_text(encoding="utf-8"))
    pay = data["groups"][0]["tickets"][0]["payout"]
    assert pay["payout_source"] == "live_cdp"
    assert float(pay["display_min_x"]) == 3.2


def test_swapped_positional_ids_do_not_cross_paint(tmp_path, monkeypatch):
    """Rebuild swaps two slips into each other's {date}|{group}|{index} slots."""
    monkeypatch.setattr(cpd, "ROOT", tmp_path)
    monkeypatch.setenv("PROPORACLE_REQUIRE_LIVE_PAYOUT", "1")
    (tmp_path / "data" / "reports").mkdir(parents=True)
    date = "2026-09-24"
    legs_a = [_leg("Alice", "Assists"), _leg("Bob", "Rebounds")]
    legs_b = [_leg("Carol", "Points"), _leg("Dave", "Steals", line=0.5)]
    # Capture when index 1 was A (floor 2.5) and index 2 was B (floor 4.0).
    captured = [
        {
            "ticket_id": "2026-09-24|G70|1",
            "play": "Power",
            "n_legs": 2,
            "legs": legs_a,
            "ticket_type_captured": "power",
            "status": "ok",
            "power_min_x": 2.5,
        },
        {
            "ticket_id": "2026-09-24|G70|2",
            "play": "Power",
            "n_legs": 2,
            "legs": legs_b,
            "ticket_type_captured": "power",
            "status": "ok",
            "power_min_x": 4.0,
        },
    ]
    # After rebuild, slot 1 holds B's legs and slot 2 holds A's legs.
    swapped = {
        "date": date,
        "groups": [
            {
                "name": "G70",
                "tickets": [
                    {
                        "ticket_id": "2026-09-24|G70|1",
                        "play": "Power",
                        "n_legs": 2,
                        "legs": [dict(x) for x in legs_b],
                        "payout": {"payout_source": "pending_live", "min_payout_x": 5.0},
                    },
                    {
                        "ticket_id": "2026-09-24|G70|2",
                        "play": "Power",
                        "n_legs": 2,
                        "legs": [dict(x) for x in legs_a],
                        "payout": {"payout_source": "pending_live", "min_payout_x": 5.0},
                    },
                ],
            }
        ],
    }
    tickets_path = tmp_path / "tickets.json"
    tickets_path.write_text(__import__("json").dumps(swapped), encoding="utf-8")
    result = cpd.write_payout_patch_and_apply_to_tickets(
        tickets_path=tickets_path,
        captured=captured,
        date_str=date,
    )
    data = __import__("json").loads(tickets_path.read_text(encoding="utf-8"))
    t1, t2 = data["groups"][0]["tickets"]
    # Sig rematch (not ID): slot 1 is B → 4.0; slot 2 is A → 2.5.
    assert float(t1["payout"]["display_min_x"]) == 4.0
    assert t1["payout"]["payout_source"] == "live_cdp"
    assert float(t2["payout"]["display_min_x"]) == 2.5
    assert t2["payout"]["payout_source"] == "live_cdp"
    assert int(result.get("n_id_sig_mismatch") or 0) == 2
    # Explicit: neither got the floor that belonged to the other ID alone.
    assert float(t1["payout"]["display_min_x"]) != 2.5
    assert float(t2["payout"]["display_min_x"]) != 4.0
