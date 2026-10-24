"""N-correct lookup: slip pin, same-day CDP, mix_by_delta, fallback."""
from __future__ import annotations

import json
from pathlib import Path

from utils.n_correct_payout import (
    goblin_delta_sig,
    resolve_n_correct,
)


def test_override_used_when_no_scrape(tmp_path: Path):
    reports = tmp_path / "data" / "reports"
    reports.mkdir(parents=True)
    payload = {
        "date": "2026-08-27",
        "entries": [
            {
                "n_legs": 3,
                "n_s": 0,
                "n_g": 3,
                "product": "Power",
                "n_correct": {"3": 2.0},
                "note": "slip pin 2x",
            }
        ],
    }
    (reports / "payout_overrides_2026-08-27.json").write_text(
        json.dumps(payload), encoding="utf-8"
    )
    legs = [
        {"pick_type": "Goblin", "line": 3.5, "standard_line": 5.5, "p": 0.76},
        {"pick_type": "Goblin", "line": 2.5, "standard_line": 5.0, "p": 0.76},
        {"pick_type": "Goblin", "line": 1.5, "standard_line": 3.5, "p": 0.76},
    ]
    got = resolve_n_correct(legs, "Power", "goblin", date="2026-08-27", repo=tmp_path)
    assert got["n_correct"][3] == 2.0
    assert got["payout_source"] == "n_correct_live"
    other = resolve_n_correct(legs, "Power", "goblin", date="2026-08-26", repo=tmp_path)
    assert other["payout_source"] == "n_correct_median"
    assert other["n_correct"][3] == 2.0


def test_scrape_beats_slip_pin(tmp_path: Path):
    reports = tmp_path / "data" / "reports"
    reports.mkdir(parents=True)
    (reports / "payout_overrides_2026-08-27.json").write_text(
        json.dumps(
            {
                "date": "2026-08-27",
                "entries": [
                    {
                        "n_legs": 3,
                        "n_s": 0,
                        "n_g": 3,
                        "product": "Power",
                        "n_correct": {"3": 9.9},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    live = tmp_path / "ui_runner" / "data"
    live.mkdir(parents=True)
    (live / "payout_ladder_live_cdp.json").write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "date": "2026-08-27",
                        "n_legs": "3",
                        "leg_composition": "0S+3G+0D",
                        "goblin_deltas": ["2", "2.5", "2"],
                        "power_payout_x": "2.0",
                        "source": "live_cdp",
                        "ticket_id": "scrape",
                        "captured_at": "2026-08-27T16:32:00-04:00",
                        "last_captured_at": "2026-08-27T16:32:00-04:00",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    legs = [
        {"pick_type": "Goblin", "line": 3.5, "standard_line": 5.5, "p": 0.76},
        {"pick_type": "Goblin", "line": 2.5, "standard_line": 5.0, "p": 0.76},
        {"pick_type": "Goblin", "line": 1.5, "standard_line": 3.5, "p": 0.76},
    ]
    got = resolve_n_correct(legs, "Power", "goblin", date="2026-08-27", repo=tmp_path)
    assert got["n_correct"][3] == 2.0
    assert got["payout_source"] == "n_correct_live"
    assert got["captured_at"] == "2026-08-27T16:32:00-04:00"


def test_same_day_cdp_matches_delta(tmp_path: Path):
    live = tmp_path / "ui_runner" / "data"
    live.mkdir(parents=True)
    (live / "payout_ladder_live_cdp.json").write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "date": "2026-08-27",
                        "n_legs": "3",
                        "leg_composition": "0S+3G+0D",
                        "goblin_deltas": ["2", "2.5", "2"],
                        "power_payout_x": "2.0",
                        "source": "live_cdp",
                        "ticket_id": "z",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    legs = [
        {"pick_type": "Goblin", "line": 3.5, "standard_line": 5.5, "p": 0.76},
        {"pick_type": "Goblin", "line": 2.5, "standard_line": 5.0, "p": 0.76},
        {"pick_type": "Goblin", "line": 1.5, "standard_line": 3.5, "p": 0.76},
    ]
    assert goblin_delta_sig(legs) == "2+2+2.5"
    got = resolve_n_correct(legs, "Power", "goblin", date="2026-08-27", repo=tmp_path)
    assert got["n_correct"][3] == 2.0
    assert got["payout_source"] == "n_correct_live"


def test_mix_by_delta_when_no_pin(tmp_path: Path):
    reports = tmp_path / "data" / "reports"
    reports.mkdir(parents=True)
    (reports / "predicted_payout_tables_latest.json").write_text(
        json.dumps(
            {
                "mix_by_delta": [
                    {
                        "n_legs": 3,
                        "n_s": 0,
                        "n_g": 3,
                        "composition": "0S+3G",
                        "goblin_delta_sig": "2+2+2.5",
                        "power_x": 2.1,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    legs = [
        {"pick_type": "Goblin", "line": 3.5, "standard_line": 5.5, "p": 0.76},
        {"pick_type": "Goblin", "line": 2.5, "standard_line": 5.0, "p": 0.76},
        {"pick_type": "Goblin", "line": 1.5, "standard_line": 3.5, "p": 0.76},
    ]
    got = resolve_n_correct(legs, "Power", "goblin", date="2026-08-20", repo=tmp_path)
    assert got["n_correct"][3] == 2.1
    assert got["payout_source"] == "n_correct_delta"


def test_three_leg_quotes_p25_ahead_of_the_gap_median(tmp_path: Path):
    reports = tmp_path / "data" / "reports"
    reports.mkdir(parents=True)
    (reports / "payout_capture_2026-08-01.json").write_text(
        json.dumps(
            {
                "date": "2026-08-01",
                "slips": [
                    {
                        "status": "ok",
                        "ticket_id": f"a-{power}",
                        "date": "2026-08-01",
                        "power_min_x": power,
                        "legs": [
                            {"pick_type": "Goblin", "prop_type": "Points"},
                            {"pick_type": "Goblin", "prop_type": "Rebounds"},
                            {"pick_type": "Goblin", "prop_type": "Assists"},
                        ],
                    }
                    for power in (1.7, 2.0, 2.2, 2.8)
                ],
            }
        ),
        encoding="utf-8",
    )
    (reports / "predicted_payout_tables_latest.json").write_text(
        json.dumps(
            {
                "mix_by_delta": [
                    {
                        "n_legs": 3,
                        "composition": "0S+3G",
                        "goblin_delta_sig": "2+2+2.5",
                        "power_x": 2.9,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    legs = [
        {"pick_type": "Goblin", "line": 3.5, "standard_line": 5.5, "p": 0.76},
        {"pick_type": "Goblin", "line": 2.5, "standard_line": 5.0, "p": 0.76},
        {"pick_type": "Goblin", "line": 1.5, "standard_line": 3.5, "p": 0.76},
    ]
    got = resolve_n_correct(legs, "Power", "goblin", date="2026-08-20", repo=tmp_path)
    assert got["payout_source"] == "n_correct_p25"
    assert got["n_correct"][3] == 1.7
    assert "min" in got["note"]


def _goblin3(power: float, tid: str, date: str = "2026-08-01") -> dict:
    return {
        "status": "ok",
        "ticket_id": tid,
        "date": date,
        "power_min_x": power,
        "legs": [
            {"pick_type": "Goblin"},
            {"pick_type": "Goblin"},
            {"pick_type": "Goblin"},
        ],
    }


def test_same_ticket_counts_once_and_july_cluster_is_out(tmp_path: Path):
    reports = tmp_path / "data" / "reports"
    reports.mkdir(parents=True)
    slips = [_goblin3(p, f"s{i}") for i, p in enumerate((1.4, 1.6, 1.8, 2.0, 2.2, 2.4))]
    slips.append(_goblin3(1.4, "s0", date="2026-08-01"))
    slips.append(_goblin3(1.0, "july", date="2026-07-12"))
    (reports / "payout_capture_2026-08-01.json").write_text(
        json.dumps({"date": "2026-08-01", "slips": slips}),
        encoding="utf-8",
    )
    legs = [
        {"pick_type": "Goblin", "line": 3.5, "standard_line": 5.5},
        {"pick_type": "Goblin", "line": 2.5, "standard_line": 5.0},
        {"pick_type": "Goblin", "line": 1.5, "standard_line": 3.5},
    ]
    got = resolve_n_correct(legs, "Power", "goblin", date="2026-08-20", repo=tmp_path)
    assert got["payout_source"] == "n_correct_p25"
    assert "p25" in got["note"]
    assert "n=6" in got["note"]
    assert got["n_correct"][3] == 1.65


def test_thin_one_goblin_two_leg_stays_on_the_2x_floor(tmp_path: Path):
    reports = tmp_path / "data" / "reports"
    reports.mkdir(parents=True)
    slips = []
    for i, power in enumerate((2.3, 2.5, 2.9)):
        slips.append(
            {
                "status": "ok",
                "ticket_id": f"m{i}",
                "date": "2026-08-01",
                "power_min_x": power,
                "legs": [
                    {"pick_type": "Goblin"},
                    {"pick_type": "Standard"},
                ],
            }
        )
    (reports / "payout_capture_2026-08-01.json").write_text(
        json.dumps({"date": "2026-08-01", "slips": slips}),
        encoding="utf-8",
    )
    legs = [
        {"pick_type": "Goblin", "line": 0.5, "standard_line": 1.5},
        {"pick_type": "Standard", "line": 1.5, "standard_line": 1.5},
    ]
    got = resolve_n_correct(legs, "Power", "mix", date="2026-08-20", repo=tmp_path)
    assert got["payout_source"] == "n_correct_p25"
    assert got["n_correct"][2] == 2.0
    assert "mix floor" in got["note"]
    assert "n=3" in got["note"]


def test_two_leg_goblin_does_not_use_p25(tmp_path: Path):
    reports = tmp_path / "data" / "reports"
    reports.mkdir(parents=True)
    (reports / "payout_capture_2026-08-01.json").write_text(
        json.dumps(
            {
                "date": "2026-08-01",
                "slips": [
                    {
                        "status": "ok",
                        "ticket_id": "g2",
                        "date": "2026-08-01",
                        "power_min_x": 1.2,
                        "legs": [
                            {"pick_type": "Goblin"},
                            {"pick_type": "Goblin"},
                        ],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    legs = [
        {"pick_type": "Goblin", "line": 0.5, "standard_line": 1.5},
        {"pick_type": "Goblin", "line": 0.5, "standard_line": 1.5},
    ]
    got = resolve_n_correct(legs, "Power", "goblin", date="2026-08-20", repo=tmp_path)
    assert got["payout_source"] != "n_correct_p25"


def test_long_slip_uses_composition_low_not_hardcoded_high(tmp_path: Path):
    live = tmp_path / "ui_runner" / "data"
    live.mkdir(parents=True)
    (live / "payout_ladder_live_cdp.json").write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "n_legs": "6",
                        "leg_composition": "1S+5G+0D",
                        "power_payout_x": "4.5",
                        "source": "live_cdp",
                    },
                    {
                        "n_legs": "6",
                        "leg_composition": "1S+5G+0D",
                        "power_payout_x": "6.0",
                        "source": "live_cdp",
                    },
                    {
                        "n_legs": "6",
                        "leg_composition": "1S+5G+0D",
                        "power_payout_x": "9.5",
                        "source": "live_cdp",
                    },
                    {
                        "n_legs": "6",
                        "leg_composition": "1S+5G+0D",
                        "power_payout_x": "13.0",
                        "source": "live_cdp",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    legs = [{"pick_type": "Standard", "line": 9.5, "standard_line": 9.5}] + [
        {"pick_type": "Goblin", "line": 1.5, "standard_line": 3.0} for _ in range(5)
    ]
    got = resolve_n_correct(legs, "Power", "mix", date="2026-10-06", repo=tmp_path)
    assert got["payout_source"] == "n_correct_low"
    assert got["n_correct"][6] == 4.5
    assert got["n_correct"][6] < 8.75


def test_thin_cell_quotes_the_minimum_not_the_tenth_percentile(tmp_path: Path):
    live = tmp_path / "ui_runner" / "data"
    live.mkdir(parents=True)
    rows = [
        {"n_legs": "6", "leg_composition": "1S+5G+0D", "power_payout_x": x, "source": "live_cdp", "goblin_deltas": ["1"]}
        for x in ("4.5", "6.0", "9.5", "13.0")
    ]
    rows.append(
        {
            "n_legs": "6",
            "leg_composition": "1S+5G+0D",
            "power_payout_x": "8.75",
            "source": "live_cdp",
            "goblin_deltas": [],
            "flex_n_correct": {},
        }
    )
    (live / "payout_ladder_live_cdp.json").write_text(json.dumps({"rows": rows}), encoding="utf-8")
    legs = [{"pick_type": "Standard", "line": 9.5, "standard_line": 9.5}] + [
        {"pick_type": "Goblin", "line": 1.5, "standard_line": 3.0} for _ in range(5)
    ]
    got = resolve_n_correct(legs, "Power", "mix", date="2026-10-06", repo=tmp_path)
    assert got["n_correct"][6] == 4.5
    assert "min" in got["note"]


def test_long_slip_without_scrapes_stays_under_ev_floor(tmp_path: Path):
    legs = [{"pick_type": "Standard", "line": 9.5}] + [
        {"pick_type": "Goblin", "line": 0.5} for _ in range(5)
    ]
    got = resolve_n_correct(legs, "Power", "mix", date="2026-10-06", repo=tmp_path)
    assert got["payout_source"] == "n_correct_unpriced"
    assert got["n_correct"][6] < 1.4

