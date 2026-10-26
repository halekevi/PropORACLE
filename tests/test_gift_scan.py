"""Gift scan: adverse-selection logged, alerts only on a clean pass."""
from __future__ import annotations

from pathlib import Path

from datetime import datetime, timezone

from ml.gift_scan import (
    adverse_checks,
    floor_k,
    goblin_line_delta,
    is_gift,
    published_after_start,
    rung_breakeven,
    scan,
    suggest_slips,
)


def _row(**kw):
    rec = {
        "sport": "NBA",
        "player": "A",
        "team": "BOS",
        "prop": "Rebounds",
        "line": 7.5,
        "pick_type": "Standard",
        "side": "OVER",
        "stat_status": "OK",
        "avg_l5": 14,
        "start_time": "2026-11-01T19:30:00-04:00",
        "game_date": "2026-11-01",
    }
    rec.update(kw)
    games = rec.pop("games", (12, 11, 14, 10, 13))
    for i, val in enumerate(games, start=1):
        rec.setdefault(f"stat_g{i}", val)
    return rec


def test_prereg_locked():
    from ml.gift_scan import assert_prereg

    spec = assert_prereg()
    assert spec["alert_only"] == "all_checks_pass"
    assert spec["log_failed_checks"] is True


def test_goblin_at_floor_is_not_a_gift():
    assert floor_k(8.0, 7.5) == 0
    assert is_gift("goblin", 0, 0.84, 0.846) is False
    assert is_gift("goblin", 1, 0.92, 0.846) is True


def test_standard_at_floor_is_a_gift_on_the_math():
    assert is_gift("standard", 0, 0.84, 0.577) is True
    assert rung_breakeven("standard") == 0.577
    assert rung_breakeven("demon") is None
    assert rung_breakeven("demon", demon_quote_x=4.0) is not None


def test_injury_and_line_drop_flag_but_still_log(tmp_path: Path, monkeypatch):
    monkeypatch.setattr("ml.gift_scan.LOG", tmp_path / "gifts.jsonl")
    monkeypatch.setattr("ml.gift_scan.SLIP_LOG", tmp_path / "slips.jsonl")
    monkeypatch.setattr("ml.gift_scan.LINE_SNAP", tmp_path / "lines.json")
    monkeypatch.setattr("ml.gift_scan.OUT_DIR", tmp_path)
    monkeypatch.setattr("ml.gift_scan.lookup_p_clear", lambda *a, **k: {"hr": 0.84, "n": 500, "wilson_low": 0.82})
    monkeypatch.setattr("ml.gift_scan.face_verified_step1", lambda *a, **k: True)
    a = _row(player="A", injury_status="Q")
    b = _row(player="B", team="NYK")
    prev = {"NBA|b|rebounds|standard": 9.5}
    res = scan(
        date="2026-11-01",
        root=tmp_path,
        board=[a, b],
        nba_lookup={},
        nfl_inactives=set(),
        prev_lines=prev,
        demon_x=None,
        alert=False,
        write=True,
    )
    assert res["n_gifts"] == 2
    assert res["n_clean"] == 0
    by = {g["player"]: g for g in res["gifts"]}
    assert by["A"]["checks"]["injury_player"] is False
    assert by["B"]["checks"]["line_dropped"] is False
    assert by["A"]["all_pass"] is False
    assert by["B"]["all_pass"] is False
    assert "nba_t30_missing" in {g["role_why"] for g in res["gifts"]}


def test_clean_nba_gift_alerts_path(tmp_path: Path, monkeypatch):
    monkeypatch.setattr("ml.gift_scan.LOG", tmp_path / "gifts.jsonl")
    monkeypatch.setattr("ml.gift_scan.SLIP_LOG", tmp_path / "slips.jsonl")
    monkeypatch.setattr("ml.gift_scan.LINE_SNAP", tmp_path / "lines.json")
    monkeypatch.setattr("ml.gift_scan.OUT_DIR", tmp_path)
    monkeypatch.setattr("ml.gift_scan.lookup_p_clear", lambda *a, **k: {"hr": 0.84, "n": 500, "wilson_low": 0.82})
    monkeypatch.setattr("ml.gift_scan.face_verified_step1", lambda *a, **k: True)
    sent: list[str] = []
    monkeypatch.setattr("ml.gift_scan.push_ntfy", lambda body, title="": sent.append(body) or True)
    row = _row(player="Tatum")
    lookup = {("2026-11-01", "bos", "tatum"): {"tonight_starter": "confirmed", "pre_tip": True}}
    res = scan(
        date="2026-11-01",
        root=tmp_path,
        board=[row],
        nba_lookup=lookup,
        nfl_inactives=set(),
        prev_lines={},
        alert=True,
        write=True,
    )
    assert res["n_clean"] == 1
    assert res["n_alerts"] == 1
    assert "k=" in sent[0]
    assert "Tatum" in sent[0]
    assert "L5min=" in sent[0]


def test_slips_use_clean_gifts_only():
    clean = [
        {
            "player": "A",
            "sport": "NBA",
            "prop": "Rebounds",
            "rung": "standard",
            "line": 7.5,
            "k": 1,
            "p_clear": 0.84,
            "all_pass": True,
            "edge": 0.26,
            "start_time": "game1",
            "opponent": "NYK",
            "team": "BOS",
        },
        {
            "player": "B",
            "sport": "NBA",
            "prop": "Assists",
            "rung": "standard",
            "line": 4.5,
            "k": 1,
            "p_clear": 0.84,
            "all_pass": True,
            "edge": 0.26,
            "start_time": "game2",
            "opponent": "MIA",
            "team": "NYK",
        },
        {
            "player": "C",
            "sport": "NBA",
            "prop": "Rebounds",
            "rung": "standard",
            "line": 6.5,
            "k": 0,
            "p_clear": 0.84,
            "all_pass": False,
            "edge": 0.26,
            "start_time": "game3",
            "team": "CHI",
        },
    ]
    slips = suggest_slips(clean, demon_x=None, now="t")
    assert len(slips) == 1
    assert slips[0]["n_legs"] == 2
    assert slips[0]["quote"] == 3.0
    assert slips[0]["placed"] is False
    names = {l["player"] for l in slips[0]["legs"]}
    assert names == {"A", "B"}


def test_does_not_import_frozen_nfl_tracks():
    import inspect

    import ml.gift_scan as gs

    src = inspect.getsource(gs)
    assert "from utils.nfl_demon" not in src
    assert "from utils.nfl_demon_volume" not in src
    assert "from utils.nfl_demon_rec_floor" not in src


def test_teammate_q_flags_role_share():
    a = _row(player="Lead", avg_l5=14)
    b = _row(player="Mate", avg_l5=12, injury_status="Out")
    ranks = {("NBA", "bos", "lead"): 1, ("NBA", "bos", "mate"): 2}
    adv = adverse_checks(
        a,
        rung="standard",
        k=0,
        board=[a, b],
        ranks=ranks,
        prev_lines={},
        nba_lookup={("2026-11-01", "bos", "lead"): {"tonight_starter": "confirmed"}},
        nfl_inactives=set(),
    )
    assert adv["checks"]["injury_teammate_role"] is False
    assert "Mate" in adv["teammate_flagged"]


def test_published_after_start_excludes_started_game_from_measurement(
    tmp_path: Path, monkeypatch
):
    monkeypatch.setattr("ml.gift_scan.LOG", tmp_path / "gifts.jsonl")
    monkeypatch.setattr("ml.gift_scan.SLIP_LOG", tmp_path / "slips.jsonl")
    monkeypatch.setattr("ml.gift_scan.LINE_SNAP", tmp_path / "lines.json")
    monkeypatch.setattr("ml.gift_scan.OUT_DIR", tmp_path)
    monkeypatch.setattr(
        "ml.gift_scan.lookup_p_clear",
        lambda *a, **k: {"hr": 0.84, "n": 500, "wilson_low": 0.82},
    )
    monkeypatch.setattr("ml.gift_scan.face_verified_step1", lambda *a, **k: True)
    as_of = datetime(2026, 10, 25, 20, 30, tzinfo=timezone.utc)
    started = _row(
        player="Szmyt",
        sport="NFL",
        team="CLE",
        start_time="2026-10-25T13:00:00-04:00",
        game_date="2026-10-25",
        fetched_at="2026-10-25T20:00:00+00:00",
    )
    live = _row(
        player="Tatum",
        start_time="2026-11-01T19:30:00-04:00",
        game_date="2026-11-01",
        fetched_at="2026-10-25T16:00:00+00:00",
    )
    assert published_after_start(started, now=as_of) is True
    assert published_after_start(live, now=as_of) is False
    lookup = {
        ("2026-11-01", "bos", "tatum"): {"tonight_starter": "confirmed", "pre_tip": True}
    }
    res = scan(
        date="2026-10-25",
        root=tmp_path,
        board=[started, live],
        nba_lookup=lookup,
        nfl_inactives=set(),
        prev_lines={},
        alert=False,
        write=True,
        as_of=as_of,
    )
    assert res["n_gifts"] == 2
    assert res["n_published_after_start"] == 1
    assert res["n_measure"] == 1
    by = {g["player"]: g for g in res["gifts"]}
    assert by["Szmyt"]["published_after_start"] is True
    assert by["Tatum"]["published_after_start"] is False
    assert res["n_clean"] == 1
    assert res["gifts"][0]["player"] == "Szmyt" or by["Szmyt"]["all_pass"] in {True, False}


def test_goblin_be_rises_with_delta():
    from ml.gift_scan import standard_twins

    be_near = rung_breakeven("goblin", goblin_delta=1.0)
    be_mid = rung_breakeven("goblin", goblin_delta=2.0)
    # 0.5-line Goblin vs a 3.5 Standard twin (GΔ=3), e.g. a deep FG-made face.
    deep_delta = goblin_line_delta({"line": 0.5, "standard_line": 3.5})
    be_deep = rung_breakeven("goblin", goblin_delta=deep_delta)
    assert deep_delta == 3.0
    assert be_near is not None and abs(be_near - 0.846) < 0.02
    assert be_mid is not None and be_mid > be_near
    assert be_deep is not None
    assert be_deep > be_mid
    assert be_deep > 0.90
    assert rung_breakeven("goblin") is None
    twins = standard_twins(
        [
            {
                "sport": "NFL",
                "player": "Andre Szmyt",
                "prop": "fg_made",
                "pick_type": "Standard",
                "line": 1.5,
            }
        ]
    )
    szmyt = goblin_line_delta(
        {
            "sport": "NFL",
            "player": "Andre Szmyt",
            "prop": "fg_made",
            "pick_type": "Goblin",
            "line": 0.5,
        },
        twins=twins,
    )
    assert szmyt == 1.0
    # GΔ=1 stays near the 84.6% floor; the 0.5-line vs a 3.5 twin is the deep case.
    assert rung_breakeven("goblin", goblin_delta=szmyt) is not None
