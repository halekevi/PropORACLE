"""Golf shared windows + push-aware decided counts."""

from __future__ import annotations

import pandas as pd

from utils.golf_windows import (
    count_vs_line,
    counts_from_stat_g_row,
    player_windows,
    select_window,
)


def _rounds() -> pd.DataFrame:
    # Newest first when sorted: Oct 5 Port Royal, Oct 4 elsewhere, …
    rows = [
        {"tournament_date": "2026-10-05", "round": 2, "course_id": "port_royal", "pars": 12, "strokes": 69},
        {"tournament_date": "2026-10-05", "round": 1, "course_id": "port_royal", "pars": 11, "strokes": 70},
        {"tournament_date": "2026-10-04", "round": 2, "course_id": "other", "pars": 8, "strokes": 66},
        {"tournament_date": "2026-10-04", "round": 1, "course_id": "other", "pars": 10, "strokes": 67},
        {"tournament_date": "2026-10-03", "round": 2, "course_id": "port_royal", "pars": 11, "strokes": 71},
        {"tournament_date": "2026-10-03", "round": 1, "course_id": "port_royal", "pars": 9, "strokes": 68},
        {"tournament_date": "2026-10-02", "round": 1, "course_id": "other", "pars": 14, "strokes": 65},
        {"tournament_date": "2026-10-01", "round": 1, "course_id": "port_royal", "pars": 7, "strokes": 72},
    ]
    return pd.DataFrame(rows)


def test_l5_all_date_order():
    win = select_window(_rounds(), kind="l5_all")
    assert len(win) == 5
    dates = list(win["round_date"].astype(str))
    assert dates == [
        "2026-10-05",
        "2026-10-05",
        "2026-10-04",
        "2026-10-04",
        "2026-10-03",
    ]
    # Same date: higher round_num first
    assert list(win["round_num"].astype(int))[:2] == [2, 1]


def test_l5_course_filter():
    win = select_window(_rounds(), kind="l5_course", course_id="port_royal", n_course=5)
    assert len(win) == 5
    assert set(win["course_id"]) == {"port_royal"}
    # Newest Port Royal rounds only
    assert list(win["pars"].astype(int)) == [12, 11, 11, 9, 7]


def test_l5_course_missing_id_empty():
    win = select_window(_rounds(), kind="l5_course", course_id="", n_course=5)
    assert win.empty


def test_skill_window_n20():
    # Build 25 rounds
    rows = [
        {"tournament_date": f"2026-09-{d:02d}", "round": 1, "course_id": "x", "pars": 10}
        for d in range(1, 26)
    ]
    win = select_window(pd.DataFrame(rows), kind="skill_window")
    assert len(win) == 20


def test_as_of_excludes_same_day_and_later():
    win = select_window(_rounds(), kind="l5_all", as_of="2026-10-04")
    # Fixture has 4 rounds before 2026-10-04 (Oct 3×2, Oct 2, Oct 1).
    assert len(win) == 4
    assert all(str(d) < "2026-10-04" for d in win["round_date"].astype(str))
    # Same-day and later must not leak in.
    win_live = select_window(_rounds(), kind="l5_all", as_of="2026-10-06")
    assert len(win_live) == 5


def test_ford_integer_line_push_heavy():
    # 11/12/11/11/11 vs Pars 11.0 → 1 over, 4 push, decided=1, thin
    c = count_vs_line([11, 12, 11, 11, 11], 11.0)
    assert c.over == 1
    assert c.under == 0
    assert c.push == 4
    assert c.decided == 1
    assert c.thin_decided is True
    assert c.hit_rate_decided_over == 1.0
    # L5=5 gate needs 5 decided hits — not met
    assert c.over < 5


def test_three_decided_two_push_not_five_five():
    # 3 under + 2 push must not look like 5/5 under
    c = count_vs_line([10, 10, 10, 11, 11], 11.0)
    assert c.under == 3
    assert c.push == 2
    assert c.decided == 3
    assert c.thin_decided is False
    assert c.hit_rate_decided_under == 1.0
    assert c.under != 5


def test_half_point_no_push():
    c = count_vs_line([66, 64, 65, 66, 67], 68.5)
    assert c.push == 0
    assert c.decided == 5
    assert c.under == 5
    assert c.thin_decided is False


def test_counts_from_stat_g_row():
    row = pd.Series({f"stat_g{i}": v for i, v in enumerate([11, 12, 11, 11, 11], start=1)})
    c = counts_from_stat_g_row(row, line=11.0)
    assert c.push == 4
    assert c.over == 1


def test_player_windows_course_vs_all():
    out = player_windows(
        _rounds(),
        stat_col="pars",
        line=11.0,
        course_id="port_royal",
        n_course=4,
    )
    assert out["l5_all_n"] == 5
    assert out["l5_course_n"] == 4
    assert out["skill_window_n"] == 8  # only 8 rounds in fixture
    # Course window pars 12,11,11,9 vs 11 → over=1, under=1, push=2
    assert out["l5_course_over"] == 1
    assert out["l5_course_under"] == 1
    assert out["l5_course_push"] == 2
    assert out["l5_course_decided"] == 2
    assert out["l5_course_thin_decided"] is True
