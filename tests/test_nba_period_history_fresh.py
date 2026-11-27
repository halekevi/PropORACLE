import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "Sports" / "NBA" / "scripts"))
import step4_db_reader as r  # noqa: E402


def _period_db(path: Path, max_date: str, actual: float) -> None:
    c = sqlite3.connect(path)
    c.execute("CREATE TABLE nba1q (id INTEGER PRIMARY KEY, game_date TEXT, espn_player_id TEXT, "
              "player TEXT, prop_type TEXT, actual REAL, segment TEXT)")
    c.execute("INSERT INTO nba1q (game_date, espn_player_id, player, prop_type, actual, segment) "
              "VALUES (?, 'john collins', 'John Collins', 'rebounds', ?, '1Q')", (max_date, actual))
    c.commit()
    c.close()


def _con(tmp_path: Path, main_period: str, attached_period: str, box_max: str) -> sqlite3.Connection:
    main = tmp_path / "main.db"
    _period_db(main, main_period, 9.0)
    c = sqlite3.connect(main)
    c.execute("CREATE TABLE nba (game_date TEXT)")
    c.execute("INSERT INTO nba VALUES (?)", (box_max,))
    c.commit()
    side = tmp_path / "side.db"
    _period_db(side, attached_period, 1.0)
    c.execute("ATTACH DATABASE ? AS ref0", (str(side),))
    return c


def test_reads_freshest_period_table_not_main(tmp_path):
    con = _con(tmp_path, "2026-10-24", "2026-11-24", "2026-11-25")
    assert r._nba1q_table(con) == "ref0.nba1q"
    assert r.get_vals_nba1q(con, "", "reb", 5, player_name="John Collins") == [1.0]


def test_stale_period_history_fails_step4(tmp_path):
    con = _con(tmp_path, "2026-10-24", "2026-10-24", "2026-11-25")
    with pytest.raises(r.StalePeriodHistory):
        r.assert_period_history_fresh(con, "nba1q")


def test_off_day_lag_within_limit_passes(tmp_path):
    con = _con(tmp_path, "2026-10-24", "2026-11-24", "2026-11-26")
    assert r.assert_period_history_fresh(con, "nba1h") == "ref0.nba1q"


def test_period_overrides_expire(tmp_path, monkeypatch):
    fresh = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
    p = tmp_path / "ov.csv"
    p.write_text(
        "sport,player,prop_norm,vals,as_of\n"
        f"nba1h,Fresh Guy,pra,1|2|3,{fresh}\n"
        "nba1h,Old Guy,pra,9|9|9,2026-08-23\n"
        "nba1h,No Date,pra,7|7|7,\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(r, "PERIOD_OVERRIDES_PATH", p)
    monkeypatch.setattr(r, "_PERIOD_OVERRIDES_CACHE", None)
    assert r._period_override_vals("nba1h", "Fresh Guy", "pra", 5) == [1.0, 2.0, 3.0]
    assert r._period_override_vals("nba1h", "Old Guy", "pra", 5) == []
    assert r._period_override_vals("nba1h", "No Date", "pra", 5) == []
