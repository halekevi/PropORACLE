"""step6e must use the root box DB and fail closed if `nba` is missing."""
from __future__ import annotations

import sqlite3
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
STEP6E = REPO / "Sports" / "NBA" / "scripts" / "step6e_attach_intel.py"


def _tiny_slate(path: Path) -> None:
    path.write_text(
        "player,prop_type,line,team,opp\n"
        "Nikola Jokic,Points,24.5,DEN,OKC\n",
        encoding="utf-8",
    )


def test_step6e_fails_closed_when_nba_table_missing(tmp_path: Path) -> None:
    db = tmp_path / "period_only.db"
    con = sqlite3.connect(str(db))
    con.execute("CREATE TABLE nba1q (player TEXT, game_date TEXT)")
    con.commit()
    con.close()
    src = tmp_path / "in.csv"
    out = tmp_path / "out.csv"
    _tiny_slate(src)
    proc = subprocess.run(
        [
            sys.executable,
            str(STEP6E),
            "--input",
            str(src),
            "--output",
            str(out),
            "--db",
            str(db),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 2, proc.stderr
    assert "ALERT [step6e]" in proc.stderr
    assert "no nba table" in proc.stderr
    assert not out.exists()


def test_step6e_fails_closed_when_db_file_missing(tmp_path: Path) -> None:
    src = tmp_path / "in.csv"
    out = tmp_path / "out.csv"
    _tiny_slate(src)
    missing = tmp_path / "nope.db"
    proc = subprocess.run(
        [
            sys.executable,
            str(STEP6E),
            "--input",
            str(src),
            "--output",
            str(out),
            "--db",
            str(missing),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 2, proc.stderr
    assert "ALERT [step6e]" in proc.stderr
    assert not out.exists()
