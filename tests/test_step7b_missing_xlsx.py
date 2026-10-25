from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_step7b_exits_nonzero_when_explicit_xlsx_missing(tmp_path: Path):
    missing = tmp_path / "nope.xlsx"
    r = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "step7b_edge_score.py"),
            "--sport",
            "NBA",
            "--repo-root",
            str(tmp_path),
            "--step7-xlsx",
            str(missing),
        ],
        capture_output=True,
        text=True,
    )
    assert r.returncode != 0
    blob = (r.stderr or "") + (r.stdout or "")
    assert "step7-xlsx not found" in blob


def test_step7b_cbb_skips_scoring_and_exits_ok(tmp_path: Path):
    xlsx = tmp_path / "step6_ranked_cbb.xlsx"
    xlsx.write_bytes(b"PK\x03\x04" + b"\x00" * 64)
    r = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "step7b_edge_score.py"),
            "--sport",
            "CBB",
            "--repo-root",
            str(tmp_path),
            "--step7-xlsx",
            str(xlsx),
        ],
        capture_output=True,
        text=True,
    )
    blob = (r.stdout or "") + (r.stderr or "")
    assert r.returncode == 0, blob
    assert "no model for CBB" in blob
    assert "leave ml_prob blank" in blob


def test_step7b_wcbb_skips_scoring_and_exits_ok(tmp_path: Path):
    xlsx = tmp_path / "step6_ranked_wcbb.xlsx"
    xlsx.write_bytes(b"PK\x03\x04" + b"\x00" * 64)
    r = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "step7b_edge_score.py"),
            "--sport",
            "WCBB",
            "--repo-root",
            str(tmp_path),
            "--step7-xlsx",
            str(xlsx),
        ],
        capture_output=True,
        text=True,
    )
    blob = (r.stdout or "") + (r.stderr or "")
    assert r.returncode == 0, blob
    assert "no model for WCBB" in blob
