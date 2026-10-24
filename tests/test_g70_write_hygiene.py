"""G70 golf skip + latest-pointer bump (pipeline hygiene, not grading)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

from build_goblin70_tickets import (  # noqa: E402
    bump_g70_latest_pointer,
    g70_json_valid,
    golf_skip_warning,
    golf_xlsx_missing,
    remaining_g70_sports,
    skip_golf_g70,
)


def test_skip_golf_flag_and_env(tmp_path: Path):
    root = tmp_path
    assert skip_golf_g70(root, "2026-09-20", flag=True) is True
    assert skip_golf_g70(root, "2026-09-20", flag=False, env={}) is True
    assert (
        skip_golf_g70(
            root,
            "2026-09-20",
            flag=False,
            env={"PROPORACLE_SKIP_GOLF_G70": "1"},
        )
        is True
    )
    xlsx = root / "outputs" / "2026-09-20" / "golf"
    xlsx.mkdir(parents=True)
    (xlsx / "step8_golf_direction_clean.xlsx").write_bytes(b"pk")
    assert golf_xlsx_missing(root, "2026-09-20") is False
    assert skip_golf_g70(root, "2026-09-20", flag=False, env={}) is False
    assert (
        skip_golf_g70(
            root,
            "2026-09-20",
            flag=False,
            env={"PROPORACLE_SKIP_GOLF_G70": "1"},
        )
        is True
    )


def test_golf_xlsx_missing_ignores_csv(tmp_path: Path):
    golf = tmp_path / "outputs" / "2026-09-20" / "golf"
    golf.mkdir(parents=True)
    (golf / "step8_golf_direction.csv").write_text("player\n", encoding="utf-8")
    assert golf_xlsx_missing(tmp_path, "2026-09-20") is True


def test_remaining_sports_and_warning():
    rows = [
        {"sport": "MLB"},
        {"sport": "SOCCER"},
        {"sport": "WNBA"},
        {"sport": "GOLF"},
        {"sport": "Tennis"},
        {"sport": "NFL"},
        {"sport": "CFB"},
    ]
    keep = remaining_g70_sports(rows)
    assert keep == ["MLB", "WNBA", "Soccer", "Tennis"]
    assert "NFL" not in keep
    msg = golf_skip_warning(rows)
    assert msg.startswith("WARNING: Golf step8 xlsx missing — golf excluded from G70 pool")
    assert "4 remaining sports: MLB, WNBA, Soccer, Tennis" in msg
    assert "NFL" not in msg


def test_g70_json_valid_and_bump(tmp_path: Path):
    dated = tmp_path / "reports" / "goblin70_tickets_2026-09-20.json"
    dated.parent.mkdir()
    payload = {"date": "2026-09-20", "pool": {"goblin_70": 1}}
    dated.write_text(json.dumps(payload), encoding="utf-8")
    assert g70_json_valid(dated) == (True, "ok")
    empty = tmp_path / "empty.json"
    empty.write_text("", encoding="utf-8")
    ok, why = g70_json_valid(empty)
    assert ok is False
    assert why == "empty"

    peer = tmp_path / "PropORACLE_main_cp"
    written = bump_g70_latest_pointer(
        dated, "2026-09-20", peer_roots=[peer]
    )
    local = dated.parent / "goblin70_tickets_latest.json"
    remote = peer / "data" / "reports" / "goblin70_tickets_latest.json"
    assert local in written
    assert remote in written
    assert json.loads(remote.read_text(encoding="utf-8"))["date"] == "2026-09-20"


def test_bump_warns_on_invalid(tmp_path: Path, capsys):
    dated = tmp_path / "bad.json"
    dated.write_text("{not json", encoding="utf-8")
    assert bump_g70_latest_pointer(dated, "2026-09-20", peer_roots=[]) == []
    err = capsys.readouterr().out
    assert "WARNING: G70 dated JSON invalid" in err
    assert "latest pointer not updated" in err
