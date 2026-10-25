from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def test_cbb_step8_writes_board_without_ml_prob(tmp_path: Path):
    inp = tmp_path / "step6_ranked_cbb.xlsx"
    out = tmp_path / "step8_cbb_direction_clean.xlsx"
    pd.DataFrame(
        {
            "player": ["A"],
            "prop_norm": ["pts"],
            "pick_type": ["Goblin"],
            "line": [12.5],
            "projection": [15.0],
            "edge": [2.5],
            "tier": ["D"],
            "final_bet_direction": ["OVER"],
        }
    ).to_excel(inp, sheet_name="ALL", index=False)

    r = subprocess.run(
        [
            sys.executable,
            str(ROOT / "Sports" / "CBB" / "scripts" / "pipeline" / "step8_add_direction_context_cbb.py"),
            "--input",
            str(inp),
            "--output",
            str(out),
            "--date",
            "2026-02-21",
            "--sport",
            "CBB",
        ],
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )
    blob = (r.stdout or "") + (r.stderr or "")
    assert r.returncode == 0, blob
    assert out.is_file()
    df = pd.read_excel(out, sheet_name="ALL")
    assert len(df) == 1
    assert str(df["sport"].iloc[0]).upper() == "CBB"
    assert str(df["pick_type"].iloc[0]) == "Goblin"
    if "ml_prob" in df.columns:
        assert pd.isna(df["ml_prob"].iloc[0]) or str(df["ml_prob"].iloc[0]).strip() == ""


def test_cbb_step8_reads_excel_all_not_eligible_parquet(tmp_path: Path):
    inp = tmp_path / "step6_ranked_cbb.xlsx"
    out = tmp_path / "step8_cbb_direction_clean.xlsx"
    all_df = pd.DataFrame(
        {
            "player": ["A", "B", "C"],
            "prop_norm": ["pts", "reb", "ast"],
            "pick_type": ["Goblin", "Demon", "Standard"],
            "line": [12.5, 4.5, 8.5],
            "projection": [15.0, 6.0, 9.0],
            "edge": [2.5, 1.5, 0.5],
            "tier": ["D", "D", "D"],
            "final_bet_direction": ["OVER", "OVER", "OVER"],
        }
    )
    eligible = all_df.loc[all_df["pick_type"].eq("Standard")].copy()
    with pd.ExcelWriter(inp, engine="xlsxwriter") as w:
        all_df.to_excel(w, sheet_name="ALL", index=False)
        eligible.to_excel(w, sheet_name="ELIGIBLE", index=False)
    eligible.to_parquet(tmp_path / "step6_ranked_cbb.parquet", index=False)

    r = subprocess.run(
        [
            sys.executable,
            str(ROOT / "Sports" / "CBB" / "scripts" / "pipeline" / "step8_add_direction_context_cbb.py"),
            "--input",
            str(inp),
            "--output",
            str(out),
            "--sport",
            "CBB",
        ],
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )
    blob = (r.stdout or "") + (r.stderr or "")
    assert r.returncode == 0, blob
    df = pd.read_excel(out, sheet_name="ALL")
    assert len(df) == 3
    assert set(df["pick_type"].astype(str)) == {"Goblin", "Demon", "Standard"}
