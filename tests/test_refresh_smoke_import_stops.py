"""A failing smoke-import must stop the refresh wrapper before lock or fetch."""
from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WRAPPER = ROOT / "scripts" / "run_refresh_with_log.ps1"

_FAKE_SMOKE_FAIL = """#requires -Version 5.1
param(
    [Parameter(Mandatory = $true)]
    [string]$Root,
    [string]$Label = "smoke-import"
)
Write-Host "[$Label] FAILED — aborting before fetch/grade."
exit 2
"""

_FAKE_LATE_FETCH = """#requires -Version 5.1
param(
    [switch]$NoOverwrite,
    [string]$RunLabel = "TEST",
    [switch]$SkipPayout
)
$marker = Join-Path (Split-Path $PSScriptRoot -Parent) "FETCHED"
Set-Content -LiteralPath $marker -Value "late-fetch-ran $RunLabel"
"""

_FAKE_SNAPSHOT = """#requires -Version 5.1
param()
$marker = Join-Path (Split-Path $PSScriptRoot -Parent) "SNAPSHOT"
Set-Content -LiteralPath $marker -Value "snapshot-ran"
"""


def _pwsh() -> str:
    for name in ("pwsh", "powershell"):
        found = shutil.which(name)
        if found:
            return found
    raise RuntimeError("pwsh/powershell not on PATH")


def test_failing_smoke_import_stops_before_lock_and_fetch(tmp_path: Path) -> None:
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    (tmp_path / "data" / "cache").mkdir(parents=True)
    shutil.copy(WRAPPER, scripts / "run_refresh_with_log.ps1")
    (scripts / "Invoke-SmokeImport.ps1").write_text(_FAKE_SMOKE_FAIL, encoding="utf-8")
    (scripts / "run_nba_late_fetch.ps1").write_text(_FAKE_LATE_FETCH, encoding="utf-8")
    (scripts / "log_prop_snapshot.ps1").write_text(_FAKE_SNAPSHOT, encoding="utf-8")

    proc = subprocess.run(
        [
            _pwsh(),
            "-NoProfile",
            "-File",
            str(scripts / "run_refresh_with_log.ps1"),
            "-RunLabel",
            "TESTABORT",
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=60,
    )
    combined = (proc.stdout or "") + "\n" + (proc.stderr or "")
    assert proc.returncode != 0, combined
    assert "stopping before lock/fetch" in combined or "FAILED — aborting" in combined
    assert not (tmp_path / "FETCHED").exists(), "late fetch ran after smoke-import failed"
    assert not (tmp_path / "SNAPSHOT").exists(), "snapshot ran after smoke-import failed"
    lock = tmp_path / "data" / "cache" / "refresh.lock"
    assert not lock.exists(), f"lock written after smoke-import failed: {lock.read_text(encoding='utf-8')}"
