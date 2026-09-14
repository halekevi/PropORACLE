"""Refresh windows must not overlap fetch/payout (8AM vs 9AM)."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REFRESH = (ROOT / "scripts" / "run_refresh_with_log.ps1").read_text(encoding="utf-8")
LATE = (ROOT / "scripts" / "run_nba_late_fetch.ps1").read_text(encoding="utf-8")
REGISTER = (ROOT / "scripts" / "Register_Daily_Task.ps1").read_text(encoding="utf-8")


def test_refresh_holds_lock_through_payout():
    assert "Hold the lock through payout CDP" in REFRESH
    assert "Payout scrape runs AFTER publish and lock release" not in REFRESH
    assert "Global\\PropORACLE_RefreshWindow" in REFRESH
    assert "$SkipFetchIfWaitedMinutes = 20" in REFRESH
    # Payout must sit inside the try that owns refresh.lock.
    payout_at = REFRESH.find("Payout CDP after fetch")
    finally_at = REFRESH.find("Lock released")
    assert payout_at > 0
    assert finally_at > payout_at


def test_refresh_skips_unconditional_goblin70_before_publish():
    # Publish-LiveSite Ensure-DualCard handles drift; late_fetch builds when lines move.
    assert "[switch]$RebuildGoblin70" in REFRESH
    assert "ignoring stale/other line stamp" in REFRESH
    assert "$forcePayout = $false" in REFRESH
    # Default publish path must not always rebuild Goblin-70.
    publish_fn_start = REFRESH.find("function Publish-RefreshWindow")
    publish_fn_end = REFRESH.find("$scriptExit = 0", publish_fn_start)
    body = REFRESH[publish_fn_start:publish_fn_end]
    assert "if ($RebuildGoblin70)" in body
    assert body.count("Ensuring Goblin-70 dual card before publish") == 1


def test_late_fetch_parent_keeps_lock_for_payout():
    assert "while still holding refresh.lock" in LATE
    assert "after refresh.lock is released so the next window can still fetch" not in LATE


def test_register_9am_waits_for_8am_payout():
    assert "through payout CDP" in REGISTER
    assert "Skips stacked fetch if it queued 20+ min" in REGISTER
