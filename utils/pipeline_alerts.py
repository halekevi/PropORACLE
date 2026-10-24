"""Append pipeline warnings to logs/pipeline_alerts.log and optionally ntfy."""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_LOG = _REPO / "logs" / "pipeline_alerts.log"


def _webhook() -> str:
    return (
        str(os.environ.get("PROPORACLE_ALERT_WEBHOOK") or "").strip()
        or str(os.environ.get("PROPORACLE_NTFY_URL") or "").strip()
    )


def _critical_webhook(hook: str) -> str:
    """Quiet topic that carries only FAIL-level alerts (see Publish-LiveSite.ps1 Send-PublishPush).

    The main topic gets ~20 routine WARN lines per pipeline run, which is how twelve publish
    refusals went unnoticed on 2026-10-23/24. Default for ntfy is ``<main topic>-critical``.
    """
    crit = str(os.environ.get("PROPORACLE_ALERT_WEBHOOK_CRITICAL") or "").strip()
    if crit:
        return crit
    if "ntfy." in hook:
        return hook.rstrip("/") + "-critical"
    return ""


def _post(hook: str, *, title: str, text: str, priority: str, tags: str) -> str:
    """POST one message; return a short status note ("ntfy 200", "error: ...")."""
    try:
        if "ntfy." in hook:
            req = urllib.request.Request(
                hook,
                data=text.encode("utf-8"),
                method="POST",
                headers={
                    "Content-Type": "text/plain; charset=utf-8",
                    "Title": title,
                    "Priority": priority,
                    "Tags": tags,
                },
            )
        else:
            body = json.dumps({"content": text, "text": text}).encode("utf-8")
            req = urllib.request.Request(
                hook,
                data=body,
                method="POST",
                headers={"Content-Type": "application/json"},
            )
        with urllib.request.urlopen(req, timeout=10) as resp:
            return f"{'ntfy' if 'ntfy.' in hook else 'webhook'} {getattr(resp, 'status', 200)}"
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        return f"error: {exc}"


def emit_pipeline_alert(
    stage: str, reason: str, *, notify: bool = True, critical: bool = False
) -> str:
    """Write one alert line; post to ntfy/Discord when a webhook is configured.

    ``critical=True`` marks a FAIL (live site stale, publish refused, grader dead): the line is
    logged as FAIL, posted to the main topic at ntfy priority ``urgent`` and additionally to the
    quiet critical topic. Returns the push status note so callers can log it.
    """
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    stage_s = str(stage or "pipeline").strip() or "pipeline"
    reason_s = str(reason or "").strip() or "(no reason)"
    level = "FAIL" if critical else "WARN"
    line = f"[{ts}] {level} ({stage_s}): {reason_s}"
    print(f"  [ALERT] {line}")
    try:
        _LOG.parent.mkdir(parents=True, exist_ok=True)
        with _LOG.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except OSError:
        pass
    if not notify:
        return "not sent (notify=False)"
    hook = _webhook()
    if not hook:
        return "no hook (PROPORACLE_ALERT_WEBHOOK unset)"
    text = f"PropORACLE pipeline {level} ({stage_s}): {reason_s}"
    if not critical:
        return _post(hook, title="PropORACLE pipeline WARN", text=text, priority="default", tags="warning")
    notes = [_post(hook, title=f"PropORACLE {stage_s} FAILED", text=text, priority="urgent", tags="rotating_light")]
    crit = _critical_webhook(hook)
    if crit and crit != hook:
        notes.append(_post(crit, title=f"PropORACLE {stage_s} FAILED", text=text, priority="urgent", tags="rotating_light"))
    note = "; ".join(notes)
    try:
        with _LOG.open("a", encoding="utf-8") as fh:
            fh.write(f"[{ts}] PUSH ({stage_s}): {note}\n")
    except OSError:
        pass
    return note
