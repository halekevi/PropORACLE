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


def emit_pipeline_alert(stage: str, reason: str, *, notify: bool = True) -> None:
    """Write one alert line; post to ntfy/Discord when a webhook is configured."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    stage_s = str(stage or "pipeline").strip() or "pipeline"
    reason_s = str(reason or "").strip() or "(no reason)"
    line = f"[{ts}] WARN ({stage_s}): {reason_s}"
    print(f"  [ALERT] {line}")
    try:
        _LOG.parent.mkdir(parents=True, exist_ok=True)
        with _LOG.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except OSError:
        pass
    if not notify:
        return
    hook = _webhook()
    if not hook:
        return
    text = f"PropORACLE pipeline WARN ({stage_s}): {reason_s}"
    try:
        if "ntfy." in hook:
            req = urllib.request.Request(
                hook,
                data=text.encode("utf-8"),
                method="POST",
                headers={
                    "Content-Type": "text/plain; charset=utf-8",
                    "Title": "PropORACLE pipeline WARN",
                    "Priority": "default",
                    "Tags": "warning",
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
        urllib.request.urlopen(req, timeout=10).read()
    except (urllib.error.URLError, TimeoutError, OSError):
        pass
