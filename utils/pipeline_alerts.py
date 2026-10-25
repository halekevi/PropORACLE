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


_SPOOL = _REPO / "logs" / "pipeline_alerts_pending.jsonl"
DIGEST_MAX_CHARS = 3800  # ntfy body cap is 4 KiB


def _digest_enabled() -> bool:
    """Routine WARNs are spooled and sent as one digest per run (flush_pipeline_alert_digest).

    ~20 WARN pushes per run buried the publish FAILs on 2026-10-23/24. Set
    PROPORACLE_ALERT_DIGEST=0 to go back to one push per warning.
    """
    raw = str(os.environ.get("PROPORACLE_ALERT_DIGEST") or "").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _run_label() -> str:
    return str(os.environ.get("PROPORACLE_RUN_LABEL") or "").strip()


def _spool(stage: str, reason: str, ts: str) -> str:
    rec = {"ts": ts, "stage": stage, "reason": reason, "run": _run_label()}
    try:
        _SPOOL.parent.mkdir(parents=True, exist_ok=True)
        with _SPOOL.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
        return "spooled for digest"
    except OSError as exc:
        return f"spool error: {exc}"


def pending_alerts() -> list[dict]:
    out: list[dict] = []
    try:
        with _SPOOL.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except ValueError:
                    continue
    except OSError:
        pass
    return out


def flush_pipeline_alert_digest(label: str = "", *, min_age_sec: float = 0.0) -> str:
    """Post one message listing every spooled WARN, then clear the spool.

    ``min_age_sec`` > 0 only flushes when the oldest spooled line is at least that old
    (used by the hourly freshness task to drain a run that died before its own flush).
    Returns a status note.
    """
    recs = pending_alerts()
    if not recs:
        return "digest: nothing pending"
    if min_age_sec > 0:
        try:
            oldest = datetime.strptime(recs[0]["ts"], "%Y-%m-%d %H:%M:%S UTC").replace(tzinfo=timezone.utc)
        except (KeyError, ValueError):
            oldest = datetime.now(timezone.utc)
        if (datetime.now(timezone.utc) - oldest).total_seconds() < min_age_sec:
            return f"digest: {len(recs)} pending, younger than {int(min_age_sec)}s - not flushed"
    label = label or _run_label() or next((r.get("run") for r in recs if r.get("run")), "") or "pipeline"
    # Group identical stage+reason (the same warning often fires in several windows).
    counts: dict[tuple[str, str], int] = {}
    for r in recs:
        key = (str(r.get("stage") or ""), str(r.get("reason") or ""))
        counts[key] = counts.get(key, 0) + 1
    by_stage: dict[str, list[str]] = {}
    for (stage, reason), n in counts.items():
        by_stage.setdefault(stage, []).append(f"{reason}" + (f" (x{n})" if n > 1 else ""))
    lines = [f"PropORACLE pipeline digest ({label}): {len(recs)} warning(s), {len(counts)} distinct"]
    for stage in sorted(by_stage):
        lines.append(f"[{stage}]")
        lines.extend(f" - {r}" for r in by_stage[stage])
    text = "\n".join(lines)
    if len(text) > DIGEST_MAX_CHARS:
        text = text[: DIGEST_MAX_CHARS - 40].rstrip() + f"\n… (+{len(counts)} distinct; see logs/pipeline_alerts.log)"
    hook = _webhook()
    note = "no hook" if not hook else _post(
        hook, title=f"PropORACLE pipeline digest ({label})", text=text, priority="low", tags="page_facing_up"
    )
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    try:
        with _LOG.open("a", encoding="utf-8") as fh:
            fh.write(f"[{ts}] DIGEST ({label}): {len(recs)} warning(s) -> {note}\n")
    except OSError:
        pass
    try:
        _SPOOL.unlink(missing_ok=True)
    except OSError:
        pass
    return f"digest: {len(recs)} warning(s) -> {note}"


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
        if _digest_enabled():
            return _spool(stage_s, reason_s, ts)
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
