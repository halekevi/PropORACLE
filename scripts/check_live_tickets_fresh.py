"""Independent freshness check on what Railway actually serves at /tickets.

Why: on 2026-10-23/24 every publish window for ~20 hours was refused on the pre-publish tree
check, the alerts were buried among routine pipeline WARNs, and /tickets served the 5:15 AM
card all day. This check does not look at the pipeline or at git; it fetches the live
``/api/tickets-latest`` and asks three questions:

1. Is the served slate ``date`` the one /tickets should show right now (ET today, or ET
   tomorrow once the 9 PM day-ahead window has had time to land)?
2. Is ``generated_at`` recent enough for the time of day?
3. Is the served card behind what origin/main carries (GitHub raw / Railway cache lag)?

A failed 1 or 2 is a FAIL (urgent push to the main topic + the quiet critical topic via
``utils.pipeline_alerts``); 3 is a WARN. Status is written to logs/LAST_LIVE_FRESHNESS.json and
the same reason is not re-pushed more often than once per hour.

Usage::

    py -3.14 scripts/check_live_tickets_fresh.py            # check + alert
    py -3.14 scripts/check_live_tickets_fresh.py --no-alert # print only
    py -3.14 scripts/check_live_tickets_fresh.py --json

Env: PROPORACLE_LIVE_BASE_URL (default the Railway app), PROPORACLE_ALERT_WEBHOOK(_CRITICAL).
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from utils.pipeline_alerts import emit_pipeline_alert  # noqa: E402

ET = ZoneInfo("America/New_York")
DEFAULT_BASE = "https://web-production-f280f.up.railway.app"
STATUS_PATH = _REPO / "logs" / "LAST_LIVE_FRESHNESS.json"
REALERT_MINUTES = 60

# generated_at must be at most this old, by ET hour of day. Windows: 1AM / 5AM / 8AM / 9AM /
# 10:30 / 1PM / 4:30 / 9PM / 11PM / 12AM, so during the day a card older than ~5h means at
# least two windows failed to land.
def max_age_hours(et_now: datetime) -> float:
    h = et_now.hour
    if 9 <= h < 18:
        return 5.0
    if 18 <= h < 24:
        return 7.0
    return 9.0  # overnight (1AM / 5AM windows)


def expected_dates(et_now: datetime) -> list[str]:
    """Slate dates /tickets may legitimately show right now (first = preferred)."""
    today = et_now.date()
    tomorrow = today + timedelta(days=1)
    hm = et_now.hour * 60 + et_now.minute
    if hm >= 21 * 60 + 45:
        return [tomorrow.isoformat()]
    if hm >= 20 * 60:
        return [tomorrow.isoformat(), today.isoformat()]
    return [today.isoformat()]


def _ssl_context():
    """certifi bundle when present: the Windows store Python 3.14 reads here rejects the Railway
    chain ("certificate has expired") while requests/certifi accept it."""
    import ssl

    try:
        import certifi

        return ssl.create_default_context(cafile=certifi.where())
    except Exception:  # noqa: BLE001
        return ssl.create_default_context()


def _fetch_json(url: str, timeout: int = 30) -> dict:
    req = urllib.request.Request(url, headers={"Cache-Control": "no-cache", "User-Agent": "proporacle-freshness/1"})
    with urllib.request.urlopen(req, timeout=timeout, context=_ssl_context()) as resp:
        return json.loads(resp.read().decode("utf-8-sig"))


def _parse_generated_at(s: str) -> datetime | None:
    s = str(s or "").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S UTC", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _origin_main_card() -> dict | None:
    """generated_at/date of ui_runner/templates/tickets_latest.json on origin/main (best effort)."""
    try:
        subprocess.run(["git", "-C", str(_REPO), "fetch", "-q", "origin", "main"], check=False, timeout=60,
                       capture_output=True)
        raw = subprocess.run(
            ["git", "-C", str(_REPO), "show", "origin/main:ui_runner/templates/tickets_latest.json"],
            check=True, timeout=60, capture_output=True,
        ).stdout
        j = json.loads(raw.decode("utf-8-sig"))
        return {"date": j.get("date"), "generated_at": j.get("generated_at"), "mode": j.get("mode")}
    except Exception:  # noqa: BLE001 - git may be absent / offline; the live check still stands alone
        return None


def _prev_status() -> dict:
    try:
        return json.loads(STATUS_PATH.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return {}


def _should_push(prev: dict, reason: str, now_utc: datetime) -> bool:
    if prev.get("ok") is not False or prev.get("reason") != reason:
        return True
    last = _parse_generated_at(prev.get("last_push_at") or "")
    return last is None or (now_utc - last) >= timedelta(minutes=REALERT_MINUTES)


def run(*, base: str, alert: bool) -> dict:
    now_utc = datetime.now(timezone.utc)
    et_now = now_utc.astimezone(ET)
    url = base.rstrip("/") + "/api/tickets-latest"
    out: dict = {
        "checked_at": now_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "et_now": et_now.strftime("%Y-%m-%d %H:%M ET"),
        "url": url,
        "expected_dates": expected_dates(et_now),
        "ok": False,
        "level": "FAIL",
        "reason": "",
        "warnings": [],
    }
    try:
        card = _fetch_json(url)
    except (urllib.error.URLError, TimeoutError, OSError, ValueError) as exc:
        out["reason"] = f"live /api/tickets-latest unreachable or not JSON: {exc}"
        return out

    served_date = str(card.get("date") or "")[:10]
    gen = _parse_generated_at(card.get("generated_at") or "")
    mode = str(card.get("mode") or "")
    groups = card.get("groups") or []
    n_tickets = sum(len(g.get("tickets") or []) for g in groups if isinstance(g, dict))
    out.update({
        "served_date": served_date,
        "served_generated_at": card.get("generated_at"),
        "served_mode": mode,
        "served_groups": len(groups),
        "served_tickets": n_tickets,
        "holds_applied": [h.get("hold") for h in (card.get("holds_applied") or []) if isinstance(h, dict)],
    })

    problems: list[str] = []
    if served_date not in out["expected_dates"]:
        problems.append(f"served slate date {served_date or '(none)'} but /tickets should show {' or '.join(out['expected_dates'])}")
    if gen is None:
        problems.append(f"generated_at unparseable: {card.get('generated_at')!r}")
    else:
        age_h = (now_utc - gen).total_seconds() / 3600.0
        out["served_age_hours"] = round(age_h, 2)
        limit = max_age_hours(et_now)
        out["max_age_hours"] = limit
        if age_h > limit:
            problems.append(f"served card generated {age_h:.1f}h ago (limit {limit:.0f}h at this hour) - publish windows are not landing")
    # Dual card = Goblin-70 groups first, mixer under. The `mode` tag alone is not proof: the
    # 2026-10-25 01:22 card said goblin70+graded_main and carried NFL mixer groups only
    # (NBA step4 died, combined + Goblin-70 were skipped, a bypass path pushed the stale file).
    names = [str(g.get("group_name") or "") for g in groups if isinstance(g, dict)]
    has_g70 = any(("Goblin-70" in n) or ("YOLO Goblin" in n) for n in names)
    has_mixer = any(not (("Goblin-70" in n) or ("YOLO Goblin" in n)) for n in names)
    out["served_has_goblin70"] = has_g70
    out["served_has_mixer"] = has_mixer
    if "goblin70+graded_main" not in mode:
        out["warnings"].append(f"served mode is {mode!r}, not the dual card")
    if not groups:
        problems.append("served card has no groups")
    elif not has_g70:
        problems.append("served card has no Goblin-70 groups (mixer-only) - Goblin-70 build did not land")

    origin = _origin_main_card()
    if origin:
        out["origin_main"] = origin
        og = _parse_generated_at(origin.get("generated_at") or "")
        if og and gen and (og - gen) > timedelta(minutes=10):
            out["warnings"].append(
                f"Railway serves generated_at {card.get('generated_at')} but origin/main carries {origin.get('generated_at')} - GitHub raw / Railway cache lag"
            )

    if problems:
        out["reason"] = "; ".join(problems)
    else:
        out["ok"] = True
        out["level"] = "WARN" if out["warnings"] else "OK"

    prev = _prev_status()
    out["last_push_at"] = prev.get("last_push_at")
    if alert:
        if not out["ok"]:
            if _should_push(prev, out["reason"], now_utc):
                note = emit_pipeline_alert(
                    "live-tickets-stale",
                    f"{out['reason']} (served {served_date} gen {card.get('generated_at')}; {url})",
                    critical=True,
                )
                out["push"] = note
                out["last_push_at"] = out["checked_at"]
            else:
                out["push"] = "suppressed (same reason within the last hour)"
        elif out["warnings"]:
            emit_pipeline_alert("live-tickets-lag", "; ".join(out["warnings"]))
    try:
        STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
        STATUS_PATH.write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")
    except OSError:
        pass
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--base", default=os.environ.get("PROPORACLE_LIVE_BASE_URL") or DEFAULT_BASE)
    ap.add_argument("--no-alert", action="store_true", help="print only; do not push or write WARN/FAIL lines")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()
    res = run(base=a.base, alert=not a.no_alert)
    if not a.no_alert:
        # Drain a WARN digest left behind by a run that died before its own flush.
        try:
            from utils.pipeline_alerts import flush_pipeline_alert_digest

            res["digest"] = flush_pipeline_alert_digest("orphaned run", min_age_sec=90 * 60)
        except Exception as exc:  # noqa: BLE001
            res["digest"] = f"error: {exc}"
    if a.json:
        print(json.dumps(res, indent=2, ensure_ascii=False))
    else:
        tag = "OK" if res["ok"] else "FAIL"
        print(f"[live-fresh] {tag} {res['et_now']} served date={res.get('served_date')} gen={res.get('served_generated_at')} "
              f"age={res.get('served_age_hours')}h groups={res.get('served_groups')} tickets={res.get('served_tickets')}")
        if res.get("reason"):
            print(f"  reason: {res['reason']}")
        for w in res.get("warnings", []):
            print(f"  warn: {w}")
        if res.get("push"):
            print(f"  push: {res['push']}")
    return 0 if res["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
