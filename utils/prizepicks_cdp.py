"""Shared PrizePicks CDP (Chrome DevTools) in-page fetch helpers.

Used when HTTP is blocked by DataDome. Attach to an already-warmed Chrome on
--remote-debugging-port (default 9222), then call api.prizepicks.com via the
page's authenticated fetch().
"""
from __future__ import annotations

from pathlib import Path
from typing import Any


def connect_over_cdp(playwright: Any, cdp_url: str, *, timeout_ms: int = 30_000):
    """Attach to Chrome. Explicit timeout avoids Playwright's 180s default hang."""
    cdp = (cdp_url or "").strip()
    if not cdp:
        raise ValueError("cdp_url is required")
    print(f"🌐 Connecting to existing Chrome via CDP: {cdp} (timeout={timeout_ms}ms)")
    return playwright.chromium.connect_over_cdp(cdp, timeout=int(timeout_ms))


def probe_cdp_attach(cdp_url: str, *, timeout_ms: int = 8_000) -> bool:
    """True only if Playwright can attach. json/version can succeed while connect hangs."""
    cdp = (cdp_url or "").strip()
    if not cdp:
        return False
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        return False
    try:
        with sync_playwright() as p:
            p.chromium.connect_over_cdp(cdp, timeout=int(timeout_ms))
        return True
    except Exception:
        return False


def align_cdp_context_for_datadome(context: Any) -> None:
    try:
        context.grant_permissions(
            ["geolocation", "notifications"],
            origin="https://app.prizepicks.com",
        )
    except Exception:
        pass
    try:
        context.set_geolocation({"latitude": 33.7490, "longitude": -84.3880})
    except Exception:
        pass


def pick_cdp_warmed_page(context: Any, league_id: str | None = None) -> Any | None:
    try:
        pages = list(context.pages)
    except Exception:
        return None
    league_needle = f"league_id={league_id}" if league_id else ""
    scored: list[tuple[int, Any]] = []
    for pg in pages:
        try:
            url = (pg.url or "").lower()
        except Exception:
            continue
        if "prizepicks.com" not in url:
            continue
        score = 10 if "app.prizepicks.com" in url else 0
        if "/board" in url:
            score += 20
        if league_needle and league_needle in url:
            score += 50
        scored.append((score, pg))
    if not scored:
        return None
    scored.sort(key=lambda t: t[0], reverse=True)
    return scored[0][1]


def cdp_board_ready(page: Any, league_id: str | None = None) -> bool:
    """True if this tab can run in-page fetch(). Any PP /board is enough — do not
    require league_id= in the URL (navigating leagues is what DataDome flags).
    """
    try:
        url = (page.url or "").lower()
    except Exception:
        return False
    if "app.prizepicks.com" not in url:
        return False
    return "/board" in url or "prizepicks.com" in url


def fetch_projections_inpage(
    page: Any,
    league_id: str,
    *,
    per_page: int = 250,
    request_timeout_ms: int = 25_000,
    max_pages: int = 20,
) -> tuple[list[dict], list[dict], int, str]:
    """In-page fetch for one league_id. Returns (data, included, status, url).

    Merges pregame (in_game=false) and live (in_game=true) boards and paginates
    each. Returning the first non-empty URL used to drop FG/FT/2PT markets that
    only exist on the pregame board after Popular/core stats.
    """
    payload = page.evaluate(
        """async ({ leagueId, perPage, timeoutMs, maxPages }) => {
            const hdrs = () => ({
                "accept": "application/json, text/plain, */*",
                "accept-language": (navigator.languages && navigator.languages.length)
                    ? navigator.languages.join(",") : "en-US,en;q=0.9",
                "referer": window.location.href,
                "x-requested-with": "XMLHttpRequest",
            });
            const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
            const fetchJson = async (url) => {
                const ctrl = new AbortController();
                const timer = setTimeout(() => ctrl.abort(), timeoutMs);
                try {
                    const r = await fetch(url, {
                        credentials: "include",
                        headers: hdrs(),
                        mode: "cors",
                        signal: ctrl.signal,
                    });
                    clearTimeout(timer);
                    if (!r.ok) return { ok: false, status: r.status, data: [], included: [], url };
                    const j = await r.json();
                    return {
                        ok: true,
                        status: r.status,
                        data: Array.isArray(j?.data) ? j.data : [],
                        included: Array.isArray(j?.included) ? j.included : [],
                        url,
                    };
                } catch (e) {
                    clearTimeout(timer);
                    return { ok: false, status: 0, data: [], included: [], url, error: String(e) };
                }
            };

            const seen = new Set();
            const allData = [];
            const allIncluded = [];
            let lastStatus = 0;
            let lastUrl = '';
            // Pregame first: FG Made/Attempted, FT, 2PT live on in_game=false.
            const flags = ["false", "true"];
            for (const inGame of flags) {
                for (let pageNum = 1; pageNum <= maxPages; pageNum++) {
                    const url = `https://api.prizepicks.com/projections?league_id=${leagueId}`
                        + `&per_page=${perPage}&single_stat=true&in_game=${inGame}`
                        + `&game_mode=pickem&page=${pageNum}&page[number]=${pageNum}`
                        + `&page[size]=${perPage}`;
                    const res = await fetchJson(url);
                    lastStatus = res.status || lastStatus;
                    lastUrl = res.url || lastUrl;
                    if (!res.ok) {
                        if (pageNum === 1) break;
                        break;
                    }
                    const data = res.data || [];
                    const included = res.included || [];
                    let added = 0;
                    for (const row of data) {
                        const id = row && row.id != null ? String(row.id) : '';
                        if (!id || seen.has(id)) continue;
                        seen.add(id);
                        allData.push(row);
                        added += 1;
                    }
                    for (const obj of included) allIncluded.push(obj);
                    if (data.length === 0 || added === 0) break;
                    if (data.length < perPage) break;
                    await sleep(350);
                }
            }
            return { data: allData, included: allIncluded, status: lastStatus || (allData.length ? 200 : 0), url: lastUrl };
        }""",
        {
            "leagueId": str(league_id),
            "perPage": int(per_page),
            "timeoutMs": int(request_timeout_ms),
            "maxPages": int(max_pages),
        },
    )
    data = list((payload or {}).get("data") or [])
    included = list((payload or {}).get("included") or [])
    status = int((payload or {}).get("status") or 0)
    url = str((payload or {}).get("url") or "")
    return data, included, status, url


def projection_counts_from_leagues_payload(body: Any) -> dict[str, int]:
    """Map PrizePicks league id -> projections_count. Unknown shape returns {}."""
    rows: list = []
    if isinstance(body, dict):
        raw = body.get("data") or body.get("leagues") or []
        rows = raw if isinstance(raw, list) else []
    elif isinstance(body, list):
        rows = body
    out: dict[str, int] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        attrs = row.get("attributes") if isinstance(row.get("attributes"), dict) else row
        lid = str(row.get("id") or (attrs or {}).get("id") or "").strip()
        raw_n = None
        if isinstance(attrs, dict):
            for key in ("projections_count", "projection_count", "projectionsCount"):
                if attrs.get(key) is not None:
                    raw_n = attrs.get(key)
                    break
        if not lid or raw_n is None:
            continue
        try:
            out[lid] = int(raw_n)
        except (TypeError, ValueError):
            continue
    return out


def fetch_league_projection_counts_inpage(page: Any) -> dict[str, int]:
    """One in-page GET /leagues. Empty dict means 'do not skip boards'."""
    try:
        raw = page.evaluate(
            """async () => {
                const ctrl = new AbortController();
                const timer = setTimeout(() => ctrl.abort(), 12000);
                try {
                    const r = await fetch("https://api.prizepicks.com/leagues", {
                        credentials: "include",
                        signal: ctrl.signal,
                        headers: {
                            "accept": "application/json",
                            "referer": window.location.href,
                            "x-requested-with": "XMLHttpRequest",
                        },
                    });
                    let body = null;
                    try { body = await r.json(); } catch (e) { body = null; }
                    return { status: r.status, body };
                } finally {
                    clearTimeout(timer);
                }
            }"""
        )
    except Exception as exc:
        print(f"  [leagues] count lookup failed ({type(exc).__name__}: {exc}) — fetching every board")
        return {}
    status = int((raw or {}).get("status") or 0)
    counts = projection_counts_from_leagues_payload((raw or {}).get("body"))
    if status != 200 or not counts or not any(n > 0 for n in counts.values()):
        print(f"  [leagues] count lookup status={status} leagues={len(counts)} — fetching every board")
        return {}
    print(f"  [leagues] projection counts for {len(counts)} leagues")
    return counts


def league_projection_counts_via_cdp(cdp_url: str, *, timeout_ms: int = 15_000) -> dict[str, int]:
    """Attach once and read /leagues. Does not open a new PrizePicks tab."""
    from playwright.sync_api import sync_playwright

    cdp = (cdp_url or "").strip()
    if not cdp:
        return {}
    with sync_playwright() as p:
        browser = connect_over_cdp(p, cdp, timeout_ms=timeout_ms)
        if not browser.contexts:
            return {}
        context = browser.contexts[0]
        page = pick_cdp_warmed_page(context)
        if page is None or not cdp_board_ready(page):
            print("  [leagues] no warmed PrizePicks tab — fetching every board")
            return {}
        return fetch_league_projection_counts_inpage(page)


def _league_counts_cache_path() -> Path:
    return Path(__file__).resolve().parents[1] / "data" / "cache" / "pp_league_counts.json"


def read_shared_league_counts(max_age_s: int = 7200) -> dict[str, int] | None:
    """Return this window's /leagues counts, or None when missing, stale, or empty."""
    import json
    import time

    path = _league_counts_cache_path()
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        age = time.time() - float(payload.get("saved_at") or 0)
        raw = payload.get("counts") or {}
        if age > max_age_s or not isinstance(raw, dict) or not raw:
            return None
        counts = {str(k): int(v) for k, v in raw.items()}
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return None
    if not counts or not any(n > 0 for n in counts.values()):
        return None
    return counts


def write_shared_league_counts(counts: dict[str, int]) -> None:
    import json
    import time

    if not counts or not any(int(n) > 0 for n in counts.values()):
        return
    path = _league_counts_cache_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"saved_at": time.time(), "counts": {str(k): int(v) for k, v in counts.items()}}),
        encoding="utf-8",
    )


def shared_league_counts(cdp_url: str = "", page: Any = None) -> dict[str, int]:
    """One /leagues read per window, shared across board processes.

    A missing, failed, or empty read returns {} so callers fetch every board.
    A real zero for a league is a skip. Do not treat {} as 'every league is empty'.
    """
    cached = read_shared_league_counts()
    if cached:
        print(
            f"  [leagues] shared projection counts ({len(cached)} leagues, "
            "file age under 2h, no extra /leagues call)"
        )
        return cached
    counts: dict[str, int] = {}
    if page is not None:
        counts = fetch_league_projection_counts_inpage(page)
    elif (cdp_url or "").strip():
        counts = league_projection_counts_via_cdp(cdp_url)
    if counts:
        write_shared_league_counts(counts)
    return counts


def session_fetch_projections(
    league_id: str,
    *,
    cdp_url: str = "",
    playwright: bool = False,
    per_page: int = 250,
    attach_timeout_ms: int = 30_000,
    request_timeout_ms: int = 25_000,
    max_pages: int = 20,
) -> tuple[list[dict], list[dict], int]:
    """Fetch one league via warmed CDP Chrome, or a launched Playwright Chromium.

    Pipeline contract: HTTP first (caller), then this with cdp_url, then playwright=True.
    Returns (data, included, http_status).
    """
    from playwright.sync_api import sync_playwright

    lid = str(league_id).strip()
    cdp = (cdp_url or "").strip()
    if not cdp and not playwright:
        raise ValueError("session_fetch_projections requires cdp_url or playwright=True")

    with sync_playwright() as p:
        opened_new = False
        launched = False
        if cdp:
            browser = connect_over_cdp(p, cdp, timeout_ms=attach_timeout_ms)
            if not browser.contexts:
                raise RuntimeError("CDP browser has no contexts; start Chrome with --remote-debugging-port.")
            context = browser.contexts[0]
            align_cdp_context_for_datadome(context)
            page = pick_cdp_warmed_page(context, lid)
            if page is not None:
                print(f"  Reusing warmed PP tab: {page.url}")
            else:
                page = context.new_page()
                opened_new = True
                print("  No warmed PP tab — opened new page (solve DataDome in Chrome if 403).")
        else:
            launched = True
            browser = p.chromium.launch(headless=False)
            context = browser.new_context()
            align_cdp_context_for_datadome(context)
            page = context.new_page()
            opened_new = True
            print("  [playwright] launched Chromium for PrizePicks board fetch")

        page.set_default_timeout(max(30_000, int(request_timeout_ms) + 5_000))
        if not cdp_board_ready(page, lid):
            board_url = f"https://app.prizepicks.com/board?league_id={lid}"
            page.goto("https://app.prizepicks.com/", wait_until="domcontentloaded", timeout=30_000)
            page.wait_for_timeout(2000)
            page.goto(board_url, wait_until="domcontentloaded", timeout=30_000)
            page.wait_for_timeout(3000)
        else:
            try:
                page.bring_to_front()
            except Exception:
                pass
            page.wait_for_timeout(800)

        data, included, status, url = fetch_projections_inpage(
            page,
            lid,
            per_page=per_page,
            request_timeout_ms=request_timeout_ms,
            max_pages=max_pages,
        )
        print(f"  [PP session] league_id={lid} status={status} rows={len(data)} url={url}")
        if opened_new:
            try:
                page.close()
            except Exception:
                pass
        if launched:
            try:
                browser.close()
            except Exception:
                pass
        return data, included, status
