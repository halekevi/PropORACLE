#!/usr/bin/env python3
"""Scrape all-Demon Power N-correct from a live PrizePicks CDP session.

Never stores 1st-place as the quote. Writes
``data/reports/demon_n_correct_YYYY-MM-DD.json`` and
``data/reports/demon_n_correct_latest.json``.

  py -3.14 scripts/scrape_demon_n_correct.py

Does not ticket Demons. Does not write /tickets.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

import collect_payout_data as P  # noqa: E402

OUT_DIR = ROOT / "data" / "reports"
LATEST = OUT_DIR / "demon_n_correct_latest.json"
LEAGUES = ((7, "NBA"), (9, "NFL"), (2, "MLB"), (3, "WNBA"))
N_LEGS = (2, 3, 4, 5, 6)
# Standard Power 1st-place table. These are never Demon N-correct.
STD_POWER_FIRST = {2: 3.0, 3: 6.0, 4: 10.0, 5: 20.0, 6: 37.5}
_N_CORRECT_RE = re.compile(
    r"(\d+)\s*correct\s*pays[\s\n]*(\d+\.?\d*)[Xx]",
    re.IGNORECASE,
)
_FIRST_RE = re.compile(
    r"1st\s*place\s*pays[\s\n]*(\d+\.?\d*)[Xx]",
    re.IGNORECASE,
)


def _power_section(text: str) -> str:
    pp = re.search(
        r"Power\s*Play\b(.*?)(?:Reversion lineup|Entry Fee|Deposit Funds|$)",
        text,
        re.IGNORECASE | re.DOTALL,
    )
    return pp.group(0) if pp else text


def _to_win_and_entry(text: str) -> tuple[float | None, float | None]:
    """To Win / Entry Fee boxes. Never account Balance."""
    cleaned = re.sub(r"Balance:[\s$0-9.,]+", " ", text, flags=re.I)
    to_win = None
    m = re.search(
        r"To\s*Win[\s\n]*\$?\s*([0-9][0-9,]*(?:\.\d+)?)",
        cleaned,
        re.IGNORECASE,
    )
    if m:
        try:
            to_win = float(m.group(1).replace(",", ""))
        except ValueError:
            to_win = None
        if to_win is not None and to_win < 1:
            to_win = None
    entry = None
    em = re.search(
        r"Entry\s*Fee[\s\n]*\$?\s*([0-9][0-9,]*(?:\.\d+)?)",
        cleaned,
        re.IGNORECASE,
    )
    if em:
        try:
            entry = float(em.group(1).replace(",", ""))
        except ValueError:
            entry = None
        if entry is not None and entry <= 0:
            entry = None
    return to_win, entry


def power_n_correct_x(slip: dict[str, Any], n_legs: int) -> float | None:
    """Raw Power 'N correct pays' figure. Not trusted by itself."""
    text = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
    if not text:
        return None
    hits = _N_CORRECT_RE.findall(_power_section(text))
    for n_s, x_s in hits:
        try:
            if int(n_s) != int(n_legs):
                continue
            val = float(x_s)
        except (TypeError, ValueError):
            continue
        if val > 1.0:
            return val
    return None


def parse_demon_power_quote(
    text: str,
    *,
    n_legs: int,
    entry_amount: float | None = None,
) -> dict[str, Any]:
    """Power N-correct for one slip. Fail closed unless To Win / Entry agrees.

    Never stores 1st-place. Standard Power 3/6/10/20/37.5 is the wrong panel.
    A 10× jump 2-leg → 3-leg with no To Win is untrusted (Balance parsed as
    To Win, or the wrong panel).
    """
    text = str(text or "")
    section = _power_section(text)
    n_correct = None
    hits = _N_CORRECT_RE.findall(section)
    for n_s, x_s in hits:
        try:
            if int(n_s) != int(n_legs):
                continue
            n_correct = float(x_s)
        except (TypeError, ValueError):
            continue
    first = None
    fm = _FIRST_RE.search(section)
    if fm:
        try:
            first = float(fm.group(1))
        except ValueError:
            first = None
    to_win, entry = _to_win_and_entry(text)
    if entry is None and entry_amount and entry_amount > 0:
        entry = float(entry_amount)
    implied = None
    if to_win is not None and entry is not None and entry > 0:
        implied = round(to_win / entry, 4)
    lineup_type = "power"
    reversion = "reversion lineup" in text.lower()
    reasons: list[str] = []
    quote = implied if implied is not None else n_correct
    if first is not None and n_legs in STD_POWER_FIRST:
        if first == STD_POWER_FIRST[n_legs] or (
            n_correct is not None and abs(n_correct - STD_POWER_FIRST[n_legs]) < 1e-6
        ):
            if implied is None:
                reasons.append("looks_like_standard_power_first_place_panel")
    if n_correct is not None and implied is not None:
        if abs(n_correct - implied) / implied > 0.08:
            # 40 vs 4.0, 90 vs 9.0, 215 vs 21.5, 540 vs 54.0
            if abs(n_correct / 10.0 - implied) / implied <= 0.08:
                reasons.append("label_10x_vs_to_win")
                quote = implied
            else:
                reasons.append("n_correct_disagrees_with_to_win")
    if implied is None:
        reasons.append("missing_to_win_entry")
        if quote is not None and n_legs in STD_POWER_FIRST:
            if abs(quote - STD_POWER_FIRST[n_legs]) < 1e-6:
                reasons.append("quote_is_standard_power_first_place")
                quote = None
    trusted = bool(
        implied is not None
        and quote is not None
        and quote > 1
        and "quote_is_standard_power_first_place" not in reasons
        and "n_correct_disagrees_with_to_win" not in reasons
        and "looks_like_standard_power_first_place_panel" not in reasons
    )
    if not trusted:
        quote = implied if implied is not None and implied > 1 else None
        if quote is None:
            reasons.append("quote_untrusted")
    return {
        "n_legs": int(n_legs),
        "n_correct_raw": n_correct,
        "first_place_x": first,
        "to_win": to_win,
        "entry": entry,
        "implied_x": implied,
        "power_x": quote if trusted else None,
        "lineup_type": lineup_type,
        "reversion_disclaimer": reversion,
        "quote_trusted": trusted,
        "invalid_reason": None if trusted else ",".join(reasons) or "quote_untrusted",
    }


def ladder_is_implausible(quotes_by_k: dict[int, float]) -> bool:
    """Same-mix 3-leg BE below 6-leg BE (or a 10× 2→3 jump) is not a real table."""
    def be(k: int, x: float) -> float:
        return float(x ** (-1.0 / k))

    q2, q3, q6 = quotes_by_k.get(2), quotes_by_k.get(3), quotes_by_k.get(6)
    if q2 and q3 and q3 > q2 * 8:
        return True
    if q3 and q6 and be(3, q3) + 0.005 < be(6, q6):
        return True
    return False


def _unique_demons(cards: list[dict]) -> list[dict]:
    seen: set[str] = set()
    out: list[dict] = []
    for c in cards:
        if str(c.get("pick_type") or "").lower() != "demon":
            continue
        player = P._norm(c.get("player"))
        if not player or player in seen:
            continue
        seen.add(player)
        out.append(c)
    return out


def _click_all_demon(frame, page, demons: list[dict], n: int, delay_sec: float) -> dict[str, Any]:
    rec: dict[str, Any] = {
        "n_legs": n,
        "n_demon": n,
        "n_goblin": 0,
        "n_standard": 0,
        "power_min_x": None,
        "power_first_x": None,
        "status": "failed",
        "error": None,
        "legs": [],
    }
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.dismiss_modal(frame, page)
    P.set_ticket_type(frame, "power")
    frame.wait_for_timeout(int(max(0.1, delay_sec) * 1000))
    clicked = 0
    legs: list[dict] = []
    current_tab = None
    for card in demons[:n]:
        tab = str(card.get("source_filter") or "Popular")
        ok = False
        try:
            if tab != current_tab:
                P.dismiss_modal(frame, page)
                tloc = frame.get_by_text(tab, exact=True).first
                if tloc.count() == 0:
                    tloc = frame.get_by_text(tab, exact=False).first
                tloc.click(force=True, timeout=2000)
                frame.wait_for_timeout(900)
                P._scroll_board_for_lazy_load(page)
                current_tab = tab
            P.dismiss_modal(frame, page)
            fresh = P.get_all_cards(frame)
            resolved = P.resolve_leg_card(card, fresh)
            if resolved is not None:
                ok = P.click_leg(frame, resolved, "OVER")
                if ok:
                    card = resolved
        except Exception as exc:
            print(f"  [WARN] demon click {card.get('player')}: {exc}")
        if not ok:
            ok = P.add_leg(
                frame,
                page,
                {
                    "player": card.get("player"),
                    "prop_type": card.get("prop_type"),
                    "direction": "OVER",
                },
            )
        if ok:
            clicked += 1
            legs.append(
                {
                    "player": card.get("player"),
                    "prop_type": card.get("prop_type"),
                    "line": card.get("line"),
                    "pick_type": "demon",
                }
            )
        frame.wait_for_timeout(int(max(0.05, delay_sec * 0.5) * 1000))
    rec["legs"] = legs
    if clicked != n:
        rec["error"] = f"clicked_{clicked}_of_{n}"
        return rec
    slip = P.read_slip(frame, n_legs=clicked, ticket_type="power") or {}
    n_sel = slip.get("n_selected")
    try:
        n_sel_i = int(n_sel) if n_sel is not None else int(clicked)
    except (TypeError, ValueError):
        n_sel_i = int(clicked)
    if n_sel_i != n:
        rec["error"] = f"slip_n_{n_sel_i}_expected_{n}"
        return rec
    parsed = parse_demon_power_quote(
        str(slip.get("raw_slip_section") or slip.get("raw_text") or ""),
        n_legs=n,
        entry_amount=20.0,
    )
    rec.update(parsed)
    rec["power_min_x"] = parsed.get("power_x")
    rec["power_first_x"] = parsed.get("first_place_x")
    rec["never_1st_place"] = True
    rec["fixed_onestep_table"] = False
    if not parsed.get("quote_trusted"):
        rec["status"] = "untrusted"
        rec["error"] = parsed.get("invalid_reason") or "quote_untrusted"
        rec["power_x"] = None
        rec["power_min_x"] = None
    else:
        rec["status"] = "ok"
        rec["error"] = None
    print(
        f"  [DEMON] {n}-leg trusted={parsed.get('quote_trusted')} "
        f"N-correct={parsed.get('power_x')} raw={parsed.get('n_correct_raw')} "
        f"to_win={parsed.get('to_win')} 1st={parsed.get('first_place_x')} "
        f"({parsed.get('invalid_reason')})"
    )
    return rec


def capture(*, cdp_url: str, delay_sec: float, date_str: str) -> dict[str, Any]:
    captured_at = datetime.now(timezone.utc).isoformat()
    p, browser, context, page = P.connect_existing_browser(cdp_url)
    page.wait_for_timeout(500)
    rows: list[dict[str, Any]] = []
    league_used = None
    try:
        demons: list[dict] = []
        frame = None
        for league_id, label in LEAGUES:
            try:
                url = f"https://app.prizepicks.com/board?league_id={league_id}"
                print(f"[demon-n-correct] {label} -> {url}")
                page.goto(url, wait_until="domcontentloaded", timeout=45_000)
                page.wait_for_timeout(2500)
            except Exception as exc:
                print(f"[demon-n-correct] navigate {label} skipped: {exc}")
                continue
            frame = P.find_prizepicks_frame(page)
            P.ensure_popular_filter(frame, page)
            P.dismiss_modal(frame, page)
            cards = P.expand_card_pool(frame, page)
            demons = _unique_demons(cards)
            print(f"[demon-n-correct] {label} demons={len(demons)}")
            if len(demons) >= 6:
                league_used = label
                break
            if len(demons) >= 2 and league_used is None:
                league_used = label
        if frame is None or len(demons) < 2:
            return {
                "ok": False,
                "error": "need_at_least_2_demon_faces",
                "n_demon_faces": len(demons),
                "rows": [],
            }
        for n in N_LEGS:
            if len(demons) < n:
                print(f"[demon-n-correct] skip {n}-leg (only {len(demons)} faces)")
                continue
            print(f"[demon-n-correct] {n}-leg all-Demon Power")
            rec = _click_all_demon(frame, page, demons, n, delay_sec)
            rec["payout_source"] = "live_cdp"
            rec["captured_at"] = captured_at
            rec["league"] = league_used
            rec["never_1st_place"] = True
            rows.append(rec)
            try:
                P.clear_slip(frame)
                _, frame = P.verify_slip_empty(frame, page)
            except Exception:
                pass
    finally:
        try:
            browser.close()
        except Exception:
            pass
        try:
            p.stop()
        except Exception:
            pass
    trusted = [r for r in rows if r.get("quote_trusted") and r.get("power_x")]
    by_k = {
        int(r["n_legs"]): float(r["power_x"])
        for r in trusted
        if r.get("n_legs") and r.get("power_x")
    }
    payload = {
        "track": "demon_n_correct",
        "captured_at": captured_at,
        "payout_source": "live_cdp",
        "never_1st_place": True,
        "ticketed": False,
        "fixed_onestep_table": False,
        "date": date_str,
        "league": league_used,
        "n_ok": len(trusted),
        "six_leg_power_x": None,
        "ladder_implausible": ladder_is_implausible(by_k),
        "rows": rows,
    }
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    dated = OUT_DIR / f"demon_n_correct_{date_str}.json"
    dated.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    LATEST.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(
        f"[demon-n-correct] wrote {dated} trusted={len(trusted)} "
        f"implausible_ladder={payload['ladder_implausible']}"
    )
    return payload


def main() -> int:
    ap = argparse.ArgumentParser(description="Scrape all-Demon Power N-correct (not 1st-place)")
    ap.add_argument("--cdp-url", default="http://127.0.0.1:9222")
    ap.add_argument("--delay-sec", type=float, default=0.5)
    ap.add_argument("--date", default="")
    args = ap.parse_args()
    date_str = str(args.date or "").strip()[:10] or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    P._acquire_payout_capture_lock()
    try:
        payload = capture(
            cdp_url=args.cdp_url,
            delay_sec=float(args.delay_sec),
            date_str=date_str,
        )
    finally:
        P._release_payout_capture_lock()
    if payload.get("ladder_implausible") or not any(
        r.get("quote_trusted") for r in payload.get("rows") or []
    ):
        print("[demon-n-correct] no trusted per-slip N-correct — Demon quotes invalid")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
