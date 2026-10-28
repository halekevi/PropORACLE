#!/usr/bin/env python3
"""Demon value scanner. ALERT ONLY — never tickets.

Reference is a Standard: two Standards pay 3x on a Power 2-pick, so
p_ref = 1/sqrt(3) ≈ 0.577. Each Demon is paired with a Standard from a
different team and a different game. Sanity: two Standards must read as
3x / 57.7%.

  py -3.14 scripts/scan_demon_value.py --date 2026-10-27 --scrape

STEP 0 (before any CDP scrape): scrape_prefilter / scrape_candidates keep
only Demon OVER rows whose exact-line Wilson low can possibly alert
(>= 0.50), with n>=15, n_cur>=3, current HR>=0.60, no team change, and
current minutes >= 0.85 × last season. Do not scrape thousands.

Adds picks, reads payout, clears the slip. Never presses Submit.
Does not write /tickets. Demons stay off Goblin-70 and the mixer.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

from utils.demon_value import (  # noqa: E402
    _f,
    _fold,
    _fold_prop,
    _pick,
    format_list_section,
    grade_pending,
    implied_p_from_pair,
    list_standards,
    load_latest,
    load_reference,
    FORMULA_DIFFERENT_GAME_BELOW,
    ALERT_SLIP_SHAPE,
    UNCONNECTED_PAIR_NEED,
    one_per_player,
    pair_id,
    pair_is_settled,
    pair_is_unconnected,
    pair_quotes_complete,
    pick_standards_for_demon,
    pick_two_demons_different_games,
    refs_unconnected_from_demon,
    same_game,
    quote_key,
    reference_p,
    same_demon_two_standards,
    scan,
    scrape_candidates,
    trust_mix_n_correct,
    trust_two_standard_n_correct,
    trusted_quote,
    two_demon_product_check,
    two_standard_sanity,
    two_standards_different_teams,
    write_pricing_check,
)

from scrape_demon_n_correct import parse_demon_power_quote  # noqa: E402


def _load_board(date_str: str, root: Path) -> list[dict[str, Any]]:
    from ml.gift_scan import load_boards

    return load_boards(root, date_str)


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        rec = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return rec if isinstance(rec, dict) else {}


# Read-only: add picks, read payout, clear slip. Never Submit Lineup.
_READ_ONLY = True
_SUBMIT_NAMES = ("Submit Lineup", "Submit", "Confirm Entry", "Place Entry")


def _never_submit(frame) -> None:
    """Same rule as the entries review: do not click Submit."""
    _ = frame
    return


def _player_aliases(name: object) -> set[str]:
    """Match 'Jaime Jaquez' to board/PP 'Jaime Jaquez Jr.'."""
    folded = _fold(name)
    if not folded:
        return set()
    stripped = re.sub(r"(?:jr|sr|iii|ii|iv)$", "", folded)
    keys = {folded}
    if stripped:
        keys.add(stripped)
        keys.update({stripped + "jr", stripped + "sr"})
    return {k for k in keys if k}


def _cdp_up(url: str) -> bool:
    probe = str(url or "").rstrip("/") + "/json/version"
    try:
        with urlopen(probe, timeout=3) as resp:
            return 200 <= int(resp.status) < 300
    except (URLError, TimeoutError, OSError):
        return False


def load_shortlist_csv(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8", newline="") as fh:
        for rec in csv.DictReader(fh):
            player = str(rec.get("player") or "").strip()
            prop = str(rec.get("prop") or rec.get("prop_type") or "").strip()
            line = _f(rec.get("line"))
            if not player or not prop or line is None:
                continue
            opp = str(rec.get("opp") or rec.get("opp_team") or rec.get("opponent") or "").strip()
            rows.append(
                {
                    "player": player,
                    "team": str(rec.get("team") or "").strip(),
                    "opp": opp,
                    "opp_team": opp,
                    "prop": prop,
                    "prop_type": prop,
                    "line": line,
                    "wilson_lo": _f(rec.get("wilson_lo")),
                    "pick_type": "demon",
                    "side": "OVER",
                    "sport": str(rec.get("sport") or "NBA").strip() or "NBA",
                }
            )
    return rows


def write_candidate_list(
    rows: list[dict[str, Any]],
    *,
    date_str: str,
    reason: str,
    scrape_ran: bool,
) -> Path:
    dest = ROOT / "data" / "reports" / f"demon_value_candidates_{date_str}.json"
    dest.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "date": date_str,
        "never_submit": True,
        "ticketed": False,
        "scrape_ran": scrape_ran,
        "reason": reason,
        "n": len(rows),
        "candidates": [
            {
                "player": r.get("player"),
                "team": r.get("team"),
                "opp": r.get("opp") or r.get("opp_team"),
                "prop": r.get("prop") or r.get("prop_type"),
                "line": r.get("line"),
                "wilson_lo": r.get("wilson_lo"),
                "pick_type": "demon",
                "side": "OVER",
            }
            for r in rows
        ],
    }
    dest.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return dest


def _attach_team(card: dict[str, Any], by_player: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Copy team/sport/minutes from the board. Never overwrite live prop/line."""
    out = dict(card)
    rec = None
    for key in _player_aliases(card.get("player")):
        rec = by_player.get(key)
        if rec:
            break
    if rec:
        if rec.get("player"):
            out["player"] = rec.get("player")
        if not out.get("team"):
            out["team"] = rec.get("team")
        if not out.get("sport"):
            out["sport"] = rec.get("sport")
        out.setdefault("minutes_proj", rec.get("minutes_proj") or rec.get("projected_minutes"))
        out.setdefault("injury_status", rec.get("injury_status") or rec.get("player_injury_status"))
        out.setdefault("game_date", rec.get("game_date"))
        out.setdefault("matchup", rec.get("matchup"))
        out.setdefault("opp", rec.get("opp") or rec.get("opp_team") or rec.get("opponent"))
        out.setdefault("opp_team", rec.get("opp_team") or rec.get("opp") or rec.get("opponent"))
        out.setdefault("pp_game_id", rec.get("pp_game_id") or rec.get("game_id"))
        out.setdefault("game_id", rec.get("pp_game_id") or rec.get("game_id"))
        out.setdefault("start_time", rec.get("start_time") or rec.get("game_time"))
        out.setdefault("side", rec.get("side") or "OVER")
    out["prop"] = out.get("prop_type") or out.get("prop")
    out["prop_type"] = out.get("prop_type") or out.get("prop")
    return out


def _fill_entry_20(frame) -> bool:
    """Type $20 in Entry Fee so To Win populates. Not Submit."""
    try:
        return bool(
            frame.evaluate(
                """
                () => {
                  const setVal = (input, v) => {
                    const proto = Object.getOwnPropertyDescriptor(
                      window.HTMLInputElement.prototype, 'value'
                    );
                    if (proto && proto.set) proto.set.call(input, v);
                    else input.value = v;
                    input.dispatchEvent(new Event('input', {bubbles: true}));
                    input.dispatchEvent(new Event('change', {bubbles: true}));
                    input.dispatchEvent(new KeyboardEvent('keyup', {bubbles: true}));
                  };
                  const nodes = [...document.querySelectorAll('input')];
                  for (const input of nodes) {
                    const near = ((input.getAttribute('aria-label') || '')
                      + (input.placeholder || '')
                      + ((input.parentElement && input.parentElement.innerText) || '')
                    ).slice(0, 80);
                    if (/entry/i.test(near) || input.type === 'number' || input.inputMode === 'decimal') {
                      setVal(input, '20');
                      return true;
                    }
                  }
                  return false;
                }
                """
            )
        )
    except Exception:
        return False


def _quote_from_slip(parsed: dict[str, Any], *, two_standards: bool = False) -> dict[str, Any] | None:
    q = trusted_quote(parsed)
    if q is not None:
        return q
    if two_standards:
        return trust_two_standard_n_correct(parsed)
    return trust_mix_n_correct(parsed)


def _click_two_and_read(
    P,
    frame,
    page,
    a: dict[str, Any],
    b: dict[str, Any],
    delay_sec: float,
    *,
    two_standards: bool = False,
) -> dict[str, Any] | None:
    """Power 2-pick: click A, click B, read N-correct, clear. Never Submit."""
    _never_submit(frame)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, "power")
    frame.wait_for_timeout(int(max(0.1, delay_sec) * 1000))
    spec_a = {
        "player": a.get("player"),
        "prop_type": a.get("prop_type") or a.get("prop"),
        "line": a.get("line"),
        "pick_type": str(a.get("pick_type") or "standard").strip().lower(),
        "direction": "OVER",
    }
    spec_b = {
        "player": b.get("player"),
        "prop_type": b.get("prop_type") or b.get("prop"),
        "line": b.get("line"),
        "pick_type": str(b.get("pick_type") or "standard").strip().lower(),
        "direction": "OVER",
    }
    ok_a = P.add_leg(frame, page, spec_a, strict_lines=True, require_line=True)
    ok_b = P.add_leg(frame, page, spec_b, strict_lines=True, require_line=True)
    if not (ok_a and ok_b):
        P.clear_slip(frame)
        return None
    _fill_entry_20(frame)
    frame.wait_for_timeout(400)
    slip = P.read_slip(frame, n_legs=2, ticket_type="power") or {}
    text = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
    parsed = parse_demon_power_quote(text, n_legs=2)
    q = _quote_from_slip(parsed, two_standards=two_standards)
    P.clear_slip(frame)
    _never_submit(frame)
    if q is None:
        return None
    q["parsed"] = {
        k: parsed.get(k)
        for k in ("to_win", "entry", "n_correct_raw", "first_place_x", "quote_trusted")
    }
    return q


def _ref_leg(std: dict[str, Any]) -> dict[str, Any]:
    return {
        "player": std.get("player"),
        "prop": std.get("prop") or std.get("prop_type"),
        "line": std.get("line"),
        "team": std.get("team"),
        "opp": std.get("opp") or std.get("opp_team") or std.get("opponent"),
        "pp_game_id": std.get("pp_game_id") or std.get("game_id"),
        "pick_type": "standard",
    }


def _click_until_quoted(
    P,
    frame,
    page,
    demon: dict[str, Any],
    standards: list[dict[str, Any]],
    delay_sec: float,
    *,
    also_avoid: list[dict[str, Any]] | None = None,
    n: int = 8,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Demon + a different-game Standard. Try the next Standard if a click misses."""
    tried: list[str] = []
    for std in pick_standards_for_demon(demon, standards, n=n, also_avoid=also_avoid):
        raw = _click_two_and_read(P, frame, page, std, demon, delay_sec)
        if raw is not None:
            return std, raw
        tried.append(str(std.get("player") or "?"))
    if tried:
        print(
            f"[demon-value] no 2-leg quote for {demon.get('player')} "
            f"after Standards {tried}"
        )
    return None, None


def _board_by_player(board: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    by_player: dict[str, dict[str, Any]] = {}
    for row in board:
        for pn in _player_aliases(row.get("player")):
            if pn not in by_player:
                by_player[pn] = row
    return by_player


def match_shortlist_to_board(
    cands: list[dict[str, Any]],
    board: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Copy game/minutes from the board. Keep the shortlist prop/line."""
    by_player = _board_by_player(board)
    out: list[dict[str, Any]] = []
    for raw in cands:
        attached = _attach_team(raw, by_player)
        aliases = _player_aliases(raw.get("player"))
        prop_k = _fold_prop(raw.get("prop") or raw.get("prop_type"))
        line = _f(raw.get("line"))
        hit = None
        for row in board:
            if _pick(row.get("pick_type")) != "demon":
                continue
            if _fold_prop(row.get("prop") or row.get("prop_type")) != prop_k:
                continue
            if _f(row.get("line")) != line:
                continue
            if not (aliases & _player_aliases(row.get("player"))):
                continue
            hit = row
            break
        if hit is not None:
            merged = dict(hit)
            merged["pick_type"] = "demon"
            merged["side"] = "OVER"
            merged["wilson_lo"] = raw.get("wilson_lo")
            merged["prop"] = raw.get("prop") or merged.get("prop")
            merged["prop_type"] = merged.get("prop_type") or raw.get("prop")
            merged["line"] = line
            out.append(merged)
        else:
            attached["pick_type"] = "demon"
            attached["side"] = "OVER"
            attached["sport"] = attached.get("sport") or "NBA"
            out.append(attached)
    return out


def _discover_live_cards(P, frame, page, by_player: dict[str, dict[str, Any]]):
    def _click_tab(name: str) -> None:
        try:
            P.dismiss_modal(frame, page)
            loc = frame.get_by_text(name, exact=True).first
            loc.click(force=True, timeout=2000)
            frame.wait_for_timeout(700)
        except Exception:
            return

    live_std: list[dict[str, Any]] = []
    live_dem: list[dict[str, Any]] = []
    seen_s: set[str] = set()
    seen_d: set[str] = set()
    for tab in ("Popular", "Points", "3-PT Made", "Turnovers", "Rebounds"):
        _click_tab(tab)
        try:
            cards = P.get_all_cards(frame)
        except Exception:
            continue
        for c in cards:
            rec = _attach_team(c, by_player)
            pt = str(rec.get("pick_type") or "").lower()
            key = quote_key(rec)
            if pt == "standard" and rec.get("team") and key not in seen_s:
                seen_s.add(key)
                live_std.append(rec)
            elif pt == "demon" and key not in seen_d:
                seen_d.add(key)
                live_dem.append(rec)
    return live_std, live_dem


def _leg_brief(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if not row:
        return None
    return {
        "player": row.get("player"),
        "prop": row.get("prop") or row.get("prop_type"),
        "line": row.get("line"),
        "team": row.get("team"),
        "opp": row.get("opp") or row.get("opp_team") or row.get("opponent"),
        "pp_game_id": row.get("pp_game_id") or row.get("game_id"),
        "pick_type": str(row.get("pick_type") or "").strip().lower(),
    }


def _implied_from_quotes(
    demon: dict[str, Any],
    quotes: dict[str, Any],
    p_ref: float | None,
) -> tuple[float | None, float | None]:
    raw = quotes.get(quote_key(demon))
    q = trusted_quote(raw if isinstance(raw, dict) else None)
    if q is None or p_ref is None:
        return None, None
    return implied_p_from_pair(q["power_x"], p_ref), float(q["power_x"])


def _pair_log_row(check: dict[str, Any], *, date_str: str) -> dict[str, Any]:
    a, b = check.get("a") or {}, check.get("b") or {}
    return {
        "date": date_str,
        "pair_id": pair_id(a, b) if a and b else "",
        "ok": check.get("ok"),
        "below_product": check.get("below_product"),
        "unconnected": check.get("unconnected"),
        "quote_ab": check.get("quote_ab"),
        "expected": check.get("expected"),
        "delta": check.get("delta"),
        "a": check.get("a"),
        "b": check.get("b"),
        "std_a": check.get("std_a"),
        "std_b": check.get("std_b"),
        "std_a_same_game": check.get("std_a_same_game"),
        "std_b_same_game": check.get("std_b_same_game"),
    }


def run_pricing_checks_on_session(
    P,
    frame,
    page,
    *,
    live_std: list[dict[str, Any]],
    live_dem: list[dict[str, Any]],
    quotes: dict[str, Any],
    p_ref: float | None,
    delay_sec: float,
    by_player: dict[str, dict[str, Any]],
    prior_check: dict[str, Any] | None = None,
    max_pairs: int = 1,
    date_str: str = "",
) -> dict[str, Any]:
    """Different-game two-Demon product. Never Submit. p_ref stays 0.577."""
    _ = quotes
    _never_submit(frame)
    live_std = [_attach_team(r, by_player) for r in live_std]
    live_dem = [_attach_team(r, by_player) for r in live_dem]
    prior = prior_check if isinstance(prior_check, dict) else {}
    logged = list(prior.get("different_game_pairs") or [])
    if not logged and isinstance(prior.get("two_demon_different_game"), dict):
        old = dict(prior["two_demon_different_game"])
        old.setdefault("unconnected", False)
        old.setdefault("std_a_same_game", True)
        logged.append(_pair_log_row(old, date_str=str(prior.get("date") or date_str)))
    rec: dict[str, Any] = {
        "never_submit": True,
        "p_ref": p_ref,
        "formula": "implied_p = 1 / (quote_2leg * 0.577) from Demon + Standard (different game)",
        "same_demon": prior.get("same_demon") or {"ok": False, "error": "not_run"},
        "two_demon_same_game": prior.get("two_demon_same_game") or prior.get("two_demon"),
        "two_demon_different_game": {"ok": False, "error": "not_run"},
        "different_game_pairs": logged,
        "need_unconnected_pairs": UNCONNECTED_PAIR_NEED,
        "formula_ok": True,
        "note": (
            "Reference Standard must be a different game than the Demon. "
            "Accumulate 3–5 unconnected two-Demon pairs over nights. "
            "p_ref stays 0.577. Do not stack Demons."
        ),
    }
    if rec.get("two_demon_same_game") and rec["two_demon_same_game"].get("reed_ausar"):
        rec["two_demon_same_game"] = dict(rec["two_demon_same_game"])
        rec["two_demon_same_game"]["confounded"] = True
        rec["two_demon_same_game"]["why"] = "same_game_DET"

    prior_sd = rec.get("same_demon") if isinstance(rec.get("same_demon"), dict) else {}
    prior_demon = prior_sd.get("demon") if isinstance(prior_sd.get("demon"), dict) else {}
    same_demon_live = None
    for row in live_dem:
        if _fold(row.get("player")) == _fold(prior_demon.get("player")):
            same_demon_live = row
            break
    if same_demon_live is None and live_dem:
        same_demon_live = live_dem[0]
    prior_stds_ok = refs_unconnected_from_demon(
        same_demon_live,
        prior_sd.get("std_a") if isinstance(prior_sd.get("std_a"), dict) else None,
        prior_sd.get("std_b") if isinstance(prior_sd.get("std_b"), dict) else None,
    )
    if same_demon_live is not None and (not prior_sd.get("ok") or not prior_stds_ok):
        stds = pick_standards_for_demon(same_demon_live, live_std, n=2)
        if len(stds) >= 2:
            std_x, raw_x = _click_until_quoted(
                P, frame, page, same_demon_live, live_std, delay_sec, n=8
            )
            avoid = [std_x] if std_x is not None else []
            std_y, raw_y = _click_until_quoted(
                P,
                frame,
                page,
                same_demon_live,
                live_std,
                delay_sec,
                also_avoid=avoid,
                n=8,
            )
            check_sd = same_demon_two_standards(
                None if raw_x is None else raw_x.get("power_x"),
                None if raw_y is None else raw_y.get("power_x"),
            )
            check_sd["demon"] = _leg_brief(same_demon_live)
            check_sd["std_a"] = _leg_brief(std_x)
            check_sd["std_b"] = _leg_brief(std_y)
            check_sd["unconnected"] = refs_unconnected_from_demon(
                same_demon_live, std_x, std_y
            )
            rec["same_demon"] = check_sd
            print(
                f"[demon-value] same-demon {same_demon_live.get('player')} "
                f"std={None if std_x is None else std_x.get('player')}/"
                f"{None if std_y is None else std_y.get('player')} "
                f"unconnected={check_sd.get('unconnected')} "
                f"quotes={check_sd.get('quote_a')}/{check_sd.get('quote_b')} "
                f"ok={check_sd.get('ok')}"
            )

    seen_ids = {
        str(p.get("pair_id") or "")
        for p in logged
        if pair_is_settled(p) and p.get("pair_id")
    }
    last_check: dict[str, Any] | None = None
    wanted = max(1, int(max_pairs))
    got = 0
    tried_this_run: set[str] = set()
    max_attempts = max(8, wanted * 6)
    attempts = 0
    while got < wanted and attempts < max_attempts:
        attempts += 1
        pair = pick_two_demons_different_games(
            live_dem, exclude_ids=seen_ids | tried_this_run
        )
        if pair is None:
            if last_check is None:
                rec["two_demon_different_game"] = {
                    "ok": False,
                    "error": "need_two_demons_different_games",
                }
            break
        a, b = pair[0], pair[1]
        pid = pair_id(a, b)
        tried_this_run.add(pid)
        std_a, raw_a = _click_until_quoted(
            P, frame, page, a, live_std, delay_sec, also_avoid=[b], n=8
        )
        p_a = p_b = q_a_std = q_b_std = None
        if raw_a is not None and p_ref is not None:
            q_a_std = float(raw_a["power_x"])
            p_a = implied_p_from_pair(q_a_std, p_ref)
        avoid_b = [a]
        if std_a is not None:
            avoid_b.append(std_a)
        std_b, raw_b = _click_until_quoted(
            P, frame, page, b, live_std, delay_sec, also_avoid=avoid_b, n=8
        )
        if raw_b is not None and p_ref is not None:
            q_b_std = float(raw_b["power_x"])
            p_b = implied_p_from_pair(q_b_std, p_ref)
        mix = None
        if p_a is not None and p_b is not None:
            mix = _click_two_and_read(P, frame, page, a, b, delay_sec)
        check = two_demon_product_check(
            None if mix is None else mix.get("power_x"),
            p_a,
            p_b,
        )
        check["a"] = _leg_brief(a)
        check["b"] = _leg_brief(b)
        check["std_a"] = _leg_brief(std_a)
        check["std_b"] = _leg_brief(std_b)
        check["quote_a_vs_std"] = q_a_std
        check["quote_b_vs_std"] = q_b_std
        check["same_game"] = same_game(a, b)
        check["std_a_same_game"] = bool(std_a is not None and same_game(a, std_a))
        check["std_b_same_game"] = bool(std_b is not None and same_game(b, std_b))
        check["unconnected"] = pair_is_unconnected(a, b, std_a, std_b)
        check["scraped_in_one_pass"] = True
        if mix is None or p_a is None or p_b is None or std_a is None or std_b is None:
            check["ok"] = False
            check["error"] = "unreadable_quote"
            check["unconnected"] = False
        last_check = check
        rec["two_demon_different_game"] = check
        logged = [p for p in logged if str(p.get("pair_id") or "") != pid]
        logged.append(_pair_log_row(check, date_str=date_str))
        print(
            f"[demon-value] two-demon different-game {a.get('player')} + {b.get('player')} "
            f"std={None if std_a is None else std_a.get('player')}/"
            f"{None if std_b is None else std_b.get('player')} "
            f"unconnected={check.get('unconnected')} quote={check.get('quote_ab')} "
            f"expected={check.get('expected')} ok={check.get('ok')} below={check.get('below_product')}"
        )
        if pair_is_settled(check):
            seen_ids.add(pid)
            got += 1
        elif pair_quotes_complete(check) and not check.get("unconnected"):
            # Readable but still connected — do not retry this Demon pair.
            seen_ids.add(pid)
    rec["different_game_pairs"] = logged
    rec["n_unconnected_pairs"] = sum(1 for p in logged if p.get("unconnected") and p.get("ok"))
    rec["two_demon"] = rec.get("two_demon_different_game")
    unconnected_now = [p for p in logged if p.get("unconnected")]
    if any(p.get("below_product") for p in unconnected_now):
        rec["formula_ok"] = FORMULA_DIFFERENT_GAME_BELOW
        rec["alert_price_valid_for"] = ALERT_SLIP_SHAPE
        rec["note"] = (
            rec["note"]
            + " An unconnected two-Demon pair priced below the product; "
            "alert prices are valid only for Demon + Standard 2-picks."
        )
    else:
        rec["formula_ok"] = True
    rec["leg_by_leg"] = rec["formula_ok"] is True
    rec["proof"] = (
        f"{rec['n_unconnected_pairs']}/{UNCONNECTED_PAIR_NEED} unconnected different-game pairs"
    )
    _never_submit(frame)
    return rec


def scrape_pricing_check(
    *,
    board: list[dict[str, Any]],
    quotes: dict[str, Any],
    p_ref: float | None,
    cdp_url: str,
    delay_sec: float,
    date_str: str = "",
    max_pairs: int = 1,
) -> dict[str, Any]:
    """Standalone one-shot. Adds / reads / clears. Never Submit."""
    import collect_payout_data as P

    assert _READ_ONLY
    P._acquire_payout_capture_lock()
    browser = None
    _p = None
    rec: dict[str, Any] = {"ok": False, "never_submit": True}
    try:
        _p, browser, context, page = P.connect_existing_browser(cdp_url)
        page.wait_for_timeout(400)
        P.navigate_board_for_sport(page, "NBA")
        frame = P.find_prizepicks_frame(page)
        P.ensure_popular_filter(frame, page)
        by_player = _board_by_player(board)
        live_std, live_dem = _discover_live_cards(P, frame, page, by_player)
        print(
            f"[demon-value] pricing-check live standards={len(live_std)} "
            f"live demons={len(live_dem)}"
        )
        rec = run_pricing_checks_on_session(
            P,
            frame,
            page,
            live_std=live_std,
            live_dem=live_dem,
            quotes=quotes,
            p_ref=p_ref,
            delay_sec=delay_sec,
            by_player=by_player,
            prior_check=_load_json(ROOT / "data" / "reports" / "demon_value_pricing_check_latest.json"),
            max_pairs=max_pairs,
            date_str=date_str,
        )
        rec["ok"] = True
        rec["live_standards"] = len(live_std)
        rec["live_demons"] = len(live_dem)
    except Exception as exc:
        rec["ok"] = False
        rec["error"] = str(exc)
    finally:
        _never_submit(None)
        try:
            if browser is not None:
                browser.close()
        except Exception:
            pass
        try:
            if _p is not None:
                _p.stop()
        except Exception:
            pass
        P._release_payout_capture_lock()
    return rec


def scrape_pair_quotes(
    *,
    board: list[dict[str, Any]],
    reference: dict[str, Any],
    cdp_url: str,
    delay_sec: float,
    max_demons: int = 80,
    candidates: list[dict[str, Any]] | None = None,
    existing_quotes: dict[str, Any] | None = None,
    date_str: str = "",
    run_pricing_check: bool = True,
) -> dict[str, Any]:
    """Demon + Standard (other team). Sanity: two Standards = 3x.

    Read-only: add / read / clear. Never Submit.
    """
    import collect_payout_data as P

    assert _READ_ONLY
    standards_board = list_standards(board)
    pair = two_standards_different_teams(standards_board)
    P._acquire_payout_capture_lock()
    out: dict[str, dict[str, Any]] = dict(existing_quotes or {})
    sanity: dict[str, Any] = {"ok": False}
    browser = None
    _p = None
    error = None
    out_check: dict[str, Any] | None = None
    try:
        _p, browser, context, page = P.connect_existing_browser(cdp_url)
        page.wait_for_timeout(400)
        P.navigate_board_for_sport(page, "NBA")
        frame = P.find_prizepicks_frame(page)
        P.ensure_popular_filter(frame, page)
        by_player = _board_by_player(board)
        live_std, live_dem = _discover_live_cards(P, frame, page, by_player)
        std_pair = two_standards_different_teams(live_std)
        if std_pair is None:
            error = "need_two_standards_different_teams"
            return {"ok": False, "error": error, "sanity": sanity, "quotes": {}}
        a, b = std_pair
        a = _attach_team(a, by_player)
        b = _attach_team(b, by_player)
        print(
            f"[demon-value] sanity two Standards: "
            f"{a.get('player')} {a.get('team')} + {b.get('player')} {b.get('team')}"
        )
        raw = _click_two_and_read(P, frame, page, a, b, delay_sec, two_standards=True)
        if raw is None:
            error = "sanity_unreadable"
            sanity = two_standard_sanity(0.0)
            sanity["ok"] = False
            return {"ok": False, "error": error, "sanity": sanity, "quotes": {}}
        sanity = two_standard_sanity(float(raw["power_x"]))
        sanity["legs"] = [
            {"player": a.get("player"), "team": a.get("team"), "pick_type": "standard"},
            {"player": b.get("player"), "team": b.get("team"), "pick_type": "standard"},
        ]
        sanity["to_win"] = raw.get("to_win")
        sanity["entry"] = raw.get("entry")
        print(
            f"[demon-value] sanity quote={raw['power_x']} "
            f"implied_p={sanity.get('implied_p')} ok={sanity['ok']}"
        )
        if not sanity["ok"]:
            error = "sanity_parser_off"
            return {"ok": False, "error": error, "sanity": sanity, "quotes": {}}
        print(
            f"[demon-value] live standards={len(live_std)} "
            f"live demons={len(live_dem)} already_quoted={len(out)}"
        )
        pool = candidates if candidates is not None else live_dem
        scraped = 0
        attempted = 0
        have = set(out)
        for row in pool:
            if attempted >= int(max_demons):
                break
            if str(row.get("pick_type") or "").lower().find("dem") < 0:
                continue
            demon = _attach_team(row, by_player)
            if quote_key(demon) in have:
                continue
            attempted += 1
            std, q = _click_until_quoted(
                P, frame, page, demon, live_std or standards_board, delay_sec, n=8
            )
            if std is None or q is None:
                continue
            std = _attach_team(std, by_player)
            q["ref_leg"] = _ref_leg(std)
            q["ref_same_game"] = same_game(demon, std)
            q["legs"] = [q["ref_leg"], {
                "player": demon.get("player"),
                "prop_type": demon.get("prop") or demon.get("prop_type"),
                "line": demon.get("line"),
                "pick_type": "demon",
                "team": demon.get("team"),
            }]
            out[quote_key(demon)] = q
            have.add(quote_key(demon))
            scraped += 1
            print(
                f"[demon-value] {demon.get('player')} {demon.get('prop')} "
                f"{demon.get('line')} vs {std.get('player')} {std.get('team')} "
                f"→ {q.get('power_x')}x"
            )
            frame.wait_for_timeout(int(max(0.05, delay_sec * 0.5) * 1000))
        if sanity.get("ok") and run_pricing_check:
            check = run_pricing_checks_on_session(
                P,
                frame,
                page,
                live_std=live_std,
                live_dem=live_dem,
                quotes=out,
                p_ref=reference_p(reference),
                delay_sec=delay_sec,
                by_player=by_player,
                prior_check=_load_json(ROOT / "data" / "reports" / "demon_value_pricing_check_latest.json"),
                max_pairs=1,
                date_str=date_str,
            )
            out_check = check
        else:
            out_check = None
    except Exception as exc:
        error = str(exc)
    finally:
        _never_submit(None)
        try:
            if browser is not None:
                browser.close()
        except Exception:
            pass
        try:
            if _p is not None:
                _p.stop()
        except Exception:
            pass
        P._release_payout_capture_lock()
    return {
        "ok": bool(sanity.get("ok") and error is None),
        "error": error,
        "sanity": sanity,
        "quotes": out,
        "pricing_check": out_check,
        "never_submit": True,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Demon value scanner (alert only, not a ticket)")
    ap.add_argument("--date", default="", help="Slate date YYYY-MM-DD")
    ap.add_argument("--quotes-json", default="", help="Injected 2-leg Power quotes")
    ap.add_argument("--reference-json", default="", help="Reference leg with implied_p")
    ap.add_argument("--scrape", action="store_true", help="CDP-scrape Demon+Standard 2-leg Power (never Submit)")
    ap.add_argument("--pricing-check", action="store_true", help="Unconnected Demon+Standard, then two Demons (never Submit)")
    ap.add_argument("--pricing-pairs", type=int, default=1, help="Different-game two-Demon pairs to scrape this run (accumulate 3–5 over nights)")
    ap.add_argument("--max-scrape", type=int, default=80, help="Max Demons to pair after the two-Standard sanity check")
    ap.add_argument(
        "--shortlist",
        default="",
        help="CSV of Demons to price (player,team,opp,prop,line,wilson_lo). One highest wilson_lo per player.",
    )
    ap.add_argument("--cdp-url", default="http://127.0.0.1:9222")
    ap.add_argument("--delay-sec", type=float, default=0.5)
    ap.add_argument("--no-alert", action="store_true")
    ap.add_argument("--grade", action="store_true", help="Fill HIT/MISS on pending alerts")
    args = ap.parse_args()
    date_str = str(args.date or "").strip()[:10] or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if args.grade:
        from utils.nba_minutes_role import load_nba_box_frame

        boxes = None
        try:
            boxes = load_nba_box_frame()
        except Exception as exc:
            print(f"[demon-value] grade boxes skipped: {exc}")
        rec = grade_pending(boxes=boxes, as_of=date.fromisoformat(date_str) + timedelta(days=1))
        print(f"[demon-value] graded {rec['n_graded']} / {rec['n_log']}")
        return 0
    quotes: dict[str, Any] = {}
    qpath = Path(str(args.quotes_json).strip()) if str(args.quotes_json).strip() else ROOT / "data" / "reports" / f"demon_value_quotes_{date_str}.json"
    loaded = _load_json(qpath)
    if loaded:
        if isinstance(loaded.get("quotes"), dict):
            quotes = loaded["quotes"]
        else:
            quotes = {k: v for k, v in loaded.items() if isinstance(v, dict)}
    ref_path = Path(str(args.reference_json).strip()) if str(args.reference_json).strip() else None
    reference = load_reference(path=ref_path)
    board = _load_board(date_str, ROOT)
    sanity = None
    boxes = None
    try:
        from utils.nba_minutes_role import load_nba_box_frame

        boxes = load_nba_box_frame()
    except Exception as extra:
        print(f"[demon-value] NBA boxes skipped: {extra}")
    shortlist_path = Path(str(args.shortlist).strip()) if str(args.shortlist).strip() else None
    candidates: list[dict[str, Any]] | None = None
    if shortlist_path is not None:
        if not shortlist_path.exists():
            print(f"[demon-value] shortlist missing: {shortlist_path}")
            return 1
        loaded_rows = load_shortlist_csv(shortlist_path)
        candidates = match_shortlist_to_board(one_per_player(loaded_rows), board)
        cand_path = write_candidate_list(
            candidates,
            date_str=date_str,
            reason="shortlist_one_per_player",
            scrape_ran=False,
        )
        print(
            f"[demon-value] shortlist {len(loaded_rows)} rows -> "
            f"{len(candidates)} one-per-player -> {cand_path}"
        )
        for row in candidates:
            print(
                f"  {row.get('player')} {row.get('team')} {row.get('prop')} "
                f"{row.get('line')} wilson_lo={row.get('wilson_lo')}"
            )
    else:
        # STEP 0: only scrape Demons that can possibly alert. Never thousands.
        passers, pre_skipped = scrape_candidates(
            board, date_str=date_str, boxes=boxes
        )
        skip_counts: dict[str, int] = {}
        for rec in pre_skipped:
            reason = str(rec.get("why") or "skip").split(",")[0]
            skip_counts[reason] = skip_counts.get(reason, 0) + 1
        print(
            f"[demon-value] step0 scrape_prefilter candidates={len(passers)} "
            f"skipped={len(pre_skipped)} reasons={skip_counts}"
        )
        write_candidate_list(
            passers,
            date_str=date_str,
            reason="scrape_prefilter",
            scrape_ran=False,
        )
        candidates = passers
    if args.scrape:
        if not _cdp_up(args.cdp_url):
            if candidates:
                dest = write_candidate_list(
                    candidates,
                    date_str=date_str,
                    reason="cdp_down",
                    scrape_ran=False,
                )
                print(f"[demon-value] candidate list -> {dest}")
            print(
                f"[demon-value] scrape could not run: CDP {args.cdp_url} is down. "
                "No quotes invented."
            )
            return 2
        scraped = scrape_pair_quotes(
            board=board,
            reference=reference,
            cdp_url=args.cdp_url,
            delay_sec=float(args.delay_sec),
            max_demons=len(candidates) if candidates is not None else int(args.max_scrape),
            candidates=candidates,
            existing_quotes=quotes,
            date_str=date_str,
            run_pricing_check=shortlist_path is None,
        )
        sanity = scraped.get("sanity")
        dest = ROOT / "data" / "reports" / f"demon_value_quotes_{date_str}.json"
        quotes.update(scraped.get("quotes") or {})
        dest.write_text(
            json.dumps(
                {
                    "date": date_str,
                    "never_submit": True,
                    "p_ref": reference_p(reference),
                    "sanity": sanity,
                    "error": scraped.get("error"),
                    "quotes": quotes,
                    "pricing_check": scraped.get("pricing_check"),
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        if scraped.get("pricing_check"):
            check = dict(scraped["pricing_check"])
            check["date"] = date_str
            dest_check = write_pricing_check(check, date_str=date_str)
            print(
                f"[demon-value] pricing-check leg_by_leg={check.get('leg_by_leg')} -> {dest_check}"
            )
        print(
            f"[demon-value] scrape ok={scraped.get('ok')} "
            f"sanity={bool((sanity or {}).get('ok'))} "
            f"quotes={len(quotes)} -> {dest}"
        )
        if sanity and not sanity.get("ok") and sanity.get("quote_2leg"):
            print("[demon-value] two-Standard sanity failed — parser still off, no Demon priced")
            return 1
        if not scraped.get("ok"):
            print(f"[demon-value] scrape aborted: {scraped.get('error')}")
            if not quotes:
                return 1
        if candidates:
            write_candidate_list(
                candidates,
                date_str=date_str,
                reason="scraped",
                scrape_ran=True,
            )
    if args.pricing_check and not args.scrape:
        check = scrape_pricing_check(
            board=board,
            quotes=quotes,
            p_ref=reference_p(reference),
            cdp_url=args.cdp_url,
            delay_sec=float(args.delay_sec),
            date_str=date_str,
            max_pairs=int(args.pricing_pairs),
        )
        check["date"] = date_str
        dest_check = write_pricing_check(check, date_str=date_str)
        latest = load_latest()
        if latest:
            latest["pricing_check"] = {
                "leg_by_leg": check.get("leg_by_leg"),
                "formula_ok": check.get("formula_ok"),
                "proof": check.get("proof"),
                "n_unconnected_pairs": check.get("n_unconnected_pairs"),
                "need_unconnected_pairs": check.get("need_unconnected_pairs"),
                "same_demon": check.get("same_demon"),
                "two_demon": check.get("two_demon"),
            }
            latest_path = ROOT / "data" / "reports" / "demon_value_latest.json"
            latest_path.write_text(json.dumps(latest, indent=2), encoding="utf-8")
            dated = ROOT / "data" / "reports" / f"demon_value_{date_str}.json"
            dated.write_text(json.dumps(latest, indent=2), encoding="utf-8")
        print(
            f"[demon-value] pricing-check ok={check.get('ok')} "
            f"leg_by_leg={check.get('leg_by_leg')} same={check.get('same_demon', {}).get('ok')} "
            f"two={check.get('two_demon', {}).get('ok')} -> {dest_check}"
        )
        return 0
    if reference_p(reference) is None:
        print("[demon-value] no Standard p_ref — fail closed")
    elif not quotes:
        print("[demon-value] no trusted 2-leg quotes yet — fail closed (run --scrape)")
    # Manual --shortlist is tonight's override. Nightly scan() still runs STEP 0
    # scrape_prefilter on the full board so skip reasons are recorded.
    scan_board = candidates if shortlist_path is not None else board
    summary = scan(
        date_str=date_str,
        board=scan_board,
        quotes=quotes,
        reference=reference,
        boxes=boxes,
        alert=not args.no_alert,
        write=True,
    )
    print(format_list_section(summary, date=date_str))
    print(
        f"[demon-value] demons={summary['n_board_demons']} "
        f"alerts={summary['n_alerts']} skipped={summary['n_skipped']} "
        f"ticketed={summary['ticketed']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
