"""One-pass PrizePicks manual slip: cycle to the exact G70 line, then More-click.

Used by ``scripts/build_pp_manual_slip.py``. Does not submit.
A moved live line is a hard abort (never skip-and-continue). Exposure goes
through ``assert_clears_exposure`` before any board click.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from utils.manual_slip_exposure import assert_clears_exposure

PROP_ALIASES = {
    "pitchingouts": ("pitchingouts", "po"),
    "po": ("pitchingouts", "po"),
    "totalgames": ("totalgames",),
    "rebsasts": ("rebsasts", "rebasts"),
    "rebasts": ("rebsasts", "rebasts"),
    "pitcherstrikeouts": ("pitcherstrikeouts", "ks", "strikeouts"),
    "ks": ("pitcherstrikeouts", "ks", "strikeouts"),
    "hitterstrikeouts": ("hitterstrikeouts", "hitterks"),
    "hitterks": ("hitterstrikeouts", "hitterks"),
    "3ptmade": ("3ptmade", "3ptm"),
    "3ptm": ("3ptmade", "3ptm"),
    "rushattempts": ("rushattempts", "rushatts"),
    "rushatts": ("rushattempts", "rushatts"),
    "passcompletions": ("passcompletions", "passcomp"),
    "passcomp": ("passcompletions", "passcomp"),
    "receivingyards": ("receivingyards", "recyards"),
    "recyards": ("receivingyards", "recyards"),
    "longestreception": ("longestreception", "longestrec"),
    "longestrec": ("longestreception", "longestrec"),
    "receptions": ("receptions", "recs"),
    "recs": ("receptions", "recs"),
    "tacklesast": ("tacklesast", "tackles"),
    "tackles": ("tacklesast", "tackles"),
    "totalgameswon": ("totalgameswon", "gameswon"),
    "gameswon": ("totalgameswon", "gameswon"),
    # NBA board chip vs card wording
    "fgattempted": ("fgattempted", "fieldgoalsattempted", "fga"),
    "fieldgoalsattempted": ("fgattempted", "fieldgoalsattempted", "fga"),
    "fga": ("fgattempted", "fieldgoalsattempted", "fga"),
    "fgmade": ("fgmade", "fieldgoalsmade", "fgm"),
    "fgm": ("fgmade", "fieldgoalsmade", "fgm"),
    "ptsrebsasts": ("ptsrebsasts", "ptsrebast", "pra"),
    "ptsrebast": ("ptsrebsasts", "ptsrebast", "pra"),
    "pra": ("ptsrebsasts", "ptsrebast", "pra"),
    "ptsrebs": ("ptsrebs", "ptsreb"),
    "ptsreb": ("ptsrebs", "ptsreb"),
}


def _norm(s: str) -> str:
    return "".join(ch for ch in str(s or "").casefold() if ch.isalnum())


def prop_ok(want: str, got: str) -> bool:
    """Exact alias-set overlap only — never substring ('ks' in 'walksallowed')."""
    w = _norm(want)
    g = _norm(got)
    if not w or not g:
        return False
    want_keys = set(PROP_ALIASES.get(w, ())) | {w}
    got_keys = set(PROP_ALIASES.get(g, ())) | {g}
    return bool(want_keys & got_keys)


@dataclass(frozen=True)
class LegAddResult:
    ok: bool
    reason: str = ""
    player: str = ""
    live_line: Any = None
    want_line: Any = None
    live_pick: str = ""

    def loud_fail(self) -> str:
        if self.reason == "line_moved":
            return (
                f"DROPPED: LINE MOVED {self.player} "
                f"live={self.live_line} ({self.live_pick or '?'}) "
                f"want={self.want_line} — aborting; will not leave a short slip"
            )
        if self.reason == "no_card":
            return f"DROPPED: NO CARD {self.player} want={self.want_line}"
        if self.reason:
            return f"DROPPED: {self.reason.upper()} {self.player} want={self.want_line}"
        return f"DROPPED: {self.player}"


def find_card(
    payout,
    frame,
    player: str,
    prop: str,
    *,
    line: Any = None,
    pick: str | None = None,
    line_key=None,
    require_exact: bool = False,
) -> dict | None:
    cards = payout.get_all_cards(frame)
    pn = _norm(player)
    hits: list[dict] = []
    for c in cards:
        if "+" in str(c.get("player") or ""):
            continue
        cn = _norm(c.get("player"))
        if pn not in cn and cn not in pn:
            continue
        if prop_ok(prop, str(c.get("prop_type") or "")):
            hits.append(c)
    if not hits:
        return None
    want_pick = str(pick or "").strip().lower()
    if line is not None and line_key is not None:
        for c in hits:
            if line_key(c.get("line")) != line_key(line):
                continue
            if want_pick and str(c.get("pick_type") or "").lower() != want_pick:
                continue
            return c
        if require_exact:
            return None
    return hits[0]


def react_search(page, q: str) -> None:
    page.evaluate(
        """(q) => {
      const inp = document.querySelector("input[aria-label='search'], input[placeholder*='Search']");
      if (!inp) return;
      const proto = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value');
      if (proto && proto.set) proto.set.call(inp, q); else inp.value = q;
      inp.dispatchEvent(new Event('input', {bubbles:true}));
    }""",
        q,
    )


def preflight_exposure(
    legs: list[dict[str, Any]],
    *,
    slate_date: str,
    explicit_anchor_player: str,
) -> None:
    """Same ledger as mixer + session (Dampier/Overton). Raises ExposureBlocked."""
    anchor = str(explicit_anchor_player or "").strip()
    if not anchor:
        raise ValueError("explicit_anchor_player is required")
    assert_clears_exposure(
        legs,
        slate_date=slate_date,
        explicit_anchor_player=anchor,
    )


def add_cycled_leg(payout, frame, page, leg: dict[str, Any]) -> LegAddResult:
    """Cycle Standard/Goblin/Demon to the planned line, then click that card's More.

    A moved face is ``ok=False, reason=line_moved``. Callers must abort the
    whole slip (and clear it). Do not add remaining legs.
    """
    player = str(leg["player"])
    prop = str(leg.get("prop_type") or leg.get("prop") or "")
    line = leg.get("line")
    pick = str(leg.get("pick_type") or "goblin").strip().lower() or "goblin"
    card = find_card(
        payout, frame, player, prop, line=line, pick=pick, line_key=payout._line_key
    )
    if card is None:
        print(f"[SLIP] no card {player} {prop}")
        return LegAddResult(False, "no_card", player=player, want_line=line)
    print(
        f"[SLIP] found {card.get('player')} {card.get('line')} "
        f"{card.get('prop_type')} {card.get('pick_type')}"
    )
    need = payout._line_key(card.get("line")) != payout._line_key(line) or str(
        card.get("pick_type") or ""
    ).lower() != pick
    if need:
        payout.cycle_card_to_pick_type(
            frame,
            card.get("more_btn"),
            player=player,
            prop=card.get("prop_type") or prop,
            want_pick=pick,
            want_line=line,
            require_line=True,
            more_index=card.get("more_index"),
        )
        page.wait_for_timeout(400)
        frame = payout.find_prizepicks_frame(page)
        exact = find_card(
            payout,
            frame,
            player,
            prop,
            line=line,
            pick=pick,
            line_key=payout._line_key,
            require_exact=True,
        )
        if exact is not None:
            card = exact
            print(
                f"[SLIP] face on board {card.get('line')} {card.get('pick_type')} "
                f"(cycle match not required)"
            )
        else:
            any_card = find_card(payout, frame, player, prop)
            live_line = None if any_card is None else any_card.get("line")
            live_pick = "" if any_card is None else str(any_card.get("pick_type") or "")
            print(
                f"DROPPED: LINE MOVED {player} live={live_line} "
                f"({live_pick or '?'}) want={line} ({pick})"
            )
            return LegAddResult(
                False,
                "line_moved",
                player=player,
                live_line=live_line,
                want_line=line,
                live_pick=live_pick,
            )
    direction = str(leg.get("direction") or "OVER").upper()
    if direction in ("UNDER", "LESS"):
        if not payout.click_leg(frame, card, direction):
            print(f"[SLIP] no Less {player}")
            return LegAddResult(False, "no_more", player=player, want_line=line)
        page.wait_for_timeout(600)
        print(f"[SLIP] clicked {player} UNDER")
        return LegAddResult(True, player=player, live_line=card.get("line"), want_line=line)
    btn = card.get("more_btn")
    if btn is None:
        # Boards with 80+ More buttons parse in JS and keep an index, not a locator.
        if card.get("more_index") is None or not payout.click_leg(frame, card, direction):
            print(f"[SLIP] no More {player}")
            return LegAddResult(False, "no_more", player=player, want_line=line)
        page.wait_for_timeout(600)
        print(f"[SLIP] clicked {player}")
        return LegAddResult(True, player=player, live_line=card.get("line"), want_line=line)
    try:
        btn.click(timeout=1500, no_wait_after=True)
    except Exception:
        btn.click(force=True, timeout=2500, no_wait_after=True)
    page.wait_for_timeout(600)
    print(f"[SLIP] clicked {player}")
    return LegAddResult(True, player=player, live_line=card.get("line"), want_line=line)


def order_legs_tennis_first(legs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    tennis = [x for x in legs if str(x.get("sport") or "").upper() == "TENNIS"]
    rest = [x for x in legs if str(x.get("sport") or "").upper() != "TENNIS"]
    return tennis + rest


def n_correct_power(raw: str, n: int) -> float | None:
    m = re.search(
        rf"Power Play.*?{n} correct pays\s*(\d+(?:\.\d+)?)\s*X",
        raw,
        flags=re.I | re.S,
    )
    return float(m.group(1)) if m else None
