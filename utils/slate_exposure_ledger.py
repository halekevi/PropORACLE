"""Slate-day cross-ticket player exposure ledger (anchor-cap).

Supersedes the old flat ``MAX_SLIPS_PER_PLAYER = 4`` as the *primary*
exposure policy. The flat total is derived:

    max_total = max_anchor_tickets + max_nonanchor_tickets  # default 1+1=2

All checks must pass (stricter wins). See
``.cursor/rules/cross-ticket-anchor-cap.mdc``.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import Any, Iterable

from utils.player_name_utils import normalize_player_name

_BADGE_RANK = {
    "diamond": 5,
    "platinum": 4,
    "gold": 3,
    "silver": 2,
    "bronze": 1,
}


def _env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name, "") or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_bool(name: str, default: bool = True) -> bool:
    raw = str(os.getenv(name, "") or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "y", "on"}


# Primary policy knobs (anchor-cap). Flat MAX_SLIPS_PER_PLAYER must equal
# max_anchor + max_nonanchor once this ledger is the authority.
MAX_ANCHOR_TICKETS_PER_PLAYER = max(0, _env_int("PROPORACLE_SLATE_MAX_ANCHOR_TICKETS_PER_PLAYER", 1))
MAX_NONANCHOR_TICKETS_PER_PLAYER = max(
    0, _env_int("PROPORACLE_SLATE_MAX_NONANCHOR_TICKETS_PER_PLAYER", 1)
)
BLOCK_SAME_PLAYER_PROP = _env_bool("PROPORACLE_SLATE_BLOCK_SAME_PLAYER_PROP", True)


def max_total_tickets_per_player() -> int:
    """Derived flat ceiling — replaces legacy MAX_SLIPS_PER_PLAYER=4."""
    return int(MAX_ANCHOR_TICKETS_PER_PLAYER) + int(MAX_NONANCHOR_TICKETS_PER_PLAYER)


def fold_player(name: object) -> str:
    return normalize_player_name(str(name or "")).casefold()


def player_atoms(name: object) -> list[str]:
    """Split combo labels into atoms (solo name → one atom)."""
    raw = str(name or "").strip()
    if not raw or raw.lower() in {"nan", "none", "—", "-"}:
        return []
    if "+" in raw or " & " in raw or re.search(r"\band\b", raw, re.I):
        parts = [
            p.strip()
            for p in re.split(r"\s*(?:\+|&|\band\b)\s*", raw, flags=re.I)
            if p.strip()
        ]
    else:
        parts = [raw]
    out: list[str] = []
    for part in parts:
        n = fold_player(part)
        if n:
            out.append(n)
    return out


def prop_token(leg: dict[str, Any]) -> str:
    return (
        str(leg.get("prop_type") or leg.get("prop") or "")
        .strip()
        .lower()
        .replace("-", " ")
    )


def direction_token(leg: dict[str, Any]) -> str:
    d = str(leg.get("direction") or leg.get("side") or leg.get("bet_direction") or "")
    d = d.strip().upper()
    if "UNDER" in d:
        return "UNDER"
    if "OVER" in d or d in {"MORE", "O"}:
        return "OVER"
    return d or "OVER"


def player_prop_key(leg: dict[str, Any], player: str) -> str:
    """Identical-angle key (no line) — blocks McGreevy×4 copy-paste."""
    return f"{player}::{prop_token(leg)}::{direction_token(leg)}"


def _flt(v: object, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except (TypeError, ValueError):
        return default


def pick_anchor_leg(legs: Iterable[dict[str, Any]]) -> dict[str, Any] | None:
    """Deterministic anchor: badge → rank_score → |cover/edge| → first."""
    rows = [leg for leg in legs if isinstance(leg, dict)]
    if not rows:
        return None

    def sort_key(leg: dict[str, Any]) -> tuple:
        badge = str(leg.get("promo") or leg.get("badge") or "").strip().lower()
        # "Gold*" / "D/Platinum" style tags
        for tok in ("diamond", "platinum", "gold", "silver", "bronze"):
            if tok in badge:
                badge = tok
                break
        rank = _flt(leg.get("rank_score"))
        cover = abs(
            _flt(leg.get("cover"), _flt(leg.get("abs_edge"), _flt(leg.get("edge"))))
        )
        return (_BADGE_RANK.get(badge, 0), rank, cover)

    return max(rows, key=sort_key)


def anchor_players(legs: Iterable[dict[str, Any]]) -> set[str]:
    """Player atoms on the anchor leg (combo → multiple atoms)."""
    leg = pick_anchor_leg(legs)
    if not leg:
        return set()
    return set(player_atoms(leg.get("player")))


@dataclass
class PlayerExposure:
    anchor_tickets: int = 0
    nonanchor_tickets: int = 0
    props: set[str] = field(default_factory=set)


@dataclass
class SlateExposureLedger:
    """Shared day ledger for mixer + Goblin-70 + manual check CLI."""

    max_anchor: int = MAX_ANCHOR_TICKETS_PER_PLAYER
    max_nonanchor: int = MAX_NONANCHOR_TICKETS_PER_PLAYER
    block_same_prop: bool = BLOCK_SAME_PLAYER_PROP
    players: dict[str, PlayerExposure] = field(default_factory=dict)

    def _exp(self, player: str) -> PlayerExposure:
        if player not in self.players:
            self.players[player] = PlayerExposure()
        return self.players[player]

    def max_total(self) -> int:
        return int(self.max_anchor) + int(self.max_nonanchor)

    def can_add(
        self,
        legs: Iterable[dict[str, Any]],
        *,
        explicit_anchor_player: str | None = None,
    ) -> tuple[bool, str]:
        rows = [leg for leg in legs if isinstance(leg, dict)]
        if not rows:
            return True, ""

        if explicit_anchor_player:
            anchors = set(player_atoms(explicit_anchor_player))
        else:
            anchors = anchor_players(rows)

        # Per-player role on this ticket
        ticket_players: dict[str, str] = {}  # player -> "anchor"|"nonanchor"
        for leg in rows:
            atoms = player_atoms(leg.get("player"))
            for p in atoms:
                if p in anchors:
                    ticket_players[p] = "anchor"
                elif p not in ticket_players:
                    ticket_players[p] = "nonanchor"

        for p, role in ticket_players.items():
            exp = self._exp(p)
            total = exp.anchor_tickets + exp.nonanchor_tickets
            if total >= self.max_total():
                return False, f"total_cap:{p}:{total}>={self.max_total()}"
            if role == "anchor" and exp.anchor_tickets >= self.max_anchor:
                return False, f"anchor_cap:{p}:{exp.anchor_tickets}>={self.max_anchor}"
            if role == "nonanchor" and exp.nonanchor_tickets >= self.max_nonanchor:
                return (
                    False,
                    f"nonanchor_cap:{p}:{exp.nonanchor_tickets}>={self.max_nonanchor}",
                )

        if self.block_same_prop:
            for leg in rows:
                atoms = player_atoms(leg.get("player"))
                for p in atoms:
                    key = player_prop_key(leg, p)
                    if not key.endswith("::"):
                        if key in self._exp(p).props:
                            return False, f"same_prop:{key}"

        return True, ""

    def register(
        self,
        legs: Iterable[dict[str, Any]],
        *,
        explicit_anchor_player: str | None = None,
    ) -> None:
        rows = [leg for leg in legs if isinstance(leg, dict)]
        if not rows:
            return
        if explicit_anchor_player:
            anchors = set(player_atoms(explicit_anchor_player))
        else:
            anchors = anchor_players(rows)

        ticket_players: dict[str, str] = {}
        for leg in rows:
            for p in player_atoms(leg.get("player")):
                if p in anchors:
                    ticket_players[p] = "anchor"
                elif p not in ticket_players:
                    ticket_players[p] = "nonanchor"

        for p, role in ticket_players.items():
            exp = self._exp(p)
            if role == "anchor":
                exp.anchor_tickets += 1
            else:
                exp.nonanchor_tickets += 1

        if self.block_same_prop:
            for leg in rows:
                for p in player_atoms(leg.get("player")):
                    key = player_prop_key(leg, p)
                    if key and not key.endswith("::"):
                        exp = self._exp(p)
                        exp.props.add(key)

    def snapshot(self) -> dict[str, dict[str, Any]]:
        return {
            p: {
                "anchor_tickets": e.anchor_tickets,
                "nonanchor_tickets": e.nonanchor_tickets,
                "props": sorted(e.props),
            }
            for p, e in sorted(self.players.items())
        }


def trim_payload_slate_exposure(
    payload: dict[str, Any],
    *,
    include_goblin70: bool = True,
) -> dict[str, Any]:
    """
    Walk all groups in order; keep first tickets that clear the ledger.

    Unlike the old trim, Goblin-70 is included by default so the dual card
    shares one exposure ledger (the 9/19 McGreevy failure mode).
    """
    if not isinstance(payload, dict):
        return payload
    ledger = SlateExposureLedger()
    out = dict(payload)
    new_groups: list[dict] = []
    dropped = 0
    reasons: list[str] = []

    def _is_g70(ticket: dict, gname: str) -> bool:
        track = str(ticket.get("ticket_track") or "").lower()
        if "goblin70" in track or track == "g70":
            return True
        gn = (gname or "").lower()
        return "goblin-70" in gn or "goblin70" in gn

    for g in out.get("groups") or []:
        if not isinstance(g, dict):
            continue
        gname = str(g.get("group_name") or "")
        kept: list[dict] = []
        for t in g.get("tickets") or []:
            if not isinstance(t, dict):
                continue
            if not include_goblin70 and _is_g70(t, gname):
                kept.append(t)
                continue
            legs = [leg for leg in (t.get("legs") or []) if isinstance(leg, dict)]
            ok, reason = ledger.can_add(legs)
            if not ok:
                dropped += 1
                if len(reasons) < 40:
                    reasons.append(f"{t.get('ticket_id') or gname}:{reason}")
                continue
            ledger.register(legs)
            kept.append(t)
        if not kept:
            continue
        ng = dict(g)
        ng["tickets"] = kept
        new_groups.append(ng)

    out["groups"] = new_groups
    if dropped:
        out["slate_exposure_dropped"] = int(dropped)
        out["slate_exposure_reasons"] = reasons
        out["player_slip_cap_dropped"] = int(dropped)  # back-compat key
    out["slate_exposure_snapshot"] = ledger.snapshot()
    return out
