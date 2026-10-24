"""Offered-face gate: never ticket a different prop than the model cleared.

If PrizePicks does not offer the model prop/line face, re-run the L5 (and
sport keep) gate on the offered face, or skip the player. Face swaps
(Pts+Rebs → PRA, Pts+Asts → Points) change the bet.
"""
from __future__ import annotations

from typing import Any, Callable


def prop_key(prop: str) -> str:
    return str(prop or "").strip().lower().replace("(combo)", "").strip()


def faces_match(
    model_prop: str,
    offered_prop: str,
    *,
    model_line: float | None = None,
    offered_line: float | None = None,
    line_tol: float = 0.01,
) -> bool:
    if prop_key(model_prop) != prop_key(offered_prop):
        return False
    if model_line is None or offered_line is None:
        return True
    try:
        return abs(float(model_line) - float(offered_line)) <= line_tol
    except (TypeError, ValueError):
        return False


def resolve_offered_face(
    *,
    player: str,
    model_prop: str,
    model_line: float,
    model_side: str,
    offered_prop: str | None,
    offered_line: float | None,
    offered_side: str | None = None,
    gate_fn: Callable[..., bool] | None = None,
    gate_kwargs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return play / skip decision for board vs model face.

    ``gate_fn(player, prop, line, side, **gate_kwargs) -> bool`` re-runs the
    eligibility gate on the offered face when it differs from the model.
    """
    side = str(offered_side or model_side or "").strip().upper()
    if side in {"O", "U"}:
        side = "OVER" if side == "O" else "UNDER"

    if offered_prop is None or offered_line is None:
        return {
            "action": "skip",
            "reason": "offered_face_missing",
            "player": player,
            "model_prop": model_prop,
            "model_line": model_line,
            "model_side": model_side,
            "offered_prop": offered_prop,
            "offered_line": offered_line,
            "play_prop": None,
            "play_line": None,
            "play_side": None,
            "regated": False,
        }

    if faces_match(model_prop, offered_prop, model_line=model_line, offered_line=offered_line):
        return {
            "action": "play",
            "reason": "face_matches_model",
            "player": player,
            "model_prop": model_prop,
            "model_line": model_line,
            "model_side": model_side,
            "offered_prop": offered_prop,
            "offered_line": float(offered_line),
            "play_prop": offered_prop,
            "play_line": float(offered_line),
            "play_side": side,
            "regated": False,
        }

    # Face differs — must re-run gate on the offered face.
    if gate_fn is None:
        return {
            "action": "skip",
            "reason": "face_swap_no_gate",
            "player": player,
            "model_prop": model_prop,
            "model_line": model_line,
            "model_side": model_side,
            "offered_prop": offered_prop,
            "offered_line": float(offered_line),
            "play_prop": None,
            "play_line": None,
            "play_side": None,
            "regated": False,
        }

    kwargs = dict(gate_kwargs or {})
    ok = bool(gate_fn(player, offered_prop, float(offered_line), side, **kwargs))
    if not ok:
        return {
            "action": "skip",
            "reason": "offered_face_failed_gate",
            "player": player,
            "model_prop": model_prop,
            "model_line": model_line,
            "model_side": model_side,
            "offered_prop": offered_prop,
            "offered_line": float(offered_line),
            "play_prop": None,
            "play_line": None,
            "play_side": None,
            "regated": True,
        }

    return {
        "action": "play",
        "reason": "offered_face_regate_pass",
        "player": player,
        "model_prop": model_prop,
        "model_line": model_line,
        "model_side": model_side,
        "offered_prop": offered_prop,
        "offered_line": float(offered_line),
        "play_prop": offered_prop,
        "play_line": float(offered_line),
        "play_side": side,
        "regated": True,
    }
