"""Classify keep-gated slips into Goblin / Standard / Mixed construction tracks.

These tracks answer \"are we constructing efficiently?\" They are *not* the
ungated book catalog. Only slips that already cleared L5=5 + sport keep gates
belong here. YOLO stays on its own side tracks (``standard_yolo`` /
``goblin70_yolo``) and is excluded from the gated-construction totals.
"""
from __future__ import annotations

from typing import Any

from utils.ticket_70_pool import yolo_ticket_track
from utils.ticket_ev_build import slip_family

# grade_history track keys for the construction instrument.
GATED_GOBLIN = "gated_goblin"
GATED_STANDARD = "gated_standard"
GATED_MIXED = "gated_mixed"

GATED_CONSTRUCTION_TRACKS = frozenset({GATED_GOBLIN, GATED_STANDARD, GATED_MIXED})

# Side tracks that measure the off-Income YOLO packs the same way.
YOLO_SIDE_TRACKS = frozenset({"standard_yolo", "goblin70_yolo"})

TRACK_LABELS: dict[str, str] = {
    GATED_GOBLIN: "Gated Goblin-only (L5=5 keep)",
    GATED_STANDARD: "Gated Standard-only (L5=5 keep)",
    GATED_MIXED: "Gated Mixed Goblin+Standard (L5=5 keep)",
    "standard_yolo": "Standard YOLO (off card)",
    "goblin70_yolo": "Goblin-70 YOLO (off card)",
}


def _norm_family(raw: object) -> str:
    s = str(raw or "").strip().lower()
    if s in {"goblin", "g"}:
        return "goblin"
    if s in {"standard", "std", "s"}:
        return "standard"
    if s in {"mix", "mixed", "m"}:
        return "mix"
    return ""


def _family_from_legs(legs: list[Any]) -> str:
    clean = [leg for leg in legs if isinstance(leg, dict)]
    if not clean:
        return ""
    return slip_family(clean)


def _family_from_track(track: str) -> str:
    t = str(track or "").strip().lower()
    if t in {"goblin70", "goblin", GATED_GOBLIN}:
        return "goblin"
    if t in {"standard_ev", "standard", GATED_STANDARD}:
        return "standard"
    if t in {"mixed_ev", "mix", "mixed", GATED_MIXED}:
        return "mix"
    return ""


def construction_family(t: dict[str, Any], group_name: str = "") -> str | None:
    """Return ``goblin`` / ``standard`` / ``mix`` for a keep-gated card slip.

    Returns ``None`` for YOLO, Demons, NFLP-only packs, or slips we cannot
    classify. Mixer / graded_main leftovers without a Goblin-70 family are
    also ``None`` — this instrument only prices the keep-gate EV card.
    """
    if yolo_ticket_track(t, group_name) is not None:
        return None

    fam = _norm_family(t.get("family"))
    if fam:
        return fam

    track = str(t.get("ticket_track") or t.get("core_recipe") or "").strip().lower()
    from_track = _family_from_track(track)
    if from_track:
        return from_track

    # Only infer from legs when the slip already claims a G70 / EV track.
    if track in {
        "goblin70",
        "standard_ev",
        "mixed_ev",
        GATED_GOBLIN,
        GATED_STANDARD,
        GATED_MIXED,
        "goblin",
        "standard",
        "mix",
    }:
        return _family_from_legs(t.get("legs") or []) or None

    tid = str(t.get("id") or t.get("ticket_id") or "").upper()
    if tid.startswith("G") and not tid.startswith("GS"):
        return "goblin"
    if tid.startswith("S"):
        return "standard"
    if tid.startswith("M"):
        return "mix"

    name = str(
        group_name
        or t.get("web_group_name")
        or t.get("web_group")
        or t.get("_group_name")
        or ""
    ).upper()
    if "GOBLIN-70" in name or "GOBLIN 70" in name:
        if "MIX" in name:
            return "mix"
        if "STANDARD" in name:
            return "standard"
        return "goblin"
    return None


def construction_track(t: dict[str, Any], group_name: str = "") -> str | None:
    """Map a ticket to ``gated_goblin`` / ``gated_standard`` / ``gated_mixed``."""
    fam = construction_family(t, group_name)
    if fam == "goblin":
        return GATED_GOBLIN
    if fam == "standard":
        return GATED_STANDARD
    if fam == "mix":
        return GATED_MIXED
    return None


def efficiency_track(t: dict[str, Any], group_name: str = "") -> str | None:
    """Construction track, or YOLO side track when that is the right bucket."""
    yolo = yolo_ticket_track(t, group_name)
    if yolo is not None:
        return yolo
    return construction_track(t, group_name)
