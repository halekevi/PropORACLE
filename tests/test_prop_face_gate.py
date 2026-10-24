"""Offered-face must match model or re-gate."""
from __future__ import annotations

from utils.prop_face_gate import faces_match, resolve_offered_face


def test_faces_match_same_prop_line():
    assert faces_match("Pts+Rebs", "pts+rebs", model_line=27.5, offered_line=27.5)
    assert not faces_match("Pts+Rebs", "Pts+Rebs+Asts", model_line=27.5, offered_line=32.5)


def test_skip_when_swap_and_no_gate():
    out = resolve_offered_face(
        player="Franz Wagner",
        model_prop="Pts+Rebs",
        model_line=27.5,
        model_side="UNDER",
        offered_prop="Pts+Rebs+Asts",
        offered_line=32.5,
    )
    assert out["action"] == "skip"
    assert out["reason"] == "face_swap_no_gate"


def test_regate_pass_allows_offered_face():
    def gate(_player, prop, line, side):
        return prop.lower().startswith("pts+rebs+asts") and line == 32.5 and side == "UNDER"

    out = resolve_offered_face(
        player="Franz Wagner",
        model_prop="Pts+Rebs",
        model_line=27.5,
        model_side="UNDER",
        offered_prop="Pts+Rebs+Asts",
        offered_line=32.5,
        gate_fn=gate,
    )
    assert out["action"] == "play"
    assert out["regated"] is True
    assert out["play_prop"] == "Pts+Rebs+Asts"
    assert out["model_prop"] == "Pts+Rebs"


def test_match_plays_without_regate():
    out = resolve_offered_face(
        player="Isaiah Jackson",
        model_prop="Rebounds",
        model_line=7.5,
        model_side="UNDER",
        offered_prop="Rebounds",
        offered_line=7.5,
    )
    assert out["action"] == "play"
    assert out["regated"] is False
