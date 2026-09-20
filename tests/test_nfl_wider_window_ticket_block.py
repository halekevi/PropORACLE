"""NFL wider-window ticket L5 must stay loudly blocked (Week 2 decision)."""
from __future__ import annotations

import pytest

from utils.nfl_keep_gates import (
    NFL_WIDER_WINDOW_TICKET_L5_ENABLED,
    NflTicketSourceBlocked,
    NflWiderWindowTicketBlocked,
    assert_nfl_ready_source,
    assert_nfl_ticket_l5_source,
)


def test_wider_window_flag_defaults_off():
    assert NFL_WIDER_WINDOW_TICKET_L5_ENABLED is False


def test_wider_window_source_raises():
    with pytest.raises(NflWiderWindowTicketBlocked, match="NFL_WIDER_WINDOW_TICKET_BLOCKED"):
        assert_nfl_ticket_l5_source("wider_window")
    with pytest.raises(NflWiderWindowTicketBlocked):
        assert_nfl_ticket_l5_source("boxscore_cache")


def test_handcheck_l5_source_ok():
    assert_nfl_ticket_l5_source("handcheck")


def test_ready_requires_handcheck_tag():
    assert_nfl_ready_source("handcheck")
    with pytest.raises(NflTicketSourceBlocked, match="NFL_TICKET_SOURCE_BLOCKED"):
        assert_nfl_ready_source("wider_window")
    with pytest.raises(NflTicketSourceBlocked):
        assert_nfl_ready_source("nfl_standard_keep_packer")
