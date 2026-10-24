"""Power-6 Standard bucket gate — log-only (does not drop tickets)."""
from __future__ import annotations

from utils.power6_std_buckets import (
    annotate_long_parlay_power6_buckets,
    filter_long_parlay_groups_to_power6_buckets,
    power6_bucket_ok,
    team_changed_blocks_power_l5,
)


def test_nba_wnba_blocked_from_power6():
    assert not power6_bucket_ok("NBA", "UNDER", buckets=frozenset({("NHL", "UNDER")}))
    assert not power6_bucket_ok("WNBA", "OVER", buckets=frozenset({("NHL", "OVER")}))


def test_nhl_under_allowed_when_bucket_listed():
    assert power6_bucket_ok(
        "NHL", "UNDER", buckets=frozenset({("NHL", "UNDER"), ("NFL", "UNDER")})
    )


def test_tennis_under_unproven():
    assert not power6_bucket_ok(
        "TENNIS", "UNDER", buckets=frozenset({("TENNIS", "UNDER")})
    )


def test_team_changed_blocks_until_five_games():
    assert team_changed_blocks_power_l5(
        {"team_changed": 1, "games_with_new_team": 2, "min_new_team_games": 5}
    )
    assert not team_changed_blocks_power_l5(
        {"team_changed": 1, "games_with_new_team": 5, "min_new_team_games": 5}
    )


def test_annotate_is_log_only_keeps_disallowed_standard_slip():
    payload = {
        "groups": [
            {
                "group_name": "Long",
                "sport": "NBA",
                "tickets": [
                    {
                        "legs": [
                            {
                                "player": "A",
                                "sport": "NBA",
                                "pick_type": "Standard",
                                "direction": "UNDER",
                            }
                        ]
                    }
                ],
            }
        ]
    }
    out = annotate_long_parlay_power6_buckets(
        payload, buckets=frozenset({("NHL", "UNDER")})
    )
    assert len(out["groups"]) == 1
    assert len(out["groups"][0]["tickets"]) == 1
    assert out["power6_bucket_filter"]["mode"] == "log_only"
    assert out["power6_bucket_filter"]["tickets_dropped"] == 0
    assert out["power6_bucket_filter"]["tickets_would_drop"] == 1


def test_filter_alias_also_log_only():
    payload = {
        "groups": [
            {
                "group_name": "Long",
                "sport": "NBA",
                "tickets": [
                    {
                        "legs": [
                            {
                                "player": "A",
                                "sport": "NBA",
                                "pick_type": "Standard",
                                "direction": "UNDER",
                            }
                        ]
                    }
                ],
            }
        ]
    }
    out = filter_long_parlay_groups_to_power6_buckets(
        payload, buckets=frozenset({("NHL", "UNDER")})
    )
    assert out["groups"][0]["tickets"]
    assert out["power6_bucket_filter"]["tickets_dropped"] == 0
