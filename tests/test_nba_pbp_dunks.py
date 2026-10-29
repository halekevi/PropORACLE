"""Made-dunk PBP counter: putback / alley-oop / tip count; misses and blocks do not."""
from __future__ import annotations

from utils.nba_pbp_dunks import dunks_by_athlete_id, is_made_dunk_play, shooter_athlete_id


def _play(text: str, aid: str = "5104157", **extra):
    p = {
        "text": text,
        "participants": [{"athlete": {"id": aid}}],
        "shootingPlay": True,
    }
    p.update(extra)
    return p


def test_made_dunk_variants_count():
    plays = [
        _play("Victor Wembanyama makes two point driving dunk", scoringPlay=True, scoreValue=2),
        _play("Victor Wembanyama makes alley oop dunk", scoringPlay=True, scoreValue=2),
        _play("Victor Wembanyama makes two point putback dunk", scoringPlay=True, scoreValue=2),
        _play("Victor Wembanyama makes two point tip dunk", scoringPlay=True, scoreValue=2),
        _play("Victor Wembanyama makes reverse dunk", scoringPlay=True, scoreValue=2),
    ]
    assert dunks_by_athlete_id(plays) == {"5104157": 5}


def test_miss_and_block_do_not_count():
    plays = [
        _play("Victor Wembanyama misses two point dunk", scoringPlay=False),
        _play("Dunk shot blocked by Jaren Jackson Jr.", scoringPlay=False),
        _play("Wembanyama dunk blocked by Gobert", scoringPlay=False),
        _play("Victor Wembanyama makes two point layup", scoringPlay=True, scoreValue=2),
    ]
    assert dunks_by_athlete_id(plays) == {}
    assert is_made_dunk_play(plays[0]) is False
    assert is_made_dunk_play(plays[1]) is False


def test_type_text_dunk_shot_with_makes():
    play = {
        "type": {"text": "Dunk Shot"},
        "text": "Giannis Antetokounmpo makes 2-pt dunk",
        "participants": [{"athlete": {"id": "3032977"}}],
        "scoringPlay": True,
        "scoreValue": 2,
        "shootingPlay": True,
    }
    assert is_made_dunk_play(play)
    assert shooter_athlete_id(play) == "3032977"
    assert dunks_by_athlete_id([play]) == {"3032977": 1}


def test_shooting_play_without_score_is_not_a_make():
    play = _play("Victor Wembanyama dunk", shootingPlay=True, scoringPlay=False)
    assert is_made_dunk_play(play) is False
