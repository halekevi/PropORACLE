"""A planted split has to beat the best cell a shuffle can find."""
from utils.gate_shuffle import shuffle_cutoff


def test_planted_split_beats_the_shuffle_cutoff():
    groups = [("points", "d")] * 40
    outcomes = [1] * 20 + [0] * 20

    def score(labels: list[int]) -> float:
        left = sum(labels[:20]) / 20
        right = sum(labels[20:]) / 20
        return max(left, right)

    cutoff = shuffle_cutoff(groups, outcomes, score, draws=40, seed=1)
    assert score(outcomes) == 1.0
    assert cutoff < 1.0
