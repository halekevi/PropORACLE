"""Shuffle test for a gate search.

Shuffle hit/miss labels inside each prop-date group, rerun the same search,
and keep the best cell rate from each draw. A cell is not a find unless its
rate beats the 95th percentile of those best-by-chance rates.
"""
from __future__ import annotations

import random
from collections import defaultdict
from typing import Callable, Sequence


def shuffle_cutoff(
    groups: Sequence[tuple],
    outcomes: Sequence[int],
    score_fn: Callable[[list[int]], float],
    *,
    draws: int = 200,
    seed: int = 0,
) -> float:
    """95th percentile of the best cell rate under shuffled outcomes.

    ``groups[i]`` is the prop-date key for ``outcomes[i]``. Labels move only
    inside a key. ``score_fn`` receives one shuffled label list and returns
    the best cell rate that search found.
    """
    if len(groups) != len(outcomes):
        raise ValueError("groups and outcomes must be the same length")
    by: dict[tuple, list[int]] = defaultdict(list)
    for i, key in enumerate(groups):
        by[tuple(key)].append(i)
    rng = random.Random(seed)
    bests: list[float] = []
    labels = [int(v) for v in outcomes]
    for _ in range(draws):
        shuffled = labels[:]
        for idxs in by.values():
            bag = [shuffled[i] for i in idxs]
            rng.shuffle(bag)
            for i, value in zip(idxs, bag):
                shuffled[i] = value
        bests.append(float(score_fn(shuffled)))
    bests.sort()
    if not bests:
        return 0.0
    pos = min(len(bests) - 1, max(0, int(round(0.95 * (len(bests) - 1)))))
    return bests[pos]
