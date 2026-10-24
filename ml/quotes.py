"""PrizePicks N-correct payouts and per-leg breakeven / EV.

Search for gates that beat the quote, not gates that raise hit rate.
A Goblin that lifts 75% → 80% still loses if Power-3 pays 1.65x (84.6%/leg).
Flex consolation is not priced here — there is no live Flex Goblin card.
"""
from __future__ import annotations

# name, n_legs, payout_x
GOBLIN_QUOTES: tuple[tuple[str, int, float], ...] = (
    ("power2_1.40x", 2, 1.40),
    ("power3_1.65x", 3, 1.65),
    ("power3_2.00x", 3, 2.00),
    ("power3_2.50x", 3, 2.50),
)

STANDARD_QUOTES: tuple[tuple[str, int, float], ...] = (
    ("power2_3.00x", 2, 3.00),
    ("power3_6.00x", 3, 6.00),
)


def quotes_for_book(book: str) -> tuple[tuple[str, int, float], ...]:
    b = (book or "").strip().lower()
    if b.startswith("standard"):
        return STANDARD_QUOTES
    return GOBLIN_QUOTES


def breakeven(n_legs: int, payout_x: float) -> float:
    return float(payout_x ** (-1.0 / max(int(n_legs), 1)))


def ev_at(p: float | None, n_legs: int, payout_x: float) -> float | None:
    if p is None:
        return None
    return round((float(p) ** int(n_legs)) * float(payout_x) - 1.0, 4)


def score_quotes(
    hr: float | None,
    n: int,
    *,
    oos_hr: float | None = None,
    oos_n: int = 0,
    wilson_low: float | None = None,
    book: str = "Goblin OVER",
    min_n: int = 30,
    min_oos: int = 8,
) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for name, n_legs, px in quotes_for_book(book):
        be = round(breakeven(n_legs, px), 4)
        out[name] = {
            "n_legs": n_legs,
            "payout_x": px,
            "breakeven": be,
            "ev": ev_at(hr, n_legs, px),
            "oos_ev": ev_at(oos_hr, n_legs, px),
            "clears": bool(hr is not None and hr >= be and n >= min_n),
            "oos_clears": bool(oos_hr is not None and oos_hr >= be and oos_n >= min_oos),
            "wilson_clears": bool(wilson_low is not None and wilson_low >= be),
        }
    return out
