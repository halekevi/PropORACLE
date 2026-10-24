"""P(stat > line) from a fitted count / yard distribution.

One projected mean prices Goblin, Standard, and Demon: only the line changes.

Integer lines: P(X = line) is a PrizePicks push, not a miss. Use p_under =
P(X < line), not 1 - p_over. Points / combos never use Poisson (underdispersed
in the tails the promotion test gates on).
"""
from __future__ import annotations

import math
from typing import Literal

import numpy as np

Family = Literal["poisson", "negbin", "normal"]

_POINTS_COMBO = frozenset({"pts", "points", "pra", "pr", "pa", "ra", "minutes"})


def _clip01(p: float) -> float:
    if not math.isfinite(p):
        return float("nan")
    return max(0.0, min(1.0, p))


def is_integer_line(line: float) -> bool:
    if not math.isfinite(line):
        return False
    return abs(line - round(line)) < 1e-9


def p_over_poisson(mu: float, line: float) -> float:
    """P(X > line) for Poisson(mu). .5 lines → 1 - F(floor(line))."""
    if not math.isfinite(mu) or mu < 0 or not math.isfinite(line):
        return float("nan")
    k = int(math.floor(line))
    if k < 0:
        return 1.0
    try:
        from scipy.stats import poisson

        return _clip01(float(poisson.sf(k, mu)))
    except Exception:
        term = math.exp(-mu)
        cdf = term
        for i in range(1, k + 1):
            term *= mu / i
            cdf += term
            if cdf >= 1.0:
                return 0.0
        return _clip01(1.0 - cdf)


def p_under_poisson(mu: float, line: float) -> float:
    """P(X < line) for Poisson(mu)."""
    if not math.isfinite(mu) or mu < 0 or not math.isfinite(line):
        return float("nan")
    if line <= 0:
        return 0.0
    k = int(math.ceil(line) - 1)
    if k < 0:
        return 0.0
    try:
        from scipy.stats import poisson

        return _clip01(float(poisson.cdf(k, mu)))
    except Exception:
        term = math.exp(-mu)
        cdf = term
        for i in range(1, k + 1):
            term *= mu / i
            cdf += term
        return _clip01(min(cdf, 1.0))


def p_push_poisson(mu: float, line: float) -> float:
    if not is_integer_line(line) or line < 0:
        return 0.0
    k = int(round(line))
    try:
        from scipy.stats import poisson

        return _clip01(float(poisson.pmf(k, mu)))
    except Exception:
        if mu == 0:
            return 1.0 if k == 0 else 0.0
        # log pmf
        logp = -mu + k * math.log(mu) - math.lgamma(k + 1)
        return _clip01(math.exp(logp))


def p_over_negbin(mu: float, var: float, line: float) -> float:
    """Negative binomial with mean ``mu`` and variance ``var`` (var must be > mu)."""
    if not math.isfinite(mu) or mu <= 0 or not math.isfinite(var) or var <= mu:
        return p_over_poisson(mu, line)
    k = int(math.floor(line))
    if k < 0:
        return 1.0
    r = (mu * mu) / (var - mu)
    p = r / (r + mu)
    try:
        from scipy.stats import nbinom

        return _clip01(float(nbinom.sf(k, r, p)))
    except Exception:
        return p_over_poisson(mu, line)


def p_under_negbin(mu: float, var: float, line: float) -> float:
    if not math.isfinite(mu) or mu <= 0 or not math.isfinite(var) or var <= mu:
        return p_under_poisson(mu, line)
    if line <= 0:
        return 0.0
    k = int(math.ceil(line) - 1)
    if k < 0:
        return 0.0
    r = (mu * mu) / (var - mu)
    p = r / (r + mu)
    try:
        from scipy.stats import nbinom

        return _clip01(float(nbinom.cdf(k, r, p)))
    except Exception:
        return p_under_poisson(mu, line)


def p_push_negbin(mu: float, var: float, line: float) -> float:
    if not is_integer_line(line) or line < 0:
        return 0.0
    if not math.isfinite(mu) or mu <= 0 or not math.isfinite(var) or var <= mu:
        return p_push_poisson(mu, line)
    k = int(round(line))
    r = (mu * mu) / (var - mu)
    p = r / (r + mu)
    try:
        from scipy.stats import nbinom

        return _clip01(float(nbinom.pmf(k, r, p)))
    except Exception:
        return p_push_poisson(mu, line)


def p_over_normal(mu: float, sd: float, line: float) -> float:
    """P(X > line) for Normal(mu, sd). Integer lines use continuity (X>=line+1)."""
    if not math.isfinite(mu) or not math.isfinite(line):
        return float("nan")
    if not math.isfinite(sd) or sd <= 0:
        return 1.0 if mu > line else (0.0 if mu < line else 0.0)
    thresh = (line + 0.5) if is_integer_line(line) else line
    z = (thresh - mu) / sd
    try:
        from scipy.stats import norm

        return _clip01(float(norm.sf(z)))
    except Exception:
        return _clip01(0.5 * math.erfc(z / math.sqrt(2.0)))


def p_under_normal(mu: float, sd: float, line: float) -> float:
    if not math.isfinite(mu) or not math.isfinite(line):
        return float("nan")
    if not math.isfinite(sd) or sd <= 0:
        return 1.0 if mu < line else 0.0
    thresh = (line - 0.5) if is_integer_line(line) else line
    z = (thresh - mu) / sd
    try:
        from scipy.stats import norm

        return _clip01(float(norm.cdf(z)))
    except Exception:
        return _clip01(0.5 * math.erfc(-z / math.sqrt(2.0)))


def p_push_normal(mu: float, sd: float, line: float) -> float:
    if not is_integer_line(line):
        return 0.0
    if not math.isfinite(mu) or not math.isfinite(sd) or sd <= 0:
        return 1.0 if mu == line else 0.0
    # P(line-0.5 < X < line+0.5)
    try:
        from scipy.stats import norm

        return _clip01(float(norm.cdf((line + 0.5 - mu) / sd) - norm.cdf((line - 0.5 - mu) / sd)))
    except Exception:
        hi = 0.5 * math.erfc(-((line + 0.5 - mu) / sd) / math.sqrt(2.0))
        lo = 0.5 * math.erfc(-((line - 0.5 - mu) / sd) / math.sqrt(2.0))
        return _clip01(hi - lo)


def choose_family(
    mu: float,
    var: float,
    *,
    n: float,
    count_like: bool,
    stat_col: str = "",
) -> Family:
    col = str(stat_col or "").strip().lower()
    if col in _POINTS_COMBO or (not count_like):
        return "normal"
    if math.isfinite(mu) and mu >= 12.0:
        return "normal"
    if n >= 8 and math.isfinite(var) and math.isfinite(mu) and var > mu * 1.15:
        return "negbin"
    return "poisson"


def p_over_line(
    mu: float,
    line: float,
    *,
    var: float | None = None,
    n: float = 0.0,
    count_like: bool = True,
    family: Family | None = None,
    stat_col: str = "",
) -> tuple[float, Family]:
    """Return (P(X > line), family used). Push mass is *not* in this probability."""
    p_over, _p_under, _p_push, fam = line_probs(
        mu, line, var=var, n=n, count_like=count_like, family=family, stat_col=stat_col
    )
    return p_over, fam


def line_probs(
    mu: float,
    line: float,
    *,
    var: float | None = None,
    n: float = 0.0,
    count_like: bool = True,
    family: Family | None = None,
    stat_col: str = "",
) -> tuple[float, float, float, Family]:
    """(P(X > line), P(X < line), P(X = line) on integer lines, family)."""
    v = float(var) if var is not None and math.isfinite(var) else float("nan")
    fam = family or choose_family(mu, v, n=n, count_like=count_like, stat_col=stat_col)
    if fam == "normal":
        sd = math.sqrt(v) if math.isfinite(v) and v > 0 else max(math.sqrt(max(mu, 0.0)), 1.0)
        return p_over_normal(mu, sd, line), p_under_normal(mu, sd, line), p_push_normal(mu, sd, line), fam
    if fam == "negbin":
        return (
            p_over_negbin(mu, v, line),
            p_under_negbin(mu, v, line),
            p_push_negbin(mu, v, line),
            fam,
        )
    return p_over_poisson(mu, line), p_under_poisson(mu, line), p_push_poisson(mu, line), fam
