"""High-confidence ml_prob caps after linear scalars.

Linear scalars cannot unsaturate p≈1.0. Graded Jul–Aug 2026: MLB Goblin OVER
p>=0.90 hits ~61%; Soccer Std+Gob August mean_p ~0.99 hits ~40%.
Tennis is left uncapped (honest ranking + calibration).
"""
from __future__ import annotations

import numpy as np
import pandas as pd

# (sport, pick_type, direction) -> max displayed probability
ML_PROB_TAIL_CAPS: dict[tuple[str, str, str], float] = {
    ("MLB", "goblin", "OVER"): 0.72,
    ("SOCCER", "goblin", "OVER"): 0.52,
    ("SOCCER", "goblin", "UNDER"): 0.72,
    ("SOCCER", "standard", "OVER"): 0.55,
}


def apply_ml_prob_tail_caps(
    p: np.ndarray,
    *,
    sport: str,
    pick_lower: pd.Series,
    dir_upper: pd.Series,
) -> np.ndarray:
    out = np.asarray(p, dtype=float).copy()
    if len(out) == 0:
        return out
    spu = str(sport or "").strip().upper()
    ptv = pick_lower.astype(str).str.strip().str.lower()
    drv = dir_upper.astype(str).str.strip().str.upper()
    for (s0, p0, d0), cap in ML_PROB_TAIL_CAPS.items():
        if spu != s0:
            continue
        m = ptv.eq(p0) & drv.eq(d0)
        if not bool(m.any()):
            continue
        idx = np.where(m.to_numpy())[0]
        out[idx] = np.minimum(out[idx], float(cap))
    return out
