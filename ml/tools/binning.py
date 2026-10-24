"""optbinning wrapper. Finds where a single feature's hit rate actually changes."""
from __future__ import annotations

from typing import Any


def available() -> str:
    try:
        import optbinning  # noqa: F401

        return getattr(__import__("optbinning"), "__version__", "present")
    except Exception:
        return "MISSING"


def bin_feature(x, y, *, name: str, min_n: int = 40) -> dict[str, Any]:
    ver = available()
    if ver == "MISSING":
        return {"status": "MISSING", "name": name, "bins": []}
    if x is None or len(x) < min_n:
        return {"status": "ok", "version": ver, "name": name, "bins": []}
    try:
        from optbinning import OptimalBinning
    except Exception as exc:
        return {"status": f"error:{exc}", "name": name, "bins": []}
    try:
        optb = OptimalBinning(name=name, dtype="numerical", monotonic_trend="auto", min_bin_size=max(0.04, min_n / max(len(x), 1)))
        optb.fit(x, y)
        table = optb.binning_table.build()
    except Exception as exc:
        return {"status": f"error:{exc}", "version": ver, "name": name, "bins": []}
    bins: list[dict[str, Any]] = []
    for rec in table.to_dict(orient="records"):
        bins.append(
            {
                "bin": str(rec.get("Bin", rec.get("bin", ""))),
                "n": rec.get("Count", rec.get("n")),
                "event_rate": rec.get("Event rate", rec.get("event_rate")),
            }
        )
    return {"status": "ok", "version": ver, "name": name, "bins": bins[:16]}
