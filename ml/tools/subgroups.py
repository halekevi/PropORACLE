"""pysubgroup wrapper. Closest off-the-shelf match to scan_gate_thresholds."""
from __future__ import annotations

from typing import Any


def available() -> str:
    try:
        import pysubgroup  # noqa: F401

        return getattr(__import__("pysubgroup"), "__version__", "present")
    except Exception:
        return "MISSING"


def discover(
    df,
    *,
    target: str = "hit",
    ignore: tuple[str, ...] = (),
    depth: int = 2,
    k: int = 12,
    min_n: int = 40,
    min_support: int = 50,
) -> dict[str, Any]:
    """Return readable AND-slices ranked by pysubgroup StandardQF.

    ``min_support`` is a hard floor on subgroup size via MinSupportConstraint.
    Without it beam search walks into empty slices and StandardQF divides by
    zero (the hang seen on the all-sports pass).
    """
    ver = available()
    if ver == "MISSING":
        return {"status": "MISSING", "rules": []}
    try:
        import pysubgroup as ps
    except Exception as exc:
        return {"status": f"error:{exc}", "rules": []}
    if df is None or len(df) < min_n:
        return {"status": "ok", "version": ver, "rules": []}
    drop = set(ignore) | {target}
    work = df.drop(columns=[c for c in drop if c in df.columns], errors="ignore").copy()
    work[target] = df[target].astype(int)
    # pysubgroup wants discrete selectors; cap high-card columns
    for col in list(work.columns):
        if col == target:
            continue
        if work[col].dtype == object or str(work[col].dtype) == "bool":
            work[col] = work[col].astype(str)
            if work[col].nunique(dropna=True) > 12:
                work.drop(columns=[col], inplace=True)
    try:
        from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout

        target_spec = ps.BinaryTarget(target, 1)
        searchspace = ps.create_selectors(work, ignore=[target])
        # Drop selectors that alone cover fewer than min_support rows.
        kept = []
        for sel in searchspace:
            try:
                if int(sel.covers(work).sum()) >= min_support:
                    kept.append(sel)
            except Exception:
                continue
        constraints = []
        if hasattr(ps, "MinSupportConstraint"):
            constraints.append(ps.MinSupportConstraint(min_support))
        task = ps.SubgroupDiscoveryTask(
            work,
            target_spec,
            kept or searchspace,
            result_set_size=k,
            depth=depth,
            qf=ps.StandardQF(0.5),
            min_quality=0.0,
            constraints=constraints,
        )

        def _run():
            return ps.BeamSearch().execute(task)

        with ThreadPoolExecutor(max_workers=1) as pool:
            result = pool.submit(_run).result(timeout=25)
    except FuturesTimeout:
        return {"status": "timeout", "version": ver, "rules": []}
    except Exception as exc:
        return {"status": f"error:{exc}", "version": ver, "rules": []}
    rules: list[dict[str, Any]] = []
    try:
        rows = list(result.to_descriptions())
    except Exception:
        rows = list(result)
    for item in rows:
        if isinstance(item, (tuple, list)):
            q = item[0] if item else None
            sg = item[1] if len(item) > 1 else item[0]
        else:
            q = getattr(item, "quality", None)
            sg = getattr(item, "subgroup", item)
        n = None
        hits = None
        try:
            mask = sg.covers(work)
            n = int(mask.sum())
            hits = int(work.loc[mask, target].sum())
        except Exception:
            pass
        rules.append(
            {
                "rule": str(sg),
                "quality": round(float(q), 4) if q is not None else None,
                "n": n,
                "hits": hits,
                "hr": (round(hits / n, 4) if n else None),
            }
        )
    return {"status": "ok", "version": ver, "rules": rules}
