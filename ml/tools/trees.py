"""Shallow sklearn tree. Use n_node_samples — sklearn 1.8 tree.value is a class fraction."""
from __future__ import annotations

from typing import Any

import numpy as np


def available() -> str:
    try:
        import sklearn

        return sklearn.__version__
    except Exception:
        return "MISSING"


def _rate(h: int, n: int) -> float | None:
    if n <= 0:
        return None
    return round(h / n, 4)


def fit_depth3(X, y, *, feature_names: list[str], min_leaf: int = 25) -> dict[str, Any]:
    ver = available()
    if ver == "MISSING":
        return {"status": "MISSING", "rules": []}
    if X is None or len(X) < max(40, min_leaf * 2):
        return {"status": "ok", "version": ver, "rules": []}
    try:
        from sklearn.tree import DecisionTreeClassifier
    except Exception as exc:
        return {"status": f"error:{exc}", "rules": []}
    y = np.asarray(y)
    if len(set(y.tolist())) < 2:
        return {"status": "ok", "version": ver, "rules": [], "note": "one-class"}
    clf = DecisionTreeClassifier(max_depth=3, min_samples_leaf=min_leaf, random_state=0)
    clf.fit(X, y)
    tree = clf.tree_
    rules: list[dict[str, Any]] = []

    def walk(node: int, parts: list[str]) -> None:
        n = int(tree.n_node_samples[node])
        if n < min_leaf:
            return
        left = int(tree.children_left[node])
        right = int(tree.children_right[node])
        if left == right:
            # value is class weight / fraction in sklearn 1.8 — not a count
            counts = tree.value[node][0]
            frac = float(counts[1]) if len(counts) > 1 else 0.0
            if frac > 1.0:
                hits = int(round(frac))
            else:
                hits = int(round(frac * n))
            rules.append({"rule": " AND ".join(parts) or "all", "hr": _rate(hits, n), "n": n, "hits": hits})
            return
        feat = feature_names[int(tree.feature[node])]
        thr = float(tree.threshold[node])
        walk(left, parts + [f"{feat}<={thr:g}"])
        walk(right, parts + [f"{feat}>{thr:g}"])

    walk(0, [])
    rules.sort(key=lambda r: (-(r.get("hr") or 0), -r["n"]))
    return {"status": "ok", "version": ver, "rules": rules[:12]}
