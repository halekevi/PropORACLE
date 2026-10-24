"""imodels FIGS wrapper. Short if-then trees that read like keep_gates."""
from __future__ import annotations

from typing import Any


def available() -> str:
    try:
        import imodels  # noqa: F401

        return getattr(__import__("imodels"), "__version__", "present")
    except Exception:
        return "MISSING"


def fit_figs(X, y, *, max_rules: int = 6, feature_names: list[str] | None = None) -> dict[str, Any]:
    ver = available()
    if ver == "MISSING":
        return {"status": "MISSING", "rules": []}
    if X is None or len(X) < 80:
        return {"status": "ok", "version": ver, "rules": [], "note": "n<80"}
    try:
        from imodels import FIGSClassifier
    except Exception as exc:
        return {"status": f"error:{exc}", "rules": []}
    try:
        clf = FIGSClassifier(max_rules=max_rules, random_state=0)
        clf.fit(X, y, feature_names=feature_names)
        text = str(clf)
    except Exception as exc:
        return {"status": f"error:{exc}", "version": ver, "rules": []}
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    return {"status": "ok", "version": ver, "rules": lines[:24], "raw": text[:4000]}
