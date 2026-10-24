"""Resolve PrizePicks N-correct / To Win multipliers from live scrapes.

Never use 1st-place. A scrape of the open slip is the payout. Anything printed
before that scrape is an estimate.

Short slips resolve in this order:
  1. Live CDP scrape with matching composition + Goblin-Δ (prefer slate date)
  2. 3-leg slips and mixed slips: 25th percentile of past Power scrapes for
     that Standard/Goblin count. The composition median flipped several
     held-out 3-leg slips. 2-leg Goblins stay off this quote; their
     scrapes straddle the 1.4x floor.
  3. mix_by_delta cell, when that percentile has no scrapes yet
  4. Dated slip pin (emergency only)
  5. Hardcoded last-known fallback

Long slips (4+ legs) do not use a gap-cell median or a hardcoded multiple.
The same Goblin gaps have paid across a wide range, and a high quote clears
the 1.4x EV floor on a ticket the board will price lower. The pre-scrape
quote is a low percentile of past scrapes for that Standard/Goblin count.
With too few scrapes the quote stays under 1.4x so the slip cannot clear.
"""
from __future__ import annotations

import json
import math
import re
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
OVERRIDE_LATEST = ROOT / "data" / "reports" / "payout_overrides_latest.json"
LIVE_CDP = ROOT / "ui_runner" / "data" / "payout_ladder_live_cdp.json"
MIX_TABLES = ROOT / "data" / "reports" / "predicted_payout_tables_latest.json"

# Last-resort tables. Goblin Power/Flex pins are the 2026-08-27 live slips.
PAY_FALLBACK: dict[tuple[str, int, str], dict[str, Any]] = {
    ("goblin", 3, "Power"): {
        "n_correct": {3: 2.0},
        "note": "0S+3G Power fallback 2x",
    },
    ("goblin", 2, "Power"): {
        "n_correct": {2: 2.2},
        "note": "0S+2G Power fallback 2.2x — confirm on the slip",
    },
    ("goblin", 3, "Flex"): {
        "n_correct": {3: 1.7, 2: 0.5},
        "note": "0S+3G Flex fallback 1.7x / 0.5x",
    },
    ("goblin", 4, "Power"): {
        "n_correct": {4: 2.4},
        "note": "0S+4G Power fallback 2.4x",
    },
    ("goblin", 5, "Power"): {
        "n_correct": {5: 6.5},
        "note": "0S+5G Power fallback 6.5x — confirm on the slip",
    },
    ("goblin", 6, "Power"): {
        "n_correct": {6: 8.5},
        "note": "0S+6G Power fallback 8.5x — confirm on the slip",
    },
    ("mix", 4, "Power"): {
        "n_correct": {4: 5.5},
        "note": "1S+3G Power fallback 5.5x — confirm on the slip",
    },
    ("mix", 5, "Power"): {
        "n_correct": {5: 7.75},
        "note": "1S+4G Power fallback 7.75x — confirm on the slip",
    },
    ("mix", 6, "Power"): {
        "n_correct": {6: 8.75},
        "note": "1S+5G Power fallback 8.75x — confirm on the slip",
    },
    ("goblin", 4, "Flex"): {
        "n_correct": {4: 1.9, 3: 0.5},
        "note": "0S+4G Flex fallback 1.9x / 0.5x",
    },
    ("goblin", 5, "Flex"): {
        "n_correct": {5: 5.5, 4: 0.875, 3: 0.325},
        "note": "0S+5G Flex fallback 5.5x / 0.875x / 0.325x — confirm on the slip",
    },
    ("goblin", 6, "Flex"): {
        "n_correct": {6: 6.0, 5: 0.75, 4: 0.325},
        "note": "0S+6G Flex fallback 6x / 0.75x / 0.325x — confirm on the slip",
    },
    ("mix", 4, "Flex"): {
        "n_correct": {4: 3.0, 3: 0.75},
        "note": "1S+3G Flex fallback 3x / 0.75x",
    },
    ("standard", 3, "Flex"): {
        "n_correct": {3: 2.25, 2: 1.25},
        "note": "3S Flex canonical 2.25x / 1.25x",
    },
    ("nflp", 3, "Power"): {
        "n_correct": {3: 2.0},
        "note": "NFLP Goblin Power fallback 2x",
    },
    ("nflp", 2, "Power"): {
        "n_correct": {2: 2.2},
        "note": "NFLP Goblin Power 2 fallback 2.2x",
    },
    ("nflp_std", 3, "Power"): {
        "n_correct": {3: 6.0},
        "note": "3S Power canonical 6x",
    },
    ("nflp_std", 2, "Power"): {
        "n_correct": {2: 3.0},
        "note": "2S Power canonical 3x",
    },
}


def _f(raw: object) -> float | None:
    try:
        v = float(raw)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if not math.isfinite(v) or v <= 0:
        return None
    return v


def _load_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def pick_type_norm(raw: object) -> str:
    s = str(raw or "").strip().lower()
    if "gob" in s:
        return "Goblin"
    if "std" in s or s == "standard":
        return "Standard"
    return str(raw or "").strip() or "Unknown"


def composition_from_legs(legs: list[dict]) -> tuple[int, int, int]:
    n_g = sum(1 for x in legs if pick_type_norm(x.get("pick_type")) == "Goblin")
    n_s = sum(1 for x in legs if pick_type_norm(x.get("pick_type")) == "Standard")
    n = len(legs)
    if n_s + n_g < n:
        n_g = n - n_s
    return n_s, n_g, n


def goblin_delta_sig(legs: list[dict]) -> str:
    parts: list[float] = []
    for x in legs:
        if pick_type_norm(x.get("pick_type")) != "Goblin":
            continue
        line = _f(x.get("line"))
        std = _f(x.get("standard_line"))
        if line is None or std is None:
            continue
        d = abs(std - line)
        if d >= 0.25:
            parts.append(d)
    if not parts:
        return ""
    return "+".join(f"{v:g}" for v in sorted(parts))


def _norm_delta_sig(raw: object) -> str:
    if isinstance(raw, (list, tuple)):
        vals: list[float] = []
        for part in raw:
            v = _f(part)
            if v is not None:
                vals.append(v)
        return "+".join(f"{v:g}" for v in sorted(vals))
    text = str(raw or "").strip()
    if not text or text in {"—", "-", "unknown"}:
        return ""
    vals = []
    for part in text.replace("|", "+").replace(",", "+").split("+"):
        v = _f(part)
        if v is not None:
            vals.append(v)
    return "+".join(f"{v:g}" for v in sorted(vals))


def _int_ladder(raw: object) -> dict[int, float]:
    out: dict[int, float] = {}
    if not isinstance(raw, dict):
        return out
    for k, v in raw.items():
        try:
            hits = int(k)
        except (TypeError, ValueError):
            continue
        fx = _f(v)
        if fx is not None:
            out[hits] = fx
    return out


def _result(
    table: dict[int, float],
    *,
    note: str,
    source: str,
    captured_at: str | None = None,
) -> dict[str, Any]:
    out = {
        "n_correct": {int(k): float(v) for k, v in table.items()},
        "note": note,
        "payout_source": source,
    }
    if captured_at:
        out["captured_at"] = str(captured_at)
    return out


# 4+ legs. Gap cells for these slips span several multiples (all-Goblin 6-leg
# with one gap pattern has paid 4x and 14x). Quote the low end, not the median.
LONG_SLIP_MIN_LEGS = 4
# p10 once a composition has enough scrapes. Five rows still put the 10th
# percentile above the minimum (5.1x vs 4.5x), and that gap flips a 6-leg
# EV test. Until the cell is this deep, quote the lowest scrape.
LOW_PERCENTILE = 10.0
MIN_SCRAPES_FOR_PERCENTILE = 20
# Under the 1.4x all-hit gate. A missing long-slip history must not clear EV.
UNPRICED_ALL_HIT_X = 1.0
# 3-leg and mixed pre-scrape quote. Not the minimum: that drops slips the
# board still pays. Not the median: that cleared slips the board priced lower.
SHORT_P25 = 25.0
# Fewer than this many scrapes: quote the lowest one. A percentile of
# two or three rows is the sample, not a rate.
MIN_SCRAPES_FOR_P25 = 5
# July 12–13 2-leg Goblins paid 2.1x–2.8x. July 14 is already 1.2x–1.4x.
# That cluster is a different price, so it stays out of the quote.
QUOTE_EXCLUDED_DATES = frozenset({"2026-07-12", "2026-07-13"})
# 1S+1G has three scrapes after the July cut, and the lowest is 2.3x.
# That is above the 2x mix floor. A thin cell must not quote up.
TWO_LEG_ONE_GOBLIN_FLOOR = 2.0


def _percentile(values: list[float], p: float) -> float:
    xs = sorted(v for v in values if v > 0)
    if not xs:
        return 0.0
    if len(xs) == 1:
        return xs[0]
    rank = (p / 100.0) * (len(xs) - 1)
    lo = int(math.floor(rank))
    hi = min(lo + 1, len(xs) - 1)
    w = rank - lo
    return xs[lo] * (1.0 - w) + xs[hi] * w


def _composition_counts(label: object) -> tuple[int, int, int] | None:
    m = re.search(
        r"(\d+)\s*S\s*\+\s*(\d+)\s*G(?:\s*\+\s*(\d+)\s*D)?",
        str(label or ""),
        re.I,
    )
    if not m:
        return None
    return int(m.group(1)), int(m.group(2)), int(m.group(3) or 0)


def _fallback_all_hit(n_s: int, n_g: int, n_legs: int) -> float | None:
    if n_g and n_s:
        family = "mix"
    elif n_g:
        family = "goblin"
    else:
        family = "standard"
    pay = PAY_FALLBACK.get((family, n_legs, "Power"))
    if pay is None and family == "mix":
        pay = PAY_FALLBACK.get(("goblin", n_legs, "Power"))
    if not pay:
        return None
    return _f((pay.get("n_correct") or {}).get(n_legs))


def _row_has_deltas(rec: dict[str, Any]) -> bool:
    raw = rec.get("goblin_deltas")
    if isinstance(raw, (list, tuple)):
        return any(str(x).strip() for x in raw)
    return bool(str(raw or "").strip())


def _row_has_flex(rec: dict[str, Any]) -> bool:
    flex = rec.get("flex_n_correct")
    if isinstance(flex, dict) and flex:
        return True
    return _f(rec.get("flex_payout_x")) is not None


def _unconfirmed_fallback(rec: dict[str, Any], n_s: int, n_g: int, n_legs: int, power: float) -> bool:
    """True when this row is the hardcoded mix fallback stored as a scrape.

    The 1S+5G table pinned 8.75x. A ladder row with that exact multiple, no
    Goblin gaps, and no Flex ladder is not a captured N-correct line.
    """
    pinned = _fallback_all_hit(n_s, n_g, n_legs)
    if pinned is None or abs(power - pinned) > 1e-6:
        return False
    return not _row_has_deltas(rec) and not _row_has_flex(rec)


def composition_power_samples(
    n_s: int,
    n_g: int,
    n_legs: int,
    *,
    repo: Path | None = None,
) -> list[float]:
    """Power N-correct from ladder scrapes of this Standard/Goblin count.

    Demon slips are left out. A demon changes the board price, and borrowing
    a Goblin scrape would write the estimate up.
    """
    root = repo or ROOT
    payload = _load_json(root / "ui_runner" / "data" / "payout_ladder_live_cdp.json")
    picked: dict[tuple, float] = {}
    for rec in payload.get("rows") or []:
        if not isinstance(rec, dict):
            continue
        src = str(rec.get("source") or "").strip().lower()
        if src and src not in {"live_cdp", "cdp", "live", "ok"}:
            continue
        try:
            rec_n = int(float(rec.get("n_legs") or 0))
        except (TypeError, ValueError):
            continue
        if rec_n != n_legs:
            continue
        counts = _composition_counts(rec.get("leg_composition") or rec.get("composition"))
        if counts is None:
            continue
        rec_s, rec_g, rec_d = counts
        if rec_d or rec_s != n_s or rec_g != n_g:
            continue
        power = _f(rec.get("power_payout_x") or rec.get("power_min_x"))
        if power is None:
            continue
        if _unconfirmed_fallback(rec, n_s, n_g, n_legs, power):
            continue
        row_date = str(rec.get("date") or "")[:10]
        if row_date in QUOTE_EXCLUDED_DATES:
            continue
        tid = str(rec.get("ticket_id") or "").strip()
        # Same ticket logged twice (two reads of one slip) counts once.
        # The later row wins. A row with no id stays on its own payout.
        if tid:
            ident = ("id", row_date, tid, n_legs)
        else:
            ident = ("row", row_date, round(power, 4), n_legs, len(picked))
        picked[ident] = power
    return list(picked.values())


def uses_composition_p25(n_s: int, n_g: int, n_legs: int) -> bool:
    """3-leg slips and any mix. A 2-leg Goblin is not quoted this way."""
    if n_legs >= LONG_SLIP_MIN_LEGS or n_legs < 2:
        return False
    if n_legs >= 3:
        return True
    return n_s > 0 and n_g > 0


_CAPTURE_CACHE: dict[str, dict[tuple[int, int, int], tuple[float, ...]]] = {}


def _capture_pick_counts(legs: list[dict]) -> tuple[int, int, int, int]:
    n_g = n_s = n_d = 0
    for leg in legs:
        kind = str(leg.get("pick_type") or "").lower()
        if "demon" in kind:
            n_d += 1
        elif "std" in kind or kind == "standard":
            n_s += 1
        elif "gob" in kind:
            n_g += 1
    n = len(legs)
    if n and n_s + n_g + n_d < n:
        n_g += n - (n_s + n_g + n_d)
    return n, n_s, n_g, n_d


def _load_capture_groups(root: Path) -> dict[tuple[int, int, int], tuple[float, ...]]:
    """Power N-correct from logged scrapes, keyed by (legs, standards, goblins)."""
    reports = root / "data" / "reports"
    picked: dict[tuple, tuple[int, int, int, float]] = {}
    paths = sorted(reports.glob("payout_capture*.json"))
    paths += sorted(reports.glob("payout_pre_tip_capture*.json"))
    paths += sorted(reports.glob("payout_ladder_validation_capture_*.json"))
    paths += sorted(reports.glob("payout_scrape_log_*.jsonl"))
    for path in paths:
        recs: list[tuple[str, dict]] = []
        if path.suffix == ".jsonl":
            try:
                text = path.read_text(encoding="utf-8")
            except OSError:
                continue
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(rec, dict):
                    recs.append((str(rec.get("captured_at") or path.stem)[:10], rec))
        else:
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            date = str(payload.get("date") or "")[:10] if isinstance(payload, dict) else ""
            slips = payload.get("slips") if isinstance(payload, dict) else None
            if not isinstance(slips, list):
                continue
            for rec in slips:
                if isinstance(rec, dict):
                    recs.append((str(rec.get("date") or date)[:10], rec))
        for date, rec in recs:
            status = str(rec.get("status") or "ok").lower()
            if status not in {"ok", "partial", ""}:
                continue
            power = _f(rec.get("power_min_x") or rec.get("power_payout_x") or rec.get("min_x"))
            if power is None or power <= 0:
                continue
            raw_legs = rec.get("legs") if isinstance(rec.get("legs"), list) else rec.get("lines")
            legs = [leg for leg in (raw_legs or []) if isinstance(leg, dict)]
            if not legs:
                continue
            n, n_s, n_g, n_d = _capture_pick_counts(legs)
            if n < 2 or n_d:
                continue
            if date in QUOTE_EXCLUDED_DATES:
                continue
            tid = str(rec.get("ticket_id") or "").strip()
            if tid:
                ident = ("id", date, tid, n)
            else:
                ident = ("row", date, round(power, 4), n, len(picked))
            picked[ident] = (n, n_s, n_g, power)
    grouped: dict[tuple[int, int, int], list[float]] = {}
    for n, n_s, n_g, power in picked.values():
        grouped.setdefault((n, n_s, n_g), []).append(power)
    for n, n_s, n_g, power in _paid_stamp_rows(root):
        grouped.setdefault((n, n_s, n_g), []).append(power)
    return {k: tuple(v) for k, v in grouped.items()}


def _paid_stamp_rows(root: Path) -> list[tuple[int, int, int, float]]:
    """Settled Entry → To Win multiples, keyed like a scrape."""
    path = root / "data" / "reports" / "composition_paid_stamps.json"
    if not path.is_file():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    rows = payload.get("stamps") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return []
    out: list[tuple[int, int, int, float]] = []
    for rec in rows:
        if not isinstance(rec, dict):
            continue
        power = _f(rec.get("power_min_x") or rec.get("power_payout_x"))
        if power is None or power <= 0:
            continue
        try:
            n = int(rec.get("n_legs"))
            n_s = int(rec.get("n_standard"))
            n_g = int(rec.get("n_goblin"))
        except (TypeError, ValueError):
            continue
        out.append((n, n_s, n_g, power))
    return out


def capture_composition_samples(
    n_s: int,
    n_g: int,
    n_legs: int,
    *,
    repo: Path | None = None,
) -> list[float]:
    """Logged Power scrapes for this composition. Demon slips are left out."""
    root = repo or ROOT
    cache_key = str(root.resolve())
    grouped = _CAPTURE_CACHE.get(cache_key)
    if grouped is None:
        grouped = _load_capture_groups(root)
        _CAPTURE_CACHE[cache_key] = grouped
    return list(grouped.get((n_legs, n_s, n_g), ()))


def lookup_composition_p25(
    n_s: int,
    n_g: int,
    n_legs: int,
    product: str,
    *,
    repo: Path | None = None,
) -> dict[str, Any] | None:
    """25th percentile of logged Power scrapes. Flex is not quoted from these."""
    if product.lower() != "power":
        return None
    if not uses_composition_p25(n_s, n_g, n_legs):
        return None
    samples = capture_composition_samples(n_s, n_g, n_legs, repo=repo)
    if not samples:
        return None
    if len(samples) < MIN_SCRAPES_FOR_P25:
        quote = min(samples)
        how = "min"
        if n_legs == 2 and n_s == 1 and n_g == 1:
            quote = min(quote, TWO_LEG_ONE_GOBLIN_FLOOR)
            how = "mix floor"
    else:
        quote = _percentile(samples, SHORT_P25)
        how = "p25"
    return _result(
        {n_legs: round(quote, 4)},
        note=(
            f"composition {how} {n_s}S+{n_g}G n={len(samples)} "
            f"range {min(samples):g}–{max(samples):g}"
        ),
        source="n_correct_p25",
    )


def lookup_composition_low(
    n_s: int,
    n_g: int,
    n_legs: int,
    product: str,
    *,
    repo: Path | None = None,
) -> dict[str, Any] | None:
    """Lowest confirmed Power scrape for this composition.

    Flex is not quoted from the Power sample. A long Flex slip with no Flex
    ladder of its own stays unpriced. The 10th percentile is used only after
    MIN_SCRAPES_FOR_PERCENTILE rows. Fewer than that quotes the minimum, so a
    thin cell cannot clear EV above the lowest board price we have seen.
    """
    if product.lower() != "power":
        return None
    samples = composition_power_samples(n_s, n_g, n_legs, repo=repo)
    if not samples:
        return None
    if len(samples) < MIN_SCRAPES_FOR_PERCENTILE:
        quote = min(samples)
        how = "min"
    else:
        quote = _percentile(samples, LOW_PERCENTILE)
        how = f"p{LOW_PERCENTILE:.0f}"
    return _result(
        {n_legs: round(quote, 4)},
        note=(
            f"composition {how} {n_s}S+{n_g}G n={len(samples)} "
            f"range {min(samples):g}–{max(samples):g}"
        ),
        source="n_correct_low",
    )


def unpriced_long_slip(n_legs: int, product: str) -> dict[str, Any]:
    return _result(
        {n_legs: UNPRICED_ALL_HIT_X},
        note=(
            f"{n_legs}-leg {product} has no scrape history for this mix — "
            f"{UNPRICED_ALL_HIT_X:g}x stays under the 1.4x floor until a slip is scraped"
        ),
        source="n_correct_unpriced",
    )


def fallback_pay(family: str, n_legs: int, product: str) -> dict[str, Any]:
    pay = PAY_FALLBACK.get((family, n_legs, product))
    if pay is None:
        pay = PAY_FALLBACK.get(("goblin", n_legs, product))
    if pay is None:
        return _result({n_legs: 2.0}, note="missing payout table — confirm on the slip", source="n_correct_median")
    return _result(pay["n_correct"], note=str(pay["note"]), source="n_correct_median")


def load_overrides(date: str, *, repo: Path | None = None) -> list[dict[str, Any]]:
    root = repo or ROOT
    dated = root / "data" / "reports" / f"payout_overrides_{date}.json"
    latest = root / "data" / "reports" / "payout_overrides_latest.json"
    payload = _load_json(dated) or _load_json(latest)
    if str(payload.get("date") or "")[:10] != str(date or "")[:10]:
        return []
    entries = payload.get("entries") or []
    return [e for e in entries if isinstance(e, dict)]


def _match_entry(entry: dict[str, Any], n_s: int, n_g: int, n_legs: int, product: str) -> bool:
    if int(entry.get("n_legs") or 0) != n_legs:
        return False
    if str(entry.get("product") or "").strip().lower() != product.lower():
        return False
    if "n_s" in entry and int(entry.get("n_s") or 0) != n_s:
        return False
    if "n_g" in entry and int(entry.get("n_g") or 0) != n_g:
        return False
    return True


def lookup_override(
    date: str,
    n_s: int,
    n_g: int,
    n_legs: int,
    product: str,
    *,
    repo: Path | None = None,
) -> dict[str, Any] | None:
    for entry in load_overrides(date, repo=repo):
        if not _match_entry(entry, n_s, n_g, n_legs, product):
            continue
        table = _int_ladder(entry.get("n_correct"))
        if not table:
            continue
        return _result(
            table,
            note=str(entry.get("note") or f"slip pin {date} N-correct / To Win"),
            source="n_correct_live",
        )
    return None


def lookup_live_cdp_scrape(
    date: str,
    n_s: int,
    n_g: int,
    n_legs: int,
    product: str,
    delta_sig: str,
    *,
    repo: Path | None = None,
) -> dict[str, Any] | None:
    """N-correct from PrizePicks CDP scrapes (payout_ladder_live_cdp.json)."""
    if not delta_sig:
        return None
    root = repo or ROOT
    payload = _load_json(root / "ui_runner" / "data" / "payout_ladder_live_cdp.json")
    hits: list[dict[str, Any]] = []
    want = f"{n_s}S+{n_g}G"
    for rec in payload.get("rows") or []:
        if not isinstance(rec, dict):
            continue
        src = str(rec.get("source") or "").strip().lower()
        if src and src not in {"live_cdp", "cdp", "live", "ok"}:
            continue
        try:
            rec_n = int(float(rec.get("n_legs") or 0))
        except (TypeError, ValueError):
            continue
        if rec_n != n_legs:
            continue
        comp = str(rec.get("leg_composition") or rec.get("composition") or "").upper().replace(" ", "")
        if want not in comp:
            continue
        rec_sig = _norm_delta_sig(rec.get("goblin_deltas") or rec.get("goblin_delta_sig"))
        if rec_sig != delta_sig:
            continue
        hits.append(rec)
    if not hits:
        return None
    date_s = str(date or "")[:10]
    hits.sort(
        key=lambda r: (
            1 if str(r.get("date") or "")[:10] == date_s else 0,
            str(r.get("last_captured_at") or r.get("captured_at") or ""),
            str(r.get("date") or ""),
            str(r.get("ticket_id") or ""),
        )
    )
    rec = hits[-1]
    rec_date = str(rec.get("date") or "")[:10]
    captured_at = str(rec.get("last_captured_at") or rec.get("captured_at") or "").strip() or None
    if product.lower() == "flex":
        table = _int_ladder(rec.get("flex_n_correct"))
        if not table:
            return None
        return _result(
            table,
            note=f"CDP scrape Flex {want} Δ={delta_sig} ({rec_date})",
            source="n_correct_live",
            captured_at=captured_at,
        )
    power = _f(rec.get("power_payout_x") or rec.get("power_min_x"))
    if power is None:
        return None
    return _result(
        {n_legs: power},
        note=f"CDP scrape Power {want} Δ={delta_sig} ({rec_date})",
        source="n_correct_live",
        captured_at=captured_at,
    )


lookup_live_cdp_today = lookup_live_cdp_scrape


def lookup_mix_by_delta(
    n_s: int,
    n_g: int,
    n_legs: int,
    product: str,
    delta_sig: str,
    *,
    repo: Path | None = None,
) -> dict[str, Any] | None:
    if not delta_sig:
        return None
    root = repo or ROOT
    payload = _load_json(root / "data" / "reports" / "predicted_payout_tables_latest.json")
    want = f"{n_s}S+{n_g}G"
    for rec in payload.get("mix_by_delta") or []:
        if not isinstance(rec, dict):
            continue
        if int(rec.get("n_legs") or 0) != n_legs:
            continue
        if str(rec.get("composition") or "") != want:
            continue
        if _norm_delta_sig(rec.get("goblin_delta_sig")) != delta_sig:
            continue
        if product.lower() == "flex":
            table = _int_ladder(rec.get("flex_ladder"))
            if not table:
                return None
            return _result(
                table,
                note=f"mix_by_delta Flex {want} Δ={delta_sig}",
                source="n_correct_delta",
            )
        power = _f(rec.get("power_x"))
        if power is None:
            continue
        return _result(
            {n_legs: power},
            note=f"mix_by_delta Power {want} Δ={delta_sig}",
            source="n_correct_delta",
        )
    return None


def resolve_n_correct(
    legs: list[dict],
    product: str,
    family: str,
    *,
    date: str = "",
    repo: Path | None = None,
) -> dict[str, Any]:
    n_s, n_g, n_legs = composition_from_legs(legs)
    if n_legs <= 0:
        n_legs = len(legs)
    if n_legs >= LONG_SLIP_MIN_LEGS:
        # A prior slip with the same gaps is not this slip. Quote the low end
        # of the composition, which cannot clear EV on a payout we have not seen.
        found = lookup_composition_low(n_s, n_g, n_legs, product, repo=repo)
        if found:
            return found
        return unpriced_long_slip(n_legs, product)
    delta_sig = goblin_delta_sig(legs)
    found = lookup_live_cdp_scrape(
        date, n_s, n_g, n_legs, product, delta_sig, repo=repo
    )
    if found:
        return found
    if uses_composition_p25(n_s, n_g, n_legs):
        found = lookup_composition_p25(n_s, n_g, n_legs, product, repo=repo)
        if found:
            return found
    found = lookup_mix_by_delta(n_s, n_g, n_legs, product, delta_sig, repo=repo)
    if found:
        return found
    found = lookup_override(date, n_s, n_g, n_legs, product, repo=repo)
    if found:
        return found
    return fallback_pay(family, n_legs, product)
