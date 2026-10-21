from pathlib import Path
import re
h = Path(r"H:\PropORACLE\_tmp_tickets_live.html").read_text(encoding="utf-8", errors="replace")
checks = [
    "data-payout-rate",
    "payout_desc",
    "Payout rate",
    "revealTicketPaint",
    "Sort groups. Payout",
    'aria-label="Sort ticket groups"',
    "data-p-win",
]
lines = [f"{c}: {c in h}" for c in checks]
lines.append(f"count sort select: {h.count('ticket-sort-select')}")
lines.append(f"count data-payout-rate: {h.count('data-payout-rate')}")
for m in re.findall(r"<meta[^>]+>", h):
    if "build" in m.lower() or "git" in m.lower() or "proporacle" in m.lower():
        lines.append(m)
Path(r"H:\PropORACLE\_tmp_live_checks.txt").write_text("\n".join(lines), encoding="utf-8")
print("wrote checks")
