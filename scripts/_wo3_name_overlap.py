"""Why NFL board names miss the box player_norm."""
import json
import sqlite3
import sys
import unicodedata
from pathlib import Path

ROOT = Path(r"H:\PropORACLE")
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]
from build_goblin70_tickets import load_today_board


def fold(name):
    text = unicodedata.normalize("NFKD", str(name or ""))
    return "".join(ch for ch in text.lower() if ch.isalnum())


rows = [r for r in load_today_board("2026-10-08") if str(r.get("sport")).upper() == "NFL"]
board = {fold(r.get("player")) for r in rows}
print("board", len(board), "sample", list(rows)[0].get("player") if rows else None)
c = sqlite3.connect(ROOT / "data" / "cache" / "proporacle_ref.db")
box = set()
for (payload,) in c.execute("SELECT payload FROM espn_boxscore_rows WHERE sport='nfl' LIMIT 3000"):
    p = json.loads(payload)
    box.add(fold(p.get("player_norm")))
print("box sample", len(box), "overlap", len(board & box))
print("board example", next(iter(board)))
print("box example", next(iter(box)))
