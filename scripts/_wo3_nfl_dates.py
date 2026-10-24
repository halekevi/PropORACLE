import json
import sqlite3
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(r"H:\PropORACLE")


def iso(day):
    text = str(day or "")
    if len(text) >= 8 and text[:8].isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return text[:10]


def fold(name):
    text = unicodedata.normalize("NFKD", str(name or ""))
    return "".join(ch for ch in text.lower() if ch.isalnum())


c = sqlite3.connect(ROOT / "data" / "cache" / "proporacle_ref.db")
games = defaultdict(set)
for (payload,) in c.execute("SELECT payload FROM espn_boxscore_rows WHERE sport='nfl'"):
    p = json.loads(payload)
    day = iso(p.get("game_date"))
    if "2026-09-01" <= day < "2026-10-08":
        games[fold(p.get("player_norm"))].add(day)
counts = Counter(len(v) for v in games.values())
print("players", len(games), "by_n", dict(sorted(counts.items())))
print("dak", sorted(games.get("dakprescott", [])))
import sys
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]
from build_goblin70_tickets import load_today_board
board = [r for r in load_today_board("2026-10-08") if str(r.get("sport")).upper()=="NFL"]
hit = sum(1 for r in board if fold(r.get("player")) in games)
print("board", len(board), "hit", hit, "example", board[0].get("player"), fold(board[0].get("player")))
