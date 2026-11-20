"""Top Edges helpers stay in home JS; the home Top Edges section itself is retired."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PAGES = [
    ROOT / "ui_runner" / "templates" / "index.html",
    ROOT / "mobile" / "www" / "index.html",
]


def test_home_pages_drop_top_edges_section():
    for path in PAGES:
        text = path.read_text(encoding="utf-8")
        assert 'id="top-edges"' not in text, path
        assert 'id="top-edges-board"' not in text, path
        assert 'href="#props-board"' in text, path
        assert "Jump to props board" in text, path


def test_top_edges_helpers_still_exclude_demons_and_show_l5_l10():
    for path in PAGES:
        text = path.read_text(encoding="utf-8")
        assert "function isDemonPick" in text, path
        assert "isDemonPick(basePick)" in text, path
        assert "isTopEdgesBoardPick" in text, path
        assert "diversifyBySport" in text, path
        assert "edgeHitCountsLabel" in text, path
        assert "L5 O${" in text or "L5 O" in text, path
        assert "${hitPct}% · L${lineDisp}" not in text, path
        assert "L5 O / U" in text, path
        assert "L10 O / U" in text, path
        assert "top_edges_badge_board.js" in text, path
        assert "TopEdgesBadgeBoard.rankFromSlate" in text, path
        assert "renderBadgeTopEdges" in text, path
