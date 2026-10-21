from pathlib import Path
p = Path(r"C:/Users/halek/.cursor/projects/h-PropORACLE/canvases/active-sport-prop-hit-rates.canvas.tsx")
t = p.read_text(encoding="utf-8")
old = 'note: "Sep 18 step1 board check"'
new = 'note: "live books; 2B/3B/SB Demon-only"'
if old not in t:
    print("already updated or missing")
else:
    p.write_text(t.replace(old, new, 1), encoding="utf-8")
    print("ok")
