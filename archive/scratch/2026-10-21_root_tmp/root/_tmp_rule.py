from pathlib import Path
t=Path(r"H:\PropORACLE\.cursor\rules\best-props-active-sports.mdc").read_text(encoding="utf-8")
i=t.find("**Tennis Goblin OVER**")
print(t[i:i+450])
