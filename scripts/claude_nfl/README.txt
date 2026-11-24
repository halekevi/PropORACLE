Claude's NFL Standard/Demon builder (reads the NFL game log directly; the pipeline's own NFL Standard list is empty because most props are unmapped).

These tickets are the NFL Standard source of truth until the WO pipeline folds this prop mapping in.
9AM Final must not replace them with empty tickets/nfl_standard/.

Run order (Python 3, pandas, pyarrow, openpyxl):
  python nfl_build.py     -> outputs/<date>/ranked/claude_nfl_standard_*.csv and /tmp/nfl_A.pkl
  python nfl_build_d.py   -> claude_nfl_demon_*.csv and /tmp/nfl_D.pkl
  python nfl_tix.py       -> outputs/<date>/claude_NFL_STANDARD_TICKETS_<date>.md
Edit D (date) and R (repo root) at the top of each file. Rebuild after inactives (~11:30 AM ET) when Kevin says "rebuild NFL".
