# graded-data

Data-only branch: graded props exports (graded_props_<date>.json) that the Flask app serves on
/grades and uses for /income. They used to be force-added to main by the daily pipeline, which
made git status on the live tree take ~6 s (1.1 GB of modified tracked JSON) and tripped publishes.

- Layout: graded_props/graded_props_<YYYY-MM-DD>.json, plus index.json (dates list).
- Written by scripts/Publish-GradedData.ps1 -Date <date> after the grader (regrades overwrite).
- Read by the app via PROPORACLE_GRADED_RAW_BASE (defaults to this branch's raw URL) into
  DATA_ROOT/graded_props on demand, after PROPORACLE_GRADED_PROPS_DIR / templates / archive.
- No code lives here. Never merge this branch into main.
