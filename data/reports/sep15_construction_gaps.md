# Sep-15 grades — construction gaps (not a bad-sport day)

## Verdict

**Construction.** Yesterday’s 5.3% (1/19) is consistent with keep-gate leakage +
cross-ticket leg reuse, not “MLB/soccer can’t hit.” Re-running Mesa / Palacios /
Wood / Jensen through `mlb_goblin_keep_eligible` **today** returns `keep=False`
(BA and/or L5 fail). They should never have been ticketable under the documented
gates.

## Evidence from live `tickets_latest` (2026-09-15, main_cp)

| Player | Prop | Slash BA | L5 on ticket | keep now | Where it appeared |
|--|--|--|--|--|--|
| Victor Mesa Jr. | H+R+RBI 0.5 | **.227** | 5 | **fail** | X-Sport Goblin-70 Power 3, MLB Goblin-70 Power 3 |
| Richie Palacios | H+R+RBI 0.5 | **.227** | 5 | **fail** | X-Sport Goblin-70 Flex 4 |
| James Wood | H+R+RBI 0.5 | **.268** | **4** | **fail** | MLB Core Power 2 #1 + mixer Goblin 2/3-leg (**8×** reuse) |
| Carter Jensen | H+R+RBI 0.5 | **.230** | **4** | **fail** | MLB Core Power 2 #2 + mixer (**4×**) |
| Michael Massey | Hits / TB 0.5 | .280 | 5 | pass | legit under BA gate |

Gate: BA≥.275 + L5=5 + Opp pitch Weak|Below (`utils/mlb_keep_gates.py`).

## Gap 1 — keep gate not enforced on every product surface

`mlb_keep_gates` / `_leg_mlb_construction_banned` **do** exist and correctly reject
these rows in isolation. Failures:

1. **`filter_main_high_prob_payload` short-circuited CORE (and STRONG) past the MLB ban**
   — `if t.get("core_build"): kept.append; continue` ran *before* the construction
   check. Core Power could publish keep-failing MLB hitters onto MAIN.
2. **Goblin-70** calls `goblin_70_eligible` → `mlb_goblin_keep_eligible` in code;
   Mesa fails that check on today’s board reload. He still landed on the published
   Goblin-70 card — treat as a separate enforce/fill regression to verify on the
   next G70 rebuild (not “gate doesn’t exist”).

**Fix shipped:** MLB construction ban now runs for **all** MAIN slips before
CORE/STRONG short-circuits; `_prepare_core_pipeline_pool` also drops MLB legs
that fail keep.

**Replay against real Sep-15 data** (`scripts/replay_sep15_mlb_keep_fix.py`):

| Check | Result |
|--|--|
| CORE pool prep on Sep-15 MLB step8 | **PASS** — 15 flagged goblin counting legs in → 0 out |
| `filter_main` on published CORE+mixer (MAIN mode) | **PASS** — Wood / Jensen / Mesa / Palacios all dropped |
| Live dual card `pool_mode` | `'goblin70'` — filter_main **early-returns** on the merged artifact |

ce04cad8c is confirmed for the MAIN/CORE build path. It does **not** scrub the
already-merged dual card mid-flight (see Gap 1b).

## Gap 1b — G70 / dual-card staleness (**fixed**) + keep scrub

Rebuilds are automatic (day-ahead / 1AM / 5AM / late-fetch / MLB fill). The
structural bug was: merge set `pool_mode=goblin70`, and `filter_main` treated
that as "skip all hygiene," so CORE/mixer keep failures rode the card's entire
lifetime across every subsequent G70 refresh (one-way door, not a timing gap).

**Fix:** `pool_mode` routes MAIN *structure* rules only. Construction hygiene
(`scrub_payload_construction_hygiene`) always runs on CORE/mixer — including
dual-card `goblin70`. G70 `--write-web` calls the same filter after merge.
G70-family slips are not re-checked from published web legs (incomplete L10 /
own-def would false-drop pitchers); they are rebuilt from the board each G70 run.

**Lightweight keep scrub (addresses the remaining G70 window):**
`scripts/scrub_goblin70_keep_live.py` re-matches published G70 legs to today's
board and drops slips that fail `goblin_70_eligible` / keep. Scheduled every
30m via `PropOracle - G70 Keep Scrub` (`run_g70_keep_scrub.ps1`). Off-board legs
are left alone (no false-drop).

**Rebuild freshness alert:** each successful `--write-web` stamps
`logs/LAST_G70_REBUILD.json`. `assert_g70_freshness.py` (default max 6h) writes
`logs/LAST_G70_STALE.alert` and exits 2 when the chain goes quiet — so consecutive
failed windows cannot silently stretch the exposure.

## Gap 2 — MAX_SLIPS_PER_PLAYER did not hold inside CORE+mixer

Isak/Mbappé hit **5×** inside CORE+mixer (not explained by G70 siloing).

Root causes:

1. **`_ticket_cap_register` treated empty `defaultdict` as falsy** (`if not counts:
   return`), so the shared counter never incremented from an empty start — the
   stated cap of 4 was effectively dead for most builders.
2. CORE is **injected after** the win-rate mixer, and the mixer used a fresh
   per-call counter during candidate gen — so even a working register would not
   span CORE ↔ mixer without a post-assemble trim.

**Fix:** register uses `if counts is None`; win-rate `_try_pick` shares one
counter across sports×sizes; `trim_payload_max_slips_per_player` runs after CORE
inject and on every dual-card merge. Sep-15 replay: Isak/Mbappé 5 → 4.

Cross-pool (CORE/mixer ↔ G70) registry remains a separate follow-up.

## Follow-ups (still open)

- Cross-pool **leg-fingerprint** exposure registry shared by CORE / mixer / G70.
- Optional: assert `mlb_keep_failing_legs == 0` on MAIN-family payload in CI/smoke.
