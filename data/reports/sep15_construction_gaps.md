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

## Gap 2 — no cross-ticket leg exposure cap

Same-player-same-ticket block exists. **Same leg across many tickets does not.**

Sep-15 combined snapshot reuse (same player|prop|line|dir):

- James Wood H+R+RBI 0.5 — **8** tickets
- Carter Jensen H+R+RBI 0.5 — **4**
- Also soccer: Isak Shots **8**, Mbappé SOT **7** (same pattern, other sport)

`MAX_SLIPS_PER_PLAYER = 4` is a soft per-player slip count inside some builders;
it does **not** cap identical leg fingerprints across CORE + mixer + Goblin-70.

## Tomorrow’s slate

Tennis / Soccer / WNBA keep catalogs are the strong ones — when gates fire.
MLB is fine **if** keep is enforced (yesterday proves the opposite path).
CFB/NFL remain the thinner / allowlist categories — more caution there than on
gated MLB.

## Follow-ups (not done here)

- Cross-ticket **leg-fingerprint** exposure cap (CORE ↔ mixer ↔ G70).
- Confirm next Goblin-70 rebuild cannot admit BA<.275 H+R+RBI/TB (Mesa-class).
- Optional: assert `mlb_keep_failing_legs == 0` on MAIN payload in CI/smoke.
