# NBA Standard UNDER — generic gate vs proj_edge scout at scraped quotes

Window 2026-02-19 → 2026-10-20 (89 slates). Walk-forward box + cell priors; hold = thin player or thin cell. Scout candidate pool = unheld legs only. `slate_minutes` rescales `proj_mu` only on a pre-tip snapshot (0 joined / 96 stored keys). Post-tip confirmed is never an input.

Breakevens: power2_3.00x 57.7% · power3_6.00x 55.0%.

| sleeve | HR | n | Wilson lo | EV@3.0x | EV@6.0x | Wilson clears power3_6.00x |
|---|---|---|---|---|---|---|
| ungated Standard UNDER | 53.3% | 16477 | 0.5253 | -0.148 | -0.0919 | no |
| GATE L5=5 + L10>=8 + D | 47.5% | 40 | 0.3294 | -0.3231 | -0.357 | no |
| scout top-N proj_edge (matched gate volume) | 57.5% | 40 | 0.4219 | -0.0081 | 0.1407 | no |
| scout top-N proj_ev_edge (matched) | 65.0% | 40 | 0.4951 | 0.2675 | 0.6478 | no |
| scout top-3/day proj_edge | 53.0% | 249 | 0.4681 | -0.1569 | -0.1061 | no |
| gate AND scout | 0.0% | 1 | 0.0 | -1.0 | -1.0 | no |
| scout minus gate | 59.0% | 39 | 0.4342 | 0.0434 | 0.2307 | no |
| GATE high proj_edge half | 40.0% | 20 | 0.2188 | -0.52 | -0.616 | no |
| GATE low proj_edge half | 55.0% | 20 | 0.3421 | -0.0925 | -0.0017 | no |
| pre-tip slate_minutes (joined only) | — | 0 | — | — | — | no |
| GATE + pre-tip slate_minutes | — | 0 | — | — | — | no |
| scout top-N + pre-tip slate_minutes | — | 0 | — | — | — | no |

## GATE by prop (n>=20)

| prop | HR | n | Wilson lo | clears power2_3.00x | clears power3_6.00x |
|---|---|---|---|---|---|

## Scout top-N by prop (n>=20)

| prop | HR | n | Wilson lo | clears power2_3.00x | clears power3_6.00x |
|---|---|---|---|---|---|
| Points | 52.0% | 25 | 0.335 | no | no |

NBA Standard UNDER stays shadow until a sleeve's Wilson lower bound clears the scraped quote.
