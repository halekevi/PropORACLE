# NBA Standard UNDER — generic gate vs proj_edge scout at scraped quotes

Window 2026-02-19 → 2026-10-20 (89 slates). Walk-forward box + cell priors; hold = thin player or thin cell. Scout candidate pool = unheld legs only.

Breakevens: power2_3.00x 57.7% · power3_6.00x 55.0%.

| sleeve | HR | n | Wilson lo | EV@3.0x | EV@6.0x | Wilson clears power3_6.00x |
|---|---|---|---|---|---|---|
| ungated Standard UNDER | 53.3% | 16477 | 0.5253 | -0.148 | -0.0919 | no |
| GATE L5=5 + L10>=8 + D | 46.3% | 41 | 0.3206 | -0.3557 | -0.4029 | no |
| scout top-N proj_edge (matched gate volume) | 56.1% | 41 | 0.4104 | -0.0559 | 0.0592 | no |
| scout top-N proj_ev_edge (matched) | 65.8% | 41 | 0.5055 | 0.301 | 0.7135 | no |
| scout top-3/day proj_edge | 55.8% | 249 | 0.4961 | -0.0651 | 0.0438 | no |
| gate AND scout | 50.0% | 2 | 0.0945 | -0.25 | -0.25 | no |
| scout minus gate | 56.4% | 39 | 0.4098 | -0.0454 | 0.077 | no |
| GATE high proj_edge half | 28.6% | 21 | 0.1381 | -0.7551 | -0.8601 | no |
| GATE low proj_edge half | 65.0% | 20 | 0.4329 | 0.2675 | 0.6478 | no |

## GATE by prop (n>=20)

| prop | HR | n | Wilson lo | clears power2_3.00x | clears power3_6.00x |
|---|---|---|---|---|---|

## Scout top-N by prop (n>=20)

| prop | HR | n | Wilson lo | clears power2_3.00x | clears power3_6.00x |
|---|---|---|---|---|---|
| Points | 51.8% | 27 | 0.3399 | no | no |

NBA Standard UNDER stays shadow until a sleeve's Wilson lower bound clears the scraped quote.
