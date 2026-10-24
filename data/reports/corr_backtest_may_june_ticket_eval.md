# Leg-correlation backtest

Naive vs. correlation-adjusted joint probability vs. actual ticket win rate.
If `mean_adjusted_p` tracks `actual_win_rate` (and its May->June drop) better than
`mean_naive_p` does, the correlation adjustment is the right lever. If not, the
categories or join keys need a second look before this ships to the packer/pricer.

**Population note:** `ticket_eval_YYYY-MM-DD.html` decided articles (grade_history-aligned May–June source). `combined_slate_tickets_*.json` is the oversized grade-pool and is not used here.

Tickets graded: 10589 | skipped ungraded: 0 | mixed (cross-sport): 0 (0.0%)
Resolution rate (among 2+ leg normalized): 100.0%

## Resolution rate by month
- 2026-05: 7218/7218 (100.0%)
- 2026-06: 3371/3371 (100.0%)

## Leg-count mix (graded tickets)

- 2026-05: 2-leg=2572 (35.6%), 3-leg=1840 (25.5%), 4-leg=1267 (17.6%), 5-leg=931 (12.9%), 6-leg=608 (8.4%)
- 2026-06: 2-leg=1158 (34.4%), 3-leg=904 (26.8%), 4-leg=598 (17.7%), 5-leg=433 (12.8%), 6-leg=278 (8.2%)

## By sport and month
(check this first -- a single sport's residual shouldn't hide in the pooled number)

```
                n_tickets  actual_win_rate  mean_naive_p  mean_adjusted_p
sport  month                                                             
MLB    2026-05       4627           0.2434        0.3752           0.3757
       2026-06        913           0.2037        0.4319           0.4309
NBA    2026-05       1144           0.2159        0.2678           0.2472
NBA1H  2026-05          7           0.8571        0.7369           0.7130
NBA1Q  2026-05          7           0.1429        0.6623           0.5436
       2026-06          2           0.5000        0.4050           0.3569
NHL    2026-05        200           0.2000        0.3012           0.3068
       2026-06         59           0.2203        0.3669           0.5025
SOCCER 2026-05        227           0.2687        0.1238           0.1238
       2026-06       1017           0.0787        0.4153           0.4153
TENNIS 2026-06         49           0.3265        0.1036           0.1036
WNBA   2026-05       1006           0.1431        0.2921           0.2796
       2026-06       1331           0.1773        0.4820           0.4461
```

## By leg-count and month
(original diagnostic was specifically 2-leg — read that row before pooled)

```
                n_tickets  actual_win_rate  mean_naive_p  mean_adjusted_p
n_legs month                                                             
2      2026-05       2572           0.3468        0.4313           0.4292
       2026-06       1158           0.2478        0.5093           0.5074
3      2026-05       1840           0.2266        0.3384           0.3326
       2026-06        904           0.1670        0.4633           0.4512
4      2026-05       1267           0.1436        0.2699           0.2645
       2026-06        598           0.1070        0.3964           0.3788
5      2026-05        931           0.0977        0.2397           0.2316
       2026-06        433           0.0508        0.3456           0.3249
6      2026-05        608           0.0707        0.2259           0.2203
       2026-06        278           0.0288        0.3255           0.2959
```

## Pooled by month

```
         n_tickets  actual_win_rate  mean_naive_p  mean_adjusted_p
month                                                             
2026-05       7218           0.2251        0.3373           0.3326
2026-06       3371           0.1578        0.4407           0.4286
```

## 2-leg only — by sport and month

```
                n_tickets  actual_win_rate  mean_naive_p  mean_adjusted_p
sport  month                                                             
MLB    2026-05       1586           0.3777        0.4708           0.4710
       2026-06        343           0.3178        0.5131           0.5122
NBA    2026-05        444           0.3333        0.3586           0.3482
NBA1H  2026-05          4           0.7500        0.8233           0.8233
NBA1Q  2026-05          5           0.2000        0.7105           0.6253
       2026-06          2           0.5000        0.4050           0.3569
NHL    2026-05         92           0.3043        0.4004           0.4059
       2026-06         21           0.3333        0.4088           0.4539
SOCCER 2026-05         76           0.3816        0.2335           0.2335
       2026-06        431           0.1392        0.5543           0.5543
TENNIS 2026-06         29           0.4828        0.1320           0.1320
WNBA   2026-05        365           0.2301        0.3888           0.3854
       2026-06        332           0.2892        0.4870           0.4785
```

## 2-leg only — pooled by month

```
         n_tickets  actual_win_rate  mean_naive_p  mean_adjusted_p
month                                                             
2026-05       2572           0.3468        0.4313           0.4292
2026-06       1158           0.2478        0.5093           0.5074
```

## Sport-mix vs within-sport decomposition

Base `2026-05` WR=22.5% (n=7218) -> compare `2026-06` WR=15.8% (n=3371); total d=-6.7 pp.

- **Counterfactual WR** (compare mix x base per-sport WR) = **21.0%**
- **mix_effect** = CF - base = **-1.5 pp**
- **within_sport_effect** = compare - CF = **-5.3 pp**
- Check: mix + within = -6.7 pp (should match total -6.7 pp).

**Read:** within_sport_effect dominates -- sports got worse individually; recalibration (not just a share cap) is the real fix. Fallback base WR (22.5%) used for sports with no 2026-05 tickets: TENNIS.

Per-sport contributions (negative mix_contrib = share grew into a weaker base-WR sport, or share left a stronger one):

```
 sport  n_base  n_compare  share_base  share_compare  wr_base  wr_compare  mix_contrib  within_contrib
   MLB    4627        913      0.6410         0.2708   0.2434      0.2037      -0.0901         -0.0107
   NBA    1144          0      0.1585         0.0000   0.2159         NaN      -0.0342          0.0000
   NHL     200         59      0.0277         0.0175   0.2000      0.2203      -0.0020          0.0004
 NBA1H       7          0      0.0010         0.0000   0.8571         NaN      -0.0008          0.0000
 NBA1Q       7          2      0.0010         0.0006   0.1429      0.5000      -0.0001          0.0002
TENNIS       0         49      0.0000         0.0145   0.2251      0.3265       0.0033          0.0015
  WNBA    1006       1331      0.1394         0.3948   0.1431      0.1773       0.0366          0.0135
SOCCER     227       1017      0.0314         0.3017   0.2687      0.0787       0.0726         -0.0573
```
