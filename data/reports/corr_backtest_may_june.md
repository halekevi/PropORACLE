# Leg-correlation backtest

Naive vs. correlation-adjusted joint probability vs. actual ticket win rate.
If `mean_adjusted_p` tracks `actual_win_rate` (and its May->June drop) better than
`mean_naive_p` does, the correlation adjustment is the right lever. If not, the
categories or join keys need a second look before this ships to the packer/pricer.

**Population note:** `combined_slate_tickets_*.json` is the grade-pool union of
runs (full generated MAIN), not the pruned live `tickets_latest.json` card.
Original May→June bleed was on realized/published outcomes — compare carefully.

Tickets graded: 15141 | skipped ungraded: 36357 | mixed (cross-sport): 4687 (31.0%)
Resolution rate (among 2+ leg normalized): 29.4%

## Resolution rate by month
- 2026-05: 10890/39967 (27.2%)
- 2026-06: 4251/11531 (36.9%)

## Leg-count mix (graded tickets)

- 2026-05: 2-leg=1339 (12.3%), 3-leg=939 (8.6%), 4-leg=621 (5.7%), 5-leg=4266 (39.2%), 6-leg=3725 (34.2%)
- 2026-06: 2-leg=1038 (24.4%), 3-leg=584 (13.7%), 4-leg=488 (11.5%), 5-leg=1119 (26.3%), 6-leg=1022 (24.0%)

## By sport and month
(check this first -- a single sport's residual shouldn't hide in the pooled number)

```
                n_tickets  actual_win_rate  mean_naive_p  mean_adjusted_p
sport  month                                                             
MLB    2026-05       3907           0.0929        0.4164           0.4202
       2026-06        794           0.1562        0.3425           0.3495
NBA    2026-05       1158           0.1459        0.5209           0.4497
NBA1H  2026-05         14           0.9286        0.9718           0.9288
NBA1Q  2026-05         57           0.3158        0.9191           0.5987
       2026-06          1           0.0000        0.5184           0.5362
NHL    2026-05        280           0.1286        0.5582           0.6173
       2026-06         40           0.2000        0.3206           0.5420
Soccer 2026-05        107           0.1215        0.4594           0.4692
       2026-06         64           0.3125        0.4451           0.4226
TENNIS 2026-06          3           0.6667        0.4700           0.4700
Tennis 2026-05        406           0.1502        0.1135           0.1135
       2026-06        102           0.2549        0.2969           0.2969
WNBA   2026-05       1488           0.1169        0.2331           0.2211
       2026-06       2033           0.1540        0.2589           0.2399
mixed  2026-05       3473           0.1241        0.4454           0.4201
       2026-06       1214           0.1376        0.2226           0.2073
```

## By leg-count and month
(original diagnostic was specifically 2-leg — read that row before pooled)

```
                n_tickets  actual_win_rate  mean_naive_p  mean_adjusted_p
n_legs month                                                             
2      2026-05       1339           0.2547        0.5707           0.5704
       2026-06       1038           0.3073        0.4529           0.4479
3      2026-05        939           0.1448        0.4462           0.4431
       2026-06        584           0.1935        0.3149           0.3086
4      2026-05        621           0.0709        0.3502           0.3431
       2026-06        488           0.1250        0.2183           0.2173
5      2026-05       4266           0.1088        0.4058           0.3878
       2026-06       1119           0.0965        0.2023           0.1874
6      2026-05       3725           0.0787        0.3516           0.3274
       2026-06       1022           0.0577        0.1519           0.1339
```

## Pooled by month

```
         n_tickets  actual_win_rate  mean_naive_p  mean_adjusted_p
month                                                             
2026-05      10890           0.1174        0.4078           0.3918
2026-06       4251           0.1553        0.2687           0.2583
```

## 2-leg only — by sport and month

```
                n_tickets  actual_win_rate  mean_naive_p  mean_adjusted_p
sport  month                                                             
MLB    2026-05        249           0.2169        0.6709           0.6699
       2026-06        191           0.2723        0.4928           0.4932
NBA    2026-05        180           0.2833        0.7424           0.7350
NBA1H  2026-05          8           0.8750        0.9801           0.9730
NBA1Q  2026-06          1           0.0000        0.5184           0.5362
NHL    2026-05         75           0.2800        0.7336           0.7612
       2026-06          9           0.5556        0.5499           0.6189
Soccer 2026-05         11           0.4545        0.6851           0.6851
       2026-06         52           0.3077        0.4824           0.4725
TENNIS 2026-06          2           1.0000        0.5184           0.5184
Tennis 2026-05        116           0.3017        0.2478           0.2478
       2026-06         45           0.3556        0.3994           0.3994
WNBA   2026-05        384           0.2422        0.4076           0.4055
       2026-06        526           0.3137        0.4563           0.4462
mixed  2026-05        316           0.2373        0.6576           0.6576
       2026-06        212           0.2972        0.4075           0.4075
```

## 2-leg only — pooled by month

```
         n_tickets  actual_win_rate  mean_naive_p  mean_adjusted_p
month                                                             
2026-05       1339           0.2547        0.5707           0.5704
2026-06       1038           0.3073        0.4529           0.4479
```