# MLB within-sport softening — concentration slices (May vs June 2026)

Question: is MLB ticket WR soft (≈24%→20%) uniform, or hiding in one slice
(stat / park / team / pick_type / leg count) the way WC Soccer and thin-player were?

## Verdict

**Not a true within-cell decay — it's a pick-type mix shift inside MLB.**

| | May | June |
|--|--|--|
| MLB ticket WR | 24.3% (n=4627) | 20.4% (n=913) |
| all_goblin share | 93% (n=4257) | 30% (n=262) |
| all_standard share | 7% (n=310) | 70% (n=626) |
| all_goblin WR | 24.9% | **28.6%** (better) |
| all_standard WR | 15.5% | **17.7%** (better) |

Inside-MLB Kitagawa on `tier_mix`: **mix = −5.8pp**, **within = +1.8pp** (of −4.0pp total).
June's Goblin→Standard share change alone more than explains the soft; holding May
tier WRs, the June mix predicts CF ≈18.5%, and actual June (20.4%) is *above* that.

So the leftover “MLB within-sport softening” from the survivors decomp was mislabeled
within-sport skill decay — it is the same class of finding as post-WC mix: **what got
packed changed** (Goblin board thinned / Standard packed), not MLB props getting worse.

### Ruled out as primary mechanisms

- **Park factors:** ticket pitcher-park tag looks soft (−9.6pp) but Kitagawa on park is
  within-dominated and confounded by the tier shift; graded props by park bucket move
  ≤1.3pp. Coors (COL) tickets actually *improved*.
- **Underlying prop hit rates:** graded MLB hit 30.0%→30.4% (flat). Goblin graded hit
  *up* (+2.1pp). Soft is not “June baseball was harder to hit.”
- **Stat / team slices on tickets:** Pitcher Strikeouts tickets (−16pp, June n=95) and
  some team tags look ugly, but graded pitch-K hit only −1.3pp — small-n + mix
  confounds, not a WC-style concentrated mechanism.

### Related but separate signal

Graded **calibration gap** (mean_pred − hit) widens in June across the board
(model mean_prob 40%→52% while hit flat) — especially Demons and pitcher props.
That is the same overconfidence signature as the Standard decile-10 work, not the
ticket WR soft itself. Player-level shrinkage remains the right fix for that.

### Still open (narrowed)

Why did June's MLB card flip from ~93% Goblin tickets to ~70% Standard — packing /
board availability / Goblin-70 volume — not whether MLB got worse. That is an
operations/composition question, not a new calibration bug.

## Baseline (MLB-primary tickets from ticket_eval)

- `2026-05`: n=4627 WR=24.3%
- `2026-06`: n=913 WR=20.4%

- **Δ WR** = **-4.0 pp**

## Mix vs within *inside* MLB (Kitagawa by slice dimension)

- **n_legs**: mix=+0.8pp  within=-4.7pp  (May 24.3% → June 20.4%; CF 25.1%)
- **tier**: mix=-5.8pp  within=+1.8pp  (May 24.3% → June 20.4%; CF 18.5%)
- **dom_stat**: mix=+2.9pp  within=-6.9pp  (May 24.3% → June 20.4%; CF 27.2%)
- **park_tag**: mix=+0.2pp  within=-4.2pp  (May 24.3% → June 20.4%; CF 24.5%)

## By n_legs (min_n=40 both months)

```
 n_legs  n_may  n_jun   wr_may   wr_jun      d_pp  wilson_may  wilson_jun
      3   1174    225 0.252129 0.186667 -6.546281    0.000013    0.000037
      2   1586    343 0.377680 0.317784 -5.989544    0.000020    0.000066
      6    446     79 0.067265 0.025316 -4.194812    0.000003    0.000002
      4    813    155 0.164822 0.135484 -2.933778    0.000008    0.000029
      5    608    111 0.110197 0.108108 -0.208926    0.000005    0.000026
```

Buckets with Δ≤−2pp: 4 / 5; June n in those = 802 / 913

## By pick-type mix (min_n=40)

```
        tier  n_may  n_jun   wr_may   wr_jun     d_pp  wilson_may  wilson_jun
all_standard    310    626 0.154839 0.177316 2.247758    0.000019    0.000012
  all_goblin   4257    262 0.249002 0.286260 3.725790    0.000003    0.000071
```

## By dominant leg stat (min_n=40)

```
          dom_stat  n_may  n_jun   wr_may   wr_jun       d_pp  wilson_may  wilson_jun
Pitcher Strikeouts    186     95 0.247312 0.084211 -16.310130    0.000076    0.000019
    Hits+Runs+RBIs    344    346 0.313953 0.228324  -8.562979    0.000064    0.000035
 Hitter Strikeouts   1313     41 0.244478 0.219512  -2.496610    0.000011    0.000275
       Total Bases   1171     50 0.229718 0.260000   3.028181    0.000011    0.000311
              Hits   1245     59 0.237751 0.305085   6.733374    0.000011    0.000356
```

Buckets with Δ≤−2pp: 3 / 5; June n in those = 482 / 591

## By park tag (player-team PF proxy; min_n=40)

```
        park_tag  n_may  n_jun   wr_may   wr_jun      d_pp  wilson_may  wilson_jun
has_pitcher_park   1450    244 0.226897 0.131148 -9.574901    0.000008    0.000017
 has_hitter_park   1053    210 0.208927 0.176190 -3.273640    0.000010    0.000035
    neutral_only   2124    459 0.271657 0.254902 -1.675529    0.000008    0.000033
```

Buckets with Δ≤−2pp: 2 / 3; June n in those = 454 / 913

## Graded MLB props (underlying hit rate, not tickets)

- `2026-05`: n=103786 hit=30.0%  mean_model_prob=40.0%
- `2026-06`: n=189046 hit=30.4%  mean_model_prob=51.7%

## Graded props by pick_type (min_n=200)

```
pick_type_key  n_may  n_jun   wr_may   wr_jun      d_pp   wilson_may   wilson_jun
     standard   5435  12494 0.504324 0.500960 -0.336337 9.755115e-06 4.192451e-06
        demon  77347 140756 0.202619 0.204375  0.175558 1.252693e-07 6.997959e-08
       goblin  21004  35796 0.604028 0.624874  2.084648 3.490624e-06 2.175773e-06
```

### Graded clean tables

**pick_type**
```
pick_type_key  n_may  n_jun   wr_may   wr_jun      d_pp   wilson_may   wilson_jun
     standard   5435  12494 0.504324 0.500960 -0.336337 9.755115e-06 4.192451e-06
        demon  77347 140756 0.202619 0.204375  0.175558 1.252693e-07 6.997959e-08
       goblin  21004  35796 0.604028 0.624874  2.084648 3.490624e-06 2.175773e-06
```

**stat_type**
```
            stat_type  n_may  n_jun   wr_may   wr_jun      d_pp   wilson_may   wilson_jun
                 RBIs   1614  14686 0.224907 0.196990 -2.791673 7.323234e-06 6.251958e-07
        Pitching Outs    467    791 0.513919 0.490518 -2.340030 1.174775e-04 6.374355e-05
       Hits+Runs+RBIs   4101  36161 0.351865 0.336965 -1.490016 6.682310e-06 6.992946e-07
    Hitter Strikeouts  13675  15480 0.379890 0.366344 -1.354664 2.309366e-06 1.907640e-06
   Pitcher Strikeouts   2753   4388 0.414457 0.401778 -1.267938 1.346647e-05 7.979829e-06
                 Hits  14362  17137 0.365200 0.353562 -1.163737 2.044272e-06 1.613453e-06
         Stolen Bases   3618   4520 0.094527 0.085398 -0.912913 6.130702e-07 4.023023e-07
                 Runs  10072  12012 0.273531 0.265485 -0.804606 1.699267e-06 1.346905e-06
        Walks Allowed   1051   1809 0.444339 0.439469 -0.486940 4.007165e-05 2.281651e-05
              Doubles   2405   7644 0.149688 0.146782 -0.290636 2.252820e-06 6.824636e-07
                Walks   6951   7417 0.275500 0.273561 -0.193919 2.495707e-06 2.308019e-06
          Total Bases  29812  34477 0.283678 0.287844  0.416636 6.147988e-07 5.463684e-07
    Plate Appearances    430   1435 0.362791 0.367944  0.515355 6.745081e-05 2.074561e-05
         Hits Allowed   2199   3669 0.384266 0.394113  0.984726 1.466828e-05 9.211152e-06
              Singles   3246  10445 0.309304 0.324653  1.534919 6.640077e-06 2.258836e-06
            Home Runs   5927   6680 0.115067 0.131587  1.652018 5.490756e-07 6.321300e-07
Pitcher Fantasy Score    276   1683 0.474638 0.494949  2.031181 1.721030e-04 3.045072e-05
       Pitches Thrown    807   1329 0.442379 0.486832  4.445302 5.176825e-05 3.742274e-05
```

**park_bucket (player team PF)**
```
      park_bucket  n_may  n_jun   wr_may   wr_jun      d_pp   wilson_may   wilson_jun
pitcher_park_<=97  13997  24783 0.304137 0.291288 -1.284822 1.492102e-06 7.772402e-07
   neutral_98_104  82927 151475 0.299444 0.305602  0.615750 2.446208e-07 1.391224e-07
 hitter_park_105+   6862  12764 0.293063 0.303432  1.036828 2.839275e-06 1.629146e-06
```

**stat × direction**
```
                  stat_dir  n_may  n_jun   wr_may   wr_jun      d_pp   wilson_may   wilson_jun
  Pitcher Strikeouts UNDER    183    305 0.568306 0.491803 -7.650273 3.593664e-04 1.661152e-04
                Runs UNDER    308    406 0.571429 0.536946 -3.448276 2.156022e-04 1.462501e-04
      Pitches Thrown UNDER    172    301 0.558140 0.524917 -3.322259 3.701696e-04 1.893803e-04
             Singles UNDER    276    756 0.572464 0.542328 -3.013573 2.413860e-04 7.996040e-05
                 RBIs OVER   1613  14678 0.225046 0.196825 -2.822132 7.336409e-06 6.245348e-07
       Hits+Runs+RBIs OVER   4003  35232 0.349988 0.332056 -1.793142 6.778256e-06 6.983942e-07
         Total Bases UNDER    331    488 0.555891 0.543033 -1.285845 1.909377e-04 1.241673e-04
    Hitter Strikeouts OVER  13358  15230 0.376329 0.363624 -1.270437 2.323393e-06 1.912395e-06
                 Hits OVER  14049  16732 0.360666 0.348613 -1.205280 2.042027e-06 1.609839e-06
                Hits UNDER    313    405 0.568690 0.558025 -1.066540 2.103386e-04 1.571231e-04
         Stolen Bases OVER   3618   4520 0.094527 0.085398 -0.912913 6.130702e-07 4.023023e-07
   Pitcher Strikeouts OVER   2570   4083 0.403502 0.395053 -0.844929 1.373252e-05 8.313563e-06
                 Runs OVER   9764  11606 0.264134 0.255988 -0.814527 1.641146e-06 1.301431e-06
              Doubles OVER   2405   7644 0.149688 0.146782 -0.290636 2.252820e-06 6.824636e-07
        Walks Allowed OVER   1000   1716 0.437000 0.436480 -0.051981 4.085256e-05 2.375475e-05
                Walks OVER   6855   7343 0.272064 0.271551 -0.051305 2.471585e-06 2.299143e-06
   Hitter Strikeouts UNDER    317    250 0.529968 0.532000  0.203155 1.829536e-04 2.335999e-04
          Total Bases OVER  29481  33989 0.280621 0.284180  0.355876 6.091743e-07 5.410401e-07
        Pitching Outs OVER    379    652 0.493404 0.501534  0.813005 1.344674e-04 8.050967e-05
         Hits Allowed OVER   2104   3486 0.377376 0.391853  1.447670 1.482701e-05 9.592537e-06
Pitcher Fantasy Score OVER    238   1428 0.478992 0.495098  1.610644 2.029265e-04 3.590804e-05
            Home Runs OVER   5927   6680 0.115067 0.131587  1.652018 5.490756e-07 6.321300e-07
              Singles OVER   2970   9689 0.284848 0.307668  2.282001 6.219131e-06 2.202602e-06
    Plate Appearances OVER    373   1393 0.305630 0.367552  6.192202 5.651010e-05 2.132898e-05
       Pitches Thrown OVER    635   1028 0.411024 0.475681  6.465731 5.750008e-05 4.638667e-05
```

## Tickets containing team (min_n=40 both)

```
team_key  n_may  n_jun   wr_may   wr_jun       d_pp  wilson_may  wilson_jun
      TB    450     85 0.235556 0.094118 -14.143791    0.000029    0.000026
     LAA    514     82 0.219844 0.085366 -13.447850    0.000022    0.000022
     NYM    381     51 0.178478 0.058824 -11.965416    0.000020    0.000017
     CIN    593    148 0.246206 0.128378 -11.782736    0.000024    0.000027
      KC    423     85 0.231678 0.129412 -10.226672    0.000030    0.000048
     ATH    535     58 0.200000 0.103448  -9.655172    0.000018    0.000046
      SF    640    134 0.221875 0.126866  -9.500933    0.000018    0.000029
     PHI    795     43 0.201258 0.116279  -8.497879    0.000012    0.000077
      SD    403     71 0.223325 0.140845  -8.247999    0.000029    0.000068
     CLE    345    121 0.188406 0.107438  -8.096778    0.000024    0.000024
     TEX    383     92 0.227154 0.152174  -7.498013    0.000031    0.000061
     CHC    724     40 0.198895 0.125000  -7.389503    0.000013    0.000096
     SEA    571     82 0.201401 0.134146  -6.725471    0.000017    0.000053
     HOU    295     85 0.274576 0.211765  -6.281157    0.000058    0.000124
     PIT    478     94 0.211297 0.159574  -5.172260    0.000022    0.000065
     TOR    347     95 0.190202 0.147368  -4.283331    0.000025    0.000055
     LAD    496    131 0.181452 0.145038  -3.641344    0.000016    0.000039
     BAL    586     86 0.196246 0.162791  -3.345504    0.000016    0.000074
     MIL    420    113 0.261905 0.230088  -3.181627    0.000038    0.000109
     NYY    551     77 0.206897 0.194805  -1.209136    0.000018    0.000117
     ATL    509     51 0.153242 0.176471   2.322894    0.000011    0.000146
     MIA    374    112 0.195187 0.232143   3.695569    0.000024    0.000112
     STL    420    111 0.178571 0.216216   3.764479    0.000018    0.000099
     WSH    350    124 0.182857 0.233871   5.101382    0.000023    0.000103
     COL    552     79 0.164855 0.253165   8.830948    0.000012    0.000187
```

Buckets with Δ≤−2pp: 19 / 30; June n in those = 1696 / 2855

## Calibration gap (graded): mean_pred − hit rate

**Calibration gap growth by pick_u** (positive d_gap = more overconfident in June)
```
  pick_u   gap_may  gap_jun  d_gap_pp  hit_may  hit_jun  n_may  n_jun
   demon  0.133841 0.275461 14.162066 0.202619 0.204375  77347 140756
standard -0.035957 0.044025  7.998187 0.504324 0.500960   5435  12494
  goblin  0.012102 0.030620  1.851802 0.604028 0.624874  21004  35796
```

**Calibration gap growth by stat_type** (positive d_gap = more overconfident in June)
```
         stat_type   gap_may  gap_jun  d_gap_pp  hit_may  hit_jun  n_may  n_jun
     Pitching Outs -0.077452 0.178339 25.579131 0.513919 0.490518    467    791
Pitcher Strikeouts -0.034445 0.204977 23.942191 0.414457 0.401778   2753   4388
           Doubles  0.105504 0.292083 18.657886 0.149688 0.146782   2405   7644
 Plate Appearances -0.022456 0.162554 18.501005 0.362791 0.367944    430   1435
           Singles  0.032602 0.199583 16.698104 0.309304 0.324653   3246  10445
       Total Bases  0.087016 0.231022 14.400685 0.283678 0.287844  29812  34477
      Hits Allowed -0.019523 0.123444 14.296673 0.384266 0.394113   2199   3669
    Pitches Thrown -0.021043 0.121246 14.228890 0.442379 0.486832    807   1329
              RBIs  0.137841 0.276060 13.821857 0.224907 0.196990   1614  14686
              Runs  0.102028 0.235316 13.328799 0.273531 0.265485  10072  12012
             Walks  0.102932 0.228995 12.606284 0.275500 0.273561   6951   7417
              Hits  0.106269 0.215630 10.936011 0.365200 0.353562  14362  17137
```

**Calibration gap growth by park_bucket** (positive d_gap = more overconfident in June)
```
      park_bucket  gap_may  gap_jun  d_gap_pp  hit_may  hit_jun  n_may  n_jun
pitcher_park_<=97 0.083010 0.219212 13.620162 0.304137 0.291288  13997  24783
 hitter_park_105+ 0.099356 0.210841 11.148510 0.293063 0.303432   6862  12764
   neutral_98_104 0.103311 0.213193 10.988173 0.299444 0.305602  82927 151475
```

## Working read

- Inside-MLB Kitagawa on `n_legs`: WITHIN-dominated (mix +0.8 / within -4.7 of -4.0pp)
- Inside-MLB Kitagawa on `tier`: MIX-dominated (mix -5.8 / within +1.8 of -4.0pp)
- Inside-MLB Kitagawa on `dom_stat`: WITHIN-dominated (mix +2.9 / within -6.9 of -4.0pp)
- Inside-MLB Kitagawa on `park_tag`: WITHIN-dominated (mix +0.2 / within -4.2 of -4.0pp)