# Floor persistence (2026-10-25)

Line-free, as-of L5 min. Gift-scan Step 2. Frozen NFL tracks untouched.

Cells n≥50: **1299**. BH-keep P(k=0)>0.70: **314**.

`n` is not box-row count. A floor cell only counts a game when the
player already has five prior games **and** that L5 minimum is > 0.
A floor of 0 is vacuous (next game is almost always ≥ 0), so sparse
counting props (assists, key passes, yellows, TDs as a floor) shrink.

## CFB

| Prop | Role | Kind | n | HR | Wilson | BH |
|---|---|---|---:|---:|---:|---|
| fg_made | all | floor | 613 | 0.7553 | 0.7197 | Y |
| fg_made | top2 | floor | 613 | 0.7553 | 0.7197 | Y |
| fg_made | lead | floor | 607 | 0.7578 | 0.7222 | Y |
| interceptions | all | floor | 56 | 0.375 | 0.2601 |  |
| interceptions | top2 | floor | 56 | 0.375 | 0.2601 |  |
| interceptions | lead | floor | 56 | 0.375 | 0.2601 |  |
| kicking_points | all | floor | 3026 | 0.8599 | 0.8471 | Y |
| kicking_points | top2 | floor | 3024 | 0.8598 | 0.847 | Y |
| kicking_points | lead | floor | 2969 | 0.8619 | 0.849 | Y |
| pass_tds | all | floor | 870 | 0.7517 | 0.722 | Y |
| pass_tds | top2 | floor | 870 | 0.7517 | 0.722 | Y |
| pass_tds | lead | floor | 864 | 0.7546 | 0.7249 | Y |
| pass_yards | all | floor | 3188 | 0.7983 | 0.784 | Y |
| pass_yards | top2 | floor | 3182 | 0.7992 | 0.7849 | Y |
| pass_yards | lead | floor | 2972 | 0.8126 | 0.7982 | Y |
| player_tds | all | threshold | 159158 | 0.0806 | 0.0793 |  |
| player_tds | top2 | threshold | 19039 | 0.2521 | 0.246 |  |
| player_tds | lead | threshold | 9054 | 0.3069 | 0.2975 |  |
| rec_tds | all | floor | 60 | 0.4667 | 0.3463 |  |
| rec_tds | top2 | floor | 60 | 0.4667 | 0.3463 |  |
| rec_tds | lead | floor | 55 | 0.4727 | 0.3469 |  |
| rec_yards | all | floor | 13427 | 0.8017 | 0.7949 | Y |
| rec_yards | top2 | floor | 5851 | 0.7975 | 0.787 | Y |
| rec_yards | lead | floor | 3095 | 0.7935 | 0.7789 | Y |
| receptions | all | floor | 14513 | 0.8943 | 0.8892 | Y |
| receptions | top2 | floor | 6557 | 0.8752 | 0.867 | Y |
| receptions | lead | floor | 3488 | 0.8612 | 0.8494 | Y |
| rush_rec_yards | all | floor | 22435 | 0.8136 | 0.8084 | Y |
| rush_rec_yards | top2 | floor | 6628 | 0.795 | 0.7851 | Y |
| rush_rec_yards | lead | floor | 3437 | 0.792 | 0.7781 | Y |
| rush_tds | all | floor | 262 | 0.6679 | 0.6088 |  |
| rush_tds | top2 | floor | 262 | 0.6679 | 0.6088 |  |
| rush_tds | lead | floor | 243 | 0.6626 | 0.601 |  |
| rush_yards | all | floor | 8621 | 0.7853 | 0.7765 | Y |
| rush_yards | top2 | floor | 5741 | 0.7971 | 0.7865 | Y |
| rush_yards | lead | floor | 3173 | 0.8021 | 0.7879 | Y |
| sacks | all | floor | 55 | 0.4364 | 0.3137 |  |
| sacks | top2 | floor | 52 | 0.4231 | 0.2987 |  |
| tackles | all | floor | 52029 | 0.9224 | 0.92 | Y |
| tackles | top2 | floor | 7468 | 0.8528 | 0.8446 | Y |
| tackles | lead | floor | 3763 | 0.8403 | 0.8282 | Y |
| tfl | all | floor | 650 | 0.5462 | 0.5077 |  |
| tfl | top2 | floor | 544 | 0.5478 | 0.5058 |  |
| tfl | lead | floor | 376 | 0.5559 | 0.5053 |  |
| xp_made | all | floor | 2138 | 0.8527 | 0.837 | Y |
| xp_made | top2 | floor | 2137 | 0.8526 | 0.8369 | Y |
| xp_made | lead | floor | 2110 | 0.8531 | 0.8373 | Y |

## MLB

| Prop | Role | Kind | n | HR | Wilson | BH |
|---|---|---|---:|---:|---:|---|
| balls_counted | all | floor | 20368 | 0.851 | 0.846 | Y |
| balls_counted | top2 | floor | 7409 | 0.8129 | 0.8039 | Y |
| balls_counted | lead | floor | 3979 | 0.7982 | 0.7854 | Y |
| balls_counted | all_hitter | floor | 20368 | 0.851 | 0.846 | Y |
| balls_counted | lead_hitter | floor | 3979 | 0.7982 | 0.7854 | Y |
| balls_counted | top2_hitter | floor | 7409 | 0.8129 | 0.8039 | Y |
| balls_thrown | all | floor | 1592 | 0.8367 | 0.8177 | Y |
| balls_thrown | top2 | floor | 1531 | 0.8347 | 0.8153 | Y |
| balls_thrown | lead | floor | 1364 | 0.8416 | 0.8213 | Y |
| balls_thrown | all_pitcher | floor | 1592 | 0.8367 | 0.8177 | Y |
| balls_thrown | lead_pitcher | floor | 1364 | 0.8416 | 0.8213 | Y |
| balls_thrown | top2_pitcher | floor | 1531 | 0.8347 | 0.8153 | Y |
| batters_faced | all | floor | 10142 | 0.8741 | 0.8675 | Y |
| batters_faced | top2 | floor | 7402 | 0.8568 | 0.8486 | Y |
| batters_faced | lead | floor | 5076 | 0.8572 | 0.8473 | Y |
| batters_faced | all_pitcher | floor | 10142 | 0.8741 | 0.8675 | Y |
| batters_faced | lead_pitcher | floor | 5076 | 0.8572 | 0.8473 | Y |
| batters_faced | top2_pitcher | floor | 7402 | 0.8568 | 0.8486 | Y |
| earned_runs | all | floor | 1909 | 0.7899 | 0.7711 | Y |
| earned_runs | top2 | floor | 1901 | 0.7901 | 0.7712 | Y |
| earned_runs | lead | floor | 1836 | 0.7925 | 0.7733 | Y |
| earned_runs | all_pitcher | floor | 1909 | 0.7899 | 0.7711 | Y |
| earned_runs | lead_pitcher | floor | 1836 | 0.7925 | 0.7733 | Y |
| earned_runs | top2_pitcher | floor | 1901 | 0.7901 | 0.7712 | Y |
| fantasy_score | all | floor | 10201 | 0.6825 | 0.6734 |  |
| fantasy_score | top2 | floor | 3587 | 0.6791 | 0.6637 |  |
| fantasy_score | lead | floor | 2113 | 0.6697 | 0.6493 |  |
| fantasy_score | all_hitter | floor | 10201 | 0.6825 | 0.6734 |  |
| fantasy_score | lead_hitter | floor | 2113 | 0.6697 | 0.6493 |  |
| fantasy_score | top2_hitter | floor | 3587 | 0.6791 | 0.6637 |  |
| hits | all | floor | 3406 | 0.6216 | 0.6051 |  |
| hits | top2 | floor | 2097 | 0.6447 | 0.624 |  |
| hits | lead | floor | 1427 | 0.6496 | 0.6245 |  |
| hits | all_hitter | floor | 3406 | 0.6216 | 0.6051 |  |
| hits | lead_hitter | floor | 1427 | 0.6496 | 0.6245 |  |
| hits | top2_hitter | floor | 2097 | 0.6447 | 0.624 |  |
| hits_allowed | all | floor | 4991 | 0.8469 | 0.8367 | Y |
| hits_allowed | top2 | floor | 4901 | 0.8498 | 0.8396 | Y |
| hits_allowed | lead | floor | 4459 | 0.8684 | 0.8581 | Y |
| hits_allowed | all_pitcher | floor | 4991 | 0.8469 | 0.8367 | Y |
| hits_allowed | lead_pitcher | floor | 4459 | 0.8684 | 0.8581 | Y |
| hits_allowed | top2_pitcher | floor | 4901 | 0.8498 | 0.8396 | Y |
| hits_runs_rbi | all | floor | 5715 | 0.6539 | 0.6415 |  |
| hits_runs_rbi | top2 | floor | 2560 | 0.6617 | 0.6432 |  |
| hits_runs_rbi | lead | floor | 1548 | 0.6576 | 0.6336 |  |
| hits_runs_rbi | all_hitter | floor | 5715 | 0.6539 | 0.6415 |  |
| hits_runs_rbi | lead_hitter | floor | 1548 | 0.6576 | 0.6336 |  |
| hits_runs_rbi | top2_hitter | floor | 2560 | 0.6617 | 0.6432 |  |
| hitter_strikeouts | all | floor | 3792 | 0.6659 | 0.6507 |  |
| hitter_strikeouts | top2 | floor | 2229 | 0.6671 | 0.6473 |  |
| hitter_strikeouts | lead | floor | 1530 | 0.6667 | 0.6427 |  |
| hitter_strikeouts | all_hitter | floor | 3792 | 0.6659 | 0.6507 |  |
| hitter_strikeouts | lead_hitter | floor | 1530 | 0.6667 | 0.6427 |  |
| hitter_strikeouts | top2_hitter | floor | 2229 | 0.6671 | 0.6473 |  |
| home_runs | all | threshold | 50438 | 0.105 | 0.1024 |  |
| home_runs | top2 | threshold | 21293 | 0.1118 | 0.1077 |  |
| home_runs | lead | threshold | 12167 | 0.1139 | 0.1084 |  |
| innings_pitched | all | floor | 9903 | 0.8749 | 0.8682 | Y |
| innings_pitched | top2 | floor | 7422 | 0.8653 | 0.8573 | Y |
| innings_pitched | lead | floor | 5079 | 0.8626 | 0.8528 | Y |
| innings_pitched | all_pitcher | floor | 9903 | 0.8749 | 0.8682 | Y |
| innings_pitched | lead_pitcher | floor | 5079 | 0.8626 | 0.8528 | Y |
| innings_pitched | top2_pitcher | floor | 7422 | 0.8653 | 0.8573 | Y |
| pitcher_fantasy_score | all | floor | 6198 | 0.8374 | 0.828 | Y |
| pitcher_fantasy_score | top2 | floor | 4407 | 0.8303 | 0.8189 | Y |
| pitcher_fantasy_score | lead | floor | 2765 | 0.8325 | 0.8182 | Y |
| pitcher_fantasy_score | all_pitcher | floor | 6198 | 0.8374 | 0.828 | Y |
| pitcher_fantasy_score | lead_pitcher | floor | 2765 | 0.8325 | 0.8182 | Y |
| pitcher_fantasy_score | top2_pitcher | floor | 4407 | 0.8303 | 0.8189 | Y |
| pitches_seen | all | floor | 21758 | 0.8396 | 0.8347 | Y |
| pitches_seen | top2 | floor | 7395 | 0.8042 | 0.795 | Y |
| pitches_seen | lead | floor | 3881 | 0.7939 | 0.7809 | Y |
| pitches_seen | all_hitter | floor | 21758 | 0.8396 | 0.8347 | Y |
| pitches_seen | lead_hitter | floor | 3881 | 0.7939 | 0.7809 | Y |
| pitches_seen | top2_hitter | floor | 7395 | 0.8042 | 0.795 | Y |
| pitches_thrown | all | floor | 8964 | 0.8421 | 0.8345 | Y |
| pitches_thrown | top2 | floor | 5980 | 0.8284 | 0.8187 | Y |
| pitches_thrown | lead | floor | 3872 | 0.8443 | 0.8325 | Y |
| pitches_thrown | all_pitcher | floor | 8964 | 0.8421 | 0.8345 | Y |
| pitches_thrown | lead_pitcher | floor | 3872 | 0.8443 | 0.8325 | Y |
| pitches_thrown | top2_pitcher | floor | 5980 | 0.8284 | 0.8187 | Y |
| pitches_thrown_95 | all | floor | 902 | 0.8348 | 0.8092 | Y |
| pitches_thrown_95 | top2 | floor | 892 | 0.8363 | 0.8106 | Y |
| pitches_thrown_95 | lead | floor | 847 | 0.8406 | 0.8144 | Y |
| pitches_thrown_95 | all_pitcher | floor | 902 | 0.8348 | 0.8092 | Y |
| pitches_thrown_95 | lead_pitcher | floor | 847 | 0.8406 | 0.8144 | Y |
| pitches_thrown_95 | top2_pitcher | floor | 892 | 0.8363 | 0.8106 | Y |
| pitching_outs | all | floor | 9903 | 0.8749 | 0.8682 | Y |
| pitching_outs | top2 | floor | 7452 | 0.8655 | 0.8576 | Y |
| pitching_outs | lead | floor | 5093 | 0.8628 | 0.853 | Y |
| pitching_outs | all_pitcher | floor | 9903 | 0.8749 | 0.8682 | Y |
| pitching_outs | lead_pitcher | floor | 5093 | 0.8628 | 0.853 | Y |
| pitching_outs | top2_pitcher | floor | 7452 | 0.8655 | 0.8576 | Y |
| plate_appearances | all | floor | 32965 | 0.9044 | 0.9012 | Y |
| plate_appearances | top2 | floor | 11675 | 0.907 | 0.9016 | Y |
| plate_appearances | lead | floor | 7858 | 0.9068 | 0.9002 | Y |
| plate_appearances | all_hitter | floor | 32965 | 0.9044 | 0.9012 | Y |
| plate_appearances | lead_hitter | floor | 7858 | 0.9068 | 0.9002 | Y |
| plate_appearances | top2_hitter | floor | 11675 | 0.907 | 0.9016 | Y |
| rbi | all | floor | 133 | 0.3684 | 0.2912 |  |
| rbi | top2 | floor | 93 | 0.3871 | 0.2945 |  |
| rbi | lead | floor | 70 | 0.3857 | 0.2805 |  |
| rbi | all_hitter | floor | 133 | 0.3684 | 0.2912 |  |
| rbi | lead_hitter | floor | 70 | 0.3857 | 0.2805 |  |
| rbi | top2_hitter | floor | 93 | 0.3871 | 0.2945 |  |
| runs | all | floor | 350 | 0.4286 | 0.3778 |  |
| runs | top2 | floor | 218 | 0.4083 | 0.3451 |  |
| runs | lead | floor | 170 | 0.4235 | 0.3517 |  |
| runs | all_hitter | floor | 350 | 0.4286 | 0.3778 |  |
| runs | lead_hitter | floor | 170 | 0.4235 | 0.3517 |  |
| runs | top2_hitter | floor | 218 | 0.4083 | 0.3451 |  |
| singles | all | floor | 825 | 0.4909 | 0.4569 |  |
| singles | top2 | floor | 589 | 0.4907 | 0.4505 |  |
| singles | lead | floor | 465 | 0.4968 | 0.4515 |  |
| singles | all_hitter | floor | 825 | 0.4909 | 0.4569 |  |
| singles | lead_hitter | floor | 465 | 0.4968 | 0.4515 |  |
| singles | top2_hitter | floor | 589 | 0.4907 | 0.4505 |  |
| stolen_bases | all | threshold | 50438 | 0.0601 | 0.058 |  |
| stolen_bases | top2 | threshold | 28794 | 0.0616 | 0.0589 |  |
| stolen_bases | lead | threshold | 16594 | 0.0652 | 0.0615 |  |
| strikeouts | all | floor | 5443 | 0.8381 | 0.8281 | Y |
| strikeouts | top2 | floor | 5155 | 0.8473 | 0.8373 | Y |
| strikeouts | lead | floor | 4417 | 0.8628 | 0.8523 | Y |
| strikeouts | all_pitcher | floor | 5443 | 0.8381 | 0.8281 | Y |
| strikeouts | lead_pitcher | floor | 4417 | 0.8628 | 0.8523 | Y |
| strikeouts | top2_pitcher | floor | 5155 | 0.8473 | 0.8373 | Y |
| strikes_counted | all | floor | 23236 | 0.8635 | 0.8591 | Y |
| strikes_counted | top2 | floor | 7821 | 0.8175 | 0.8088 | Y |
| strikes_counted | lead | floor | 4223 | 0.8065 | 0.7943 | Y |
| strikes_counted | all_hitter | floor | 23236 | 0.8635 | 0.8591 | Y |
| strikes_counted | lead_hitter | floor | 4223 | 0.8065 | 0.7943 | Y |
| strikes_counted | top2_hitter | floor | 7821 | 0.8175 | 0.8088 | Y |
| strikes_thrown | all | floor | 1622 | 0.8428 | 0.8243 | Y |
| strikes_thrown | top2 | floor | 1555 | 0.8386 | 0.8195 | Y |
| strikes_thrown | lead | floor | 1375 | 0.8473 | 0.8273 | Y |
| strikes_thrown | all_pitcher | floor | 1622 | 0.8428 | 0.8243 | Y |
| strikes_thrown | lead_pitcher | floor | 1375 | 0.8473 | 0.8273 | Y |
| strikes_thrown | top2_pitcher | floor | 1555 | 0.8386 | 0.8195 | Y |
| total_bases | all | floor | 3406 | 0.6013 | 0.5847 |  |
| total_bases | top2 | floor | 1653 | 0.6056 | 0.5818 |  |
| total_bases | lead | floor | 1018 | 0.6149 | 0.5847 |  |
| total_bases | all_hitter | floor | 3406 | 0.6013 | 0.5847 |  |
| total_bases | lead_hitter | floor | 1018 | 0.6149 | 0.5847 |  |
| total_bases | top2_hitter | floor | 1653 | 0.6056 | 0.5818 |  |
| triples | all | threshold | 50438 | 0.0127 | 0.0118 |  |
| triples | top2 | threshold | 38804 | 0.0128 | 0.0117 |  |
| triples | lead | threshold | 30832 | 0.0124 | 0.0112 |  |
| walks | all | floor | 251 | 0.502 | 0.4406 |  |
| walks | top2 | floor | 192 | 0.4948 | 0.4249 |  |
| walks | lead | floor | 167 | 0.491 | 0.4163 |  |
| walks | all_hitter | floor | 251 | 0.502 | 0.4406 |  |
| walks | lead_hitter | floor | 167 | 0.491 | 0.4163 |  |
| walks | top2_hitter | floor | 192 | 0.4948 | 0.4249 |  |
| walks_allowed | all | floor | 2000 | 0.806 | 0.7881 | Y |
| walks_allowed | top2 | floor | 2000 | 0.806 | 0.7881 | Y |
| walks_allowed | lead | floor | 1927 | 0.8137 | 0.7957 | Y |
| walks_allowed | all_pitcher | floor | 2000 | 0.806 | 0.7881 | Y |
| walks_allowed | lead_pitcher | floor | 1927 | 0.8137 | 0.7957 | Y |
| walks_allowed | top2_pitcher | floor | 2000 | 0.806 | 0.7881 | Y |

## NBA

| Prop | Role | Kind | n | HR | Wilson | BH |
|---|---|---|---:|---:|---:|---|
| ast | all | floor | 12143 | 0.8272 | 0.8204 | Y |
| ast | top2 | floor | 5014 | 0.844 | 0.8337 | Y |
| ast | lead | floor | 2649 | 0.8411 | 0.8267 | Y |
| blk | all | floor | 726 | 0.6377 | 0.6021 |  |
| blk | top2 | floor | 694 | 0.6412 | 0.6048 |  |
| blk | lead | floor | 568 | 0.6532 | 0.6131 |  |
| bs | all | floor | 5050 | 0.7519 | 0.7398 | Y |
| bs | top2 | floor | 2879 | 0.7669 | 0.7511 | Y |
| bs | lead | floor | 1739 | 0.7677 | 0.7473 | Y |
| double_double | all | threshold | 30856 | 0.0695 | 0.0668 |  |
| double_double | top2 | threshold | 17508 | 0.0998 | 0.0955 |  |
| double_double | lead | threshold | 7522 | 0.159 | 0.1509 |  |
| dreb | all | floor | 15632 | 0.8393 | 0.8335 | Y |
| dreb | top2 | floor | 5268 | 0.8307 | 0.8203 | Y |
| dreb | lead | floor | 2696 | 0.8208 | 0.8059 | Y |
| fg2a | all | floor | 18416 | 0.8447 | 0.8394 | Y |
| fg2a | top2 | floor | 5345 | 0.8485 | 0.8386 | Y |
| fg2a | lead | floor | 2697 | 0.842 | 0.8278 | Y |
| fg2m | all | floor | 13050 | 0.8392 | 0.8328 | Y |
| fg2m | top2 | floor | 5177 | 0.8476 | 0.8375 | Y |
| fg2m | lead | floor | 2706 | 0.833 | 0.8184 | Y |
| fg3a | all | floor | 16821 | 0.8518 | 0.8463 | Y |
| fg3a | top2 | floor | 5404 | 0.8464 | 0.8366 | Y |
| fg3a | lead | floor | 2757 | 0.8382 | 0.824 | Y |
| fg3m | all | floor | 5792 | 0.7797 | 0.7688 | Y |
| fg3m | top2 | floor | 3474 | 0.7858 | 0.7719 | Y |
| fg3m | lead | floor | 2034 | 0.7876 | 0.7693 | Y |
| fga | all | floor | 23181 | 0.8453 | 0.8405 | Y |
| fga | top2 | floor | 5365 | 0.8399 | 0.8298 | Y |
| fga | lead | floor | 2673 | 0.8268 | 0.812 | Y |
| fgm | all | floor | 17625 | 0.8424 | 0.837 | Y |
| fgm | top2 | floor | 5337 | 0.8316 | 0.8213 | Y |
| fgm | lead | floor | 2729 | 0.8208 | 0.806 | Y |
| fta | all | floor | 5311 | 0.7609 | 0.7492 | Y |
| fta | top2 | floor | 3517 | 0.7859 | 0.772 | Y |
| fta | lead | floor | 2074 | 0.7956 | 0.7777 | Y |
| ftm | all | floor | 4571 | 0.7576 | 0.745 | Y |
| ftm | top2 | floor | 3226 | 0.7836 | 0.7691 | Y |
| ftm | lead | floor | 1934 | 0.7927 | 0.774 | Y |
| minutes | all | floor | 27209 | 0.8431 | 0.8388 | Y |
| minutes | top2 | floor | 5394 | 0.8218 | 0.8114 | Y |
| minutes | lead | floor | 2752 | 0.811 | 0.796 | Y |
| oreb | all | floor | 3820 | 0.7521 | 0.7382 | Y |
| oreb | top2 | floor | 2785 | 0.7734 | 0.7575 | Y |
| oreb | lead | floor | 1740 | 0.7914 | 0.7717 | Y |
| pa | all | floor | 21687 | 0.8229 | 0.8178 | Y |
| pa | top2 | floor | 5250 | 0.8183 | 0.8076 | Y |
| pa | lead | floor | 2647 | 0.8039 | 0.7884 | Y |
| pr | all | floor | 22975 | 0.8268 | 0.8218 | Y |
| pr | top2 | floor | 5266 | 0.815 | 0.8043 | Y |
| pr | lead | floor | 2640 | 0.8076 | 0.7921 | Y |
| pra | all | floor | 23849 | 0.8283 | 0.8235 | Y |
| pra | top2 | floor | 5292 | 0.8175 | 0.8068 | Y |
| pra | lead | floor | 2664 | 0.8071 | 0.7916 | Y |
| pts | all | floor | 19049 | 0.82 | 0.8145 | Y |
| pts | top2 | floor | 5206 | 0.8108 | 0.7999 | Y |
| pts | lead | floor | 2618 | 0.8037 | 0.788 | Y |
| ra | all | floor | 21968 | 0.8434 | 0.8385 | Y |
| ra | top2 | floor | 5380 | 0.8243 | 0.814 | Y |
| ra | lead | floor | 2715 | 0.8225 | 0.8076 | Y |
| reb | all | floor | 18589 | 0.845 | 0.8397 | Y |
| reb | top2 | floor | 5293 | 0.8332 | 0.8229 | Y |
| reb | lead | floor | 2686 | 0.8306 | 0.8159 | Y |
| stl | all | floor | 2039 | 0.6748 | 0.6542 |  |
| stl | top2 | floor | 1481 | 0.6928 | 0.6688 |  |
| stl | lead | floor | 961 | 0.6909 | 0.661 |  |
| tov | all | floor | 6272 | 0.7919 | 0.7817 | Y |
| tov | top2 | floor | 3779 | 0.8153 | 0.8026 | Y |
| tov | lead | floor | 2176 | 0.812 | 0.7951 | Y |

## NFL

| Prop | Role | Kind | n | HR | Wilson | BH |
|---|---|---|---:|---:|---:|---|
| def_sacks | all | floor | 225 | 0.52 | 0.4549 |  |
| def_sacks | top2 | floor | 222 | 0.5225 | 0.457 |  |
| def_sacks | lead | floor | 195 | 0.5179 | 0.4481 |  |
| def_tackle_assists | all | floor | 8449 | 0.7802 | 0.7713 | Y |
| def_tackle_assists | top2 | floor | 4030 | 0.8117 | 0.7993 | Y |
| def_tackle_assists | lead | floor | 2266 | 0.8151 | 0.7986 | Y |
| def_tackles_solo | all | floor | 33508 | 0.9444 | 0.9419 | Y |
| def_tackles_solo | top2 | floor | 6031 | 0.8596 | 0.8506 | Y |
| def_tackles_solo | lead | floor | 3114 | 0.8417 | 0.8284 | Y |
| def_tackles_with_assist | all | floor | 229 | 0.5284 | 0.4638 |  |
| def_tackles_with_assist | top2 | floor | 220 | 0.5273 | 0.4614 |  |
| def_tackles_with_assist | lead | floor | 191 | 0.5393 | 0.4685 |  |
| fg_att | all | floor | 2218 | 0.9477 | 0.9376 | Y |
| fg_att | top2 | floor | 2218 | 0.9477 | 0.9376 | Y |
| fg_att | lead | floor | 2218 | 0.9477 | 0.9376 | Y |
| fg_made | all | floor | 1641 | 0.9043 | 0.8891 | Y |
| fg_made | top2 | floor | 1641 | 0.9043 | 0.8891 | Y |
| fg_made | lead | floor | 1641 | 0.9043 | 0.8891 | Y |
| kicking_points | all | floor | 2180 | 0.8619 | 0.8468 | Y |
| kicking_points | top2 | floor | 2180 | 0.8619 | 0.8468 | Y |
| kicking_points | lead | floor | 2180 | 0.8619 | 0.8468 | Y |
| kickoff_returns | all | floor | 216 | 0.6944 | 0.6301 |  |
| kickoff_returns | top2 | floor | 215 | 0.6977 | 0.6333 |  |
| kickoff_returns | lead | floor | 204 | 0.6961 | 0.6298 |  |
| pass_attempts | all | floor | 2649 | 0.818 | 0.8029 | Y |
| pass_attempts | top2 | floor | 2649 | 0.818 | 0.8029 | Y |
| pass_attempts | lead | floor | 2463 | 0.8335 | 0.8183 | Y |
| passing_tds | all | threshold | 77490 | 0.0305 | 0.0293 |  |
| passing_tds | top2 | threshold | 65724 | 0.0351 | 0.0337 |  |
| passing_tds | lead | threshold | 6439 | 0.3409 | 0.3294 |  |
| passing_yards | all | floor | 2539 | 0.8023 | 0.7863 | Y |
| passing_yards | top2 | floor | 2539 | 0.8023 | 0.7863 | Y |
| passing_yards | lead | floor | 2406 | 0.8204 | 0.8046 | Y |
| pat_att | all | floor | 1347 | 0.8448 | 0.8245 | Y |
| pat_att | top2 | floor | 1347 | 0.8448 | 0.8245 | Y |
| pat_att | lead | floor | 1347 | 0.8448 | 0.8245 | Y |
| pat_made | all | floor | 1232 | 0.8312 | 0.8092 | Y |
| pat_made | top2 | floor | 1232 | 0.8312 | 0.8092 | Y |
| pat_made | lead | floor | 1232 | 0.8312 | 0.8092 | Y |
| player_tds | all | threshold | 77490 | 0.0764 | 0.0746 |  |
| player_tds | top2 | threshold | 10072 | 0.2579 | 0.2495 |  |
| player_tds | lead | threshold | 4729 | 0.3216 | 0.3085 |  |
| pt_att | all | floor | 2552 | 0.8907 | 0.878 | Y |
| pt_att | top2 | floor | 2552 | 0.8907 | 0.878 | Y |
| pt_att | lead | floor | 2552 | 0.8907 | 0.878 | Y |
| pt_yards | all | floor | 2552 | 0.8264 | 0.8112 | Y |
| pt_yards | top2 | floor | 2552 | 0.8264 | 0.8112 | Y |
| pt_yards | lead | floor | 2552 | 0.8264 | 0.8112 | Y |
| punt_returns | all | floor | 324 | 0.7191 | 0.6679 |  |
| punt_returns | top2 | floor | 324 | 0.7191 | 0.6679 |  |
| punt_returns | lead | floor | 318 | 0.7233 | 0.6717 |  |
| receiving_yards | all | floor | 10433 | 0.7542 | 0.7459 | Y |
| receiving_yards | top2 | floor | 4617 | 0.7758 | 0.7636 | Y |
| receiving_yards | lead | floor | 2473 | 0.7707 | 0.7537 | Y |
| receptions | all | floor | 11241 | 0.8357 | 0.8287 | Y |
| receptions | top2 | floor | 5212 | 0.8431 | 0.8329 | Y |
| receptions | lead | floor | 2750 | 0.8407 | 0.8266 | Y |
| rush_attempts | all | floor | 6920 | 0.8327 | 0.8237 | Y |
| rush_attempts | top2 | floor | 4807 | 0.8396 | 0.829 | Y |
| rush_attempts | lead | floor | 2639 | 0.8443 | 0.8299 | Y |
| rush_rec_yards | all | floor | 15525 | 0.7756 | 0.769 | Y |
| rush_rec_yards | top2 | floor | 5116 | 0.7846 | 0.7731 | Y |
| rush_rec_yards | lead | floor | 2635 | 0.7753 | 0.759 | Y |
| rushing_yards | all | floor | 5667 | 0.7785 | 0.7675 | Y |
| rushing_yards | top2 | floor | 4308 | 0.7934 | 0.7811 | Y |
| rushing_yards | lead | floor | 2515 | 0.8087 | 0.7929 | Y |
| targets | all | floor | 16147 | 0.8737 | 0.8685 | Y |
| targets | top2 | floor | 5527 | 0.848 | 0.8383 | Y |
| targets | lead | floor | 2859 | 0.8395 | 0.8255 | Y |

## NHL

| Prop | Role | Kind | n | HR | Wilson | BH |
|---|---|---|---:|---:|---:|---|
| assists | all | floor | 64 | 0.5 | 0.381 |  |
| blocked_shots | all | floor | 1559 | 0.712 | 0.689 |  |
| blocked_shots | top2 | floor | 582 | 0.7388 | 0.7017 | Y |
| faceoffs_won | all | floor | 4103 | 0.8443 | 0.8328 | Y |
| faceoffs_won | top2 | floor | 1053 | 0.8452 | 0.8221 | Y |
| goals | all | threshold | 22750 | 0.1513 | 0.1467 |  |
| goals | top2 | threshold | 2444 | 0.2226 | 0.2065 |  |
| goals | lead | threshold | 154 | 0.2857 | 0.2203 |  |
| goals_allowed | all | floor | 670 | 0.8642 | 0.8362 | Y |
| hits | all | floor | 3796 | 0.7727 | 0.7591 | Y |
| hits | top2 | floor | 866 | 0.7945 | 0.7663 | Y |
| plus_minus | all | floor | 79 | 0.4051 | 0.3037 |  |
| points | all | floor | 348 | 0.5546 | 0.5021 |  |
| points | top2 | floor | 179 | 0.5866 | 0.5134 |  |
| saves | all | floor | 913 | 0.8335 | 0.808 | Y |
| saves | top2 | floor | 496 | 0.7883 | 0.7502 | Y |
| saves | lead | floor | 331 | 0.7795 | 0.7317 | Y |
| shots_on_goal | all | floor | 5411 | 0.7755 | 0.7641 | Y |
| shots_on_goal | top2 | floor | 976 | 0.7797 | 0.7526 | Y |
| toi | all | floor | 18915 | 0.8279 | 0.8224 | Y |
| toi | top2 | floor | 1935 | 0.815 | 0.7971 | Y |
| toi | lead | floor | 897 | 0.8629 | 0.8388 | Y |

## Soccer

| Prop | Role | Kind | n | HR | Wilson | BH |
|---|---|---|---:|---:|---:|---|
| clearances | all | floor | 70 | 0.8714 | 0.7734 | Y |
| clearances | top2 | floor | 53 | 0.8868 | 0.7742 | Y |
| dribble_attempts | all | floor | 71 | 0.9577 | 0.883 | Y |
| dribble_attempts | top2 | floor | 57 | 0.9649 | 0.8808 | Y |
| fc | all | floor | 2604 | 0.6363 | 0.6177 |  |
| fc | top2 | floor | 1715 | 0.6501 | 0.6273 |  |
| fc | lead | floor | 1073 | 0.6645 | 0.6357 |  |
| g | all | threshold | 105905 | 0.0628 | 0.0614 |  |
| g | top2 | threshold | 29261 | 0.1035 | 0.1 |  |
| g | lead | threshold | 12456 | 0.1451 | 0.139 |  |
| g_a | all | threshold | 112784 | 0.0966 | 0.0949 |  |
| g_a | top2 | threshold | 26071 | 0.1618 | 0.1573 |  |
| g_a | lead | threshold | 14705 | 0.1751 | 0.1691 |  |
| minutes_played | all | floor | 7982 | 0.9526 | 0.9478 | Y |
| minutes_played | top2 | floor | 7982 | 0.9526 | 0.9478 | Y |
| minutes_played | lead | floor | 7982 | 0.9526 | 0.9478 | Y |
| pa | all | floor | 507 | 0.8225 | 0.7868 | Y |
| pa | top2 | floor | 250 | 0.812 | 0.759 | Y |
| pa | lead | floor | 180 | 0.8056 | 0.7416 | Y |
| sh | all | floor | 4794 | 0.737 | 0.7243 | Y |
| sh | top2 | floor | 3201 | 0.7641 | 0.7491 | Y |
| sh | lead | floor | 2021 | 0.7734 | 0.7546 | Y |
| sog | all | floor | 662 | 0.6465 | 0.6094 |  |
| sog | top2 | floor | 624 | 0.649 | 0.6108 |  |
| sog | lead | floor | 488 | 0.6578 | 0.6146 |  |
| sv | all | floor | 1589 | 0.7955 | 0.7749 | Y |
| sv | top2 | floor | 113 | 0.7876 | 0.7034 | Y |
| sv | lead | floor | 86 | 0.8023 | 0.706 | Y |

## WNBA

| Prop | Role | Kind | n | HR | Wilson | BH |
|---|---|---|---:|---:|---:|---|
| ast | all | floor | 4716 | 0.8251 | 0.814 | Y |
| ast | top2 | floor | 2387 | 0.8454 | 0.8304 | Y |
| ast | lead | floor | 1300 | 0.8385 | 0.8175 | Y |
| blk | all | floor | 340 | 0.6971 | 0.6462 |  |
| blk | top2 | floor | 333 | 0.6907 | 0.6391 |  |
| blk | lead | floor | 291 | 0.7148 | 0.6604 |  |
| double_double | all | threshold | 13641 | 0.0542 | 0.0505 |  |
| double_double | top2 | threshold | 9765 | 0.0685 | 0.0637 |  |
| double_double | lead | threshold | 4415 | 0.1128 | 0.1038 |  |
| dreb | all | floor | 154 | 0.7987 | 0.7285 | Y |
| dreb | top2 | floor | 74 | 0.8108 | 0.7071 | Y |
| fg2a | all | floor | 7722 | 0.8436 | 0.8353 | Y |
| fg2a | top2 | floor | 2622 | 0.8516 | 0.8375 | Y |
| fg2a | lead | floor | 1336 | 0.8406 | 0.82 | Y |
| fg2m | all | floor | 4891 | 0.8289 | 0.8181 | Y |
| fg2m | top2 | floor | 2418 | 0.8437 | 0.8287 | Y |
| fg2m | lead | floor | 1284 | 0.8372 | 0.816 | Y |
| fg3a | all | floor | 6153 | 0.8459 | 0.8367 | Y |
| fg3a | top2 | floor | 2518 | 0.8459 | 0.8313 | Y |
| fg3a | lead | floor | 1328 | 0.8328 | 0.8118 | Y |
| fg3m | all | floor | 1308 | 0.7225 | 0.6976 | Y |
| fg3m | top2 | floor | 1014 | 0.7416 | 0.7138 | Y |
| fg3m | lead | floor | 638 | 0.7492 | 0.7142 | Y |
| fga | all | floor | 9813 | 0.8458 | 0.8385 | Y |
| fga | top2 | floor | 2649 | 0.8377 | 0.8231 | Y |
| fga | lead | floor | 1339 | 0.817 | 0.7954 | Y |
| fgm | all | floor | 6556 | 0.8379 | 0.8287 | Y |
| fgm | top2 | floor | 2576 | 0.8463 | 0.8318 | Y |
| fgm | lead | floor | 1339 | 0.835 | 0.8141 | Y |
| fta | all | floor | 2143 | 0.749 | 0.7302 | Y |
| fta | top2 | floor | 1489 | 0.7656 | 0.7434 | Y |
| fta | lead | floor | 920 | 0.7717 | 0.7435 | Y |
| ftm | all | floor | 1871 | 0.7509 | 0.7308 | Y |
| ftm | top2 | floor | 1393 | 0.7602 | 0.7371 | Y |
| ftm | lead | floor | 871 | 0.7566 | 0.727 | Y |
| minutes | all | floor | 11824 | 0.8361 | 0.8293 | Y |
| minutes | top2 | floor | 2665 | 0.8139 | 0.7987 | Y |
| minutes | lead | floor | 1385 | 0.8123 | 0.7909 | Y |
| pa | all | floor | 175 | 0.7657 | 0.6977 | Y |
| pa | top2 | floor | 52 | 0.8077 | 0.681 | Y |
| pr | all | floor | 181 | 0.8232 | 0.7611 | Y |
| pr | top2 | floor | 50 | 0.82 | 0.692 | Y |
| pra | all | floor | 197 | 0.8071 | 0.7463 | Y |
| pra | top2 | floor | 51 | 0.7843 | 0.6537 |  |
| pts | all | floor | 7475 | 0.8123 | 0.8033 | Y |
| pts | top2 | floor | 2556 | 0.8177 | 0.8022 | Y |
| pts | lead | floor | 1310 | 0.7992 | 0.7767 | Y |
| ra | all | floor | 177 | 0.8192 | 0.7559 | Y |
| ra | top2 | floor | 53 | 0.7547 | 0.6243 |  |
| reb | all | floor | 7035 | 0.8394 | 0.8306 | Y |
| reb | top2 | floor | 2543 | 0.8348 | 0.8199 | Y |
| reb | lead | floor | 1343 | 0.8302 | 0.8092 | Y |
| stl | all | floor | 925 | 0.7059 | 0.6758 |  |
| stl | top2 | floor | 744 | 0.7191 | 0.6857 |  |
| stl | lead | floor | 482 | 0.7033 | 0.6611 |  |
| tov | all | floor | 3029 | 0.7943 | 0.7796 | Y |
| tov | top2 | floor | 1820 | 0.8055 | 0.7867 | Y |
| tov | lead | floor | 1067 | 0.8135 | 0.789 | Y |

## Measured but n<50 (positive L5 floor too rare)

| Sport | Prop | n | HR |
|---|---|---:|---:|
| MLB | doubles | 1 | 0.0 |
| MLB | first_inning_runs_allowed | 2 | 0.0 |
| MLB | first_inning_walks_allowed | 4 | 0.0 |
| NHL | pim | 26 | 0.5769 |
| Soccer | a | 9 | 0.3333 |
| Soccer | kp | 10 | 0.9 |
| Soccer | tk | 44 | 0.9318 |
| Soccer | yc | 3 | 0.3333 |
| WNBA | bs | 43 | 0.7442 |
| WNBA | oreb | 40 | 0.8 |

## Out until attach is fixed

- Tennis / Golf — no box table in this run
- CBB boxes empty in proporacle_ref.db — out until attach exists
- Football YPC / longest reception / completions / period props — mapping-fail
- Quarters-with-X / NBA1Q — period boxes not used as a floor
- CFB minutes — ESPN payload MIN is a dummy (often 1.0), not a floor
