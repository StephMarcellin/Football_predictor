---
schema: gold
rows: 86342
---
# features_final

#gold

Table finale ML — 1 ligne par équipe par match. Jointure de features_rolling × features_whoscored × features_draw. Inclut les différentiels équipe vs adversaire, H2H, Giant Killer, league_draw_rate et final_match_id.


## Intégrité
**Clé déclarée :** (date, team, opponent, league_source) — ⚠️ **40950 doublons**

## Lineage
**Sources :** [[features_draw]], [[features_rolling]], [[features_whoscored]], [[h2h_history]]
**Alimente :** —

## Features & profiling  (86342 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `date` | DATE | 100.0% | 0 | 2417 |  | Date du match |
| `team` | VARCHAR | 100.0% | 0 | 153 |  | Nom normalisé de l'équipe |
| `opponent` | VARCHAR | 100.0% | 0 | 422 |  | Nom normalisé de l'adversaire |
| `venue` | VARCHAR | 100.0% | 0 | 3 | top: Away (43382), Home (42728), Neutral (232) |  |
| `is_home` | INTEGER | 100.0% | 0 | 2 | moy 0.495 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | 1 si l'équipe joue à domicile, 0 sinon |
| `season` | VARCHAR | 100.0% | 0 | 9 | top: 2017-2018 (29948), 2018-2019 (13655), 2020-2021 (13361) |  |
| `league_source` | VARCHAR | 100.0% | 0 | 28 |  | Compétition source |
| `comp_category` | VARCHAR | 100.0% | 0 | 5 | top: Big5 (73168), D2 (5756), Cup (3943) |  |
| `match_id` | BIGINT | 84.7% | 13174 | 16108 |  | Identifiant Understat du match |
| `result_1n2` | VARCHAR | 99.1% | 810 | 3 | top: H (52205), A (22390), D (10937) | Résultat du point de vue de l'équipe : H, D, A |
| `formation` | VARCHAR | 100.0% | 0 | 29 |  |  |
| `days_since_last_match` | BIGINT | 97.5% | 2200 | 145 | moy 6.448 · méd 4.0 · min/max 0.0/1911.0 · p10/p90 0.0/14.0 |  |
| `is_return_from_break` | INTEGER | 100.0% | 0 | 2 | moy 0.015 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.0 |  |
| `is_short_rest` | INTEGER | 100.0% | 0 | 2 | moy 0.419 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/1.0 |  |
| `season_att_rating` | DOUBLE | 24.8% | 64950 | 59 | moy 6.813 · méd 6.8 · min/max 6.44/7.14 · p10/p90 6.56/7.13 |  |
| `season_def_rating` | DOUBLE | 24.8% | 64950 | 59 | moy 6.813 · méd 6.8 · min/max 6.44/7.14 · p10/p90 6.56/7.13 |  |
| `ws_dribbles_pg` | DOUBLE | 16.7% | 71880 | 95 | moy 8.286 · méd 8.1 · min/max 4.1/15.5 · p10/p90 5.9/10.8 |  |
| `ws_fouled_pg` | DOUBLE | 16.7% | 71880 | 84 | moy 11.333 · méd 11.4 · min/max 6.9/16.0 · p10/p90 9.3/13.3 |  |
| `ws_shots_ot_pg` | DOUBLE | 16.7% | 71880 | 63 | moy 4.384 · méd 4.2 · min/max 2.2/9.4 · p10/p90 3.1/5.9 |  |
| `opp_season_att_rating` | DOUBLE | 24.8% | 64950 | 59 | moy 6.813 · méd 6.8 · min/max 6.44/7.14 · p10/p90 6.56/7.13 |  |
| `opp_season_def_rating` | DOUBLE | 24.8% | 64950 | 59 | moy 6.813 · méd 6.8 · min/max 6.44/7.14 · p10/p90 6.56/7.13 |  |
| `opp_ws_dribbles_pg` | DOUBLE | 16.7% | 71880 | 95 | moy 8.286 · méd 8.1 · min/max 4.1/15.5 · p10/p90 5.9/10.8 |  |
| `opp_ws_shots_ot_pg` | DOUBLE | 16.7% | 71880 | 63 | moy 4.384 · méd 4.2 · min/max 2.2/9.4 · p10/p90 3.1/5.9 |  |
| `opp_odds_pinnacle` | DOUBLE | 82.7% | 14905 | 1685 | moy 4.459 · méd 2.74 · min/max 1.05/42.94 · p10/p90 1.31/10.47 |  |
| `opp_pinnacle_prob` | DOUBLE | 82.7% | 14905 | 29516 | moy 0.391 · méd 0.355 · min/max 0.023/0.925 · p10/p90 0.093/0.746 |  |
| `opp_market_prob` | DOUBLE | 36.3% | 54957 | 22674 | moy 0.386 · méd 0.356 · min/max 0.025/0.916 · p10/p90 0.11/0.718 |  |
| `np_xg_roll_3` | DOUBLE | 78.6% | 18516 | 30886 | moy 1.317 · méd 1.209 · min/max 0.031/4.893 · p10/p90 0.63/2.148 |  |
| `np_xg_roll_venue_3` | DOUBLE | 77.9% | 19112 | 30294 | moy 1.277 · méd 1.211 · min/max 0.014/5.934 · p10/p90 0.416/2.073 |  |
| `np_xg_conceded_roll_3` | DOUBLE | 78.6% | 18516 | 30880 | moy 1.29 · méd 1.273 · min/max 0.031/4.68 · p10/p90 0.506/2.223 |  |
| `xg_net_roll_3` | DOUBLE | 78.6% | 18516 | 30963 | moy 0.027 · méd -0.141 · min/max -4.195/4.195 · p10/p90 -1.141/1.201 |  |
| `shot_quality_ratio_3` | DOUBLE | 78.5% | 18592 | 30891 | moy 0.108 · méd 0.099 · min/max 0.015/0.446 · p10/p90 0.07/0.179 |  |
| `shot_quality_ratio_venue_3` | DOUBLE | 77.8% | 19182 | 30299 | moy 0.099 · méd 0.094 · min/max 0.01/0.489 · p10/p90 0.052/0.15 |  |
| `shot_accuracy_roll_3` | DOUBLE | 89.5% | 9053 | 927 | moy 0.376 · méd 0.371 · min/max 0.0/1.0 · p10/p90 0.256/0.516 |  |
| `save_rate_roll_3` | DOUBLE | 90.2% | 8428 | 227 | moy 0.686 · méd 0.692 · min/max 0.0/2.0 · p10/p90 0.5/0.909 |  |
| `poss_roll_3` | DOUBLE | 90.4% | 8263 | 235 | moy 51.916 · méd 52.0 · min/max 20.0/100.0 · p10/p90 36.667/64.667 |  |
| `poss_roll_venue_3` | DOUBLE | 88.4% | 10052 | 240 | moy 51.148 · méd 51.333 · min/max 17.0/100.0 · p10/p90 35.667/68.0 |  |
| `ppda_roll_3` | DOUBLE | 78.6% | 18516 | 30353 | moy 12.302 · méd 11.257 · min/max 2.538/85.561 · p10/p90 6.128/18.73 |  |
| `ppda_allowed_roll_3` | DOUBLE | 78.6% | 18516 | 30380 | moy 12.738 · méd 12.739 · min/max 2.538/76.636 · p10/p90 6.085/19.125 |  |
| `ppda_ratio_roll_3` | DOUBLE | 78.6% | 18516 | 30974 | moy 1.209 · méd 0.919 · min/max 0.077/13.003 · p10/p90 0.468/2.488 |  |
| `defensive_actions_roll_3` | DOUBLE | 91.4% | 7431 | 163 | moy 19.998 · méd 20.0 · min/max 0.0/52.0 · p10/p90 13.667/27.333 |  |
| `fouls_per_tackle_roll_3` | DOUBLE | 89.4% | 9184 | 1608 | moy 1.258 · méd 1.172 · min/max 0.0/19.0 · p10/p90 0.794/1.762 |  |
| `xg_overperformance_3` | DOUBLE | 78.6% | 18516 | 29804 | moy 0.146 · méd 0.118 · min/max -1.0/10.119 · p10/p90 -0.501/0.813 |  |
| `red_card_rate_roll_3` | DOUBLE | 92.0% | 6934 | 5 | moy 0.073 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.333 |  |
| `sterility_index_3` | DOUBLE | 78.5% | 18592 | 30902 | moy 528.67 · méd 536.223 · min/max 51.592/2686.456 · p10/p90 315.97/743.132 |  |
| `shots_faced_per_goal_conceded_3` | DOUBLE | 88.6% | 9864 | 240 | moy 3.226 · méd 2.667 · min/max 0.0/33.0 · p10/p90 1.75/5.0 |  |
| `sterility_weighted_3` | DOUBLE | 78.5% | 18592 | 30902 | moy 557.858 · méd 550.368 · min/max 23.733/3707.309 · p10/p90 300.026/883.092 |  |
| `press_resistance_3` | DOUBLE | 78.5% | 18592 | 30821 | moy 4.385 · méd 4.303 · min/max 0.853/11.345 · p10/p90 2.952/5.547 |  |
| `shield_efficiency_3` | DOUBLE | 78.4% | 18626 | 30795 | moy 32.98 · méd 28.722 · min/max -17.712/96.999 · p10/p90 16.969/49.452 |  |
| `roll_save_pct_3` | DOUBLE | 90.2% | 8428 | 1503 | moy 68.92 · méd 68.9 · min/max -100.0/100.0 · p10/p90 48.9/92.85 |  |
| `roll_sota_3` | DOUBLE | 91.4% | 7431 | 49 | moy 3.878 · méd 3.667 · min/max 0.0/15.0 · p10/p90 1.667/6.667 |  |
| `win_rate_roll_3` | DOUBLE | 92.0% | 6934 | 5 | moy 0.414 · méd 0.333 · min/max 0.0/1.0 · p10/p90 0.0/1.0 |  |
| `points_pg_roll_3` | DOUBLE | 92.0% | 6934 | 11 | moy 1.451 · méd 1.333 · min/max 0.0/3.0 · p10/p90 0.333/3.0 |  |
| `opp_np_xg_3` | DOUBLE | 78.6% | 18516 | 30886 | moy 1.317 · méd 1.209 · min/max 0.031/4.893 · p10/p90 0.63/2.148 |  |
| `opp_np_xg_venue_3` | DOUBLE | 77.9% | 19112 | 30294 | moy 1.277 · méd 1.211 · min/max 0.014/5.934 · p10/p90 0.416/2.073 |  |
| `opp_np_xg_conceded_3` | DOUBLE | 78.6% | 18516 | 30880 | moy 1.29 · méd 1.273 · min/max 0.031/4.68 · p10/p90 0.506/2.223 |  |
| `opp_xg_net_3` | DOUBLE | 78.6% | 18516 | 30963 | moy 0.027 · méd -0.141 · min/max -4.195/4.195 · p10/p90 -1.141/1.201 |  |
| `opp_sqr_3` | DOUBLE | 78.5% | 18592 | 30891 | moy 0.108 · méd 0.099 · min/max 0.015/0.446 · p10/p90 0.07/0.179 |  |
| `opp_shot_accuracy_3` | DOUBLE | 81.0% | 16369 | 882 | moy 0.378 · méd 0.375 · min/max 0.0/1.0 · p10/p90 0.256/0.516 |  |
| `opp_ppda_3` | DOUBLE | 78.6% | 18516 | 30353 | moy 12.302 · méd 11.257 · min/max 2.538/85.561 · p10/p90 6.128/18.73 |  |
| `opp_ppda_allowed_3` | DOUBLE | 78.6% | 18516 | 30380 | moy 12.738 · méd 12.739 · min/max 2.538/76.636 · p10/p90 6.085/19.125 |  |
| `opp_ppda_ratio_3` | DOUBLE | 78.6% | 18516 | 30974 | moy 1.209 · méd 0.919 · min/max 0.077/13.003 · p10/p90 0.468/2.488 |  |
| `opp_defensive_actions_3` | DOUBLE | 81.5% | 15951 | 155 | moy 20.4 · méd 20.0 · min/max 0.0/52.0 · p10/p90 14.0/27.333 |  |
| `opp_xg_opi_3` | DOUBLE | 78.6% | 18516 | 29804 | moy 0.146 · méd 0.118 · min/max -1.0/10.119 · p10/p90 -0.501/0.813 |  |
| `opp_save_rate_3` | DOUBLE | 81.1% | 16293 | 227 | moy 0.684 · méd 0.692 · min/max 0.0/2.0 · p10/p90 0.5/0.909 |  |
| `opp_red_card_rate_3` | DOUBLE | 82.1% | 15488 | 5 | moy 0.071 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.333 |  |
| `opp_win_rate_3` | DOUBLE | 82.1% | 15488 | 5 | moy 0.401 · méd 0.333 · min/max 0.0/1.0 · p10/p90 0.0/1.0 |  |
| `opp_points_pg_3` | DOUBLE | 82.1% | 15488 | 11 | moy 1.406 · méd 1.333 · min/max 0.0/3.0 · p10/p90 0.333/3.0 |  |
| `xg_net_diff_3` | DOUBLE | 76.1% | 20596 | 30908 | moy -0.0 · méd 0.0 · min/max -6.15/6.15 · p10/p90 -2.262/2.262 |  |
| `sqr_diff_3` | DOUBLE | 76.1% | 20670 | 30890 | moy 0.0 · méd 0.0 · min/max -0.41/0.41 · p10/p90 -0.073/0.073 |  |
| `ppda_diff_3` | DOUBLE | 76.1% | 20596 | 30955 | moy 0.0 · méd 0.0 · min/max -75.119/75.119 · p10/p90 -8.101/8.101 |  |
| `ppda_ratio_diff_3` | DOUBLE | 76.1% | 20596 | 30964 | moy 0.0 · méd 0.0 · min/max -12.624/12.624 · p10/p90 -1.542/1.542 |  |
| `xg_opi_diff_3` | DOUBLE | 76.1% | 20596 | 30813 | moy 0.0 · méd 0.0 · min/max -11.119/11.119 · p10/p90 -0.742/0.742 |  |
| `save_rate_diff_3` | DOUBLE | 78.5% | 18529 | 5248 | moy -0.0 · méd 0.0 · min/max -1.375/1.375 · p10/p90 -0.25/0.25 |  |
| `defensive_actions_diff_3` | DOUBLE | 79.0% | 18162 | 477 | moy -0.003 · méd 0.0 · min/max -34.0/34.0 · p10/p90 -6.0/6.0 |  |
| `keeper_form_diff_3` | DOUBLE | 78.5% | 18529 | 8236 | moy -0.005 · méd 0.0 · min/max -125.017/125.017 · p10/p90 -21.667/21.667 |  |
| `red_card_rate_diff_3` | DOUBLE | 79.5% | 17694 | 13 | moy -0.0 · méd 0.0 · min/max -1.0/1.0 · p10/p90 -0.333/0.333 |  |
| `sterility_diff_3` | DOUBLE | 76.1% | 20670 | 30890 | moy -0.0 · méd 0.0 · min/max -2265.091/2265.091 · p10/p90 -247.545/247.545 |  |
| `shots_faced_per_goal_conceded_diff_3` | DOUBLE | 75.9% | 20824 | 3895 | moy -0.0 · méd 0.0 · min/max -24.0/24.0 · p10/p90 -2.4/2.4 |  |
| `sterility_weighted_diff_3` | DOUBLE | 76.1% | 20670 | 30890 | moy -0.0 · méd 0.0 · min/max -3328.081/3328.081 · p10/p90 -444.426/444.426 |  |
| `press_resistance_diff_3` | DOUBLE | 76.1% | 20670 | 30890 | moy 0.0 · méd 0.0 · min/max -7.082/7.082 · p10/p90 -1.831/1.831 |  |
| `shield_efficiency_diff_3` | DOUBLE | 76.0% | 20738 | 30821 | moy 0.0 · méd 0.0 · min/max -77.785/77.785 · p10/p90 -23.664/23.664 |  |
| `win_rate_diff_3` | DOUBLE | 79.5% | 17694 | 17 | moy -0.0 · méd 0.0 · min/max -1.0/1.0 · p10/p90 -0.667/0.667 |  |
| `points_pg_diff_3` | DOUBLE | 79.5% | 17694 | 47 | moy -0.001 · méd 0.0 · min/max -3.0/3.0 · p10/p90 -2.0/2.0 |  |
| `np_xg_roll_5` | DOUBLE | 78.8% | 18324 | 31125 | moy 1.293 · méd 1.146 · min/max 0.031/4.68 · p10/p90 0.68/2.179 |  |
| `np_xg_roll_venue_5` | DOUBLE | 78.0% | 19022 | 30432 | moy 1.363 · méd 1.302 · min/max 0.014/4.937 · p10/p90 0.606/2.183 |  |
| `np_xg_conceded_roll_5` | DOUBLE | 78.8% | 18324 | 31125 | moy 1.321 · méd 1.368 · min/max 0.031/4.68 · p10/p90 0.724/1.886 |  |
| `xg_net_roll_5` | DOUBLE | 78.8% | 18324 | 31163 | moy -0.028 · méd -0.164 · min/max -4.195/4.195 · p10/p90 -1.078/1.084 |  |
| `shot_quality_ratio_5` | DOUBLE | 78.7% | 18400 | 31087 | moy 0.106 · méd 0.096 · min/max 0.015/0.446 · p10/p90 0.069/0.146 |  |
| `shot_quality_ratio_venue_5` | DOUBLE | 78.0% | 19022 | 30464 | moy 0.104 · méd 0.096 · min/max 0.01/0.48 · p10/p90 0.069/0.154 |  |
| `shot_accuracy_roll_5` | DOUBLE | 89.7% | 8859 | 1827 | moy 0.377 · méd 0.376 · min/max 0.0/1.0 · p10/p90 0.27/0.47 |  |
| `save_rate_roll_5` | DOUBLE | 90.5% | 8214 | 438 | moy 0.698 · méd 0.706 · min/max 0.0/2.0 · p10/p90 0.533/0.875 |  |
| `poss_roll_5` | DOUBLE | 90.7% | 8069 | 506 | moy 51.399 · méd 51.0 · min/max 20.0/100.0 · p10/p90 38.4/66.6 |  |
| `poss_roll_venue_5` | DOUBLE | 88.5% | 9889 | 507 | moy 51.551 · méd 50.8 · min/max 17.0/100.0 · p10/p90 38.6/67.0 |  |
| `ppda_roll_5` | DOUBLE | 78.8% | 18324 | 30977 | moy 12.16 · méd 11.737 · min/max 2.538/71.0 · p10/p90 7.023/18.025 |  |
| `ppda_allowed_roll_5` | DOUBLE | 78.8% | 18324 | 30979 | moy 12.508 · méd 12.018 · min/max 2.538/71.0 · p10/p90 6.216/18.475 |  |
| `ppda_ratio_roll_5` | DOUBLE | 78.8% | 18324 | 31166 | moy 1.191 · méd 0.942 · min/max 0.077/13.003 · p10/p90 0.485/2.488 |  |
| `defensive_actions_roll_5` | DOUBLE | 91.6% | 7237 | 339 | moy 20.453 · méd 20.0 · min/max 0.0/52.0 · p10/p90 14.4/27.8 |  |
| `fouls_per_tackle_roll_5` | DOUBLE | 89.6% | 8990 | 3317 | moy 1.22 · méd 1.178 · min/max 0.0/19.0 · p10/p90 0.815/1.643 |  |
| `xg_overperformance_5` | DOUBLE | 78.8% | 18324 | 30628 | moy 0.206 · méd 0.215 · min/max -1.0/10.119 · p10/p90 -0.311/0.664 |  |
| `red_card_rate_roll_5` | DOUBLE | 92.0% | 6934 | 11 | moy 0.068 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.2 |  |
| `sterility_index_5` | DOUBLE | 78.7% | 18400 | 31094 | moy 521.905 · méd 512.184 · min/max 51.592/2686.456 · p10/p90 373.989/727.204 |  |
| `shots_faced_per_goal_conceded_5` | DOUBLE | 89.8% | 8814 | 516 | moy 3.567 · méd 3.143 · min/max 0.0/55.0 · p10/p90 2.0/6.0 |  |
| `sterility_weighted_5` | DOUBLE | 78.7% | 18400 | 31094 | moy 538.091 · méd 534.628 · min/max 23.733/3707.309 · p10/p90 334.479/729.494 |  |
| `press_resistance_5` | DOUBLE | 78.7% | 18400 | 31058 | moy 4.428 · méd 4.23 · min/max 0.853/11.345 · p10/p90 3.049/5.758 |  |
| `shield_efficiency_5` | DOUBLE | 78.7% | 18432 | 31012 | moy 32.515 · méd 30.338 · min/max -2.374/96.999 · p10/p90 19.646/45.8 |  |
| `roll_save_pct_5` | DOUBLE | 90.5% | 8214 | 3890 | moy 70.336 · méd 72.5 · min/max -100.0/100.0 · p10/p90 52.5/88.9 |  |
| `roll_sota_5` | DOUBLE | 91.6% | 7237 | 102 | moy 4.053 · méd 3.8 · min/max 0.0/15.0 · p10/p90 2.0/6.4 |  |
| `win_rate_roll_5` | DOUBLE | 92.0% | 6934 | 11 | moy 0.422 · méd 0.4 · min/max 0.0/1.0 · p10/p90 0.0/1.0 |  |
| `points_pg_roll_5` | DOUBLE | 92.0% | 6934 | 28 | moy 1.485 · méd 1.4 · min/max 0.0/3.0 · p10/p90 0.4/3.0 |  |
| `opp_np_xg_5` | DOUBLE | 78.8% | 18324 | 31125 | moy 1.293 · méd 1.146 · min/max 0.031/4.68 · p10/p90 0.68/2.179 |  |
| `opp_np_xg_venue_5` | DOUBLE | 78.0% | 19022 | 30432 | moy 1.363 · méd 1.302 · min/max 0.014/4.937 · p10/p90 0.606/2.183 |  |
| `opp_np_xg_conceded_5` | DOUBLE | 78.8% | 18324 | 31125 | moy 1.321 · méd 1.368 · min/max 0.031/4.68 · p10/p90 0.724/1.886 |  |
| `opp_xg_net_5` | DOUBLE | 78.8% | 18324 | 31163 | moy -0.028 · méd -0.164 · min/max -4.195/4.195 · p10/p90 -1.078/1.084 |  |
| `opp_sqr_5` | DOUBLE | 78.7% | 18400 | 31087 | moy 0.106 · méd 0.096 · min/max 0.015/0.446 · p10/p90 0.069/0.146 |  |
| `opp_shot_accuracy_5` | DOUBLE | 81.3% | 16177 | 1721 | moy 0.379 · méd 0.376 · min/max 0.0/1.0 · p10/p90 0.273/0.47 |  |
| `opp_ppda_5` | DOUBLE | 78.8% | 18324 | 30977 | moy 12.16 · méd 11.737 · min/max 2.538/71.0 · p10/p90 7.023/18.025 |  |
| `opp_ppda_allowed_5` | DOUBLE | 78.8% | 18324 | 30979 | moy 12.508 · méd 12.018 · min/max 2.538/71.0 · p10/p90 6.216/18.475 |  |
| `opp_ppda_ratio_5` | DOUBLE | 78.8% | 18324 | 31166 | moy 1.191 · méd 0.942 · min/max 0.077/13.003 · p10/p90 0.485/2.488 |  |
| `opp_defensive_actions_5` | DOUBLE | 81.7% | 15759 | 315 | moy 20.915 · méd 20.4 · min/max 0.0/52.0 · p10/p90 14.6/29.2 |  |
| `opp_xg_opi_5` | DOUBLE | 78.8% | 18324 | 30628 | moy 0.206 · méd 0.215 · min/max -1.0/10.119 · p10/p90 -0.311/0.664 |  |
| `opp_save_rate_5` | DOUBLE | 81.4% | 16094 | 434 | moy 0.697 · méd 0.706 · min/max 0.0/2.0 · p10/p90 0.533/0.87 |  |
| `opp_red_card_rate_5` | DOUBLE | 82.1% | 15488 | 11 | moy 0.066 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.2 |  |
| `opp_win_rate_5` | DOUBLE | 82.1% | 15488 | 11 | moy 0.409 · méd 0.4 · min/max 0.0/1.0 · p10/p90 0.0/1.0 |  |
| `opp_points_pg_5` | DOUBLE | 82.1% | 15488 | 28 | moy 1.443 · méd 1.2 · min/max 0.0/3.0 · p10/p90 0.4/3.0 |  |
| `xg_net_diff_5` | DOUBLE | 76.4% | 20402 | 31112 | moy -0.0 · méd 0.0 · min/max -6.15/6.15 · p10/p90 -2.056/2.056 |  |
| `sqr_diff_5` | DOUBLE | 76.3% | 20480 | 31080 | moy 0.0 · méd 0.0 · min/max -0.41/0.41 · p10/p90 -0.061/0.061 |  |
| `ppda_diff_5` | DOUBLE | 76.4% | 20402 | 31152 | moy 0.0 · méd 0.0 · min/max -54.889/54.889 · p10/p90 -8.101/8.101 |  |
| `ppda_ratio_diff_5` | DOUBLE | 76.4% | 20402 | 31158 | moy -0.0 · méd 0.0 · min/max -12.624/12.624 · p10/p90 -1.338/1.338 |  |
| `xg_opi_diff_5` | DOUBLE | 76.4% | 20402 | 31017 | moy -0.0 · méd 0.0 · min/max -11.119/11.119 · p10/p90 -0.561/0.561 |  |
| `save_rate_diff_5` | DOUBLE | 78.8% | 18325 | 11703 | moy -0.0 · méd 0.0 · min/max -1.286/1.286 · p10/p90 -0.278/0.278 |  |
| `defensive_actions_diff_5` | DOUBLE | 79.2% | 17972 | 917 | moy -0.003 · méd 0.0 · min/max -34.0/34.0 · p10/p90 -5.8/5.8 |  |
| `keeper_form_diff_5` | DOUBLE | 78.8% | 18325 | 14046 | moy -0.004 · méd 0.0 · min/max -100.0/100.0 · p10/p90 -22.68/22.68 |  |
| `red_card_rate_diff_5` | DOUBLE | 79.5% | 17694 | 41 | moy 0.0 · méd 0.0 · min/max -1.0/1.0 · p10/p90 -0.2/0.2 |  |
| `sterility_diff_5` | DOUBLE | 76.3% | 20480 | 31080 | moy -0.0 · méd 0.0 · min/max -2265.091/2265.091 · p10/p90 -214.625/214.625 |  |
| `shots_faced_per_goal_conceded_diff_5` | DOUBLE | 77.5% | 19465 | 8194 | moy -0.0 · méd 0.0 · min/max -45.0/45.0 · p10/p90 -2.889/2.889 |  |
| `sterility_weighted_diff_5` | DOUBLE | 76.3% | 20480 | 31080 | moy -0.0 · méd 0.0 · min/max -3328.081/3328.081 · p10/p90 -302.355/302.355 |  |
| `press_resistance_diff_5` | DOUBLE | 76.3% | 20480 | 31080 | moy -0.0 · méd 0.0 · min/max -6.925/6.925 · p10/p90 -1.868/1.868 |  |
| `shield_efficiency_diff_5` | DOUBLE | 76.2% | 20542 | 31017 | moy 0.0 · méd 0.0 · min/max -77.785/77.785 · p10/p90 -18.877/18.877 |  |
| `win_rate_diff_5` | DOUBLE | 79.5% | 17694 | 67 | moy -0.0 · méd 0.0 · min/max -1.0/1.0 · p10/p90 -0.6/0.6 |  |
| `points_pg_diff_5` | DOUBLE | 79.5% | 17694 | 175 | moy -0.001 · méd 0.0 · min/max -3.0/3.0 · p10/p90 -1.6/1.6 |  |
| `np_xg_roll_10` | DOUBLE | 79.0% | 18144 | 31328 | moy 1.404 · méd 1.296 · min/max 0.031/4.68 · p10/p90 0.784/2.246 |  |
| `np_xg_roll_venue_10` | DOUBLE | 78.0% | 19022 | 30455 | moy 1.43 · méd 1.298 · min/max 0.014/4.937 · p10/p90 0.773/2.484 |  |
| `np_xg_conceded_roll_10` | DOUBLE | 79.0% | 18144 | 31327 | moy 1.265 · méd 1.314 · min/max 0.031/4.68 · p10/p90 0.792/1.698 |  |
| `xg_net_roll_10` | DOUBLE | 79.0% | 18144 | 31343 | moy 0.139 · méd -0.059 · min/max -4.195/4.195 · p10/p90 -0.803/1.372 |  |
| `shot_quality_ratio_10` | DOUBLE | 79.0% | 18144 | 31347 | moy 0.11 · méd 0.108 · min/max 0.015/0.446 · p10/p90 0.071/0.15 |  |
| `shot_quality_ratio_venue_10` | DOUBLE | 78.0% | 19022 | 30467 | moy 0.106 · méd 0.099 · min/max 0.01/0.48 · p10/p90 0.078/0.14 |  |
| `shot_accuracy_roll_10` | DOUBLE | 90.0% | 8599 | 4190 | moy 0.375 · méd 0.37 · min/max 0.0/1.0 · p10/p90 0.296/0.459 |  |
| `save_rate_roll_10` | DOUBLE | 90.8% | 7945 | 1032 | moy 0.704 · méd 0.711 · min/max 0.0/2.0 · p10/p90 0.585/0.844 |  |
| `poss_roll_10` | DOUBLE | 91.0% | 7809 | 1264 | moy 51.949 · méd 50.8 · min/max 20.0/100.0 · p10/p90 41.4/65.3 |  |
| `poss_roll_venue_10` | DOUBLE | 88.5% | 9889 | 1291 | moy 52.119 · méd 51.1 · min/max 17.0/100.0 · p10/p90 40.4/66.8 |  |
| `ppda_roll_10` | DOUBLE | 79.0% | 18144 | 31215 | moy 11.915 · méd 11.517 · min/max 2.538/71.0 · p10/p90 6.892/17.304 |  |
| `ppda_allowed_roll_10` | DOUBLE | 79.0% | 18144 | 31207 | moy 12.97 · méd 11.642 · min/max 2.538/71.0 · p10/p90 7.707/19.043 |  |
| `ppda_ratio_roll_10` | DOUBLE | 79.0% | 18144 | 31348 | moy 1.087 · méd 1.033 · min/max 0.077/13.003 · p10/p90 0.412/1.831 |  |
| `defensive_actions_roll_10` | DOUBLE | 91.9% | 6977 | 806 | moy 20.768 · méd 20.2 · min/max 0.0/52.0 · p10/p90 15.6/28.1 |  |
| `fouls_per_tackle_roll_10` | DOUBLE | 89.9% | 8730 | 7961 | moy 1.177 · méd 1.181 · min/max 0.0/19.0 · p10/p90 0.836/1.517 |  |
| `xg_overperformance_10` | DOUBLE | 79.0% | 18144 | 30970 | moy 0.134 · méd 0.094 · min/max -1.0/10.119 · p10/p90 -0.215/0.573 |  |
| `red_card_rate_roll_10` | DOUBLE | 92.0% | 6934 | 25 | moy 0.068 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.2 |  |
| `sterility_index_10` | DOUBLE | 79.0% | 18144 | 31350 | moy 499.366 · méd 480.372 · min/max 51.592/2686.456 · p10/p90 394.051/621.805 |  |
| `shots_faced_per_goal_conceded_10` | DOUBLE | 90.3% | 8401 | 1337 | moy 3.506 · méd 3.0 · min/max 0.0/95.0 · p10/p90 2.231/5.25 |  |
| `sterility_weighted_10` | DOUBLE | 79.0% | 18144 | 31350 | moy 520.998 · méd 520.195 · min/max 23.733/3707.309 · p10/p90 355.995/669.341 |  |
| `press_resistance_10` | DOUBLE | 79.0% | 18144 | 31321 | moy 4.334 · méd 4.373 · min/max 0.853/11.345 · p10/p90 3.143/5.538 |  |
| `shield_efficiency_10` | DOUBLE | 79.0% | 18172 | 31278 | moy 33.263 · méd 31.52 · min/max 0.0/96.999 · p10/p90 21.377/43.602 |  |
| `roll_save_pct_10` | DOUBLE | 90.8% | 7945 | 10330 | moy 70.85 · méd 70.678 · min/max -50.0/100.0 · p10/p90 58.332/85.425 |  |
| `roll_sota_10` | DOUBLE | 91.9% | 6977 | 258 | moy 4.019 · méd 4.0 · min/max 0.0/15.0 · p10/p90 2.2/6.0 |  |
| `win_rate_roll_10` | DOUBLE | 92.0% | 6934 | 33 | moy 0.436 · méd 0.4 · min/max 0.0/1.0 · p10/p90 0.1/1.0 |  |
| `points_pg_roll_10` | DOUBLE | 92.0% | 6934 | 92 | moy 1.535 · méd 1.3 · min/max 0.0/3.0 · p10/p90 0.7/3.0 |  |
| `opp_np_xg_10` | DOUBLE | 79.0% | 18144 | 31328 | moy 1.404 · méd 1.296 · min/max 0.031/4.68 · p10/p90 0.784/2.246 |  |
| `opp_np_xg_venue_10` | DOUBLE | 78.0% | 19022 | 30455 | moy 1.43 · méd 1.298 · min/max 0.014/4.937 · p10/p90 0.773/2.484 |  |
| `opp_np_xg_conceded_10` | DOUBLE | 79.0% | 18144 | 31327 | moy 1.265 · méd 1.314 · min/max 0.031/4.68 · p10/p90 0.792/1.698 |  |
| `opp_xg_net_10` | DOUBLE | 79.0% | 18144 | 31343 | moy 0.139 · méd -0.059 · min/max -4.195/4.195 · p10/p90 -0.803/1.372 |  |
| `opp_sqr_10` | DOUBLE | 79.0% | 18144 | 31347 | moy 0.11 · méd 0.108 · min/max 0.015/0.446 · p10/p90 0.071/0.15 |  |
| `opp_shot_accuracy_10` | DOUBLE | 81.6% | 15921 | 3931 | moy 0.377 · méd 0.37 · min/max 0.0/1.0 · p10/p90 0.3/0.459 |  |
| `opp_ppda_10` | DOUBLE | 79.0% | 18144 | 31215 | moy 11.915 · méd 11.517 · min/max 2.538/71.0 · p10/p90 6.892/17.304 |  |
| `opp_ppda_allowed_10` | DOUBLE | 79.0% | 18144 | 31207 | moy 12.97 · méd 11.642 · min/max 2.538/71.0 · p10/p90 7.707/19.043 |  |
| `opp_ppda_ratio_10` | DOUBLE | 79.0% | 18144 | 31348 | moy 1.087 · méd 1.033 · min/max 0.077/13.003 · p10/p90 0.412/1.831 |  |
| `opp_defensive_actions_10` | DOUBLE | 82.0% | 15503 | 735 | moy 21.274 · méd 20.25 · min/max 0.0/52.0 · p10/p90 15.6/28.7 |  |
| `opp_xg_opi_10` | DOUBLE | 79.0% | 18144 | 30970 | moy 0.134 · méd 0.094 · min/max -1.0/10.119 · p10/p90 -0.215/0.573 |  |
| `opp_save_rate_10` | DOUBLE | 81.7% | 15832 | 1008 | moy 0.703 · méd 0.706 · min/max 0.0/2.0 · p10/p90 0.585/0.833 |  |
| `opp_red_card_rate_10` | DOUBLE | 82.1% | 15488 | 24 | moy 0.066 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.2 |  |
| `opp_win_rate_10` | DOUBLE | 82.1% | 15488 | 33 | moy 0.424 · méd 0.3 · min/max 0.0/1.0 · p10/p90 0.1/1.0 |  |
| `opp_points_pg_10` | DOUBLE | 82.1% | 15488 | 92 | moy 1.499 · méd 1.3 · min/max 0.0/3.0 · p10/p90 0.7/3.0 |  |
| `xg_net_diff_10` | DOUBLE | 76.6% | 20218 | 31296 | moy 0.0 · méd 0.0 · min/max -6.15/6.15 · p10/p90 -1.92/1.92 |  |
| `sqr_diff_10` | DOUBLE | 76.6% | 20218 | 31342 | moy 0.0 · méd 0.0 · min/max -0.41/0.41 · p10/p90 -0.057/0.057 |  |
| `ppda_diff_10` | DOUBLE | 76.6% | 20218 | 31338 | moy -0.0 · méd 0.0 · min/max -54.889/54.889 · p10/p90 -6.867/6.867 |  |
| `ppda_ratio_diff_10` | DOUBLE | 76.6% | 20218 | 31342 | moy 0.0 · méd 0.0 · min/max -12.624/12.624 · p10/p90 -1.173/1.173 |  |
| `xg_opi_diff_10` | DOUBLE | 76.6% | 20218 | 31283 | moy 0.0 · méd 0.0 · min/max -11.119/11.119 · p10/p90 -0.475/0.475 |  |
| `save_rate_diff_10` | DOUBLE | 79.1% | 18053 | 21416 | moy -0.0 · méd 0.0 · min/max -1.286/1.286 · p10/p90 -0.179/0.179 |  |
| `defensive_actions_diff_10` | DOUBLE | 79.5% | 17710 | 1981 | moy -0.004 · méd 0.0 · min/max -34.0/34.0 · p10/p90 -4.4/4.4 |  |
| `keeper_form_diff_10` | DOUBLE | 79.1% | 18053 | 23431 | moy -0.003 · méd 0.0 · min/max -100.0/100.0 · p10/p90 -18.796/18.796 |  |
| `red_card_rate_diff_10` | DOUBLE | 79.5% | 17694 | 125 | moy 0.0 · méd 0.0 · min/max -1.0/1.0 · p10/p90 -0.1/0.1 |  |
| `sterility_diff_10` | DOUBLE | 76.6% | 20218 | 31342 | moy -0.0 · méd 0.0 · min/max -2265.091/2265.091 · p10/p90 -172.181/172.181 |  |
| `shots_faced_per_goal_conceded_diff_10` | DOUBLE | 78.0% | 18980 | 17094 | moy 0.0 · méd 0.0 · min/max -85.0/85.0 · p10/p90 -2.563/2.563 |  |
| `sterility_weighted_diff_10` | DOUBLE | 76.6% | 20218 | 31342 | moy -0.0 · méd 0.0 · min/max -3328.081/3328.081 · p10/p90 -215.396/215.396 |  |
| `press_resistance_diff_10` | DOUBLE | 76.6% | 20218 | 31342 | moy -0.0 · méd 0.0 · min/max -6.925/6.925 · p10/p90 -2.097/2.097 |  |
| `shield_efficiency_diff_10` | DOUBLE | 76.5% | 20274 | 31285 | moy 0.0 · méd 0.0 · min/max -77.785/77.785 · p10/p90 -17.837/17.837 |  |
| `win_rate_diff_10` | DOUBLE | 79.5% | 17694 | 318 | moy -0.0 · méd 0.0 · min/max -1.0/1.0 · p10/p90 -0.6/0.6 |  |
| `points_pg_diff_10` | DOUBLE | 79.5% | 17694 | 736 | moy -0.001 · méd 0.0 · min/max -3.0/3.0 · p10/p90 -1.6/1.6 |  |
| `xg_net_diff` | DOUBLE | 76.4% | 20402 | 31112 | moy -0.0 · méd 0.0 · min/max -6.15/6.15 · p10/p90 -2.056/2.056 | Différentiel xg_net W=5 équipe vs adversaire |
| `tactical_advantage` | DOUBLE | 23.4% | 66142 | 205 | moy 0.0 · méd 0.0 · min/max -0.6/0.6 · p10/p90 -0.26/0.26 | Écart rating offensif équipe vs rating défensif adversaire |
| `ws_dribble_style_diff` | DOUBLE | 16.7% | 71880 | 457 | moy 0.0 · méd 0.0 · min/max -10.4/10.4 · p10/p90 -2.8/2.8 |  |
| `ws_fouled_diff` | DOUBLE | 16.7% | 71880 | 271 | moy -0.0 · méd 0.0 · min/max -6.9/6.9 · p10/p90 -2.4/2.4 |  |
| `sqr_diff` | DOUBLE | 76.3% | 20480 | 31080 | moy 0.0 · méd 0.0 · min/max -0.41/0.41 · p10/p90 -0.061/0.061 |  |
| `ppda_diff` | DOUBLE | 76.4% | 20402 | 31152 | moy 0.0 · méd 0.0 · min/max -54.889/54.889 · p10/p90 -8.101/8.101 |  |
| `ppda_ratio_diff` | DOUBLE | 76.4% | 20402 | 31158 | moy -0.0 · méd 0.0 · min/max -12.624/12.624 · p10/p90 -1.338/1.338 |  |
| `xg_opi_diff` | DOUBLE | 76.4% | 20402 | 31017 | moy -0.0 · méd 0.0 · min/max -11.119/11.119 · p10/p90 -0.561/0.561 |  |
| `save_rate_diff` | DOUBLE | 78.8% | 18325 | 11703 | moy -0.0 · méd 0.0 · min/max -1.286/1.286 · p10/p90 -0.278/0.278 |  |
| `defensive_actions_diff` | DOUBLE | 79.2% | 17972 | 917 | moy -0.003 · méd 0.0 · min/max -34.0/34.0 · p10/p90 -5.8/5.8 |  |
| `keeper_form_diff` | DOUBLE | 78.8% | 18325 | 14046 | moy -0.004 · méd 0.0 · min/max -100.0/100.0 · p10/p90 -22.68/22.68 |  |
| `red_card_rate_diff` | DOUBLE | 79.5% | 17694 | 41 | moy 0.0 · méd 0.0 · min/max -1.0/1.0 · p10/p90 -0.2/0.2 |  |
| `sterility_diff` | DOUBLE | 76.3% | 20480 | 31080 | moy -0.0 · méd 0.0 · min/max -2265.091/2265.091 · p10/p90 -214.625/214.625 |  |
| `press_resistance_diff` | DOUBLE | 76.3% | 20480 | 31080 | moy -0.0 · méd 0.0 · min/max -6.925/6.925 · p10/p90 -1.868/1.868 |  |
| `shield_efficiency_diff` | DOUBLE | 76.2% | 20542 | 31017 | moy 0.0 · méd 0.0 · min/max -77.785/77.785 · p10/p90 -18.877/18.877 |  |
| `rest_days_diff` | BIGINT | 83.5% | 14214 | 193 | moy 0.0 · méd 0.0 · min/max -1842.0/1842.0 · p10/p90 -5.0/5.0 |  |
| `draw_rate_diff` | DOUBLE | 79.5% | 17694 | 59 | moy 0.0 · méd 0.0 · min/max -1.0/1.0 · p10/p90 -0.4/0.4 |  |
| `draw_affinity` | DOUBLE | 79.5% | 17694 | 35 | moy 0.06 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.24 |  |
| `form_att_vs_def_gap` | DOUBLE | 86.8% | 11390 | 5 | moy -1.875 · méd -2.0 · min/max -4.0/0.0 · p10/p90 -3.0/-1.0 |  |
| `form_mid_dominance` | DOUBLE | 87.3% | 11000 | 7 | moy -0.001 · méd 0.0 · min/max -3.0/3.0 · p10/p90 -2.0/2.0 |  |
| `odds_pinnacle_team` | DOUBLE | 82.7% | 14905 | 1685 | moy 4.459 · méd 2.74 · min/max 1.05/42.94 · p10/p90 1.31/10.47 |  |
| `odds_pinnacle_draw` | DOUBLE | 82.7% | 14905 | 792 | moy 4.74 · méd 4.55 · min/max 2.08/20.38 · p10/p90 3.41/6.1 |  |
| `odds_pinnacle_opp` | DOUBLE | 82.7% | 14905 | 1685 | moy 4.459 · méd 2.74 · min/max 1.05/42.94 · p10/p90 1.31/10.47 |  |
| `odds_avg_team` | DOUBLE | 36.3% | 54957 | 1436 | moy 3.928 · méd 2.68 · min/max 1.05/38.04 · p10/p90 1.33/8.65 |  |
| `odds_avg_draw` | DOUBLE | 36.3% | 54957 | 672 | moy 4.458 · méd 4.03 · min/max 2.09/16.44 · p10/p90 3.25/5.56 |  |
| `odds_avg_opp` | DOUBLE | 36.3% | 54957 | 1436 | moy 3.928 · méd 2.68 · min/max 1.05/38.04 · p10/p90 1.33/8.65 |  |
| `pinnacle_prob_team` | DOUBLE | 82.7% | 14905 | 29516 | moy 0.391 · méd 0.355 · min/max 0.023/0.925 · p10/p90 0.093/0.746 |  |
| `pinnacle_prob_draw` | DOUBLE | 82.7% | 14905 | 14850 | moy 0.219 · méd 0.216 · min/max 0.047/0.467 · p10/p90 0.16/0.286 |  |
| `pinnacle_prob_opp` | DOUBLE | 82.7% | 14905 | 29516 | moy 0.391 · méd 0.355 · min/max 0.023/0.925 · p10/p90 0.093/0.746 |  |
| `market_prob_team` | DOUBLE | 36.3% | 54957 | 22674 | moy 0.386 · méd 0.356 · min/max 0.025/0.916 · p10/p90 0.11/0.718 |  |
| `market_prob_draw` | DOUBLE | 36.3% | 54957 | 11383 | moy 0.228 · méd 0.238 · min/max 0.059/0.456 · p10/p90 0.172/0.294 |  |
| `market_prob_opp` | DOUBLE | 36.3% | 54957 | 22674 | moy 0.386 · méd 0.356 · min/max 0.025/0.916 · p10/p90 0.11/0.718 |  |
| `pinnacle_edge` | DOUBLE | 82.7% | 14905 | 29652 | moy -0.0 · méd 0.0 · min/max -0.901/0.901 · p10/p90 -0.653/0.653 | Edge Pinnacle — prob_victoire - prob_défaite |
| `market_edge` | DOUBLE | 36.3% | 54957 | 22734 | moy -0.0 · méd 0.0 · min/max -0.891/0.891 · p10/p90 -0.608/0.608 | Edge marché moyen |
| `h2h_win_rate` | DOUBLE | 42.6% | 49542 | 29 | moy 0.418 · méd 0.333 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | Taux de victoire H2H sur les 10 derniers matchs |
| `h2h_draw_rate` | DOUBLE | 42.6% | 49542 | 26 | moy 0.198 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.5 | Taux de nul H2H sur les 10 derniers matchs |
| `h2h_goals_scored` | DOUBLE | 42.6% | 49542 | 142 | moy 1.22 · méd 1.0 · min/max 0.0/9.0 · p10/p90 0.0/2.333 |  |
| `h2h_goals_conceded` | DOUBLE | 42.6% | 49542 | 127 | moy 1.131 · méd 1.0 · min/max 0.0/9.0 · p10/p90 0.0/2.0 |  |
| `h2h_xg_diff` | DOUBLE | 42.6% | 49542 | 21560 | moy 0.089 · méd 0.022 · min/max -7.0/9.0 · p10/p90 -1.0/1.159 | Différentiel xG moyen en H2H |
| `h2h_n_matches` | BIGINT | 42.6% | 49542 | 10 | moy 17.252 · méd 5.0 · min/max 1.0/64.0 · p10/p90 1.0/64.0 | Nombre de matchs H2H disponibles |
| `league_draw_rate` | DOUBLE | 65.2% | 30054 | 169 | moy 0.238 · méd 0.229 · min/max 0.0/1.0 · p10/p90 0.218/0.271 | Taux de nul historique de la ligue (saisons précédentes) |
| `rating_ratio_att` | DOUBLE | 23.4% | 66142 | 2088 | moy 1.0 · méd 1.0 · min/max 0.916/1.092 · p10/p90 0.964/1.038 | Ratio rating offensif équipe / rating offensif adversaire (Giant Killer) |
| `rating_ratio_def` | DOUBLE | 23.4% | 66142 | 2088 | moy 1.0 · méd 1.0 · min/max 0.916/1.092 · p10/p90 0.964/1.038 | Ratio rating défensif équipe / rating défensif adversaire |
| `upset_risk_index` | DOUBLE | 23.4% | 66142 | 2089 | moy 0.508 · méd 0.458 · min/max 0.0/1.092 · p10/p90 0.0/1.038 | Index de surprise — GREATEST(ratio_att, ratio_def) × is_home |
| `ws_field_tilt_actions` | DOUBLE | 67.4% | 28176 | 15981 | moy 0.257 · méd 0.256 · min/max 0.0/0.654 · p10/p90 0.183/0.327 |  |
| `ws_high_turnover_rate` | DOUBLE | 67.4% | 28176 | 9338 | moy 0.072 · méd 0.071 · min/max 0.0/0.6 · p10/p90 0.044/0.112 |  |
| `ws_deep_completion_rt` | DOUBLE | 67.4% | 28176 | 8732 | moy 0.051 · méd 0.052 · min/max 0.0/0.2 · p10/p90 0.033/0.067 |  |
| `ws_momentum_delta` | DOUBLE | 49.1% | 43985 | 12036 | moy 1.099 · méd 0.938 · min/max 0.0/33.333 · p10/p90 0.719/1.57 |  |
| `ws_counter_shot_rate` | DOUBLE | 67.4% | 28185 | 81 | moy 0.025 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.091 |  |
| `ws_set_piece_pressure` | DOUBLE | 67.4% | 28177 | 2366 | moy 0.074 · méd 0.067 · min/max 0.0/0.333 · p10/p90 0.048/0.106 |  |
| `ws_attack_left_pct` | DOUBLE | 67.4% | 28176 | 13281 | moy 0.365 · méd 0.363 · min/max 0.0/0.667 · p10/p90 0.282/0.445 |  |
| `ws_attack_center_pct` | DOUBLE | 67.4% | 28176 | 12148 | moy 0.263 · méd 0.26 · min/max 0.0/0.484 · p10/p90 0.189/0.333 |  |
| `ws_attack_right_pct` | DOUBLE | 67.4% | 28176 | 13347 | moy 0.371 · méd 0.368 · min/max 0.0/1.0 · p10/p90 0.294/0.451 |  |
| `ws_zone_def_pct` | DOUBLE | 67.4% | 28176 | 16722 | moy 0.297 · méd 0.308 · min/max 0.0/0.684 · p10/p90 0.178/0.387 |  |
| `ws_zone_mid_pct` | DOUBLE | 67.4% | 28176 | 15584 | moy 0.454 · méd 0.45 · min/max 0.223/0.677 · p10/p90 0.387/0.531 |  |
| `ws_zone_att_pct` | DOUBLE | 67.4% | 28176 | 15858 | moy 0.249 · méd 0.252 · min/max 0.0/0.643 · p10/p90 0.176/0.318 |  |
| `ws_counter_attack_dna` | DOUBLE | 67.4% | 28185 | 201 | moy 0.22 · méd 0.217 · min/max 0.0/1.0 · p10/p90 0.045/0.4 |  |
| `ws_midfield_control_idx` | DOUBLE | 67.4% | 28176 | 16503 | moy 0.521 · méd 0.527 · min/max 0.081/0.919 · p10/p90 0.277/0.764 |  |
| `ws_defensive_line_height` | DOUBLE | 67.4% | 28178 | 20846 | moy 27.018 · méd 25.748 · min/max 11.592/55.95 · p10/p90 21.912/34.318 |  |
| `ws_flank_exposure_asymm` | DOUBLE | 67.4% | 28185 | 3111 | moy -0.038 · méd -0.03 · min/max -1.0/1.0 · p10/p90 -0.25/0.169 |  |
| `squad_avg_form_5` | DOUBLE | 62.5% | 32413 | 21120 | moy 57.327 · méd 55.717 · min/max 3.0/106.778 · p10/p90 46.879/68.458 | Forme moyenne du squad sur 5 matchs |
| `squad_xg_quality_5` | DOUBLE | 62.5% | 32413 | 21169 | moy 0.057 · méd 0.055 · min/max 0.0/0.201 · p10/p90 0.045/0.078 |  |
| `squad_regularity` | DOUBLE | 62.5% | 32413 | 44 | moy 0.775 · méd 0.867 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | Stabilité du squad |
| `squad_top3_share` | DOUBLE | 62.5% | 32413 | 13347 | moy 0.347 · méd 0.346 · min/max 0.26/0.778 · p10/p90 0.318/0.383 |  |
| `has_ws_events` | INTEGER | 100.0% | 0 | 2 | moy 0.674 · méd 1.0 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | 1 si données WhoScored disponibles pour ce match |
| `f1_mutual_cancel_idx` | DOUBLE | 73.4% | 22972 | 15259 | moy 0.006 · méd 0.006 · min/max 0.002/0.033 · p10/p90 0.004/0.007 | Index d'annulation mutuelle (draw signal) |
| `f2_defensive_mirror` | DOUBLE | 65.0% | 30246 | 10655 | moy 0.941 · méd 0.951 · min/max 0.585/1.0 · p10/p90 0.865/0.992 |  |
| `f3_draw_market_dev` | DOUBLE | 73.1% | 23243 | 15170 | moy -0.035 · méd -0.052 · min/max -0.857/0.335 · p10/p90 -0.111/0.046 |  |
| `f4_momentum_convergence` | DOUBLE | 32.4% | 58390 | 5789 | moy 0.499 · méd 0.671 · min/max -25.356/1.0 · p10/p90 -0.073/0.961 |  |
| `f5_cs_mutual_rate` | DOUBLE | 73.5% | 22882 | 115 | moy 0.076 · méd 0.063 · min/max 0.0/1.0 · p10/p90 0.0/0.143 |  |
| `f7_off_def_mismatch` | DOUBLE | 16.2% | 72392 | 233 | moy 0.0 · méd 0.0 · min/max -0.71/0.71 · p10/p90 -0.25/0.25 |  |
| `f7_def_off_mismatch` | DOUBLE | 16.2% | 72392 | 233 | moy -0.0 · méd 0.0 · min/max -0.71/0.71 · p10/p90 -0.25/0.25 |  |
| `f8_press_dominance_ratio` | DOUBLE | 73.7% | 22728 | 30851 | moy -0.0 · méd -0.0 · min/max -2.512/2.512 · p10/p90 -0.627/0.627 |  |
| `f9_chance_quality_gap` | DOUBLE | 73.5% | 22882 | 30700 | moy -0.0 · méd 0.0 · min/max -0.41/0.41 · p10/p90 -0.054/0.054 |  |
| `f10_venue_power_adj` | DOUBLE | 72.7% | 23603 | 29827 | moy 0.091 · méd -0.002 · min/max -2.876/2.867 · p10/p90 -0.555/0.798 |  |
| `f11_comeback_rate` | DOUBLE | 85.8% | 12283 | 26 | moy 0.185 · méd 0.167 · min/max 0.0/1.0 · p10/p90 0.0/0.5 |  |
| `f12_red_card_resilience` | DOUBLE | 20.8% | 68353 | 6 | moy 0.144 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/1.0 |  |
| `f15_xg_yield_ratio` | DOUBLE | 73.7% | 22671 | 30007 | moy 1.148 · méd 1.074 · min/max 0.0/11.119 · p10/p90 0.696/1.756 |  |
| `f16_def_yield_ratio` | DOUBLE | 73.7% | 22671 | 30003 | moy 1.036 · méd 1.048 · min/max 0.0/11.119 · p10/p90 0.495/1.422 |  |
| `f17_shots_to_goal_eff` | DOUBLE | 73.6% | 22828 | 1530 | moy 0.116 · méd 0.111 · min/max 0.0/0.714 · p10/p90 0.042/0.192 |  |
| `f18_sot_conversion` | DOUBLE | 73.5% | 22860 | 653 | moy 0.306 · méd 0.28 · min/max 0.0/2.0 · p10/p90 0.143/0.5 |  |
| `f19_tactical_lock_idx` | DOUBLE | 73.5% | 22882 | 15362 | moy 0.706 · méd 0.411 · min/max 0.024/3.801 · p10/p90 0.118/2.112 | Index de verrouillage tactique (draw signal composite) |
| `f20_upset_composite` | DOUBLE | 71.2% | 24828 | 16290 | moy 0.635 · méd 0.165 · min/max 0.0/76.92 · p10/p90 0.0/1.498 | Index d'upset composite (draw signal) |
| `form_n_defenders` | INTEGER | 98.5% | 1307 | 3 | moy 3.79 · méd 4.0 · min/max 3.0/5.0 · p10/p90 3.0/4.0 |  |
| `form_n_midfielders` | INTEGER | 98.5% | 1311 | 5 | moy 4.303 · méd 4.0 · min/max 2.0/6.0 · p10/p90 3.0/5.0 |  |
| `form_n_attackers` | INTEGER | 97.9% | 1856 | 4 | moy 1.906 · méd 2.0 · min/max 1.0/4.0 · p10/p90 1.0/3.0 |  |
| `form_familiarity_5` | DOUBLE | 100.0% | 0 | 6 | moy 0.492 · méd 0.4 · min/max 0.0/1.0 · p10/p90 0.0/1.0 |  |
| `form_change_flag` | INTEGER | 92.0% | 6934 | 2 | moy 0.271 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/1.0 |  |
| `home_win_rate_hist` | DOUBLE | 91.6% | 7233 | 3544 | moy 0.54 · méd 0.5 · min/max 0.0/1.0 · p10/p90 0.313/0.848 |  |