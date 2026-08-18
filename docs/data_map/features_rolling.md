---
schema: gold
rows: 46887
---
# features_rolling

#gold

Rolling windows W=3/5/10 depuis intermediate.backbone. 1 ligne par équipe par match — consommée par features_draw et features_whoscored.


## Intégrité
**Clé déclarée :** (match_id, team_id) — ✅ aucun doublon

## Lineage
**Sources :** [[backbone]]
**Alimente :** [[features_draw]], [[features_final]]

## Features & profiling  (46887 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `date` | DATE | 100.0% | 0 | 2417 |  | Date du match |
| `team_id` | BIGINT | 100.0% | 0 | 153 |  |  |
| `opponent_id` | BIGINT | 97.7% | 1067 | 426 |  |  |
| `venue` | VARCHAR | 100.0% | 0 | 3 | top: Away (23660), Home (22995), Neutral (232) |  |
| `season` | VARCHAR | 100.0% | 0 | 9 | top: 2018-2019 (5623), 2017-2018 (5525), 2021-2022 (5379) |  |
| `league_source` | VARCHAR | 100.0% | 0 | 28 |  |  |
| `comp_category` | VARCHAR | 100.0% | 0 | 5 | top: Big5 (32218), D2 (5880), Europe (4565) |  |
| `match_id` | VARCHAR | 100.0% | 0 | 28499 |  | ID unique du match provenant du backbone |
| `result_1n2` | VARCHAR | 98.2% | 841 | 3 | top: H (20128), A (14654), D (11264) |  |
| `formation` | VARCHAR | 100.0% | 0 | 29 |  | Système de jeu annoncé (ex: 4-3-3) |
| `is_home` | INTEGER | 100.0% | 0 | 2 | moy 0.49 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/1.0 |  |
| `days_since_last_match` | BIGINT | 99.7% | 153 | 145 | moy 8.106 · méd 6.0 · min/max 0.0/1911.0 · p10/p90 3.0/13.0 | Nombre de jours depuis le dernier match |
| `is_return_from_break` | INTEGER | 100.0% | 0 | 2 | moy 0.028 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.0 | Flag (1/0) si le dernier match remonte à plus de 20 jours |
| `is_short_rest` | INTEGER | 100.0% | 0 | 2 | moy 0.235 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | Flag (1/0) si le repos est inférieur à 4 jours |
| `xg_overperformance_3` | DOUBLE | 66.1% | 15917 | 29796 | moy 0.088 · méd 0.041 · min/max -1.0/10.119 · p10/p90 -0.551/0.761 |  |
| `xg_conceded_overperformance_3` | DOUBLE | 66.1% | 15917 | 29870 | moy 0.09 · méd 0.041 · min/max -1.0/10.119 · p10/p90 -0.549/0.763 |  |
| `avg_gf_3` | DOUBLE | 93.9% | 2840 | 34 | moy 1.489 · méd 1.333 · min/max 0.0/12.0 · p10/p90 0.333/2.667 |  |
| `avg_ga_3` | DOUBLE | 93.9% | 2840 | 26 | moy 1.261 · méd 1.0 · min/max 0.0/8.0 · p10/p90 0.333/2.333 |  |
| `np_xg_roll_3` | DOUBLE | 66.1% | 15917 | 30879 | moy 1.304 · méd 1.217 · min/max 0.031/4.893 · p10/p90 0.655/2.065 | Moyenne npxG sur les 3 derniers matchs |
| `np_xg_roll_venue_3` | DOUBLE | 64.8% | 16513 | 30286 | moy 1.304 · méd 1.211 · min/max 0.014/5.934 · p10/p90 0.628/2.1 |  |
| `np_xg_conceded_roll_3` | DOUBLE | 66.1% | 15917 | 30872 | moy 1.304 · méd 1.238 · min/max 0.031/4.68 · p10/p90 0.675/2.012 |  |
| `xg_net_roll_3` | DOUBLE | 66.1% | 15917 | 30955 | moy 0.0 · méd -0.028 · min/max -4.195/4.195 · p10/p90 -1.075/1.126 |  |
| `shot_quality_ratio_3` | DOUBLE | 65.9% | 15993 | 30883 | moy 0.103 · méd 0.1 · min/max 0.015/0.446 · p10/p90 0.065/0.146 |  |
| `shot_quality_ratio_venue_3` | DOUBLE | 64.6% | 16583 | 30291 | moy 0.103 · méd 0.1 · min/max 0.01/0.489 · p10/p90 0.064/0.147 |  |
| `avg_shots_3` | DOUBLE | 92.9% | 3351 | 130 | moy 12.437 · méd 12.333 · min/max 0.0/44.0 · p10/p90 7.667/17.667 |  |
| `avg_sot_3` | DOUBLE | 92.9% | 3351 | 68 | moy 4.375 · méd 4.333 · min/max 0.0/25.0 · p10/p90 2.0/7.0 |  |
| `cs_rate_3` | DOUBLE | 92.9% | 3351 | 9 | moy 0.296 · méd 0.333 · min/max 0.0/2.0 · p10/p90 0.0/0.667 |  |
| `shot_accuracy_roll_3` | DOUBLE | 89.4% | 4993 | 967 | moy 0.353 · méd 0.348 · min/max 0.0/1.0 · p10/p90 0.235/0.474 |  |
| `save_rate_roll_3` | DOUBLE | 90.7% | 4376 | 237 | moy 0.705 · méd 0.714 · min/max 0.0/2.0 · p10/p90 0.5/0.909 |  |
| `roll_save_pct_3` | DOUBLE | 90.7% | 4376 | 1516 | moy 69.66 · méd 71.767 · min/max -100.0/100.0 · p10/p90 47.2/91.667 |  |
| `roll_sota_3` | DOUBLE | 92.9% | 3351 | 49 | moy 3.863 · méd 3.667 · min/max 0.0/15.0 · p10/p90 2.0/6.0 |  |
| `avg_saves_3` | DOUBLE | 92.9% | 3351 | 40 | moy 2.73 · méd 2.667 · min/max 0.0/13.0 · p10/p90 1.0/4.333 |  |
| `poss_roll_3` | DOUBLE | 91.0% | 4200 | 237 | moy 51.289 · méd 51.0 · min/max 20.0/100.0 · p10/p90 40.0/63.0 |  |
| `poss_roll_venue_3` | DOUBLE | 87.3% | 5976 | 240 | moy 51.184 · méd 51.0 · min/max 17.0/100.0 · p10/p90 39.667/63.0 |  |
| `ppda_roll_3` | DOUBLE | 66.1% | 15917 | 30346 | moy 12.416 · méd 11.571 · min/max 2.538/85.561 · p10/p90 7.576/18.184 |  |
| `ppda_allowed_roll_3` | DOUBLE | 66.1% | 15917 | 30373 | moy 12.419 · méd 11.378 · min/max 2.538/76.636 · p10/p90 7.425/18.526 |  |
| `ppda_ratio_roll_3` | DOUBLE | 66.1% | 15917 | 30966 | moy 1.174 · méd 1.015 · min/max 0.077/13.003 · p10/p90 0.497/2.029 |  |
| `defensive_actions_roll_3` | DOUBLE | 92.9% | 3351 | 164 | moy 19.231 · méd 19.333 · min/max 0.0/52.0 · p10/p90 13.667/26.0 |  |
| `fouls_per_tackle_roll_3` | DOUBLE | 89.1% | 5127 | 1649 | moy 1.304 · méd 1.222 · min/max 0.0/19.0 · p10/p90 0.806/1.897 |  |
| `sterility_index_3` | DOUBLE | 65.9% | 15993 | 30894 | moy 532.694 · méd 500.707 · min/max 51.592/2686.456 · p10/p90 324.295/775.139 |  |
| `shots_faced_per_goal_conceded_3` | DOUBLE | 87.2% | 6016 | 249 | moy 3.628 · méd 3.0 · min/max 0.0/33.0 · p10/p90 1.714/6.333 |  |
| `sterility_weighted_3` | DOUBLE | 65.9% | 15993 | 30894 | moy 544.41 · méd 503.323 · min/max 23.733/3707.309 · p10/p90 278.025/853.95 |  |
| `press_resistance_3` | DOUBLE | 65.9% | 15993 | 30813 | moy 4.403 · méd 4.303 · min/max 0.853/11.345 · p10/p90 3.021/5.892 |  |
| `shield_efficiency_3` | DOUBLE | 65.8% | 16026 | 30788 | moy 32.068 · méd 30.498 · min/max -17.712/96.999 · p10/p90 18.259/48.117 |  |
| `red_card_rate_roll_3` | DOUBLE | 93.9% | 2840 | 5 | moy 0.087 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.333 |  |
| `win_rate_roll_3` | DOUBLE | 93.9% | 2840 | 5 | moy 0.419 · méd 0.333 · min/max 0.0/1.0 · p10/p90 0.0/1.0 |  |
| `points_pg_roll_3` | DOUBLE | 93.9% | 2840 | 11 | moy 1.503 · méd 1.333 · min/max 0.0/3.0 · p10/p90 0.333/3.0 |  |
| `xg_overperformance_5` | DOUBLE | 66.5% | 15725 | 30620 | moy 0.073 · méd 0.044 · min/max -1.0/10.119 · p10/p90 -0.44/0.599 |  |
| `xg_conceded_overperformance_5` | DOUBLE | 66.5% | 15725 | 30602 | moy 0.075 · méd 0.047 · min/max -1.0/10.119 · p10/p90 -0.429/0.597 |  |
| `avg_gf_5` | DOUBLE | 93.9% | 2840 | 64 | moy 1.496 · méd 1.4 · min/max 0.0/12.0 · p10/p90 0.6/2.6 |  |
| `avg_ga_5` | DOUBLE | 93.9% | 2840 | 50 | moy 1.26 · méd 1.2 · min/max 0.0/8.0 · p10/p90 0.4/2.2 |  |
| `np_xg_roll_5` | DOUBLE | 66.5% | 15725 | 31117 | moy 1.304 · méd 1.222 · min/max 0.031/4.68 · p10/p90 0.735/1.98 | Moyenne npxG sur les 5 derniers matchs |
| `np_xg_roll_venue_5` | DOUBLE | 65.0% | 16423 | 30424 | moy 1.303 · méd 1.216 · min/max 0.014/4.937 · p10/p90 0.696/2.035 |  |
| `np_xg_conceded_roll_5` | DOUBLE | 66.5% | 15725 | 31117 | moy 1.303 · méd 1.257 · min/max 0.031/4.68 · p10/p90 0.75/1.906 |  |
| `xg_net_roll_5` | DOUBLE | 66.5% | 15725 | 31155 | moy 0.0 · méd -0.042 · min/max -4.195/4.195 · p10/p90 -0.945/1.012 | Différentiel xG moyen sur 5 matchs (produit - concédé) |
| `shot_quality_ratio_5` | DOUBLE | 66.3% | 15801 | 31079 | moy 0.103 · méd 0.101 · min/max 0.015/0.446 · p10/p90 0.07/0.139 | Qualité moyenne des tirs (npxG / shots_total) sur 5 matchs |
| `shot_quality_ratio_venue_5` | DOUBLE | 65.0% | 16423 | 30456 | moy 0.103 · méd 0.1 · min/max 0.01/0.48 · p10/p90 0.068/0.14 |  |
| `avg_shots_5` | DOUBLE | 93.3% | 3144 | 269 | moy 12.445 · méd 12.4 · min/max 0.0/44.0 · p10/p90 8.4/17.2 |  |
| `avg_sot_5` | DOUBLE | 93.3% | 3144 | 133 | moy 4.377 · méd 4.2 · min/max 0.0/25.0 · p10/p90 2.4/6.6 |  |
| `cs_rate_5` | DOUBLE | 93.3% | 3144 | 17 | moy 0.298 · méd 0.2 · min/max 0.0/2.0 · p10/p90 0.0/0.6 |  |
| `shot_accuracy_roll_5` | DOUBLE | 89.8% | 4786 | 1868 | moy 0.352 · méd 0.348 · min/max 0.0/1.0 · p10/p90 0.255/0.45 |  |
| `save_rate_roll_5` | DOUBLE | 91.2% | 4149 | 460 | moy 0.705 · méd 0.714 · min/max 0.0/2.0 · p10/p90 0.538/0.875 |  |
| `roll_save_pct_5` | DOUBLE | 91.2% | 4149 | 3955 | moy 69.691 · méd 70.84 · min/max -100.0/100.0 · p10/p90 50.3/88.34 |  |
| `roll_sota_5` | DOUBLE | 93.3% | 3144 | 106 | moy 3.854 · méd 3.8 · min/max 0.0/15.0 · p10/p90 2.0/5.667 |  |
| `avg_saves_5` | DOUBLE | 93.3% | 3144 | 83 | moy 2.725 · méd 2.667 · min/max 0.0/13.0 · p10/p90 1.4/4.2 |  |
| `poss_roll_5` | DOUBLE | 91.5% | 3993 | 507 | moy 51.327 · méd 51.0 · min/max 20.0/100.0 · p10/p90 41.0/62.2 |  |
| `poss_roll_venue_5` | DOUBLE | 87.6% | 5812 | 506 | moy 51.205 · méd 50.8 · min/max 17.0/100.0 · p10/p90 40.5/62.4 |  |
| `ppda_roll_5` | DOUBLE | 66.5% | 15725 | 30969 | moy 12.412 · méd 11.736 · min/max 2.538/71.0 · p10/p90 8.027/17.591 |  |
| `ppda_allowed_roll_5` | DOUBLE | 66.5% | 15725 | 30970 | moy 12.417 · méd 11.503 · min/max 2.538/71.0 · p10/p90 7.835/17.989 |  |
| `ppda_ratio_roll_5` | DOUBLE | 66.5% | 15725 | 31158 | moy 1.145 · méd 1.026 · min/max 0.077/13.003 · p10/p90 0.528/1.889 | Ratio pressing (PPDA / PPDA allowed) sur 5 matchs |
| `defensive_actions_roll_5` | DOUBLE | 93.3% | 3144 | 340 | moy 19.228 · méd 19.4 · min/max 0.0/52.0 · p10/p90 14.2/25.4 |  |
| `fouls_per_tackle_roll_5` | DOUBLE | 89.5% | 4920 | 3379 | moy 1.283 · méd 1.217 · min/max 0.0/19.0 · p10/p90 0.85/1.781 |  |
| `sterility_index_5` | DOUBLE | 66.3% | 15801 | 31086 | moy 518.227 · méd 495.439 · min/max 51.592/2686.456 · p10/p90 348.122/709.949 | Indice de possession stérile sur 5 matchs |
| `shots_faced_per_goal_conceded_5` | DOUBLE | 89.7% | 4849 | 532 | moy 3.562 · méd 3.0 · min/max 0.0/55.0 · p10/p90 1.917/5.667 |  |
| `sterility_weighted_5` | DOUBLE | 66.3% | 15801 | 31086 | moy 527.224 · méd 498.179 · min/max 23.733/3707.309 · p10/p90 301.408/780.183 |  |
| `press_resistance_5` | DOUBLE | 66.3% | 15801 | 31050 | moy 4.339 · méd 4.257 · min/max 0.853/11.345 · p10/p90 3.112/5.663 |  |
| `shield_efficiency_5` | DOUBLE | 66.2% | 15833 | 31004 | moy 31.618 · méd 30.337 · min/max -2.374/96.999 · p10/p90 20.14/44.751 | Efficacité défensive pondérée par les xG concédés sur 5 matchs |
| `red_card_rate_roll_5` | DOUBLE | 93.9% | 2840 | 11 | moy 0.087 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.2 |  |
| `win_rate_roll_5` | DOUBLE | 93.9% | 2840 | 11 | moy 0.422 · méd 0.4 · min/max 0.0/1.0 · p10/p90 0.0/0.8 | Taux de victoire sur les 5 derniers matchs |
| `points_pg_roll_5` | DOUBLE | 93.9% | 2840 | 28 | moy 1.512 · méd 1.4 · min/max 0.0/3.0 · p10/p90 0.4/2.6 | Points par match sur les 5 derniers matchs |
| `xg_overperformance_10` | DOUBLE | 66.8% | 15545 | 30962 | moy 0.066 · méd 0.045 · min/max -1.0/10.119 · p10/p90 -0.337/0.469 |  |
| `xg_conceded_overperformance_10` | DOUBLE | 66.8% | 15545 | 30977 | moy 0.07 · méd 0.049 · min/max -1.0/10.119 · p10/p90 -0.323/0.463 |  |
| `avg_gf_10` | DOUBLE | 93.9% | 2840 | 152 | moy 1.505 · méd 1.4 · min/max 0.0/12.0 · p10/p90 0.7/2.4 |  |
| `avg_ga_10` | DOUBLE | 93.9% | 2840 | 122 | moy 1.264 · méd 1.2 · min/max 0.0/8.0 · p10/p90 0.556/2.0 |  |
| `np_xg_roll_10` | DOUBLE | 66.8% | 15545 | 31320 | moy 1.303 · méd 1.223 · min/max 0.031/4.68 · p10/p90 0.805/1.924 | Moyenne npxG sur les 10 derniers matchs |
| `np_xg_roll_venue_10` | DOUBLE | 65.0% | 16423 | 30447 | moy 1.303 · méd 1.216 · min/max 0.014/4.937 · p10/p90 0.746/1.98 |  |
| `np_xg_conceded_roll_10` | DOUBLE | 66.8% | 15545 | 31319 | moy 1.302 · méd 1.278 · min/max 0.031/4.68 · p10/p90 0.817/1.807 |  |
| `xg_net_roll_10` | DOUBLE | 66.8% | 15545 | 31335 | moy 0.001 · méd -0.058 · min/max -4.195/4.195 · p10/p90 -0.82/0.915 |  |
| `shot_quality_ratio_10` | DOUBLE | 66.8% | 15545 | 31339 | moy 0.103 · méd 0.101 · min/max 0.015/0.446 · p10/p90 0.075/0.133 |  |
| `shot_quality_ratio_venue_10` | DOUBLE | 65.0% | 16423 | 30459 | moy 0.103 · méd 0.1 · min/max 0.01/0.48 · p10/p90 0.072/0.135 |  |
| `avg_shots_10` | DOUBLE | 93.9% | 2883 | 615 | moy 12.455 · méd 12.4 · min/max 0.0/44.0 · p10/p90 9.0/16.9 |  |
| `avg_sot_10` | DOUBLE | 93.9% | 2883 | 309 | moy 4.378 · méd 4.286 · min/max 0.0/25.0 · p10/p90 2.7/6.4 |  |
| `cs_rate_10` | DOUBLE | 93.9% | 2883 | 39 | moy 0.3 · méd 0.3 · min/max 0.0/2.0 · p10/p90 0.0/0.6 |  |
| `shot_accuracy_roll_10` | DOUBLE | 90.3% | 4525 | 4336 | moy 0.351 · méd 0.347 · min/max 0.0/1.0 · p10/p90 0.272/0.43 |  |
| `save_rate_roll_10` | DOUBLE | 91.7% | 3879 | 1073 | moy 0.705 · méd 0.711 · min/max 0.0/2.0 · p10/p90 0.579/0.84 |  |
| `roll_save_pct_10` | DOUBLE | 91.7% | 3879 | 10599 | moy 69.744 · méd 70.556 · min/max -50.0/100.0 · p10/p90 55.33/85.0 |  |
| `roll_sota_10` | DOUBLE | 93.9% | 2883 | 268 | moy 3.852 · méd 3.9 · min/max 0.0/15.0 · p10/p90 2.3/5.5 |  |
| `avg_saves_10` | DOUBLE | 93.9% | 2883 | 205 | moy 2.725 · méd 2.7 · min/max 0.0/13.0 · p10/p90 1.5/4.0 |  |
| `poss_roll_10` | DOUBLE | 92.0% | 3732 | 1266 | moy 51.353 · méd 50.8 · min/max 20.0/100.0 · p10/p90 41.8/61.7 |  |
| `poss_roll_venue_10` | DOUBLE | 87.6% | 5812 | 1299 | moy 51.206 · méd 50.778 · min/max 17.0/100.0 · p10/p90 41.111/62.0 |  |
| `ppda_roll_10` | DOUBLE | 66.8% | 15545 | 31207 | moy 12.402 · méd 11.899 · min/max 2.538/71.0 · p10/p90 8.395/16.98 |  |
| `ppda_allowed_roll_10` | DOUBLE | 66.8% | 15545 | 31199 | moy 12.407 · méd 11.553 · min/max 2.538/71.0 · p10/p90 8.182/17.511 |  |
| `ppda_ratio_roll_10` | DOUBLE | 66.8% | 15545 | 31340 | moy 1.124 · méd 1.033 · min/max 0.077/13.003 · p10/p90 0.554/1.768 |  |
| `defensive_actions_roll_10` | DOUBLE | 93.9% | 2883 | 817 | moy 19.252 · méd 19.5 · min/max 0.0/52.0 · p10/p90 14.75/25.0 |  |
| `fouls_per_tackle_roll_10` | DOUBLE | 90.1% | 4659 | 8144 | moy 1.268 · méd 1.214 · min/max 0.0/19.0 · p10/p90 0.89/1.696 |  |
| `sterility_index_10` | DOUBLE | 66.8% | 15545 | 31342 | moy 509.2 · méd 491.945 · min/max 51.592/2686.456 · p10/p90 370.653/658.301 |  |
| `shots_faced_per_goal_conceded_10` | DOUBLE | 90.6% | 4386 | 1382 | moy 3.383 · méd 3.0 · min/max 0.0/95.0 · p10/p90 2.062/5.0 |  |
| `sterility_weighted_10` | DOUBLE | 66.8% | 15545 | 31342 | moy 516.119 · méd 494.0 · min/max 23.733/3707.309 · p10/p90 326.783/721.737 |  |
| `press_resistance_10` | DOUBLE | 66.8% | 15545 | 31313 | moy 4.291 · méd 4.234 · min/max 0.853/11.345 · p10/p90 3.202/5.445 |  |
| `shield_efficiency_10` | DOUBLE | 66.8% | 15573 | 31270 | moy 31.307 · méd 30.284 · min/max 0.0/96.999 · p10/p90 21.856/41.987 |  |
| `red_card_rate_roll_10` | DOUBLE | 93.9% | 2840 | 25 | moy 0.088 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.2 |  |
| `win_rate_roll_10` | DOUBLE | 93.9% | 2840 | 33 | moy 0.424 · méd 0.4 · min/max 0.0/1.0 · p10/p90 0.1/0.8 |  |
| `points_pg_roll_10` | DOUBLE | 93.9% | 2840 | 92 | moy 1.521 · méd 1.5 · min/max 0.0/3.0 · p10/p90 0.6/2.5 |  |
| `draw_rate_3` | DOUBLE | 93.9% | 2840 | 3 | moy 0.245 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.5 |  |
| `draw_rate_5` | DOUBLE | 93.9% | 2840 | 11 | moy 0.246 · méd 0.2 · min/max 0.0/1.0 · p10/p90 0.0/0.6 | Taux de nul sur les 5 derniers matchs |
| `draw_rate_10` | DOUBLE | 93.9% | 2840 | 28 | moy 0.248 · méd 0.222 · min/max 0.0/1.0 · p10/p90 0.0/0.5 |  |
| `home_win_rate_hist` | DOUBLE | 97.6% | 1118 | 3569 | moy 0.477 · méd 0.459 · min/max 0.0/1.0 · p10/p90 0.265/0.741 | Taux de victoire à domicile sur toute l'histoire du club |
| `form_n_defenders` | INTEGER | 97.1% | 1349 | 3 | moy 3.78 · méd 4.0 · min/max 3.0/5.0 · p10/p90 3.0/4.0 | Nombre de défenseurs extrait de la formation |
| `form_n_midfielders` | INTEGER | 97.1% | 1355 | 5 | moy 4.301 · méd 4.0 · min/max 2.0/6.0 · p10/p90 3.0/5.0 |  |
| `form_n_attackers` | INTEGER | 95.9% | 1920 | 4 | moy 1.917 · méd 2.0 · min/max 1.0/4.0 · p10/p90 1.0/3.0 |  |
| `form_familiarity_5` | DOUBLE | 100.0% | 0 | 6 | moy 0.447 · méd 0.4 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | Stabilité du système sur les 5 derniers matchs (0 à 1) |
| `form_change_flag` | INTEGER | 93.9% | 2840 | 2 | moy 0.395 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | 1 si l'équipe a changé de système par rapport au match précédent |
| `ws_dribbles_pg` | DOUBLE | 30.8% | 32425 | 95 | moy 8.286 · méd 8.1 · min/max 4.1/15.5 · p10/p90 5.9/10.8 |  |
| `ws_fouled_pg` | DOUBLE | 30.8% | 32425 | 84 | moy 11.333 · méd 11.4 · min/max 6.9/16.0 · p10/p90 9.3/13.3 |  |
| `ws_shots_ot_pg` | DOUBLE | 30.8% | 32425 | 63 | moy 4.384 · méd 4.2 · min/max 2.2/9.4 · p10/p90 3.1/5.9 |  |
| `odds_pinnacle_team` | DOUBLE | 32.5% | 31643 | 994 | moy 2.858 · méd 2.28 · min/max 1.05/27.0 · p10/p90 1.38/4.967 |  |
| `odds_pinnacle_draw` | DOUBLE | 32.5% | 31643 | 792 | moy 4.19 · méd 3.74 · min/max 2.08/20.38 · p10/p90 3.23/5.66 |  |
| `odds_pinnacle_opp` | DOUBLE | 32.5% | 31643 | 1612 | moy 4.63 · méd 3.4 · min/max 1.11/42.94 · p10/p90 1.73/8.75 |  |
| `odds_avg_team` | DOUBLE | 24.7% | 35289 | 879 | moy 2.778 · méd 2.26 · min/max 1.05/22.54 · p10/p90 1.38/4.753 |  |
| `odds_avg_draw` | DOUBLE | 24.7% | 35289 | 672 | moy 4.068 · méd 3.68 · min/max 2.09/16.44 · p10/p90 3.2/5.37 |  |
| `odds_avg_opp` | DOUBLE | 24.7% | 35289 | 1363 | moy 4.327 · méd 3.26 · min/max 1.1/38.04 · p10/p90 1.72/8.103 |  |
| `pinnacle_prob_team` | DOUBLE | 32.5% | 31643 | 14957 | moy 0.441 · méd 0.427 · min/max 0.036/0.925 · p10/p90 0.196/0.704 | Probabilité implicite de victoire de l'équipe (Pinnacle) |
| `pinnacle_prob_draw` | DOUBLE | 32.5% | 31643 | 14850 | moy 0.247 · méd 0.26 · min/max 0.047/0.467 · p10/p90 0.171/0.302 |  |
| `pinnacle_prob_opp` | DOUBLE | 32.5% | 31643 | 14982 | moy 0.312 · méd 0.286 · min/max 0.023/0.883 · p10/p90 0.111/0.561 |  |
| `market_prob_team` | DOUBLE | 24.7% | 35289 | 11444 | moy 0.436 · méd 0.422 · min/max 0.043/0.916 · p10/p90 0.201/0.693 | Probabilité moyenne du marché pour la victoire de l'équipe |
| `market_prob_draw` | DOUBLE | 24.7% | 35289 | 11383 | moy 0.248 · méd 0.259 · min/max 0.059/0.456 · p10/p90 0.178/0.298 |  |
| `market_prob_opp` | DOUBLE | 24.7% | 35289 | 11455 | moy 0.316 · méd 0.292 · min/max 0.025/0.877 · p10/p90 0.118/0.554 |  |
| `season_att_rating` | DOUBLE | 28.2% | 33685 | 249 | moy 6.641 · méd 6.625 · min/max 6.42/7.115 · p10/p90 6.5/6.815 | Rating offensif de la saison précédente |
| `season_def_rating` | DOUBLE | 28.2% | 33685 | 249 | moy 6.641 · méd 6.625 · min/max 6.42/7.115 · p10/p90 6.5/6.815 | Rating défensif de la saison précédente |