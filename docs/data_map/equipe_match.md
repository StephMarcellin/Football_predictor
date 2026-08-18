---
schema: gold
rows: 46949
---
# equipe_match

#gold #famille1 #famille4

Grain (match_id, team_id). Familles CDC 1 (pré-match), 2 (forme), 3 (style), 8 (partie native backbone). Familles 4 et 8-agrégée ajoutées ultérieurement. Anti-leakage : fenêtres ROWS BETWEEN w PRECEDING AND 1 PRECEDING (match courant exclu).

## Intégrité
**Clé déclarée :** (match_id, team_id) — ✅ aucun doublon

## Lineage
**Sources :** [[backbone]], [[int_whoscored_team_season]], [[team_features_ws]]
**Alimente :** —

## Features & profiling  (46949 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 28561 |  | Identifiant unifié du match (SHA1). |
| `team_id` | BIGINT | 100.0% | 0 | 153 |  | Id canonique de l'équipe (perspective de la ligne). |
| `opponent_id` | BIGINT | 97.7% | 1067 | 426 |  | Id canonique de l'adversaire. |
| `date` | DATE | 100.0% | 0 | 2417 |  | Date du match. |
| `season` | VARCHAR | 100.0% | 0 | 9 | top: 2018-2019 (5623), 2017-2018 (5545), 2021-2022 (5379) | Saison (ex. 2023-2024). Usage technique (split temporel), pas prédicteur direct. |
| `league_source` | VARCHAR | 100.0% | 0 | 28 |  | Championnat / compétition source. |
| `venue` | VARCHAR | 100.0% | 0 | 3 | top: Away (23691), Home (23026), Neutral (232) | Lieu du match : Home / Away / Neutral. |
| `comp_category` | VARCHAR | 100.0% | 0 | 5 | top: Big5 (32218), D2 (5942), Europe (4565) | [Famille 1] Catégorie de compétition (Big5 / Cup / Europe) — proxy d'enjeu. PRE-MATCH CONNU. |
| `is_home` | INTEGER | 100.0% | 0 | 2 | moy 0.49 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | [Famille 1] 1 si l'équipe joue à domicile. PRE-MATCH CONNU. |
| `days_since_last_game` | BIGINT | 99.7% | 153 | 145 | moy 8.095 · méd 6.0 · min/max 0.0/1911.0 · p10/p90 3.0/13.0 | [Famille 1] Jours de repos depuis le dernier match officiel (tous matchs, ordre chronologique). PRE-MATCH CONNU, aucun leakage. |
| `avg_gf_rolling_3` | DOUBLE | 94.0% | 2840 | 34 | moy 1.489 · méd 1.333 · min/max 0.0/12.0 · p10/p90 0.333/2.667 | [Famille 2, ROLLING(3)] Moyenne de buts marqués sur les 3 derniers matchs strictement antérieurs. |
| `avg_ga_rolling_3` | DOUBLE | 94.0% | 2840 | 26 | moy 1.261 · méd 1.0 · min/max 0.0/8.0 · p10/p90 0.333/2.333 | [Famille 2, ROLLING(3)] Moyenne de buts encaissés sur les 3 derniers matchs strictement antérieurs. |
| `avg_np_xg_rolling_3` | DOUBLE | 66.0% | 15979 | 30879 | moy 1.304 · méd 1.217 · min/max 0.031/4.893 · p10/p90 0.655/2.065 | [Famille 2, ROLLING(3)] Moyenne de xG hors penalty produits (domination offensive réelle) sur les 3 derniers matchs strictement antérieurs. |
| `avg_np_xg_conceded_rolling_3` | DOUBLE | 66.0% | 15979 | 30872 | moy 1.304 · méd 1.238 · min/max 0.031/4.68 · p10/p90 0.675/2.012 | [Famille 2, ROLLING(3)] Moyenne de xG hors penalty concédés sur les 3 derniers matchs strictement antérieurs. |
| `avg_np_xg_diff_rolling_3` | DOUBLE | 66.0% | 15979 | 30955 | moy 0.0 · méd -0.028 · min/max -4.195/4.195 · p10/p90 -1.075/1.126 | [Famille 2, ROLLING(3)] Moyenne du différentiel de xG hors penalty (produit − concédé) sur les 3 derniers matchs strictement antérieurs. |
| `points_rolling_3` | DOUBLE | 94.0% | 2840 | 11 | moy 1.519 · méd 1.333 · min/max 0.0/3.0 · p10/p90 0.333/3.0 | [Famille 2, ROLLING(3)] Moyenne de points (3 victoire / 1 nul / 0 défaite) sur les 3 derniers matchs strictement antérieurs. |
| `win_rate_rolling_3` | DOUBLE | 94.0% | 2840 | 5 | moy 0.42 · méd 0.333 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | [Famille 2, ROLLING(3)] Taux de victoire sur les 3 derniers matchs strictement antérieurs. |
| `draw_rate_rolling_3` | DOUBLE | 94.0% | 2840 | 5 | moy 0.259 · méd 0.333 · min/max 0.0/1.0 · p10/p90 0.0/0.667 | [Famille 2, ROLLING(3)] Taux de match nul sur les 3 derniers matchs strictement antérieurs. |
| `loss_rate_rolling_3` | DOUBLE | 94.0% | 2840 | 5 | moy 0.321 · méd 0.333 · min/max 0.0/1.0 · p10/p90 0.0/0.667 | [Famille 2, ROLLING(3)] Taux de défaite sur les 3 derniers matchs strictement antérieurs. |
| `clean_sheet_rate_rolling_3` | DOUBLE | 92.9% | 3351 | 5 | moy 0.297 · méd 0.333 · min/max 0.0/1.0 · p10/p90 0.0/0.667 | [Famille 2, ROLLING(3)] Taux de clean sheet (0 but encaissé) sur les 3 derniers matchs strictement antérieurs. |
| `failed_to_score_rate_rolling_3` | DOUBLE | 94.0% | 2840 | 5 | moy 0.251 · méd 0.333 · min/max 0.0/1.0 · p10/p90 0.0/0.667 | [Famille 2, ROLLING(3)] Taux de matchs sans marquer sur les 3 derniers matchs strictement antérieurs. |
| `shots_rolling_3` | DOUBLE | 92.9% | 3351 | 130 | moy 12.44 · méd 12.333 · min/max 0.0/44.0 · p10/p90 7.667/17.667 | [Famille 2, ROLLING(3)] Moyenne de tirs sur les 3 derniers matchs strictement antérieurs. |
| `shots_ot_rolling_3` | DOUBLE | 92.9% | 3351 | 68 | moy 4.376 · méd 4.333 · min/max 0.0/25.0 · p10/p90 2.0/7.0 | [Famille 2, ROLLING(3)] Moyenne de tirs cadrés sur les 3 derniers matchs strictement antérieurs. |
| `ppda_rolling_3` | DOUBLE | 66.0% | 15979 | 30346 | moy 12.416 · méd 11.571 · min/max 2.538/85.561 · p10/p90 7.576/18.184 | [Famille 2, ROLLING(3)] Moyenne de PPDA (plus bas = pressing plus intense) sur les 3 derniers matchs strictement antérieurs. |
| `ppda_allowed_rolling_3` | DOUBLE | 66.0% | 15979 | 30373 | moy 12.419 · méd 11.378 · min/max 2.538/76.636 · p10/p90 7.425/18.526 | [Famille 2, ROLLING(3)] Moyenne de PPDA concédé (pressing subi) sur les 3 derniers matchs strictement antérieurs. |
| `field_tilt_rolling_3` | DOUBLE | 61.5% | 18067 | 28809 | moy 0.241 · méd 0.238 · min/max 0.084/0.533 · p10/p90 0.189/0.295 | [Famille 3, ROLLING(3)] Field tilt : domination territoriale (part des actions dans le tiers offensif) — moyenne sur les 3 derniers matchs antérieurs. |
| `counter_attack_dna_rolling_3` | DOUBLE | 61.5% | 18067 | 11581 | moy 0.241 · méd 0.236 · min/max 0.0/0.75 · p10/p90 0.128/0.36 | [Famille 3, ROLLING(3)] Propension à la contre-attaque (ADN de jeu en transition) — moyenne sur les 3 derniers matchs antérieurs. |
| `attack_left_pct_rolling_3` | DOUBLE | 61.5% | 18067 | 28794 | moy 0.365 · méd 0.365 · min/max 0.178/0.573 · p10/p90 0.314/0.417 | [Famille 3, ROLLING(3)] Part des attaques par le côté gauche — moyenne sur les 3 derniers matchs antérieurs. |
| `attack_center_pct_rolling_3` | DOUBLE | 61.5% | 18067 | 28784 | moy 0.261 · méd 0.259 · min/max 0.124/0.441 · p10/p90 0.218/0.306 | [Famille 3, ROLLING(3)] Part des attaques par l'axe — moyenne sur les 3 derniers matchs antérieurs. |
| `attack_right_pct_rolling_3` | DOUBLE | 61.5% | 18067 | 28790 | moy 0.374 · méd 0.373 · min/max 0.218/0.606 · p10/p90 0.322/0.427 | [Famille 3, ROLLING(3)] Part des attaques par le côté droit — moyenne sur les 3 derniers matchs antérieurs. |
| `def_exposed_left_pct_rolling_3` | DOUBLE | 61.5% | 18067 | 28793 | moy 0.365 · méd 0.365 · min/max 0.19/0.561 · p10/p90 0.32/0.411 | [Famille 3, ROLLING(3)] Exposition défensive côté gauche (part du danger concédé) — moyenne sur les 3 derniers matchs antérieurs. |
| `def_exposed_center_pct_rolling_3` | DOUBLE | 61.5% | 18067 | 28778 | moy 0.261 · méd 0.259 · min/max 0.124/0.441 · p10/p90 0.223/0.299 | [Famille 3, ROLLING(3)] Exposition défensive dans l'axe — moyenne sur les 3 derniers matchs antérieurs. |
| `def_exposed_right_pct_rolling_3` | DOUBLE | 61.5% | 18067 | 28787 | moy 0.374 · méd 0.373 · min/max 0.218/0.575 · p10/p90 0.328/0.42 | [Famille 3, ROLLING(3)] Exposition défensive côté droit — moyenne sur les 3 derniers matchs antérieurs. |
| `cross_rate_rolling_3` | DOUBLE | 61.5% | 18067 | 28743 | moy 0.037 · méd 0.037 · min/max 0.002/0.11 · p10/p90 0.024/0.052 | [Famille 3, ROLLING(3)] Taux de centres — moyenne sur les 3 derniers matchs antérieurs. |
| `through_ball_rate_rolling_3` | DOUBLE | 61.5% | 18067 | 28777 | moy 0.108 · méd 0.107 · min/max 0.037/0.244 · p10/p90 0.077/0.142 | [Famille 3, ROLLING(3)] Taux de passes en profondeur — moyenne sur les 3 derniers matchs antérieurs. |
| `long_ball_rate_rolling_3` | DOUBLE | 61.5% | 18067 | 28790 | moy 0.151 · méd 0.148 · min/max 0.032/0.381 · p10/p90 0.095/0.213 | [Famille 3, ROLLING(3)] Taux de jeu long — moyenne sur les 3 derniers matchs antérieurs. |
| `shot_six_yard_pct_rolling_3` | DOUBLE | 61.5% | 18067 | 4664 | moy 0.078 · méd 0.071 · min/max 0.0/0.5 · p10/p90 0.013/0.149 | [Famille 3, ROLLING(3)] Part des tirs pris dans les 6 mètres — moyenne sur les 3 derniers matchs antérieurs. |
| `shot_open_play_pct_rolling_3` | DOUBLE | 61.5% | 18067 | 13030 | moy 0.675 · méd 0.681 · min/max 0.0/1.0 · p10/p90 0.54/0.803 | [Famille 3, ROLLING(3)] Part des tirs dans le jeu — moyenne sur les 3 derniers matchs antérieurs. |
| `shot_set_piece_pct_rolling_3` | DOUBLE | 61.5% | 18067 | 2176 | moy 0.043 · méd 0.03 · min/max 0.0/0.571 · p10/p90 0.0/0.105 | [Famille 3, ROLLING(3)] Part des tirs sur coup de pied arrêté — moyenne sur les 3 derniers matchs antérieurs. |
| `set_piece_reliance_rolling_3` | DOUBLE | 61.5% | 18067 | 28316 | moy 0.079 · méd 0.077 · min/max 0.0/0.259 · p10/p90 0.053/0.107 | [Famille 3, ROLLING(3)] Dépendance aux coups de pied arrêtés (set-piece pressure) — moyenne sur les 3 derniers matchs antérieurs. |
| `defensive_line_height_rolling_3` | DOUBLE | 61.5% | 18067 | 28840 | moy 26.947 · méd 26.65 · min/max 13.215/51.691 · p10/p90 22.435/31.831 | [Famille 3, ROLLING(3)] Hauteur de la ligne défensive — moyenne sur les 3 derniers matchs antérieurs. |
| `yellow_cards_rolling_3` | DOUBLE | 92.9% | 3351 | 34 | moy 2.057 · méd 2.0 · min/max 0.0/9.0 · p10/p90 1.0/3.333 | [Famille 8, ROLLING(3)] Moyenne de cartons jaunes (2es jaunes inclus) sur les 3 derniers matchs antérieurs. |
| `fouls_committed_rolling_3` | DOUBLE | 92.9% | 3351 | 94 | moy 11.827 · méd 12.0 · min/max 0.0/28.0 · p10/p90 8.0/16.0 | [Famille 8, ROLLING(3)] Moyenne de fautes commises sur les 3 derniers matchs antérieurs. |
| `avg_gf_rolling_5` | DOUBLE | 94.0% | 2840 | 64 | moy 1.496 · méd 1.4 · min/max 0.0/12.0 · p10/p90 0.6/2.6 | [Famille 2, ROLLING(5)] Moyenne de buts marqués sur les 5 derniers matchs strictement antérieurs. |
| `avg_ga_rolling_5` | DOUBLE | 94.0% | 2840 | 50 | moy 1.259 · méd 1.2 · min/max 0.0/8.0 · p10/p90 0.4/2.2 | [Famille 2, ROLLING(5)] Moyenne de buts encaissés sur les 5 derniers matchs strictement antérieurs. |
| `avg_np_xg_rolling_5` | DOUBLE | 66.4% | 15787 | 31117 | moy 1.304 · méd 1.222 · min/max 0.031/4.68 · p10/p90 0.735/1.98 | [Famille 2, ROLLING(5)] Moyenne de xG hors penalty produits (domination offensive réelle) sur les 5 derniers matchs strictement antérieurs. |
| `avg_np_xg_conceded_rolling_5` | DOUBLE | 66.4% | 15787 | 31117 | moy 1.303 · méd 1.257 · min/max 0.031/4.68 · p10/p90 0.75/1.906 | [Famille 2, ROLLING(5)] Moyenne de xG hors penalty concédés sur les 5 derniers matchs strictement antérieurs. |
| `avg_np_xg_diff_rolling_5` | DOUBLE | 66.4% | 15787 | 31155 | moy 0.0 · méd -0.042 · min/max -4.195/4.195 · p10/p90 -0.945/1.012 | [Famille 2, ROLLING(5)] Moyenne du différentiel de xG hors penalty (produit − concédé) sur les 5 derniers matchs strictement antérieurs. |
| `points_rolling_5` | DOUBLE | 94.0% | 2840 | 28 | moy 1.524 · méd 1.4 · min/max 0.0/3.0 · p10/p90 0.6/2.6 | [Famille 2, ROLLING(5)] Moyenne de points (3 victoire / 1 nul / 0 défaite) sur les 5 derniers matchs strictement antérieurs. |
| `win_rate_rolling_5` | DOUBLE | 94.0% | 2840 | 11 | moy 0.422 · méd 0.4 · min/max 0.0/1.0 · p10/p90 0.0/0.8 | [Famille 2, ROLLING(5)] Taux de victoire sur les 5 derniers matchs strictement antérieurs. |
| `draw_rate_rolling_5` | DOUBLE | 94.0% | 2840 | 11 | moy 0.257 · méd 0.2 · min/max 0.0/1.0 · p10/p90 0.0/0.6 | [Famille 2, ROLLING(5)] Taux de match nul sur les 5 derniers matchs strictement antérieurs. |
| `loss_rate_rolling_5` | DOUBLE | 94.0% | 2840 | 11 | moy 0.321 · méd 0.25 · min/max 0.0/1.0 · p10/p90 0.0/0.6 | [Famille 2, ROLLING(5)] Taux de défaite sur les 5 derniers matchs strictement antérieurs. |
| `clean_sheet_rate_rolling_5` | DOUBLE | 93.3% | 3144 | 11 | moy 0.299 · méd 0.2 · min/max 0.0/1.0 · p10/p90 0.0/0.6 | [Famille 2, ROLLING(5)] Taux de clean sheet (0 but encaissé) sur les 5 derniers matchs strictement antérieurs. |
| `failed_to_score_rate_rolling_5` | DOUBLE | 94.0% | 2840 | 11 | moy 0.25 · méd 0.2 · min/max 0.0/1.0 · p10/p90 0.0/0.6 | [Famille 2, ROLLING(5)] Taux de matchs sans marquer sur les 5 derniers matchs strictement antérieurs. |
| `shots_rolling_5` | DOUBLE | 93.3% | 3144 | 269 | moy 12.448 · méd 12.4 · min/max 0.0/44.0 · p10/p90 8.4/17.2 | [Famille 2, ROLLING(5)] Moyenne de tirs sur les 5 derniers matchs strictement antérieurs. |
| `shots_ot_rolling_5` | DOUBLE | 93.3% | 3144 | 133 | moy 4.378 · méd 4.2 · min/max 0.0/25.0 · p10/p90 2.4/6.6 | [Famille 2, ROLLING(5)] Moyenne de tirs cadrés sur les 5 derniers matchs strictement antérieurs. |
| `ppda_rolling_5` | DOUBLE | 66.4% | 15787 | 30969 | moy 12.412 · méd 11.736 · min/max 2.538/71.0 · p10/p90 8.027/17.591 | [Famille 2, ROLLING(5)] Moyenne de PPDA (plus bas = pressing plus intense) sur les 5 derniers matchs strictement antérieurs. |
| `ppda_allowed_rolling_5` | DOUBLE | 66.4% | 15787 | 30970 | moy 12.417 · méd 11.503 · min/max 2.538/71.0 · p10/p90 7.835/17.989 | [Famille 2, ROLLING(5)] Moyenne de PPDA concédé (pressing subi) sur les 5 derniers matchs strictement antérieurs. |
| `field_tilt_rolling_5` | DOUBLE | 61.8% | 17932 | 28932 | moy 0.241 · méd 0.238 · min/max 0.084/0.51 · p10/p90 0.195/0.289 | [Famille 3, ROLLING(5)] Field tilt : domination territoriale (part des actions dans le tiers offensif) — moyenne sur les 5 derniers matchs antérieurs. |
| `counter_attack_dna_rolling_5` | DOUBLE | 61.8% | 17932 | 22113 | moy 0.241 · méd 0.238 · min/max 0.0/0.75 · p10/p90 0.146/0.337 | [Famille 3, ROLLING(5)] Propension à la contre-attaque (ADN de jeu en transition) — moyenne sur les 5 derniers matchs antérieurs. |
| `attack_left_pct_rolling_5` | DOUBLE | 61.8% | 17932 | 28916 | moy 0.365 · méd 0.365 · min/max 0.195/0.548 · p10/p90 0.319/0.412 | [Famille 3, ROLLING(5)] Part des attaques par le côté gauche — moyenne sur les 5 derniers matchs antérieurs. |
| `attack_center_pct_rolling_5` | DOUBLE | 61.8% | 17932 | 28913 | moy 0.261 · méd 0.259 · min/max 0.124/0.441 · p10/p90 0.222/0.302 | [Famille 3, ROLLING(5)] Part des attaques par l'axe — moyenne sur les 5 derniers matchs antérieurs. |
| `attack_right_pct_rolling_5` | DOUBLE | 61.8% | 17932 | 28912 | moy 0.374 · méd 0.373 · min/max 0.218/0.545 · p10/p90 0.328/0.421 | [Famille 3, ROLLING(5)] Part des attaques par le côté droit — moyenne sur les 5 derniers matchs antérieurs. |
| `def_exposed_left_pct_rolling_5` | DOUBLE | 61.8% | 17932 | 28918 | moy 0.365 · méd 0.365 · min/max 0.195/0.548 · p10/p90 0.328/0.403 | [Famille 3, ROLLING(5)] Exposition défensive côté gauche (part du danger concédé) — moyenne sur les 5 derniers matchs antérieurs. |
| `def_exposed_center_pct_rolling_5` | DOUBLE | 61.8% | 17932 | 28915 | moy 0.261 · méd 0.26 · min/max 0.124/0.441 · p10/p90 0.228/0.294 | [Famille 3, ROLLING(5)] Exposition défensive dans l'axe — moyenne sur les 5 derniers matchs antérieurs. |
| `def_exposed_right_pct_rolling_5` | DOUBLE | 61.8% | 17932 | 28920 | moy 0.374 · méd 0.373 · min/max 0.218/0.561 · p10/p90 0.336/0.413 | [Famille 3, ROLLING(5)] Exposition défensive côté droit — moyenne sur les 5 derniers matchs antérieurs. |
| `cross_rate_rolling_5` | DOUBLE | 61.8% | 17932 | 28889 | moy 0.037 · méd 0.037 · min/max 0.002/0.11 · p10/p90 0.025/0.051 | [Famille 3, ROLLING(5)] Taux de centres — moyenne sur les 5 derniers matchs antérieurs. |
| `through_ball_rate_rolling_5` | DOUBLE | 61.8% | 17932 | 28906 | moy 0.108 · méd 0.107 · min/max 0.038/0.231 · p10/p90 0.079/0.14 | [Famille 3, ROLLING(5)] Taux de passes en profondeur — moyenne sur les 5 derniers matchs antérieurs. |
| `long_ball_rate_rolling_5` | DOUBLE | 61.8% | 17932 | 28917 | moy 0.152 · méd 0.149 · min/max 0.032/0.367 · p10/p90 0.097/0.21 | [Famille 3, ROLLING(5)] Taux de jeu long — moyenne sur les 5 derniers matchs antérieurs. |
| `shot_six_yard_pct_rolling_5` | DOUBLE | 61.8% | 17932 | 11192 | moy 0.078 · méd 0.073 · min/max 0.0/0.5 · p10/p90 0.024/0.135 | [Famille 3, ROLLING(5)] Part des tirs pris dans les 6 mètres — moyenne sur les 5 derniers matchs antérieurs. |
| `shot_open_play_pct_rolling_5` | DOUBLE | 61.8% | 17932 | 23050 | moy 0.675 · méd 0.679 · min/max 0.0/1.0 · p10/p90 0.563/0.782 | [Famille 3, ROLLING(5)] Part des tirs dans le jeu — moyenne sur les 5 derniers matchs antérieurs. |
| `shot_set_piece_pct_rolling_5` | DOUBLE | 61.8% | 17932 | 4934 | moy 0.043 · méd 0.034 · min/max 0.0/0.571 · p10/p90 0.0/0.094 | [Famille 3, ROLLING(5)] Part des tirs sur coup de pied arrêté — moyenne sur les 5 derniers matchs antérieurs. |
| `set_piece_reliance_rolling_5` | DOUBLE | 61.8% | 17932 | 28585 | moy 0.079 · méd 0.078 · min/max 0.0/0.259 · p10/p90 0.056/0.103 | [Famille 3, ROLLING(5)] Dépendance aux coups de pied arrêtés (set-piece pressure) — moyenne sur les 5 derniers matchs antérieurs. |
| `defensive_line_height_rolling_5` | DOUBLE | 61.8% | 17932 | 28957 | moy 26.952 · méd 26.7 · min/max 13.215/51.691 · p10/p90 23.011/31.217 | [Famille 3, ROLLING(5)] Hauteur de la ligne défensive — moyenne sur les 5 derniers matchs antérieurs. |
| `yellow_cards_rolling_5` | DOUBLE | 93.3% | 3144 | 65 | moy 2.054 · méd 2.0 · min/max 0.0/9.0 · p10/p90 1.0/3.2 | [Famille 8, ROLLING(5)] Moyenne de cartons jaunes (2es jaunes inclus) sur les 5 derniers matchs antérieurs. |
| `fouls_committed_rolling_5` | DOUBLE | 93.3% | 3144 | 200 | moy 11.821 · méd 12.0 · min/max 0.0/28.0 · p10/p90 8.5/15.6 | [Famille 8, ROLLING(5)] Moyenne de fautes commises sur les 5 derniers matchs antérieurs. |
| `avg_gf_rolling_10` | DOUBLE | 94.0% | 2840 | 152 | moy 1.505 · méd 1.4 · min/max 0.0/12.0 · p10/p90 0.7/2.4 | [Famille 2, ROLLING(10)] Moyenne de buts marqués sur les 10 derniers matchs strictement antérieurs. |
| `avg_ga_rolling_10` | DOUBLE | 94.0% | 2840 | 122 | moy 1.264 · méd 1.2 · min/max 0.0/8.0 · p10/p90 0.556/2.0 | [Famille 2, ROLLING(10)] Moyenne de buts encaissés sur les 10 derniers matchs strictement antérieurs. |
| `avg_np_xg_rolling_10` | DOUBLE | 66.8% | 15607 | 31320 | moy 1.303 · méd 1.223 · min/max 0.031/4.68 · p10/p90 0.805/1.924 | [Famille 2, ROLLING(10)] Moyenne de xG hors penalty produits (domination offensive réelle) sur les 10 derniers matchs strictement antérieurs. |
| `avg_np_xg_conceded_rolling_10` | DOUBLE | 66.8% | 15607 | 31319 | moy 1.302 · méd 1.278 · min/max 0.031/4.68 · p10/p90 0.817/1.807 | [Famille 2, ROLLING(10)] Moyenne de xG hors penalty concédés sur les 10 derniers matchs strictement antérieurs. |
| `avg_np_xg_diff_rolling_10` | DOUBLE | 66.8% | 15607 | 31335 | moy 0.001 · méd -0.058 · min/max -4.195/4.195 · p10/p90 -0.82/0.915 | [Famille 2, ROLLING(10)] Moyenne du différentiel de xG hors penalty (produit − concédé) sur les 10 derniers matchs strictement antérieurs. |
| `points_rolling_10` | DOUBLE | 94.0% | 2840 | 92 | moy 1.529 · méd 1.5 · min/max 0.0/3.0 · p10/p90 0.667/2.5 | [Famille 2, ROLLING(10)] Moyenne de points (3 victoire / 1 nul / 0 défaite) sur les 10 derniers matchs strictement antérieurs. |
| `win_rate_rolling_10` | DOUBLE | 94.0% | 2840 | 33 | moy 0.425 · méd 0.4 · min/max 0.0/1.0 · p10/p90 0.1/0.8 | [Famille 2, ROLLING(10)] Taux de victoire sur les 10 derniers matchs strictement antérieurs. |
| `draw_rate_rolling_10` | DOUBLE | 94.0% | 2840 | 31 | moy 0.254 · méd 0.2 · min/max 0.0/1.0 · p10/p90 0.0/0.5 | [Famille 2, ROLLING(10)] Taux de match nul sur les 10 derniers matchs strictement antérieurs. |
| `loss_rate_rolling_10` | DOUBLE | 94.0% | 2840 | 33 | moy 0.321 · méd 0.3 · min/max 0.0/1.0 · p10/p90 0.0/0.6 | [Famille 2, ROLLING(10)] Taux de défaite sur les 10 derniers matchs strictement antérieurs. |
| `clean_sheet_rate_rolling_10` | DOUBLE | 93.9% | 2883 | 33 | moy 0.3 · méd 0.3 · min/max 0.0/1.0 · p10/p90 0.0/0.6 | [Famille 2, ROLLING(10)] Taux de clean sheet (0 but encaissé) sur les 10 derniers matchs strictement antérieurs. |
| `failed_to_score_rate_rolling_10` | DOUBLE | 94.0% | 2840 | 33 | moy 0.246 · méd 0.2 · min/max 0.0/1.0 · p10/p90 0.0/0.5 | [Famille 2, ROLLING(10)] Taux de matchs sans marquer sur les 10 derniers matchs strictement antérieurs. |
| `shots_rolling_10` | DOUBLE | 93.9% | 2883 | 615 | moy 12.457 · méd 12.4 · min/max 0.0/44.0 · p10/p90 9.0/16.9 | [Famille 2, ROLLING(10)] Moyenne de tirs sur les 10 derniers matchs strictement antérieurs. |
| `shots_ot_rolling_10` | DOUBLE | 93.9% | 2883 | 310 | moy 4.379 · méd 4.3 · min/max 0.0/25.0 · p10/p90 2.7/6.375 | [Famille 2, ROLLING(10)] Moyenne de tirs cadrés sur les 10 derniers matchs strictement antérieurs. |
| `ppda_rolling_10` | DOUBLE | 66.8% | 15607 | 31207 | moy 12.402 · méd 11.899 · min/max 2.538/71.0 · p10/p90 8.395/16.98 | [Famille 2, ROLLING(10)] Moyenne de PPDA (plus bas = pressing plus intense) sur les 10 derniers matchs strictement antérieurs. |
| `ppda_allowed_rolling_10` | DOUBLE | 66.8% | 15607 | 31199 | moy 12.407 · méd 11.553 · min/max 2.538/71.0 · p10/p90 8.182/17.511 | [Famille 2, ROLLING(10)] Moyenne de PPDA concédé (pressing subi) sur les 10 derniers matchs strictement antérieurs. |
| `field_tilt_rolling_10` | DOUBLE | 62.1% | 17775 | 29057 | moy 0.241 · méd 0.238 · min/max 0.084/0.462 · p10/p90 0.2/0.284 | [Famille 3, ROLLING(10)] Field tilt : domination territoriale (part des actions dans le tiers offensif) — moyenne sur les 10 derniers matchs antérieurs. |
| `counter_attack_dna_rolling_10` | DOUBLE | 62.1% | 17775 | 26773 | moy 0.241 · méd 0.24 · min/max 0.0/0.75 · p10/p90 0.164/0.317 | [Famille 3, ROLLING(10)] Propension à la contre-attaque (ADN de jeu en transition) — moyenne sur les 10 derniers matchs antérieurs. |
| `attack_left_pct_rolling_10` | DOUBLE | 62.1% | 17775 | 29043 | moy 0.365 · méd 0.366 · min/max 0.195/0.548 · p10/p90 0.324/0.406 | [Famille 3, ROLLING(10)] Part des attaques par le côté gauche — moyenne sur les 10 derniers matchs antérieurs. |
| `attack_center_pct_rolling_10` | DOUBLE | 62.1% | 17775 | 29039 | moy 0.261 · méd 0.259 · min/max 0.124/0.441 · p10/p90 0.225/0.3 | [Famille 3, ROLLING(10)] Part des attaques par l'axe — moyenne sur les 10 derniers matchs antérieurs. |
| `attack_right_pct_rolling_10` | DOUBLE | 62.1% | 17775 | 29043 | moy 0.374 · méd 0.373 · min/max 0.218/0.545 · p10/p90 0.333/0.415 | [Famille 3, ROLLING(10)] Part des attaques par le côté droit — moyenne sur les 10 derniers matchs antérieurs. |
| `def_exposed_left_pct_rolling_10` | DOUBLE | 62.1% | 17775 | 29041 | moy 0.365 · méd 0.365 · min/max 0.195/0.548 · p10/p90 0.335/0.397 | [Famille 3, ROLLING(10)] Exposition défensive côté gauche (part du danger concédé) — moyenne sur les 10 derniers matchs antérieurs. |
| `def_exposed_center_pct_rolling_10` | DOUBLE | 62.1% | 17775 | 29040 | moy 0.261 · méd 0.26 · min/max 0.124/0.441 · p10/p90 0.233/0.29 | [Famille 3, ROLLING(10)] Exposition défensive dans l'axe — moyenne sur les 10 derniers matchs antérieurs. |
| `def_exposed_right_pct_rolling_10` | DOUBLE | 62.1% | 17775 | 29045 | moy 0.374 · méd 0.374 · min/max 0.218/0.545 · p10/p90 0.343/0.406 | [Famille 3, ROLLING(10)] Exposition défensive côté droit — moyenne sur les 10 derniers matchs antérieurs. |
| `cross_rate_rolling_10` | DOUBLE | 62.1% | 17775 | 29016 | moy 0.037 · méd 0.037 · min/max 0.002/0.11 · p10/p90 0.026/0.049 | [Famille 3, ROLLING(10)] Taux de centres — moyenne sur les 10 derniers matchs antérieurs. |
| `through_ball_rate_rolling_10` | DOUBLE | 62.1% | 17775 | 29035 | moy 0.109 · méd 0.107 · min/max 0.038/0.231 · p10/p90 0.081/0.139 | [Famille 3, ROLLING(10)] Taux de passes en profondeur — moyenne sur les 10 derniers matchs antérieurs. |
| `long_ball_rate_rolling_10` | DOUBLE | 62.1% | 17775 | 29044 | moy 0.152 · méd 0.149 · min/max 0.032/0.367 · p10/p90 0.099/0.208 | [Famille 3, ROLLING(10)] Taux de jeu long — moyenne sur les 10 derniers matchs antérieurs. |
| `shot_six_yard_pct_rolling_10` | DOUBLE | 62.1% | 17775 | 20463 | moy 0.077 · méd 0.074 · min/max 0.0/0.5 · p10/p90 0.033/0.123 | [Famille 3, ROLLING(10)] Part des tirs pris dans les 6 mètres — moyenne sur les 10 derniers matchs antérieurs. |
| `shot_open_play_pct_rolling_10` | DOUBLE | 62.1% | 17775 | 27067 | moy 0.676 · méd 0.679 · min/max 0.0/1.0 · p10/p90 0.585/0.764 | [Famille 3, ROLLING(10)] Part des tirs dans le jeu — moyenne sur les 10 derniers matchs antérieurs. |
| `shot_set_piece_pct_rolling_10` | DOUBLE | 62.1% | 17775 | 10927 | moy 0.042 · méd 0.036 · min/max 0.0/0.571 · p10/p90 0.006/0.083 | [Famille 3, ROLLING(10)] Part des tirs sur coup de pied arrêté — moyenne sur les 10 derniers matchs antérieurs. |
| `set_piece_reliance_rolling_10` | DOUBLE | 62.1% | 17775 | 28744 | moy 0.079 · méd 0.079 · min/max 0.0/0.259 · p10/p90 0.059/0.1 | [Famille 3, ROLLING(10)] Dépendance aux coups de pied arrêtés (set-piece pressure) — moyenne sur les 10 derniers matchs antérieurs. |
| `defensive_line_height_rolling_10` | DOUBLE | 62.1% | 17775 | 29079 | moy 26.965 · méd 26.731 · min/max 13.215/49.383 · p10/p90 23.525/30.731 | [Famille 3, ROLLING(10)] Hauteur de la ligne défensive — moyenne sur les 10 derniers matchs antérieurs. |
| `yellow_cards_rolling_10` | DOUBLE | 93.9% | 2883 | 162 | moy 2.056 · méd 2.0 · min/max 0.0/9.0 · p10/p90 1.167/3.0 | [Famille 8, ROLLING(10)] Moyenne de cartons jaunes (2es jaunes inclus) sur les 10 derniers matchs antérieurs. |
| `fouls_committed_rolling_10` | DOUBLE | 93.9% | 2883 | 480 | moy 11.824 · méd 12.1 · min/max 0.0/28.0 · p10/p90 8.9/15.222 | [Famille 8, ROLLING(10)] Moyenne de fautes commises sur les 10 derniers matchs antérieurs. |
| `season_xg_per_shot_for_lag` | DOUBLE | 35.6% | 30256 | 21 | moy 0.117 · méd 0.115 · min/max 0.08/0.155 · p10/p90 0.1/0.135 | [Famille 3, SEASON-LAG, feature 28] xG par tir produit, saison précédente (jointure sur la saison N-1). Couverture ~65-85 % dès 2022-2023, NULL avant (limite source, cf. troubleshooting). |
| `season_xg_per_shot_against_lag` | DOUBLE | 35.6% | 30256 | 20 | moy 0.114 · méd 0.115 · min/max 0.07/0.16 · p10/p90 0.095/0.13 | [Famille 3, SEASON-LAG, feature 28] xG par tir concédé, saison précédente. |