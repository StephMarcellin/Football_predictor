---
schema: intermediate
rows: 390
---
# int_whoscored_team_season

#intermediate

Ratings agrégés de saison WhoScored par équipe (attaque/défense, dribbles, tirs cadrés par match, différenciés Home/Away). Alimente les colonnes season_*/ws_* de backbone.


## Intégrité
**Clé déclarée :** — (pas de test d'unicité)

## Lineage
**Sources :** [[silver.whoscored_team_season]], [[team_mapping]]
**Alimente :** [[backbone]], [[equipe_match]]

## Features & profiling  (390 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `team_id` | BIGINT | 100.0% | 0 | 123 |  | Id canonique de l'équipe (via team_mapping sur le nom WhoScored 'team'). |
| `season` | VARCHAR | 100.0% | 0 | 5 | top: 2022-2023 (98), 2023-2024 (96), 2024-2025 (96) | Saison (ex. 2023-2024). |
| `league_source` | VARCHAR | 100.0% | 0 | 5 | top: Premier League (100), Serie A (80), La Liga (80) | Championnat / source (ex. Bundesliga). |
| `ws_away_shots_conceded_pg` | DOUBLE | 100.0% | 0 | 100 | moy 13.855 · méd 13.7 · min/max 6.8/20.6 · p10/p90 10.9/16.9 | Tirs concédés par match à l'extérieur (moyenne saison). |
| `ws_away_tackles_pg` | DOUBLE | 100.0% | 0 | 83 | moy 16.609 · méd 16.5 · min/max 10.9/23.4 · p10/p90 14.1/18.9 | Tacles par match à l'extérieur. |
| `ws_away_interceptions_pg` | DOUBLE | 100.0% | 0 | 71 | moy 8.977 · méd 8.9 · min/max 5.3/13.8 · p10/p90 7.1/11.3 | Interceptions par match à l'extérieur. |
| `ws_away_fouls_pg` | DOUBLE | 100.0% | 0 | 74 | moy 12.064 · méd 12.0 · min/max 7.4/18.5 · p10/p90 9.9/14.11 | Fautes commises par match à l'extérieur. |
| `ws_away_offsides_pg` | DOUBLE | 100.0% | 0 | 23 | moy 1.684 · méd 1.6 · min/max 0.7/2.9 · p10/p90 1.2/2.3 | Hors-jeu commis par match à l'extérieur (moyenne saison). |
| `ws_away_def_rating` | DOUBLE | 100.0% | 0 | 63 | moy 6.58 · méd 6.57 · min/max 6.26/7.09 · p10/p90 6.43/6.751 | Note WhoScored globale de l'équipe (0-10, à l'extérieur). Les 4 colonnes *_rating d'un même côté sont IDENTIQUES par nature : le Rating WhoScored est la note GLOBALE de l'équipe, affichée à l'identique sur les onglets Defensive/Offensive/xG (seules les stats du tableau changent, pas la note). Ce n'est PAS un bug. La note 'overall' du site = moyenne domicile/extérieur. |
| `ws_home_shots_conceded_pg` | DOUBLE | 100.0% | 0 | 96 | moy 11.381 · méd 11.4 · min/max 5.4/18.9 · p10/p90 8.7/14.4 | Tirs concédés par match à domicile (moyenne saison). |
| `ws_home_tackles_pg` | DOUBLE | 100.0% | 0 | 93 | moy 16.231 · méd 16.2 · min/max 11.3/22.7 · p10/p90 13.8/18.8 | Tacles par match à domicile. |
| `ws_home_interceptions_pg` | DOUBLE | 100.0% | 0 | 71 | moy 8.795 · méd 8.6 · min/max 5.4/13.3 · p10/p90 6.8/10.9 | Interceptions par match à domicile. |
| `ws_home_fouls_pg` | DOUBLE | 100.0% | 0 | 80 | moy 11.746 · méd 11.7 · min/max 7.5/17.0 · p10/p90 9.5/13.81 | Fautes commises par match à domicile. |
| `ws_home_offsides_pg` | DOUBLE | 100.0% | 0 | 26 | moy 1.851 · méd 1.8 · min/max 0.7/3.7 · p10/p90 1.3/2.5 | Hors-jeu commis par match à domicile (moyenne saison). |
| `ws_home_def_rating` | DOUBLE | 100.0% | 0 | 68 | moy 6.669 · méd 6.65 · min/max 6.33/7.14 · p10/p90 6.5/6.86 | Note WhoScored globale de l'équipe (0-10, à domicile). Les 4 colonnes *_rating d'un même côté sont IDENTIQUES par nature : le Rating WhoScored est la note GLOBALE de l'équipe, affichée à l'identique sur les onglets Defensive/Offensive/xG (seules les stats du tableau changent, pas la note). Ce n'est PAS un bug. La note 'overall' du site = moyenne domicile/extérieur. |
| `ws_away_shots_pg` | DOUBLE | 100.0% | 0 | 87 | moy 11.379 · méd 11.1 · min/max 6.8/18.6 · p10/p90 9.1/14.1 | Tirs tentés par match à l'extérieur. |
| `ws_away_shots_ot_pg` | DOUBLE | 100.0% | 0 | 46 | moy 3.975 · méd 3.8 · min/max 2.2/8.0 · p10/p90 2.8/5.3 | Tirs cadrés par match à l'extérieur. |
| `ws_away_dribbles_pg` | DOUBLE | 100.0% | 0 | 84 | moy 8.032 · méd 7.9 · min/max 4.2/15.5 · p10/p90 5.8/10.2 | Dribbles réussis par match à l'extérieur. |
| `ws_away_fouled_pg` | DOUBLE | 100.0% | 0 | 75 | moy 11.198 · méd 11.25 · min/max 6.9/15.8 · p10/p90 9.2/13.2 | Fautes subies par match à l'extérieur. |
| `ws_away_att_rating` | DOUBLE | 100.0% | 0 | 63 | moy 6.58 · méd 6.57 · min/max 6.26/7.09 · p10/p90 6.43/6.751 | Note WhoScored globale de l'équipe (0-10, à l'extérieur). Les 4 colonnes *_rating d'un même côté sont IDENTIQUES par nature : le Rating WhoScored est la note GLOBALE de l'équipe, affichée à l'identique sur les onglets Defensive/Offensive/xG (seules les stats du tableau changent, pas la note). Ce n'est PAS un bug. La note 'overall' du site = moyenne domicile/extérieur. |
| `ws_home_shots_pg` | DOUBLE | 100.0% | 0 | 100 | moy 13.853 · méd 13.4 · min/max 8.6/25.2 · p10/p90 11.09/17.21 | Tirs tentés par match à domicile. |
| `ws_home_shots_ot_pg` | DOUBLE | 100.0% | 0 | 58 | moy 4.806 · méd 4.6 · min/max 2.5/9.4 · p10/p90 3.5/6.3 | Tirs cadrés par match à domicile. |
| `ws_home_dribbles_pg` | DOUBLE | 100.0% | 0 | 84 | moy 8.522 · méd 8.4 · min/max 4.1/13.9 · p10/p90 6.2/11.1 | Dribbles réussis par match à domicile. |
| `ws_home_fouled_pg` | DOUBLE | 100.0% | 0 | 75 | moy 11.459 · méd 11.4 · min/max 7.1/16.0 · p10/p90 9.69/13.4 | Fautes subies par match à domicile. |
| `ws_home_att_rating` | DOUBLE | 100.0% | 0 | 68 | moy 6.669 · méd 6.65 · min/max 6.33/7.14 · p10/p90 6.5/6.86 | Note WhoScored globale de l'équipe (0-10, à domicile). Les 4 colonnes *_rating d'un même côté sont IDENTIQUES par nature : le Rating WhoScored est la note GLOBALE de l'équipe, affichée à l'identique sur les onglets Defensive/Offensive/xG (seules les stats du tableau changent, pas la note). Ce n'est PAS un bug. La note 'overall' du site = moyenne domicile/extérieur. |
| `ws_away_xg_against` | DOUBLE | 100.0% | 0 | 355 | moy 29.676 · méd 29.7 · min/max 13.63/52.54 · p10/p90 21.829/37.227 | xG concédé, total saison à l'extérieur. |
| `ws_away_goals_against` | DOUBLE | 100.0% | 0 | 39 | moy 27.605 · méd 27.0 · min/max 10.0/57.0 · p10/p90 18.0/38.0 | Buts concédés, total saison à l'extérieur. |
| `ws_away_xg_diff_against` | DOUBLE | 100.0% | 0 | 355 | moy -2.071 · méd -2.42 · min/max -20.42/20.85 · p10/p90 -8.415/4.145 | Différentiel défensif = buts concédés − xG concédé, total saison à l'extérieur (positif = plus encaissé que la qualité des tirs). |
| `ws_away_shots_against` | DOUBLE | 100.0% | 0 | 156 | moy 256.667 · méd 256.5 · min/max 115.0/391.0 · p10/p90 202.0/314.1 | Tirs concédés, total saison à l'extérieur. |
| `ws_away_xg_per_shot_against` | DOUBLE | 100.0% | 0 | 9 | moy 0.116 · méd 0.12 · min/max 0.08/0.16 · p10/p90 0.1/0.131 | xG concédé par tir = xg_against / shots_against (à l'extérieur). |
| `ws_away_xg_against_rating` | DOUBLE | 100.0% | 0 | 63 | moy 6.58 · méd 6.57 · min/max 6.26/7.09 · p10/p90 6.43/6.751 | Note WhoScored globale de l'équipe (0-10, à l'extérieur). Les 4 colonnes *_rating d'un même côté sont IDENTIQUES par nature : le Rating WhoScored est la note GLOBALE de l'équipe, affichée à l'identique sur les onglets Defensive/Offensive/xG (seules les stats du tableau changent, pas la note). Ce n'est PAS un bug. La note 'overall' du site = moyenne domicile/extérieur. |
| `ws_away_xg_for` | DOUBLE | 100.0% | 0 | 353 | moy 24.148 · méd 22.79 · min/max 13.15/49.71 · p10/p90 17.288/33.357 | xG produit, total saison à l'extérieur. |
| `ws_away_goals_for` | DOUBLE | 100.0% | 0 | 38 | moy 22.764 · méd 22.0 · min/max 6.0/49.0 · p10/p90 14.0/34.0 | Buts marqués, total saison à l'extérieur. |
| `ws_away_xg_diff_for` | DOUBLE | 100.0% | 0 | 345 | moy -1.384 · méd -1.46 · min/max -11.93/13.34 · p10/p90 -6.354/4.012 | Différentiel offensif = buts marqués − xG produit, total saison à l'extérieur (positif = sur-performance à la finition). |
| `ws_away_shots_for` | DOUBLE | 100.0% | 0 | 139 | moy 210.885 · méd 205.5 · min/max 130.0/354.0 · p10/p90 167.0/263.1 | Tirs tentés, total saison à l'extérieur. |
| `ws_away_xg_per_shot_for` | DOUBLE | 100.0% | 0 | 10 | moy 0.113 · méd 0.11 · min/max 0.07/0.16 · p10/p90 0.09/0.14 | xG par tir = xg_for / shots_for (à l'extérieur). |
| `ws_away_xg_for_rating` | DOUBLE | 100.0% | 0 | 63 | moy 6.58 · méd 6.57 · min/max 6.26/7.09 · p10/p90 6.43/6.751 | Note WhoScored globale de l'équipe (0-10, à l'extérieur). Les 4 colonnes *_rating d'un même côté sont IDENTIQUES par nature : le Rating WhoScored est la note GLOBALE de l'équipe, affichée à l'identique sur les onglets Defensive/Offensive/xG (seules les stats du tableau changent, pas la note). Ce n'est PAS un bug. La note 'overall' du site = moyenne domicile/extérieur. |
| `ws_home_xg_against` | DOUBLE | 100.0% | 0 | 359 | moy 24.148 · méd 23.545 · min/max 11.87/50.53 · p10/p90 17.069/31.658 | xG concédé, total saison à domicile. |
| `ws_home_goals_against` | DOUBLE | 100.0% | 0 | 37 | moy 22.764 · méd 22.0 · min/max 3.0/53.0 · p10/p90 14.0/32.0 | Buts concédés, total saison à domicile. |
| `ws_home_xg_diff_against` | DOUBLE | 100.0% | 0 | 330 | moy -1.384 · méd -1.45 · min/max -11.55/12.04 · p10/p90 -6.352/3.641 | Différentiel défensif = buts concédés − xG concédé, total saison à domicile (positif = plus encaissé que la qualité des tirs). |
| `ws_home_shots_against` | DOUBLE | 100.0% | 0 | 151 | moy 210.885 · méd 209.0 · min/max 103.0/360.0 · p10/p90 160.0/267.1 | Tirs concédés, total saison à domicile. |
| `ws_home_xg_per_shot_against` | DOUBLE | 100.0% | 0 | 10 | moy 0.115 · méd 0.12 · min/max 0.06/0.16 · p10/p90 0.09/0.14 | xG concédé par tir = xg_against / shots_against (à domicile). |
| `ws_home_xg_against_rating` | DOUBLE | 100.0% | 0 | 68 | moy 6.669 · méd 6.65 · min/max 6.33/7.14 · p10/p90 6.5/6.86 | Note WhoScored globale de l'équipe (0-10, à domicile). Les 4 colonnes *_rating d'un même côté sont IDENTIQUES par nature : le Rating WhoScored est la note GLOBALE de l'équipe, affichée à l'identique sur les onglets Defensive/Offensive/xG (seules les stats du tableau changent, pas la note). Ce n'est PAS un bug. La note 'overall' du site = moyenne domicile/extérieur. |
| `ws_home_xg_for` | DOUBLE | 100.0% | 0 | 366 | moy 29.676 · méd 28.225 · min/max 14.35/58.64 · p10/p90 21.229/40.637 | xG produit, total saison à domicile. |
| `ws_home_goals_for` | DOUBLE | 100.0% | 0 | 47 | moy 27.605 · méd 26.0 · min/max 9.0/59.0 · p10/p90 17.0/41.0 | Buts marqués, total saison à domicile. |
| `ws_home_xg_diff_for` | DOUBLE | 100.0% | 0 | 350 | moy -2.071 · méd -2.295 · min/max -17.41/14.98 · p10/p90 -8.334/4.354 | Différentiel offensif = buts marqués − xG produit, total saison à domicile (positif = sur-performance à la finition). |
| `ws_home_shots_for` | DOUBLE | 100.0% | 0 | 156 | moy 256.667 · méd 247.0 · min/max 155.0/478.0 · p10/p90 207.0/317.1 | Tirs tentés, total saison à domicile. |
| `ws_home_xg_per_shot_for` | DOUBLE | 100.0% | 0 | 10 | moy 0.115 · méd 0.11 · min/max 0.07/0.16 · p10/p90 0.099/0.14 | xG par tir = xg_for / shots_for (à domicile). |
| `ws_home_xg_for_rating` | DOUBLE | 100.0% | 0 | 68 | moy 6.669 · méd 6.65 · min/max 6.33/7.14 · p10/p90 6.5/6.86 | Note WhoScored globale de l'équipe (0-10, à domicile). Les 4 colonnes *_rating d'un même côté sont IDENTIQUES par nature : le Rating WhoScored est la note GLOBALE de l'équipe, affichée à l'identique sur les onglets Defensive/Offensive/xG (seules les stats du tableau changent, pas la note). Ce n'est PAS un bug. La note 'overall' du site = moyenne domicile/extérieur. |
| `source` | VARCHAR | 100.0% | 0 | 1 | top: whoscored (390) | Source des données (WhoScored). |
| `scraped_at` | VARCHAR | 100.0% | 0 | 20 |  | Horodatage ISO du scrape. |
| `comp_category` | VARCHAR | 100.0% | 0 | 1 | top: Big5 (390) | Catégorie de compétition ; ici uniquement 'Big5' (les 5 grands championnats). |
| `raw_team` | VARCHAR | 100.0% | 0 | 123 |  | Nom d'équipe brut WhoScored, avant normalisation via team_mapping. |