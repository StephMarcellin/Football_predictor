---
schema: intermediate
rows: 46949
---
# backbone

#intermediate

Table pivot centrale — 1 ligne par équipe par match. Jointure de 7 tables silver.* produite par dbt en mode incremental. Schéma intermediate — consommée par les modèles gold.features_*.


## Intégrité
**Clé déclarée :** (match_id, team_id) — ✅ aucun doublon

## Lineage
**Sources :** [[int_fbref_keeper]], [[int_fbref_misc]], [[int_fbref_schedule]], [[int_fbref_shooting]], [[int_odds]], [[int_understat_schedule]], [[int_understat_stats]], [[int_whoscored_team_season]]
**Alimente :** [[equipe_match]], [[features_draw]], [[features_rolling]], [[features_whoscored]], [[h2h_history]], [[joueur_match]], [[rolling_corners]], [[rolling_freekicks]], [[team_corridor_profile]]

## Features & profiling  (46949 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 28561 |  | Identifiant Understat du match — NULL si le match n'existe pas dans Understat |
| `team_id` | BIGINT | 100.0% | 0 | 153 |  | Nom normalisé de l'équipe |
| `opponent_id` | BIGINT | 97.7% | 1067 | 426 |  | Nom normalisé de l'adversaire |
| `date` | DATE | 100.0% | 0 | 2417 |  | Date du match |
| `venue` | VARCHAR | 100.0% | 0 | 3 | top: Away (23691), Home (23026), Neutral (232) | Lieu du match du point de vue de l'équipe : Home ou Away |
| `season` | VARCHAR | 100.0% | 0 | 9 | top: 2018-2019 (5623), 2017-2018 (5545), 2021-2022 (5379) | Saison au format YYYY-YYYY |
| `league_source` | VARCHAR | 100.0% | 0 | 28 |  | Nom de la compétition source (ex: Premier League, Ligue 1) |
| `comp_category` | VARCHAR | 100.0% | 0 | 5 | top: Big5 (32218), D2 (5942), Europe (4565) | Catégorie de compétition : Big5, Cup, Europe |
| `result_1n2` | VARCHAR | 98.2% | 841 | 3 | top: H (20159), A (14674), D (11275) | Résultat du point de vue de l'équipe : H (victoire), D (nul), A (défaite) |
| `formation` | VARCHAR | 100.0% | 0 | 29 |  | Formation tactique de l'équipe (ex: 4-3-3) |
| `gf` | INTEGER | 100.0% | 0 | 14 | moy 1.451 · méd 1.0 · min/max 0.0/14.0 · p10/p90 0.0/3.0 | Buts marqués par l'équipe |
| `ga` | INTEGER | 100.0% | 0 | 10 | moy 1.297 · méd 1.0 · min/max 0.0/9.0 · p10/p90 0.0/3.0 | Buts encaissés par l'équipe |
| `possession` | DOUBLE | 95.7% | 1998 | 73 | moy 51.159 · méd 51.0 · min/max 15.0/100.0 · p10/p90 36.0/66.0 | Possession en pourcentage (castée depuis VARCHAR fbref_schedule) |
| `shots_on_target_faced` | INTEGER | 98.1% | 904 | 21 | moy 3.919 · méd 4.0 · min/max 0.0/20.0 · p10/p90 1.0/7.0 | Tirs cadrés subis par le gardien (sota) |
| `saves` | INTEGER | 98.1% | 904 | 19 | moy 2.754 · méd 2.0 · min/max 0.0/19.0 · p10/p90 0.0/5.0 | Arrêts du gardien |
| `save_pct` | DOUBLE | 92.1% | 3704 | 74 | moy 69.307 · méd 71.4 · min/max -200.0/100.0 · p10/p90 33.3/100.0 | Pourcentage d'arrêts (saves / shots_on_target_faced) |
| `clean_sheet` | INTEGER | 98.1% | 904 | 2 | moy 0.285 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | 1 si aucun but encaissé, 0 sinon |
| `shots_total` | INTEGER | 98.1% | 904 | 45 | moy 12.394 · méd 12.0 · min/max 0.0/47.0 · p10/p90 6.0/20.0 | Total de tirs tentés |
| `shots_on_target` | INTEGER | 98.1% | 904 | 25 | moy 4.346 · méd 4.0 · min/max 0.0/25.0 · p10/p90 1.0/8.0 | Tirs cadrés |
| `goals_per_shot` | DOUBLE | 94.0% | 2815 | 59 | moy 0.107 · méd 0.09 · min/max 0.0/1.0 · p10/p90 0.0/0.25 | Buts par tir (efficacité offensive brute) |
| `yellow_cards` | INTEGER | 98.1% | 904 | 10 | moy 2.022 · méd 2.0 · min/max 0.0/9.0 · p10/p90 0.0/4.0 | Cartons jaunes reçus |
| `second_yellow_cards` | INTEGER | 98.1% | 904 | 3 | moy 0.042 · méd 0.0 · min/max 0.0/2.0 · p10/p90 0.0/0.0 | Deuxièmes cartons jaunes (expulsion indirecte) |
| `red_cards` | INTEGER | 98.1% | 904 | 4 | moy 0.095 · méd 0.0 · min/max 0.0/3.0 · p10/p90 0.0/0.0 | Cartons rouges directs reçus |
| `fouls_committed` | INTEGER | 98.1% | 904 | 32 | moy 11.793 · méd 12.0 · min/max 0.0/31.0 · p10/p90 6.0/18.0 | Fautes commises |
| `interceptions` | INTEGER | 98.1% | 904 | 34 | moy 9.53 · méd 9.0 · min/max 0.0/38.0 · p10/p90 4.0/16.0 | Interceptions |
| `tackles_won` | INTEGER | 98.1% | 904 | 32 | moy 9.655 · méd 10.0 · min/max 0.0/31.0 · p10/p90 5.0/15.0 | Tacles remportés |
| `np_xg` | DOUBLE | 67.2% | 15391 | 30539 | moy 1.308 · méd 1.148 · min/max 0.0/7.11 · p10/p90 0.392/2.436 | Expected goals sans pénalties du point de vue de l'équipe (Understat) — NULL si match hors Understat |
| `np_xg_conceded` | DOUBLE | 67.2% | 15391 | 30539 | moy 1.308 · méd 1.148 · min/max 0.0/7.11 · p10/p90 0.393/2.436 | Expected goals sans pénalties concédés — NULL si match hors Understat |
| `ppda` | DOUBLE | 67.2% | 15393 | 6981 | moy 12.459 · méd 10.893 · min/max 2.083/193.0 · p10/p90 6.075/20.421 | PPDA de l'équipe — passes adverses autorisées par action défensive (mesure le pressing) |
| `ppda_allowed` | DOUBLE | 67.2% | 15393 | 6982 | moy 12.459 · méd 10.893 · min/max 2.083/193.0 · p10/p90 6.075/20.421 | PPDA autorisé à l'adversaire — pressing subi |
| `np_xg_diff_match` | DOUBLE | 67.2% | 15391 | 31437 | moy -0.0 · méd -0.0 · min/max -6.271/6.271 · p10/p90 -1.619/1.619 | Différentiel npxG du match (np_xg - np_xg_conceded) — peut être négatif |
| `season_att_rating` | DOUBLE | 30.8% | 32487 | 76 | moy 6.624 · méd 6.61 · min/max 6.26/7.14 · p10/p90 6.45/6.82 | Rating offensif WhoScored de la saison selon le venue (Home ou Away) |
| `season_def_rating` | DOUBLE | 30.8% | 32487 | 76 | moy 6.624 · méd 6.61 · min/max 6.26/7.14 · p10/p90 6.45/6.82 | Rating défensif WhoScored de la saison selon le venue |
| `ws_dribbles_pg` | DOUBLE | 30.8% | 32487 | 95 | moy 8.286 · méd 8.1 · min/max 4.1/15.5 · p10/p90 5.9/10.8 | Dribbles réussis par match selon le venue |
| `ws_fouled_pg` | DOUBLE | 30.8% | 32487 | 84 | moy 11.333 · méd 11.4 · min/max 6.9/16.0 · p10/p90 9.3/13.3 | Fautes subies par match selon le venue |
| `ws_shots_ot_pg` | DOUBLE | 30.8% | 32487 | 63 | moy 4.384 · méd 4.2 · min/max 2.2/9.4 · p10/p90 3.1/5.9 | Tirs cadrés par match selon le venue |
| `odds_pinnacle_team` | DOUBLE | 32.5% | 31705 | 994 | moy 2.858 · méd 2.28 · min/max 1.05/27.0 · p10/p90 1.38/4.967 | Cote Pinnacle victoire de l'équipe (H si Home, A si Away) |
| `odds_pinnacle_draw` | DOUBLE | 32.5% | 31705 | 792 | moy 4.19 · méd 3.74 · min/max 2.08/20.38 · p10/p90 3.23/5.66 | Cote Pinnacle match nul |
| `odds_pinnacle_opp` | DOUBLE | 32.5% | 31705 | 1612 | moy 4.63 · méd 3.4 · min/max 1.11/42.94 · p10/p90 1.73/8.75 | Cote Pinnacle victoire de l'adversaire |
| `odds_avg_team` | DOUBLE | 24.7% | 35351 | 879 | moy 2.778 · méd 2.26 · min/max 1.05/22.54 · p10/p90 1.38/4.753 | Cote moyenne marché victoire de l'équipe |
| `odds_avg_draw` | DOUBLE | 24.7% | 35351 | 672 | moy 4.068 · méd 3.68 · min/max 2.09/16.44 · p10/p90 3.2/5.37 | Cote moyenne marché match nul |
| `odds_avg_opp` | DOUBLE | 24.7% | 35351 | 1363 | moy 4.327 · méd 3.26 · min/max 1.1/38.04 · p10/p90 1.72/8.103 | Cote moyenne marché victoire adversaire |
| `pinnacle_prob_team` | DOUBLE | 32.5% | 31705 | 14957 | moy 0.441 · méd 0.427 · min/max 0.036/0.925 · p10/p90 0.196/0.704 | Probabilité implicite Pinnacle victoire de l'équipe |
| `pinnacle_prob_draw` | DOUBLE | 32.5% | 31705 | 14850 | moy 0.247 · méd 0.26 · min/max 0.047/0.467 · p10/p90 0.171/0.302 | Probabilité implicite Pinnacle match nul |
| `pinnacle_prob_opp` | DOUBLE | 32.5% | 31705 | 14982 | moy 0.312 · méd 0.286 · min/max 0.023/0.883 · p10/p90 0.111/0.561 | Probabilité implicite Pinnacle victoire adversaire |
| `market_prob_team` | DOUBLE | 24.7% | 35351 | 11444 | moy 0.436 · méd 0.422 · min/max 0.043/0.916 · p10/p90 0.201/0.693 | Probabilité implicite marché moyen victoire de l'équipe |
| `market_prob_draw` | DOUBLE | 24.7% | 35351 | 11383 | moy 0.248 · méd 0.259 · min/max 0.059/0.456 · p10/p90 0.178/0.298 | Probabilité implicite marché moyen match nul |
| `market_prob_opp` | DOUBLE | 24.7% | 35351 | 11455 | moy 0.316 · méd 0.292 · min/max 0.025/0.877 · p10/p90 0.118/0.554 | Probabilité implicite marché moyen victoire adversaire |