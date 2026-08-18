---
schema: gold
rows: 47137105
---
# features_draw

#gold

20 features (F1-F20) de détection de match nul + features comparatives équipe vs adversaire. Calculées depuis intermediate.backbone et gold.features_rolling. Certaines features WhoScored (F6, F13, F14) sont NULL en attente de migration whoscored.py.


## Intégrité
**Clé déclarée :** (match_id, team_id) — ⚠️ **47090218 doublons**

## Lineage
**Sources :** [[backbone]], [[events_qual]], [[features_rolling]], [[int_whoscored_events]], [[int_whoscored_match_index]], [[team_features_ws]]
**Alimente :** [[features_final]]

## Features & profiling  (47137105 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 28499 |  |  |
| `date` | DATE | 100.0% | 0 | 2417 |  | Date du match |
| `team_id` | BIGINT | 100.0% | 0 | 153 |  |  |
| `league_source` | VARCHAR | 100.0% | 0 | 28 |  | Compétition source |
| `opponent_id` | BIGINT | 100.0% | 11138 | 426 |  |  |
| `venue` | VARCHAR | 100.0% | 0 | 3 | top: Away (23553971), Home (23551370), Neutral (31764) |  |
| `season` | VARCHAR | 100.0% | 0 | 9 | top: 2017-2018 (5576819), 2018-2019 (5559732), 2021-2022 (5434296) |  |
| `f1_mutual_cancel_idx` | DOUBLE | 89.9% | 4766783 | 15496 | moy 195960.338 · méd 175154.078 · min/max 18733.809/2788137.819 · p10/p90 99788.246/310279.331 | Index d'annulation mutuelle — stérilité × stérilité adverse / (save_rate + save_rate adverse) |
| `f2_defensive_mirror` | DOUBLE | 62.8% | 17551259 | 10965 | moy 0.918 · méd 0.932 · min/max 0.375/1.0 · p10/p90 0.832/0.987 | Symétrie défensive — 1 - /zone_att_pct - zone_att_pct adverse/ |
| `f3_draw_market_dev` | DOUBLE | 38.9% | 28821144 | 13391 | moy -0.001 · méd 0.01 · min/max -0.199/0.203 · p10/p90 -0.076/0.053 | Déviation cote nul vs taux de nul historique de la ligue |
| `f4_momentum_convergence` | DOUBLE | 34.0% | 31111219 | 5949 | moy 0.393 · méd 0.661 · min/max -30.563/1.0 · p10/p90 -0.093/0.94 | Convergence de momentum — 1 - /ws_momentum_delta - ws_momentum_delta adverse/ |
| `f5_cs_mutual_rate` | DOUBLE | 95.9% | 1952825 | 52 | moy 0.076 · méd 0.04 · min/max 0.0/2.0 · p10/p90 0.0/0.24 | Taux de clean sheet mutuel — cs_rate × cs_rate adverse |
| `f6_ht_draw_tendency` | DOUBLE | 71.8% | 13289777 | 2 | moy 0.067 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.0 | Tendance au nul à la mi-temps (WhoScored) — NULL en attente migration whoscored.py |
| `f7_off_def_mismatch` | DOUBLE | 34.8% | 30712395 | 1963 | moy -0.0 · méd 0.0 · min/max -0.58/0.58 · p10/p90 -0.21/0.21 | Écart rating offensif équipe vs rating défensif adverse |
| `f7_def_off_mismatch` | DOUBLE | 34.8% | 30712395 | 1963 | moy -0.0 · méd 0.0 · min/max -0.58/0.58 · p10/p90 -0.21/0.21 | Écart rating offensif adverse vs rating défensif équipe |
| `f8_press_dominance_ratio` | DOUBLE | 90.3% | 4580751 | 31132 | moy -0.0 · méd 0.0 · min/max -2.512/2.512 · p10/p90 -0.521/0.521 | Ratio de domination du pressing (ln(PPDA adverse / PPDA équipe)) |
| `f9_chance_quality_gap` | DOUBLE | 90.1% | 4682439 | 31054 | moy -0.0 · méd 0.0 · min/max -0.41/0.41 · p10/p90 -0.046/0.046 | Écart de qualité des occasions (sqr équipe - sqr adverse) |
| `f10_venue_power_adj` | DOUBLE | 87.8% | 5730139 | 30169 | moy -0.001 · méd -0.0 · min/max -2.182/2.384 · p10/p90 -0.49/0.488 | Ajustement de puissance venue (xg_venue_5 - xg_global_5) |
| `f11_comeback_rate` | DOUBLE | 0.0% | 47137105 | 0 | moy None · méd None · min/max None/None · p10/p90 None/None | Taux de remontée au score sur les 5 derniers matchs |
| `f12_red_card_resilience` | DOUBLE | 70.5% | 13913343 | 67 | moy 0.961 · méd 0.8 · min/max 0.0/3.0 · p10/p90 0.0/3.0 | Points par match quand l'équipe a reçu un carton rouge |
| `f13_late_goal_tendency` | DOUBLE | 0.0% | 47137105 | 0 | moy None · méd None · min/max None/None · p10/p90 None/None | Tendance aux buts tardifs (WhoScored) — NULL en attente migration |
| `f14_goal_timing_variance` | DOUBLE | 0.0% | 47137105 | 0 | moy None · méd None · min/max None/None · p10/p90 None/None | Variance du timing des buts (WhoScored) — NULL en attente migration |
| `f15_xg_yield_ratio` | DOUBLE | 90.4% | 4538007 | 30622 | moy 1.072 · méd 1.043 · min/max 0.0/11.119 · p10/p90 0.56/1.599 | Ratio buts / xG (surperformance offensive) |
| `f16_def_yield_ratio` | DOUBLE | 90.4% | 4538007 | 30602 | moy 1.075 · méd 1.046 · min/max 0.0/11.119 · p10/p90 0.57/1.597 | Ratio buts concédés / xG concédé (surperformance défensive) |
| `f17_shots_to_goal_eff` | DOUBLE | 95.8% | 1984226 | 1501 | moy 0.111 · méd 0.107 · min/max 0.0/3.0 · p10/p90 0.051/0.175 | Efficacité tirs → buts (gf / shots_total) |
| `f18_sot_conversion` | DOUBLE | 95.7% | 2021016 | 670 | moy 0.317 · méd 0.313 · min/max 0.0/7.0 · p10/p90 0.167/0.471 | Conversion tirs cadrés → buts (gf / sot) |
| `f19_tactical_lock_idx` | DOUBLE | 90.1% | 4682439 | 15527 | moy 408.599 · méd 264.595 · min/max 8.334/5267.586 · p10/p90 97.704/928.912 | Index de verrouillage tactique — stérilité mutuelle × équilibre pressing × symétrie zone |
| `f20_upset_composite` | DOUBLE | 42.3% | 27190369 | 14413 | moy 1.077 · méd 0.718 · min/max 0.0/35.687 · p10/p90 0.274/2.189 | Index d'upset — (1/prob_victoire) × surperformance adverse × comeback_rate adverse |