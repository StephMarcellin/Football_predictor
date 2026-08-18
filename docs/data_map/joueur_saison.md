---
schema: gold
rows: 489194
---
# joueur_saison

#gold #famille4

Grain (match_id, team_id, player_id) — snapshot as-of-date. Familles CDC 4 (source des agrégats du onze), 5/6 NON-zonales, 8 (carton), 10 (Buteurs). PASSE 1 : source player_match_stats seule ; passe 2 à venir (xgchain, penalties, coups francs, xGOT, part des tirs). Profil = 38 dernières apparitions strictement antérieures du joueur (fenêtre ROWS 38 PRECEDING AND 1 PRECEDING, match courant exclu). Volumes normalisés PER-90 (Σ stat / Σ minutes × 90) ; ratios exacts. Passe 1 (player_match_stats) + passe 2 (xgChain/xgBuildup, part des tirs, rôles penalty/coup franc). Reste : scorer_xgot_overperformance (feature 70).

## Intégrité
**Clé déclarée :** (match_id, team_id, player_id) — ✅ aucun doublon

## Lineage
**Sources :** [[freekick_profiles]], [[int_penalties]], [[player_match_stats]], [[player_xg_chain]]
**Alimente :** [[equipe_lineup_match]], [[joueur_match]]

## Features & profiling  (489194 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 16586 |  | Identifiant unifié du match (SHA1). |
| `team_id` | BIGINT | 100.0% | 0 | 175 |  | Id canonique de l'équipe du joueur pour ce match. |
| `player_id` | INTEGER | 100.0% | 0 | 8638 |  | Identifiant du joueur. |
| `date` | DATE | 100.0% | 0 | 1515 |  | Date du match courant (le profil porté est celui d'avant ce match). |
| `season` | VARCHAR | 100.0% | 0 | 8 | top: 2017-2018 (72734), 2021-2022 (63196), 2023-2024 (62252) | Saison du match courant. |
| `league_source` | VARCHAR | 100.0% | 0 | 7 | top: La Liga (89462), Serie A (87821), Premier League (84698) |  |
| `n_apps_lag` | BIGINT | 100.0% | 0 | 39 | moy 28.545 · méd 38.0 · min/max 0.0/38.0 · p10/p90 6.0/38.0 | Nombre d'apparitions du joueur dans la fenêtre (0 à 38). Signal de confiance ; 0 = première apparition (profil NULL) → imputation famille 11. |
| `minutes_lag` | HUGEINT | 98.2% | 8745 | 3710 | moy 2115.001 · méd 2330.0 · min/max 0.0/3717.0 · p10/p90 395.0/3434.0 | Minutes cumulées du joueur sur la fenêtre. Dénominateur du per-90 et signal de fiabilité (faible volume = profil bruité). |
| `scorer_xg_per90_lag` | DOUBLE | 98.2% | 8818 | 431371 | moy 0.076 · méd 0.055 · min/max 0.0/11.982 · p10/p90 0.004/0.173 | [Famille 10, feature 69] xG (contribution) par 90 minutes = Σ xG / Σ minutes × 90. |
| `scorer_shots_per90_lag` | DOUBLE | 98.2% | 8818 | 115442 | moy 1.231 · méd 0.951 · min/max 0.0/90.0 · p10/p90 0.08/2.623 | [Famille 10, feature 68] Tirs par 90 minutes. |
| `off_chances_created_per90_lag` | DOUBLE | 98.2% | 8818 | 92862 | moy 0.87 · méd 0.785 · min/max 0.0/90.0 · p10/p90 0.051/1.758 | [Famille 5] Occasions créées par 90 minutes. |
| `off_key_passes_per90_lag` | DOUBLE | 98.2% | 8818 | 93033 | moy 0.872 · méd 0.787 · min/max 0.0/90.0 · p10/p90 0.051/1.762 | [Famille 5] Passes clés par 90 minutes. |
| `off_xg_per_shot_lag` | DOUBLE | 89.7% | 50510 | 284570 | moy 0.061 · méd 0.058 · min/max 0.01/0.428 · p10/p90 0.041/0.087 | [Famille 5/10, feature 42] Qualité de tir = Σ xG / Σ tirs sur la fenêtre (ratio exact, indépendant des minutes). |
| `def_aerial_win_rate_lag` | DOUBLE | 96.8% | 15487 | 13238 | moy 0.486 · méd 0.481 · min/max 0.0/1.0 · p10/p90 0.256/0.7 | [Famille 6, feature 50] Taux de duels aériens gagnés = Σ gagnés / Σ duels (ratio exact). |
| `def_actions_per90_lag` | DOUBLE | 98.2% | 8818 | 151013 | moy 2.382 · méd 2.387 · min/max 0.0/90.0 · p10/p90 0.498/4.048 | [Famille 6] Actions défensives (tacles + interceptions) par 90 minutes — volume non-zonal. |
| `def_errors_per90_lag` | DOUBLE | 98.2% | 8818 | 9978 | moy 0.028 · méd 0.0 · min/max 0.0/15.0 · p10/p90 0.0/0.083 | [Famille 6, feature 49] Erreurs menant à un tir/but, par 90 minutes. |
| `player_card_propensity_lag` | DOUBLE | 98.2% | 8818 | 24423 | moy 0.189 · méd 0.161 · min/max 0.0/90.0 · p10/p90 0.0/0.358 | [Famille 8, feature 60] Cartons jaunes (2es jaunes inclus) par 90 minutes. |
| `off_xgchain_per90_lag` | DOUBLE | 98.2% | 8818 | 267712 | moy 1.71 · méd 1.733 · min/max 0.0/90.0 · p10/p90 0.0/3.639 | [Famille 5, feature 39] xgChain par 90 min — xG cumulé des possessions où le joueur est impliqué (Σ xgChain / Σ minutes × 90). |
| `off_xgbuildup_per90_lag` | DOUBLE | 98.2% | 8818 | 242414 | moy 0.979 · méd 0.996 · min/max 0.0/72.0 · p10/p90 0.0/2.07 | [Famille 5, feature 40] xgBuildup par 90 min — comme xgChain mais hors tir et passe clé (implication dans la construction pure). |
| `scorer_team_shot_share_lag` | DOUBLE | 98.2% | 8641 | 33597 | moy 0.068 · méd 0.055 · min/max 0.0/1.0 · p10/p90 0.006/0.146 | [Famille 10, feature 71] Part des tirs de l'équipe pris par le joueur = Σ tirs joueur / Σ tirs équipe sur la fenêtre. Point focal offensif. |
| `scorer_penalty_taker_lag` | HUGEINT | 98.2% | 8638 | 18 | moy 0.344 · méd 0.0 · min/max 0.0/17.0 · p10/p90 0.0/1.0 | [Famille 10, feature 73] Nombre de penaltys tirés par le joueur sur la fenêtre. Fort signal P(marque) : un tireur attitré convertit ~0,76 xG par penalty. |
| `scorer_freekick_taker_lag` | HUGEINT | 98.2% | 8638 | 225 | moy 17.43 · méd 4.0 · min/max 0.0/228.0 · p10/p90 0.0/55.0 | [Famille 10, feature 74] Nombre de coups francs tirés par le joueur sur la fenêtre (rôle de tireur sur CPA). À affiner en coups francs directs quand fk_type sera exploité. |
| `profile_confidence_flag` | VARCHAR | 100.0% | 0 | 4 | top: high (359198), medium (91400), low (29958) | [Famille 11, feature 79] Fiabilité du profil : high (n_apps_lag≥20) / medium (≥5) / low (≥1) / none (première apparition). Seuils par défaut, à calibrer. |