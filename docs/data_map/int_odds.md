---
schema: intermediate
rows: 71493
---
# int_odds

#intermediate

Cotes bookmakers (Pinnacle et moyennes marché) rematchées sur match_id + team_id/opponent_id. Alimente les colonnes odds_* et les probabilités implicites de backbone.


## Intégrité
**Clé déclarée :** — (pas de test d'unicité)

## Lineage
**Sources :** [[intermediate.match_registry]], [[silver.odds]], [[team_mapping]]
**Alimente :** [[backbone]]

## Features & profiling  (71493 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 15828 |  | Identifiant unifié du match (SHA1), rattaché via match_registry. |
| `team_id` | BIGINT | 100.0% | 0 | 153 |  | Id canonique de l'équipe à DOMICILE (via team_mapping sur home_team). |
| `opponent_id` | BIGINT | 100.0% | 0 | 153 |  | Id canonique de l'équipe à l'EXTÉRIEUR (via team_mapping sur away_team). |
| `date` | DATE | 100.0% | 0 | 1628 |  | Date du match |
| `season` | VARCHAR | 100.0% | 0 | 9 | top: 2017-2018 (9142), 2018-2019 (8542), 2020-2021 (8522) | Saison au format YYYY-YYYY |
| `league_source` | VARCHAR | 100.0% | 0 | 5 | top: Premier League (16341), Ligue 1 (15459), Serie A (14936) | Nom de la compétition source |
| `result_fdc` | VARCHAR | 100.0% | 0 | 3 | top: H (30994), A (22670), D (17829) | Résultat réel du match (H/D/A) — utilisé pour calculer l'edge en backtest |
| `odds_pinnacle_h` | DOUBLE | 96.9% | 2203 | 994 | moy 2.848 · méd 2.28 · min/max 1.05/27.0 · p10/p90 1.37/4.95 | Cote Pinnacle victoire domicile |
| `odds_pinnacle_d` | DOUBLE | 96.9% | 2203 | 792 | moy 4.214 · méd 3.75 · min/max 2.08/20.38 · p10/p90 3.24/5.7 | Cote Pinnacle match nul |
| `odds_pinnacle_a` | DOUBLE | 96.9% | 2203 | 1612 | moy 4.663 · méd 3.4 · min/max 1.11/42.94 · p10/p90 1.73/8.88 | Cote Pinnacle victoire extérieur |
| `odds_avg_h` | DOUBLE | 75.3% | 17685 | 885 | moy 2.766 · méd 2.25 · min/max 1.05/22.54 · p10/p90 1.38/4.71 | Cote moyenne du marché victoire domicile |
| `odds_avg_d` | DOUBLE | 75.3% | 17685 | 676 | moy 4.083 · méd 3.69 · min/max 2.09/16.44 · p10/p90 3.21/5.38 | Cote moyenne du marché match nul |
| `odds_avg_a` | DOUBLE | 75.3% | 17685 | 1373 | moy 4.326 · méd 3.26 · min/max 1.1/38.04 · p10/p90 1.72/8.09 | Cote moyenne du marché victoire extérieur |
| `odds_max_h` | DOUBLE | 75.3% | 17685 | 593 | moy 2.926 · méd 2.34 · min/max 1.07/29.0 · p10/p90 1.42/5.0 | Meilleure cote disponible sur le marché victoire domicile |
| `odds_max_d` | DOUBLE | 75.3% | 17685 | 480 | moy 4.324 · méd 3.87 · min/max 2.2/24.0 · p10/p90 3.35/5.75 | Meilleure cote disponible match nul |
| `odds_max_a` | DOUBLE | 75.3% | 17685 | 800 | moy 4.739 · méd 3.41 · min/max 1.13/56.0 · p10/p90 1.77/9.0 | Meilleure cote disponible victoire extérieur |
| `pinnacle_prob_h` | DOUBLE | 96.9% | 2203 | 14960 | moy 0.442 · méd 0.427 · min/max 0.036/0.925 · p10/p90 0.196/0.708 | Probabilité implicite Pinnacle victoire domicile (1/cote, marginée) |
| `pinnacle_prob_d` | DOUBLE | 96.9% | 2203 | 14853 | moy 0.246 · méd 0.259 · min/max 0.047/0.467 · p10/p90 0.17/0.301 | Probabilité implicite Pinnacle match nul |
| `pinnacle_prob_a` | DOUBLE | 96.9% | 2203 | 14985 | moy 0.311 · méd 0.286 · min/max 0.023/0.883 · p10/p90 0.109/0.561 | Probabilité implicite Pinnacle victoire extérieur |
| `market_prob_h` | DOUBLE | 75.3% | 17685 | 12017 | moy 0.437 · méd 0.423 · min/max 0.043/0.916 · p10/p90 0.203/0.694 | Probabilité implicite marché moyen victoire domicile |
| `market_prob_d` | DOUBLE | 75.3% | 17685 | 11955 | moy 0.247 · méd 0.259 · min/max 0.059/0.456 · p10/p90 0.177/0.297 | Probabilité implicite marché moyen match nul |
| `market_prob_a` | DOUBLE | 75.3% | 17685 | 12029 | moy 0.316 · méd 0.293 · min/max 0.025/0.877 · p10/p90 0.118/0.554 | Probabilité implicite marché moyen victoire extérieur |