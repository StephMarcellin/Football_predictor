---
schema: intermediate
rows: 47208
---
# int_fbref_schedule

#intermediate

Feuille de match FBref (résultat, buts marqués/encaissés, possession, formation) rematchée sur match_id + team_id. Source des résultats bruts et de la possession dans backbone.


## Intégrité
**Clé déclarée :** — (pas de test d'unicité)

## Lineage
**Sources :** [[intermediate.match_registry]], [[silver.fbref_schedule]], [[team_mapping]]
**Alimente :** [[backbone]]

## Features & profiling  (47208 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 28710 |  | Identifiant unifié du match (SHA1), rattaché via match_registry. |
| `team_id` | BIGINT | 100.0% | 0 | 153 |  | Id canonique de l'équipe (converti depuis le nom de club FBref via team_mapping). |
| `opponent_id` | BIGINT | 97.7% | 1072 | 426 |  | Id canonique de l'équipe adverse (via team_mapping). |
| `date` | DATE | 100.0% | 0 | 2437 |  | Date du match |
| `time` | VARCHAR | 100.0% | 0 | 156 |  | Heure du match |
| `league_source` | VARCHAR | 100.0% | 0 | 28 |  | Nom de la compétition source (ex: Premier League, Ligue 1) |
| `day` | VARCHAR | 100.0% | 0 | 7 | top: Sat (16394), Sun (13648), Wed (4981) | Jour de la semaine |
| `venue` | VARCHAR | 100.0% | 0 | 3 | top: Away (23819), Home (23157), Neutral (232) | Lieu du match du point de vue de l'équipe : Home ou Away |
| `result` | VARCHAR | 100.0% | 0 | 4 | top: W (18906), L (15932), D (11277) | Résultat brut FBref : W (victoire), D (nul), L (défaite) |
| `gf` | INTEGER | 100.0% | 0 | 14 | moy 1.443 · méd 1.0 · min/max 0.0/14.0 · p10/p90 0.0/3.0 | Buts marqués par l'équipe |
| `ga` | INTEGER | 100.0% | 0 | 10 | moy 1.29 · méd 1.0 · min/max 0.0/9.0 · p10/p90 0.0/3.0 | Buts encaissés par l'équipe |
| `poss` | VARCHAR | 100.0% | 0 | 74 |  | Possession en pourcentage (VARCHAR — castée en DOUBLE dans gold) |
| `formation` | VARCHAR | 100.0% | 0 | 29 |  | Formation tactique de l'équipe (ex: 4-3-3) |
| `opp_formation` | VARCHAR | 100.0% | 0 | 28 |  | Formation tactique de l'adversaire |
| `season` | VARCHAR | 100.0% | 0 | 9 | top: 2018-2019 (5623), 2017-2018 (5545), 2019-2020 (5421) | Saison au format YYYY-YYYY |
| `source` | VARCHAR | 100.0% | 0 | 1 | top: fbref (47208) | Source de la donnée (fbref) |
| `scraped_at` | VARCHAR | 100.0% | 0 | 1030 |  | Timestamp du scraping |
| `comp_category` | VARCHAR | 100.0% | 0 | 5 | top: Big5 (32426), D2 (5993), Europe (4565) | Catégorie de compétition : Big5, Cup, Europe |
| `result_1n2` | VARCHAR | 97.7% | 1093 | 3 | top: H (20164), A (14674), D (11277) | Résultat encodé du point de vue de l'équipe : H (victoire domicile), D (nul), A (victoire extérieur) |