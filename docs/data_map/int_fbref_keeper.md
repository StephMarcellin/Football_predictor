---
schema: intermediate
rows: 46052
---
# int_fbref_keeper

#intermediate

Stats gardien FBref (tirs cadrés subis, arrêts, save%) rematchées sur match_id unifié et team_id normalisé. Alimente les colonnes gardien de backbone.


## Intégrité
**Clé déclarée :** — (pas de test d'unicité)

## Lineage
**Sources :** [[intermediate.match_registry]], [[silver.fbref_keeper]], [[team_mapping]]
**Alimente :** [[backbone]]

## Features & profiling  (46052 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 28060 |  | Identifiant unifié du match (SHA1), rattaché via match_registry. |
| `team_id` | BIGINT | 100.0% | 0 | 153 |  | Id canonique de l'équipe (converti depuis le nom de club FBref via team_mapping). |
| `opponent_id` | BIGINT | 97.8% | 1022 | 426 |  | Id canonique de l'équipe adverse (via team_mapping). |
| `date` | DATE | 100.0% | 0 | 2366 |  | Date du match |
| `time` | VARCHAR | 100.0% | 0 | 155 |  | Heure du match |
| `league_source` | VARCHAR | 100.0% | 0 | 28 |  | Nom de la compétition source |
| `day` | VARCHAR | 100.0% | 0 | 7 | top: Sat (15963), Sun (13212), Wed (4906) | Jour de la semaine |
| `venue` | VARCHAR | 100.0% | 0 | 3 | top: Away (23220), Home (22600), Neutral (232) | Lieu du match : Home ou Away |
| `result` | VARCHAR | 100.0% | 0 | 3 | top: W (18861), L (15927), D (11264) | Résultat brut FBref : W, D, L |
| `gf` | INTEGER | 100.0% | 0 | 14 | moy 1.476 · méd 1.0 · min/max 0.0/14.0 · p10/p90 0.0/3.0 | Buts marqués |
| `ga` | INTEGER | 100.0% | 0 | 10 | moy 1.322 · méd 1.0 · min/max 0.0/9.0 · p10/p90 0.0/3.0 | Buts encaissés |
| `sota` | INTEGER | 100.0% | 0 | 21 | moy 3.919 · méd 4.0 · min/max 0.0/20.0 · p10/p90 1.0/7.0 | Shots on target against — tirs cadrés subis par le gardien |
| `ga_keeper` | INTEGER | 100.0% | 0 | 11 | moy 1.322 · méd 1.0 · min/max 0.0/10.0 · p10/p90 0.0/3.0 | Buts encaissés par le gardien (hors CSC) |
| `saves` | INTEGER | 100.0% | 0 | 19 | moy 2.754 · méd 2.0 · min/max 0.0/19.0 · p10/p90 0.0/5.0 | Arrêts du gardien |
| `save_pct` | DOUBLE | 93.9% | 2800 | 74 | moy 69.309 · méd 71.4 · min/max -200.0/100.0 · p10/p90 33.3/100.0 | Pourcentage d'arrêts (saves / sota) |
| `pk_att` | INTEGER | 100.0% | 0 | 5 | moy 0.153 · méd 0.0 · min/max 0.0/4.0 · p10/p90 0.0/1.0 | Penaltys tentés contre le gardien |
| `pk_allowed` | INTEGER | 100.0% | 0 | 4 | moy 0.119 · méd 0.0 · min/max 0.0/3.0 · p10/p90 0.0/1.0 | Penaltys accordés (transformés contre) |
| `pk_saved` | INTEGER | 100.0% | 0 | 5 | moy 0.025 · méd 0.0 · min/max 0.0/4.0 · p10/p90 0.0/0.0 | Penaltys arrêtés |
| `pk_missed` | INTEGER | 100.0% | 0 | 3 | moy 0.009 · méd 0.0 · min/max 0.0/2.0 · p10/p90 0.0/0.0 | Penaltys manqués par l'attaquant adverse |
| `season` | VARCHAR | 100.0% | 0 | 9 | top: 2018-2019 (5613), 2017-2018 (5542), 2021-2022 (5371) | Saison au format YYYY-YYYY |
| `source` | VARCHAR | 100.0% | 0 | 1 | top: fbref (46052) | Source de la donnée (fbref) |
| `scraped_at` | VARCHAR | 100.0% | 0 | 1030 |  | Timestamp du scraping |
| `comp_category` | VARCHAR | 100.0% | 0 | 5 | top: Big5 (31488), D2 (5937), Europe (4488) | Catégorie de compétition : Big5, Cup, Europe |
| `result_1n2` | VARCHAR | 100.0% | 0 | 3 | top: H (20157), A (14631), D (11264) | Résultat encodé : H, D, A |
| `cs` | INTEGER | 100.0% | 0 | 2 | moy 0.285 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | Clean sheet — 1 si aucun but encaissé, 0 sinon. Dérivé du champ autoritaire ga_keeper (garde-fou : la colonne silver.cs est salie, valeurs 2 + incohérences, cf. troubleshooting). |