---
schema: intermediate
rows: 46052
---
# int_fbref_shooting

#intermediate

Volume de tirs FBref (tirs totaux, tirs cadrés, buts par tir) rematché sur match_id + team_id. Alimente le bloc tirs de backbone.


## Intégrité
**Clé déclarée :** — (pas de test d'unicité)

## Lineage
**Sources :** [[intermediate.match_registry]], [[silver.fbref_shooting]], [[team_mapping]]
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
| `standard_gls` | INTEGER | 100.0% | 0 | 14 | moy 1.434 · méd 1.0 · min/max 0.0/14.0 · p10/p90 0.0/3.0 | Buts marqués (hors pénaltys) |
| `standard_sh` | INTEGER | 100.0% | 0 | 45 | moy 12.394 · méd 12.0 · min/max 0.0/47.0 · p10/p90 6.0/20.0 | Total de tirs tentés |
| `standard_sot` | INTEGER | 100.0% | 0 | 25 | moy 4.346 · méd 4.0 · min/max 0.0/25.0 · p10/p90 1.0/8.0 | Tirs cadrés |
| `standard_sot_pct` | DOUBLE | 95.9% | 1911 | 262 | moy 35.555 · méd 33.3 · min/max 0.0/100.0 · p10/p90 16.7/55.6 | Pourcentage de tirs cadrés (standard_sot / standard_sh) |
| `standard_g_sh` | DOUBLE | 95.9% | 1911 | 59 | moy 0.107 · méd 0.09 · min/max 0.0/1.0 · p10/p90 0.0/0.25 | Buts par tir (efficacité offensive brute) |
| `standard_g_sot` | DOUBLE | 93.7% | 2898 | 65 | moy 0.289 · méd 0.25 · min/max 0.0/2.0 · p10/p90 0.0/0.6 | Buts par tir cadré (efficacité offensive sur les tirs dangereux) |
| `standard_pk` | INTEGER | 100.0% | 0 | 4 | moy 0.129 · méd 0.0 · min/max 0.0/3.0 · p10/p90 0.0/1.0 | Buts sur penalty |
| `standard_pkatt` | INTEGER | 100.0% | 0 | 4 | moy 0.164 · méd 0.0 · min/max 0.0/3.0 · p10/p90 0.0/1.0 | Penaltys tentés |
| `season` | VARCHAR | 100.0% | 0 | 9 | top: 2018-2019 (5613), 2017-2018 (5542), 2021-2022 (5371) | Saison au format YYYY-YYYY |
| `source` | VARCHAR | 100.0% | 0 | 1 | top: fbref (46052) | Source de la donnée (fbref) |
| `scraped_at` | VARCHAR | 100.0% | 0 | 1030 |  | Timestamp du scraping |
| `comp_category` | VARCHAR | 100.0% | 0 | 5 | top: Big5 (31488), D2 (5937), Europe (4488) | Catégorie de compétition : Big5, Cup, Europe |
| `result_1n2` | VARCHAR | 100.0% | 0 | 3 | top: H (20157), A (14631), D (11264) | Résultat encodé : H, D, A |