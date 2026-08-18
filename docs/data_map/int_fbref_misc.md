---
schema: intermediate
rows: 46052
---
# int_fbref_misc

#intermediate

Stats disciplinaires et défensives FBref (cartons, fautes commises, interceptions, tacles gagnés) rematchées sur match_id + team_id. Alimente le bloc disciplinaire de backbone.


## Intégrité
**Clé déclarée :** — (pas de test d'unicité)

## Lineage
**Sources :** [[intermediate.match_registry]], [[silver.fbref_misc]], [[team_mapping]]
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
| `crdy` | INTEGER | 100.0% | 0 | 10 | moy 2.022 · méd 2.0 · min/max 0.0/9.0 · p10/p90 0.0/4.0 | Cartons jaunes reçus |
| `crdr` | INTEGER | 100.0% | 0 | 4 | moy 0.095 · méd 0.0 · min/max 0.0/3.0 · p10/p90 0.0/0.0 | Cartons rouges directs reçus |
| `crdy2` | INTEGER | 100.0% | 0 | 3 | moy 0.042 · méd 0.0 · min/max 0.0/2.0 · p10/p90 0.0/0.0 | Deuxièmes cartons jaunes (expulsion indirecte) |
| `fls` | INTEGER | 100.0% | 0 | 32 | moy 11.794 · méd 12.0 · min/max 0.0/31.0 · p10/p90 6.0/18.0 | Fautes commises |
| `fld` | INTEGER | 100.0% | 0 | 32 | moy 11.234 · méd 11.0 · min/max 0.0/32.0 · p10/p90 6.0/17.0 | Fautes subies |
| `off` | INTEGER | 100.0% | 0 | 14 | moy 1.789 · méd 1.0 · min/max 0.0/14.0 · p10/p90 0.0/4.0 | Hors-jeux |
| `crosses` | INTEGER | 100.0% | 0 | 66 | moy 17.372 · méd 17.0 · min/max 0.0/74.0 · p10/p90 7.0/29.0 | Centres tentés |
| `int` | INTEGER | 100.0% | 0 | 34 | moy 9.53 · méd 9.0 · min/max 0.0/38.0 · p10/p90 4.0/16.0 | Interceptions |
| `tklw` | INTEGER | 100.0% | 0 | 32 | moy 9.655 · méd 10.0 · min/max 0.0/31.0 · p10/p90 5.0/15.0 | Tacles remportés |
| `pkwon` | INTEGER | 100.0% | 0 | 4 | moy 0.025 · méd 0.0 · min/max 0.0/3.0 · p10/p90 0.0/0.0 | Penaltys obtenus |
| `pkcon` | INTEGER | 100.0% | 0 | 5 | moy 0.03 · méd 0.0 · min/max 0.0/4.0 · p10/p90 0.0/0.0 | Penaltys concédés |
| `og` | INTEGER | 100.0% | 0 | 3 | moy 0.039 · méd 0.0 · min/max 0.0/2.0 · p10/p90 0.0/0.0 | Buts contre son camp |
| `season` | VARCHAR | 100.0% | 0 | 9 | top: 2018-2019 (5613), 2017-2018 (5542), 2021-2022 (5371) | Saison au format YYYY-YYYY |
| `source` | VARCHAR | 100.0% | 0 | 1 | top: fbref (46052) | Source de la donnée (fbref) |
| `scraped_at` | VARCHAR | 100.0% | 0 | 1030 |  | Timestamp du scraping |
| `comp_category` | VARCHAR | 100.0% | 0 | 5 | top: Big5 (31488), D2 (5937), Europe (4488) | Catégorie de compétition : Big5, Cup, Europe |
| `result_1n2` | VARCHAR | 100.0% | 0 | 3 | top: H (20157), A (14631), D (11264) | Résultat encodé : H, D, A |