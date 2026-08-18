---
schema: intermediate
rows: 8809886
---
# int_xt_contributions

#intermediate

Contributions Expected Threat (xT) : xT ajouté par déplacement. Grain : 1 ligne par déplacement (int_xt_actions action_kind='move' avec destination). xT_ajouté = xT(case d'arrivée) − xT(case de départ), attribué au joueur/équipe ; positif = progression vers le but adverse. Grille lue depuis machine_learning.xt_grid (étalon global). Matérialisé en table.


## Intégrité
**Clé déclarée :** (match_id, row_num) — ✅ aucun doublon

## Lineage
**Sources :** [[int_xt_actions]], [[machine_learning.xt_grid]]
**Alimente :** —

## Features & profiling  (8809886 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 11030 |  | Identifiant unifié du match (SHA1). |
| `row_num` | INTEGER | 100.0% | 0 | 1920 | moy 766.452 · méd 753.0 · min/max 0.0/1922.0 · p10/p90 150.0/1396.0 | Index de l'événement dans le match. Avec match_id, clé unique. |
| `season` | VARCHAR | 100.0% | 0 | 8 | top: 2020-2021 (1173451), 2021-2022 (1142033), 2018-2019 (1138921) | Saison du match. |
| `league_source` | VARCHAR | 100.0% | 0 | 4 | top: Premier League (2378495), Serie A (2297722), Ligue 1 (2254998) | Championnat / source des données. |
| `team_id` | BIGINT | 100.0% | 16 | 120 |  | Équipe qui effectue le déplacement. |
| `player_id` | INTEGER | 100.0% | 0 | 5788 |  | Joueur qui effectue le déplacement (passeur ou dribbleur). |
| `col_from` | INTEGER | 100.0% | 0 | 16 | moy 7.515 · méd 7.0 · min/max 0.0/15.0 · p10/p90 3.0/12.0 | Colonne (0-15) de la case de départ. |
| `row_from` | INTEGER | 100.0% | 0 | 12 | moy 6.009 · méd 6.0 · min/max 0.0/11.0 · p10/p90 1.0/11.0 | Ligne (0-11) de la case de départ. |
| `col_to` | INTEGER | 100.0% | 0 | 16 | moy 7.955 · méd 8.0 · min/max 0.0/15.0 · p10/p90 4.0/13.0 | Colonne (0-15) de la case d'arrivée. |
| `row_to` | INTEGER | 100.0% | 0 | 12 | moy 6.026 · méd 6.0 · min/max 0.0/11.0 · p10/p90 1.0/11.0 | Ligne (0-11) de la case d'arrivée. |
| `xt_from` | DOUBLE | 100.0% | 0 | 192 | moy 0.006 · méd 0.004 · min/max 0.001/0.261 · p10/p90 0.002/0.012 | Valeur xT de la case de départ (machine_learning.xt_grid). |
| `xt_to` | DOUBLE | 100.0% | 0 | 192 | moy 0.008 · méd 0.004 · min/max 0.001/0.261 · p10/p90 0.002/0.014 | Valeur xT de la case d'arrivée (machine_learning.xt_grid). |
| `xt_added` | DOUBLE | 100.0% | 0 | 26091 | moy 0.002 · méd 0.0 · min/max -0.257/0.259 · p10/p90 -0.002/0.004 | xT ajouté = xt_to − xt_from. Positif = progression vers le but. |