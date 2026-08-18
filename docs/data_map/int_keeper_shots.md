---
schema: intermediate
rows: 137523
---
# int_keeper_shots

#intermediate

Attribution du gardien à chaque tir cadré subi — brique de int_keeper_psxg. Grain : un tir cadré (SavedShot + Goal, hors CSC/penalty), avec son xGOT (machine_learning.xgot_predictions) et le gardien qui l'a subi. Deux voies : tirs arrêtés → Save miroir relié au tir par le qualifier 233 (related_event_id est NULL) ; buts → GK (slot=1) de l'équipe qui défend, période de formation active. keeper_id = COALESCE(save, lineup). Matérialisé en table.


## Intégrité
**Clé déclarée :** (match_id, row_num) — ✅ aucun doublon

## Lineage
**Sources :** [[events_qual]], [[int_shot_placement]], [[int_whoscored_events]], [[int_whoscored_lineup]], [[int_whoscored_match_index]], [[machine_learning.xgot_predictions]]
**Alimente :** [[int_keeper_psxg]]

## Features & profiling  (137523 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 16577 |  | Identifiant unifié du match (SHA1). |
| `row_num` | INTEGER | 100.0% | 0 | 1818 | moy 832.901 · méd 855.0 · min/max 2.0/1905.0 · p10/p90 205.0/1426.8 | Index du tir dans le match. Avec match_id, clé unique. |
| `season` | VARCHAR | 100.0% | 0 | 8 | top: 2017-2018 (21340), 2018-2019 (17578), 2023-2024 (17447) | Saison du match (ex. 2023-2024). |
| `league_source` | VARCHAR | 100.0% | 0 | 7 | top: Premier League (25018), Serie A (23972), La Liga (23005) | Championnat / source des données. |
| `type_id` | INTEGER | 100.0% | 0 | 2 |  | Type du tir : 15 SavedShot, 16 Goal. |
| `is_goal` | BOOLEAN | 100.0% | 0 | 2 | top: False (96479), True (41044) | Booléen : le tir a fini au fond. |
| `xgot` | DOUBLE | 100.0% | 0 | 291 | moy 0.296 · méd 0.225 · min/max 0.0/1.0 · p10/p90 0.034/0.747 | Post-Shot xG (xGOT) du tir, calibré, dans [0,1] (propagé de xgot_predictions). |
| `def_team` | BIGINT | 100.0% | 19 | 175 | moy 202606100214.449 · méd 202606100213.0 · min/max 202606100001.0/202606100426.0 · p10/p90 202606100050.0/202606100393.0 | Id canonique de l'équipe qui défend (subit le tir). |
| `keeper_id` | BIGINT | 99.2% | 1067 | 645 |  | Gardien qui subit le tir (player_id WhoScored). COALESCE(Save miroir, GK lineup). NULL si non identifié (~lineup manquant) → exclu du PSxG.  |
| `attribution_method` | VARCHAR | 100.0% | 0 | 3 | top: save (96302), lineup (40154), none (1067) | Voie d'attribution : 'save' (qual 233), 'lineup' (GK de la période), 'none' (non identifié). |
| `def_gk_available` | BOOLEAN | 100.0% | 0 | 2 | top: True (134377), False (3146) | Booléen : l'équipe qui défend a un GK lineup dans ce match (donc buts ET arrêts attribuables). Garde-fou symétrique consommé par int_keeper_psxg — FALSE = tirs non fiables (arrêts sans risque de but), exclus de l'agrégat.  |