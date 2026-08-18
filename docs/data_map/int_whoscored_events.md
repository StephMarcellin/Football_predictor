---
schema: intermediate
rows: 26271517
---
# int_whoscored_events

#intermediate

Événements bruts WhoScored (silver.stg_whoscored_events) avec team_id normalisé et match_id unifié, résolus via int_whoscored_match_index. Point d'entrée de la chaîne événementielle. Grain : un événement.


## Intégrité
**Clé déclarée :** — (pas de test d'unicité)

## Lineage
**Sources :** [[int_whoscored_match_index]], [[silver.stg_whoscored_events]]
**Alimente :** [[events_qual]], [[features_draw]], [[int_defensive_blocks]], [[int_event_enriched]], [[int_fouls_drawn]], [[int_keeper_shots]], [[int_penalties]], [[int_shot_placement]], [[int_substitutions]], [[player_match_stats]], [[team_features_ws]]

## Features & profiling  (26271517 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 98.4% | 428481 | 16586 |  | Identifiant unifié du match (SHA1), résolu via int_whoscored_match_index. NULL si le match n'est pas résolu au registre. |
| `team_id` | BIGINT | 99.3% | 178300 | 175 |  | Id canonique de l'équipe de l'événement (converti depuis l'id WhoScored via int_whoscored_match_index). |
| `event_id` | INTEGER | 100.0% | 0 | 23739 |  | Identifiant WhoScored de l'événement. ATTENTION : scopé par (match, équipe), PAS unique par match — ne pas joindre dessus sans contraindre l'équipe.  |
| `league_source` | VARCHAR | 100.0% | 0 | 7 | top: Premier League (4700125), La Liga (4556748), Serie A (4517152) | Championnat / source des données (ex. Bundesliga). |
| `season` | VARCHAR | 100.0% | 0 | 8 | top: 2017-2018 (4187231), 2020-2021 (3359720), 2018-2019 (3301341) | Saison du match (ex. 2023-2024). |
| `minute` | INTEGER | 100.0% | 0 | 120 | moy 45.678 · méd 45.0 · min/max 0.0/32767.0 · p10/p90 8.0/84.0 | Minute de jeu brute (hors temps additionnel). |
| `second` | INTEGER | 99.8% | 62560 | 61 | moy 29.157 · méd 29.0 · min/max -2.0/59.0 · p10/p90 5.0/53.0 | Seconde dans la minute. |
| `expanded_minute` | INTEGER | 100.0% | 0 | 134 | moy 47.128 · méd 47.0 · min/max 0.0/32774.0 · p10/p90 8.0/87.0 | Minute cumulée, temps additionnel inclus (ex. 45+2 → 47). |
| `period` | INTEGER | 100.0% | 0 | 4 | moy 1.538 · méd 2.0 · min/max 1.0/16.0 · p10/p90 1.0/2.0 | Période : 1 = 1re mi-temps, 2 = 2e mi-temps, 16 = pré-match (FormationSet), 14 = fin de match (End). |
| `player_id` | INTEGER | 99.0% | 259444 | 8756 |  | Identifiant WhoScored du joueur auteur de l'action. NULL possible sur certains événements. |
| `x` | DOUBLE | 100.0% | 0 | 1001 | moy 45.242 · méd 43.9 · min/max 0.0/100.0 · p10/p90 10.0/79.2 | Position X de l'action sur le terrain (0-100, croissant vers le but adverse ; repère relatif à l'équipe). |
| `y` | DOUBLE | 100.0% | 0 | 1001 | moy 49.091 · méd 49.4 · min/max 0.0/100.0 · p10/p90 7.1/90.9 | Position Y de l'action (0-100, largeur, 50 = axe central). |
| `end_x` | DOUBLE | 65.3% | 9116009 | 1001 | moy 52.048 · méd 53.1 · min/max 0.0/100.0 · p10/p90 22.1/84.8 | Position X de fin de l'action (surtout les passes ; NULL pour les événements ponctuels). |
| `end_y` | DOUBLE | 65.3% | 9116009 | 1001 | moy 50.391 · méd 50.6 · min/max 0.0/100.0 · p10/p90 8.6/91.8 | Position Y de fin de l'action (surtout les passes ; NULL pour les événements ponctuels). |
| `type_id` | INTEGER | 100.0% | 0 | 39 |  | Type d'événement WhoScored (cf. docs/Event_type_definition.txt ; ex. 1 Pass, 3 TakeOn, 4 Foul, 13-16 tirs). |
| `type_name` | VARCHAR | 100.0% | 0 | 39 |  | Libellé du type d'événement (Pass, TakeOn, Foul, Goal…). |
| `outcome_id` | INTEGER | 100.0% | 0 | 2 |  | Résultat de l'action : 1 = réussi, 0 = raté. |
| `outcome_name` | VARCHAR | 100.0% | 0 | 2 | top: Successful (20221731), Unsuccessful (6049786) | Libellé du résultat : Successful / Unsuccessful. |
| `is_touch` | BOOLEAN | 100.0% | 0 | 2 | top: True (21453935), False (4817582) | Booléen : le joueur a touché le ballon (flag WhoScored isTouch). |
| `is_shot` | BOOLEAN | 100.0% | 0 | 2 | top: False (25842718), True (428799) | Booléen : l'événement est un tir (flag WhoScored isShot). |
| `qualifiers_json` | VARCHAR | 100.0% | 0 | 18685682 |  | Tableau JSON brut des qualifiers de l'événement — SOURCE DE VÉRITÉ des qualifiers (éclaté dans events_qual). |
| `scraped_at` | VARCHAR | 100.0% | 0 | 16576 |  | Horodatage ISO du scrape du match. |
| `row_num` | INTEGER | 100.0% | 0 | 1931 | moy 781.509 · méd 778.0 · min/max 0.0/1930.0 · p10/p90 155.0/1404.0 | Index de l'événement dans le flux du match (ordre chronologique). |
| `is_goal` | BOOLEAN | 100.0% | 0 | 2 | top: False (26224249), True (47268) | Booléen : l'événement est un but (flag WhoScored isGoal). TRUE aussi pour les CSC (voir is_own_goal). |
| `is_own_goal` | BOOLEAN | 100.0% | 0 | 2 | top: False (26270132), True (1385) | Booléen : but contre son camp (CSC). |
| `related_event_id` | INTEGER | 2.3% | 25677635 | 1308 |  | Champ WhoScored relatedEventId (haut niveau de l'event). Souvent NULL — le lien réel entre events miroirs passe par le qualifier 233. |
| `related_player_id` | INTEGER | 2.3% | 25677659 | 8529 |  | Champ WhoScored relatedPlayerId (haut niveau de l'event). Souvent NULL. |
| `card_type` | VARCHAR | 0.3% | 26198314 | 3 | top: Yellow (69677), Red (2035), SecondYellow (1491) | Type de carton : Yellow / Red / SecondYellow. NULL hors événements Card. |
| `goal_mouth_y` | DOUBLE | 1.5% | 25884363 | 692 | moy 50.151 · méd 50.1 · min/max 0.2/100.0 · p10/p90 41.0/59.3 | Position horizontale où le tir atteint la ligne de but (qualifier 102 GoalMouthY). Cadre : poteaux à Y≈45.2 et 54.8, centre 50. NULL hors tirs / non renseigné.  |
| `goal_mouth_z` | DOUBLE | 1.5% | 25884363 | 136 | moy 24.264 · méd 19.0 · min/max 0.0/100.0 · p10/p90 2.8/68.1 | Hauteur où le tir atteint la ligne de but (qualifier 103 GoalMouthZ). Barre à Z≈38. NULL hors tirs / non renseigné. |
| `blocked_x` | DOUBLE | 0.9% | 26034710 | 338 | moy 92.762 · méd 94.7 · min/max 48.4/100.5 · p10/p90 82.7/99.0 | Coordonnée X (repère équipe, 0-100) du point où le tir est stoppé (qualifier 146 BlockedX). Renseigné pour une partie des tirs. |
| `blocked_y` | DOUBLE | 0.9% | 26034710 | 703 | moy 50.183 · méd 50.0 · min/max 0.0/100.0 · p10/p90 40.1/60.8 | Coordonnée Y (repère équipe, 0-100) du point où le tir est stoppé (qualifier 147 BlockedY). |