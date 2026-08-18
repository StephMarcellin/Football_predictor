---
schema: intermediate
rows: 25590860
---
# int_event_enriched

#intermediate

Événements WhoScored enrichis d'un pivot des qualifiers utiles, notamment la hiérarchie chance_creation (leading_to_goal, intentional_assist, big_chance_created, key_pass, shot_assist...). Table socle de event_values, player_possession_chains, player_network_duels et player_zone_transitions. Incrémentale sur scraped_at. Filtre : player_id IS NOT NULL.


## Intégrité
**Clé déclarée :** (match_id, team_id, player_id, row_num) — ⚠️ **1508 doublons**

## Lineage
**Sources :** [[events_qual]], [[int_whoscored_events]], [[int_whoscored_match_index]]
**Alimente :** [[event_values]], [[int_penalties]], [[int_shot_placement]], [[int_xt_actions]], [[player_network_duels]], [[player_passes_raw]], [[player_possession_chains]], [[player_zone_transitions]]

## Features & profiling  (25590860 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 16585 |  | Identifiant unifié du match (SHA1), résolu via int_whoscored_match_index. |
| `team_id` | BIGINT | 100.0% | 4625 | 175 |  | Id canonique de l'équipe de l'événement (converti depuis l'id WhoScored via int_whoscored_match_index). |
| `player_id` | INTEGER | 100.0% | 0 | 8638 |  | Identifiant WhoScored du joueur auteur de l'action. (Ici jamais NULL : la table filtre player_id IS NOT NULL.) |
| `event_id` | INTEGER | 100.0% | 0 | 23473 |  | Identifiant WhoScored de l'événement. ATTENTION : scopé par (match, équipe), PAS unique par match — ne pas joindre dessus sans contraindre l'équipe.  |
| `row_num` | INTEGER | 100.0% | 0 | 1925 | moy 779.437 · méd 776.0 · min/max 0.0/1924.0 · p10/p90 156.0/1400.0 | Index de l'événement dans le flux du match (ordre chronologique). |
| `expanded_minute` | INTEGER | 100.0% | 0 | 131 | moy 47.191 · méd 47.0 · min/max 0.0/32772.0 · p10/p90 8.0/87.0 | Minute cumulée, temps additionnel inclus (ex. 45+2 → 47). |
| `second` | INTEGER | 99.8% | 61611 | 61 | moy 29.338 · méd 29.0 · min/max -2.0/59.0 · p10/p90 5.0/53.0 | Seconde dans la minute. |
| `period` | INTEGER | 100.0% | 0 | 2 | moy 1.503 · méd 2.0 · min/max 1.0/2.0 · p10/p90 1.0/2.0 | Période : 1 = 1re mi-temps, 2 = 2e mi-temps, 16 = pré-match (FormationSet), 14 = fin de match (End). |
| `type_id` | INTEGER | 100.0% | 0 | 36 |  | Type d'événement WhoScored (cf. docs/Event_type_definition.txt ; ex. 1 Pass, 3 TakeOn, 4 Foul, 13-16 tirs). |
| `type_name` | VARCHAR | 100.0% | 0 | 36 |  | Libellé du type d'événement (Pass, TakeOn, Foul, Goal…). |
| `outcome_id` | INTEGER | 100.0% | 0 | 2 |  | Résultat de l'action : 1 = réussi, 0 = raté. |
| `is_shot` | BOOLEAN | 100.0% | 0 | 2 | top: False (25169506), True (421354) | Booléen : l'événement est un tir (flag WhoScored isShot). |
| `is_touch` | BOOLEAN | 100.0% | 0 | 2 | top: True (21091770), False (4499090) | Booléen : le joueur a touché le ballon (flag WhoScored isTouch). |
| `x` | DOUBLE | 100.0% | 0 | 1001 | moy 45.671 · méd 44.2 · min/max 0.0/100.0 · p10/p90 10.9/79.3 | Position X de l'action sur le terrain (0-100, croissant vers le but adverse ; repère relatif à l'équipe). |
| `y` | DOUBLE | 100.0% | 0 | 1001 | moy 49.538 · méd 49.8 · min/max 0.0/100.0 · p10/p90 7.8/91.1 | Position Y de l'action (0-100, largeur, 50 = axe central). |
| `end_x` | DOUBLE | 66.0% | 8705652 | 1001 | moy 52.047 · méd 53.1 · min/max 0.0/100.0 · p10/p90 22.1/84.7 | Position X de fin de l'action (surtout les passes ; NULL pour les événements ponctuels). |
| `end_y` | DOUBLE | 66.0% | 8705652 | 1001 | moy 50.384 · méd 50.6 · min/max 0.0/100.0 · p10/p90 8.6/91.8 | Position Y de fin de l'action (surtout les passes ; NULL pour les événements ponctuels). |
| `is_own_goal` | BOOLEAN | 100.0% | 0 | 2 | top: False (25589495), True (1365) | Booléen : but contre son camp (CSC). |
| `related_event_id` | INTEGER | 2.3% | 25007508 | 1308 |  | Champ WhoScored relatedEventId. Souvent NULL — le lien réel entre events miroirs passe par le qualifier 233. |
| `related_player_id` | INTEGER | 2.3% | 25007531 | 8412 |  | Champ WhoScored relatedPlayerId. Souvent NULL. |
| `card_type` | VARCHAR | 0.3% | 25520575 | 3 | top: Yellow (67104), Red (1749), SecondYellow (1432) | Type de carton : Yellow / Red / SecondYellow. NULL hors événements Card. |
| `goal_mouth_y` | DOUBLE | 1.5% | 25210416 | 692 | moy 50.152 · méd 50.1 · min/max 0.2/100.0 · p10/p90 41.0/59.3 | Position horizontale où le tir atteint la ligne de but (qualifier 102 GoalMouthY). Cadre : poteaux à Y≈45.2 et 54.8, centre 50. NULL hors tirs / non renseigné.  |
| `goal_mouth_z` | DOUBLE | 1.5% | 25210416 | 136 | moy 24.263 · méd 19.0 · min/max 0.0/100.0 · p10/p90 2.8/68.1 | Hauteur où le tir atteint la ligne de but (qualifier 103 GoalMouthZ). Barre à Z≈38. NULL hors tirs / non renseigné. |
| `blocked_x` | DOUBLE | 0.9% | 25358192 | 336 | moy 92.763 · méd 94.7 · min/max 48.4/100.5 · p10/p90 82.7/99.0 | Coordonnée X (repère équipe, 0-100) du point où le tir est stoppé (qualifier 146 BlockedX). |
| `blocked_y` | DOUBLE | 0.9% | 25358192 | 700 | moy 50.184 · méd 50.0 · min/max 0.0/100.0 · p10/p90 40.1/60.8 | Coordonnée Y (repère équipe, 0-100) du point où le tir est stoppé (qualifier 147 BlockedY). |
| `match_date` | DATE | 100.0% | 0 | 1515 |  | Date du match (rattachée via int_whoscored_match_index). |
| `season` | VARCHAR | 100.0% | 0 | 8 | top: 2017-2018 (4065629), 2018-2019 (3254798), 2021-2022 (3225852) | Saison du match (ex. 2023-2024). |
| `league_source` | VARCHAR | 100.0% | 0 | 7 | top: Premier League (4659344), La Liga (4509296), Serie A (4470776) | Championnat / source des données (ex. Bundesliga). |
| `scraped_at` | VARCHAR | 100.0% | 0 | 16294 |  | Horodatage ISO du scrape du match. |
| `is_leading_to_goal` | INTEGER | 100.0% | 0 | 2 | moy 0.0 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.0 | Flag (0/1) : l'action mène à un but (qualifier 170 LeadingToGoal). |
| `is_intentional_goal_assist` | INTEGER | 100.0% | 0 | 2 | moy 0.001 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.0 | Flag (0/1) : passe décisive intentionnelle (qualifier 11111 IntentionalGoalAssist). |
| `is_intentional_assist` | INTEGER | 100.0% | 0 | 2 | moy 0.015 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.0 | Flag (0/1) : passe intentionnelle vers une occasion (qualifier 154 IntentionalAssist). |
| `is_big_chance_created` | INTEGER | 100.0% | 0 | 2 | moy 0.002 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.0 | Flag (0/1) : grosse occasion créée (qualifier 11112 BigChanceCreated). |
| `is_key_pass` | INTEGER | 100.0% | 0 | 2 | moy 0.012 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.0 | Flag (0/1) : passe clé — mène à un tir (qualifier 11113 KeyPass). |
| `is_shot_assist` | INTEGER | 100.0% | 0 | 2 | moy 0.012 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.0 | Flag (0/1) : passe précédant directement un tir (qualifier 210 ShotAssist). |
| `is_leading_to_attempt` | INTEGER | 100.0% | 0 | 2 | moy 0.0 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.0 | Flag (0/1) : l'action mène à une tentative de tir (qualifier 169 LeadingToAttempt). |
| `has_defensive_qual` | INTEGER | 100.0% | 0 | 2 | moy 0.08 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.0 | Flag (0/1) : l'action porte le qualifier 285 Defensive (côté défensif d'un duel). |
| `has_offensive_qual` | INTEGER | 100.0% | 0 | 2 | moy 0.084 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.0 | Flag (0/1) : l'action porte le qualifier 286 Offensive (côté offensif d'un duel). |
| `has_opposite_event` | INTEGER | 100.0% | 0 | 2 | moy 0.194 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | Flag (0/1) : l'action porte le qualifier 233 OppositeRelatedEvent (lien vers l'event miroir adverse). |
| `team_score` | BIGINT | 100.0% | 0 | 10 | moy 0.645 · méd 0.0 · min/max 0.0/9.0 · p10/p90 0.0/2.0 | Buts cumulés de l'équipe de l'événement à cet instant du match (reconstruit). |
| `opp_score` | BIGINT | 100.0% | 0 | 10 | moy 0.65 · méd 0.0 · min/max 0.0/9.0 · p10/p90 0.0/2.0 | Buts cumulés de l'équipe adverse à cet instant du match (reconstruit). |