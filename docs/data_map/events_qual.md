---
schema: intermediate
rows: 129573435
---
# events_qual

#intermediate

Éclatement des qualifiers de int_whoscored_events (via qualifiers_json) : 1 ligne par (événement × qualifier). Base de tous les modèles WhoScored. Incrémentale. Les colonnes d'événement sont reprises telles quelles de int_whoscored_events ; qual_type_id/name/value sont la transformation (unnest).


## Intégrité
**Clé déclarée :** (match_id, team_id, player_id, row_num, qual_type_id) — ⚠️ **266701 doublons**

## Lineage
**Sources :** [[int_whoscored_events]], [[int_whoscored_match_index]]
**Alimente :** [[corner_profiles]], [[features_draw]], [[freekick_profiles]], [[int_defensive_blocks]], [[int_event_enriched]], [[int_fouls_drawn]], [[int_keeper_shots]], [[int_penalties]], [[int_progressive_carries]], [[int_shot_placement]], [[player_match_stats]], [[player_network_duels]], [[player_possession_chains]], [[player_xg_chain]], [[player_zone_transitions]], [[team_features_ws]], [[threat_conceded]], [[xgot_features]]

## Features & profiling  (129573435 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 98.4% | 2053444 | 16573 |  | Identifiant unifié du match (SHA1), résolu via int_whoscored_match_index. |
| `team_id` | BIGINT | 99.3% | 842477 | 175 |  | Id canonique de l'équipe de l'événement (converti depuis l'id WhoScored via int_whoscored_match_index). |
| `player_id` | INTEGER | 99.6% | 476468 | 8752 |  | Identifiant WhoScored du joueur auteur de l'action. NULL possible sur certains événements. |
| `event_id` | INTEGER | 100.0% | 0 | 2848 |  | Identifiant WhoScored de l'événement. ATTENTION : scopé par (match, équipe), PAS unique par match — ne pas joindre dessus sans contraindre l'équipe.  |
| `minute` | INTEGER | 100.0% | 0 | 120 | moy 45.584 · méd 45.0 · min/max 0.0/32767.0 · p10/p90 8.0/84.0 | Minute de jeu brute (hors temps additionnel). |
| `second` | INTEGER | 100.0% | 0 | 61 | moy 29.278 · méd 29.0 · min/max -2.0/59.0 · p10/p90 5.0/53.0 | Seconde dans la minute. |
| `expanded_minute` | INTEGER | 100.0% | 0 | 131 | moy 47.028 · méd 47.0 · min/max 0.0/32772.0 · p10/p90 8.0/87.0 | Minute cumulée, temps additionnel inclus (ex. 45+2 → 47). |
| `period` | INTEGER | 100.0% | 0 | 4 | moy 1.524 · méd 2.0 · min/max 1.0/16.0 · p10/p90 1.0/2.0 | Période : 1 = 1re mi-temps, 2 = 2e mi-temps, 16 = pré-match (FormationSet), 14 = fin de match (End). |
| `x` | DOUBLE | 100.0% | 0 | 1001 | moy 47.693 · méd 46.1 · min/max 0.0/100.0 · p10/p90 11.8/83.8 | Position X de l'action sur le terrain (0-100, croissant vers le but adverse ; repère relatif à l'équipe). |
| `y` | DOUBLE | 100.0% | 0 | 1001 | moy 49.573 · méd 49.8 · min/max 0.0/100.0 · p10/p90 7.6/91.4 | Position Y de l'action (0-100, largeur, 50 = axe central). |
| `end_x` | DOUBLE | 80.9% | 24786012 | 1001 | moy 53.314 · méd 54.3 · min/max 0.0/100.0 · p10/p90 22.5/87.2 | Position X de fin de l'action (surtout les passes ; NULL pour les événements ponctuels). |
| `end_y` | DOUBLE | 80.9% | 24786012 | 1001 | moy 50.361 · méd 50.4 · min/max 0.0/100.0 · p10/p90 8.8/91.6 | Position Y de fin de l'action (surtout les passes ; NULL pour les événements ponctuels). |
| `type_id` | INTEGER | 100.0% | 0 | 34 |  | Type d'événement WhoScored (cf. docs/Event_type_definition.txt ; ex. 1 Pass, 3 TakeOn, 4 Foul, 13-16 tirs). |
| `type_name` | VARCHAR | 100.0% | 0 | 34 |  | Libellé du type d'événement (Pass, TakeOn, Foul, Goal…). |
| `outcome_id` | INTEGER | 100.0% | 0 | 2 |  | Résultat de l'action : 1 = réussi, 0 = raté. |
| `is_touch` | BOOLEAN | 100.0% | 0 | 2 | top: True (120904826), False (8668609) | Booléen : le joueur a touché le ballon (flag WhoScored isTouch). |
| `is_shot` | BOOLEAN | 100.0% | 0 | 2 | top: False (124620802), True (4952633) | Booléen : l'événement est un tir (flag WhoScored isShot). |
| `row_num` | INTEGER | 100.0% | 0 | 1929 | moy 777.982 · méd 773.0 · min/max 0.0/1930.0 · p10/p90 156.0/1399.0 | Index de l'événement dans le flux du match (ordre chronologique). |
| `qual_type_id` | INTEGER | 100.0% | 0 | 114 |  | Id du type de qualifier (cf. docs/Qualifier_type_definition.txt ; ex. 9 Penalty, 102 GoalMouthY, 233 OppositeRelatedEvent). |
| `qual_type_name` | VARCHAR | 100.0% | 0 | 114 |  | Libellé du qualifier (displayName WhoScored). |
| `qual_value` | VARCHAR | 77.4% | 29300265 | 145552 |  | Valeur du qualifier (chaîne). NULL pour un qualifier-drapeau sans valeur. |