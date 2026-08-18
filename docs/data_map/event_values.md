---
schema: intermediate
rows: 25586235
---
# event_values

#intermediate

Valorise chaque événement individuel via cinq axes indépendants (danger_position, chance_creation, def_execution_quality, pressure_context, context_weight) combinés en une note globale action_value. Couche "value par action" consommée par les modèles de chaînes. Source : int_event_enriched. Incrémentale (clé match_id + team_id + player_id + row_num).


## Intégrité
**Clé déclarée :** (match_id, team_id, player_id, row_num) — ✅ aucun doublon

## Lineage
**Sources :** [[int_event_enriched]]
**Alimente :** [[corner_profiles]], [[freekick_profiles]], [[int_shot_placement]], [[player_xg_chain]]

## Features & profiling  (25586235 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 16585 |  | Identifiant unifié du match (SHA1), résolu via int_whoscored_match_index. |
| `team_id` | BIGINT | 100.0% | 0 | 175 |  | Id canonique de l'équipe de l'événement (converti depuis l'id WhoScored via int_whoscored_match_index). |
| `player_id` | INTEGER | 100.0% | 0 | 8638 |  | Identifiant WhoScored du joueur auteur de l'action (jamais NULL : la source filtre player_id IS NOT NULL). |
| `event_id` | INTEGER | 100.0% | 0 | 23473 |  | Identifiant WhoScored de l'événement. ATTENTION : scopé par (match, équipe), PAS unique par match — ne pas joindre dessus sans contraindre l'équipe. |
| `row_num` | INTEGER | 100.0% | 0 | 1925 | moy 779.438 · méd 776.0 · min/max 0.0/1924.0 · p10/p90 156.0/1400.0 | Index de l'événement dans le flux du match (ordre chronologique). |
| `expanded_minute` | INTEGER | 100.0% | 0 | 131 | moy 47.19 · méd 47.0 · min/max 0.0/32772.0 · p10/p90 8.0/87.0 | Minute cumulée, temps additionnel inclus (ex. 45+2 -> 47). |
| `period` | INTEGER | 100.0% | 0 | 2 | moy 1.503 · méd 2.0 · min/max 1.0/2.0 · p10/p90 1.0/2.0 | Période : 1 = 1re mi-temps, 2 = 2e mi-temps, 16 = pré-match (FormationSet), 14 = fin de match (End). |
| `type_id` | INTEGER | 100.0% | 0 | 36 |  | Type d'événement WhoScored (cf. docs/Event_type_definition.txt ; ex. 1 Pass, 3 TakeOn, 4 Foul, 13-16 tirs). |
| `type_name` | VARCHAR | 100.0% | 0 | 36 |  | Libellé du type d'événement (Pass, TakeOn, Foul, Goal…). |
| `outcome_id` | INTEGER | 100.0% | 0 | 2 |  | Résultat de l'action : 1 = réussi, 0 = raté. |
| `is_shot` | BOOLEAN | 100.0% | 0 | 2 | top: False (25164924), True (421311) | Booléen : l'événement est un tir (flag WhoScored isShot). |
| `x` | DOUBLE | 100.0% | 0 | 1001 | moy 45.671 · méd 44.2 · min/max 0.0/100.0 · p10/p90 10.9/79.3 | Position X de l'action (0-100, croissant vers le but adverse ; repère relatif à l'équipe). |
| `y` | DOUBLE | 100.0% | 0 | 1001 | moy 49.538 · méd 49.8 · min/max 0.0/100.0 · p10/p90 7.8/91.1 | Position Y de l'action (0-100, largeur, 50 = axe central). |
| `match_date` | DATE | 100.0% | 0 | 1515 |  | Date du match (rattachée via int_whoscored_match_index). |
| `season` | VARCHAR | 100.0% | 0 | 8 | top: 2017-2018 (4065629), 2018-2019 (3254798), 2021-2022 (3225852) | Saison du match (ex. 2023-2024). |
| `league_source` | VARCHAR | 100.0% | 0 | 7 | top: Premier League (4654719), La Liga (4509296), Serie A (4470776) | Championnat / source des données (ex. Bundesliga). |
| `scraped_at` | VARCHAR | 100.0% | 0 | 16579 |  | Horodatage ISO du scrape du match. |
| `danger_position` | DOUBLE | 90.5% | 2432952 | 1263 | moy 0.536 · méd 0.55 · min/max 0.0/1.0 · p10/p90 0.223/0.866 | Dangerosité de la position de l'action, normalisée 0-1. Offensif (type 1,3,13-16,42) = x/100 ; défensif (7,8,12,45,49,74) = (100-x)/100 ; duels purs (44,4,50) routés via qualifier défensif 285 / offensif 286. NULL si x manquant. |
| `chance_creation` | DECIMAL(3,2) | 100.0% | 0 | 14 | moy 0.023 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.0 | Score 0-1 de création d'occasion, par hiérarchie décroissante : but=1.00, mène au but=0.90, assist intentionnel=0.85, assist large=0.80, grosse occasion=0.75, passe clé=0.65, passe->tir=0.55, mène à un tir=0.50, tir cadré=0.40, tir raté/poteau=0.10, TakeOn réussi=0.30, GoodSkill=0.25, TakeOn raté=0.05, sinon 0.00. |
| `def_execution_quality` | DECIMAL(3,2) | 18.7% | 20788872 | 3 | moy 0.867 · méd 1.0 · min/max 0.3/1.0 · p10/p90 0.3/1.0 | Qualité d'exécution d'une action défensive, 0-1 : action défensive réussie (7,8,12,49,74)=1.00, Challenge (45)=0.60, tentative ratée=0.30 ; duels purs défensifs (qual 285) idem. NULL pour les actions offensives (axe non applicable). |
| `pressure_context` | DECIMAL(4,2) | 100.0% | 0 | 5 | moy 0.253 · méd 0.2 · min/max 0.0/1.0 · p10/p90 0.0/0.8 | Proxy 0-1 de duel/pression physique : l'action est un duel (44,4,45,50,7)=0.80 ; liée à un duel via qual 233=0.60 ; +0.20 si dans sa propre moitié (x<50) ; plafonné à 1.0. Proxy limité par l'absence de tracking. |
| `context_weight` | DECIMAL(3,2) | 100.0% | 0 | 8 | moy 0.531 · méd 0.4 · min/max 0.3/1.0 · p10/p90 0.3/0.85 | Criticité narrative selon le score et la minute, calibrée à la main : >=75' menée=1.00, menant=0.85, nul-avec-buts=0.75, 0-0=0.60 ; avant 75' menée=0.70, nul-avec-buts=0.50, menant=0.40, 0-0=0.30. |
| `action_value` | DOUBLE | 90.5% | 2432952 | 79929 | moy 0.11 · méd 0.0 · min/max 0.0/0.999 · p10/p90 0.0/0.494 | Note globale combinant les axes. Offensif (1,3,13-16,42 + duels offensifs) = SQRT(danger_position * chance_creation) * (0.5 + 0.3*pressure_context + 0.2*context_weight) ; défensif (7,8,12,45,49,74 + duels défensifs) = danger_position * def_execution_quality * (...). NULL si x manquant. |