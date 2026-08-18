---
schema: intermediate
rows: 25255712
---
# player_possession_chains

#intermediate

Segmente le flux d'événements WhoScored en chaînes de possession. Pour chaque événement, détermine l'équipe réellement en possession (possession certaine vs indéterminée), puis marque le déclencheur de chaque chaîne via chain_trigger (corner, free_kick, counter_attack, open_play, recovery, throw_in, goal_kick...). Source : int_event_enriched. Incrémentale sur match_id. Brique de base de corner_profiles, player_passes_raw et player_xg_chain. Une ligne par action.


## Intégrité
**Clé déclarée :** (match_id, row_num) — ⚠️ **5062 doublons**

## Lineage
**Sources :** [[event_types]], [[events_qual]], [[int_event_enriched]]
**Alimente :** [[corner_profiles]], [[freekick_profiles]], [[int_progressive_carries]], [[int_shot_creating_actions]], [[player_passes_raw]], [[player_xg_chain]], [[rolling_corners]], [[rolling_freekicks]], [[threat_conceded]]

## Features & profiling  (25255712 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 16573 |  | Identifiant unifié du match (SHA1), résolu via int_whoscored_match_index. |
| `season` | VARCHAR | 100.0% | 0 | 8 | top: 2017-2018 (4027445), 2018-2019 (3221829), 2021-2022 (3181276) | Saison du match (ex. 2023-2024). |
| `league_source` | VARCHAR | 100.0% | 0 | 7 | top: Premier League (4605190), La Liga (4442072), Serie A (4410750) | Championnat / source des données (ex. Bundesliga). |
| `chain_id` | VARCHAR | 100.0% | 0 | 5226231 |  | Identifiant de la chaîne de possession = match_id // '_' // numéro de chaîne. Regroupe les actions d'une même possession. |
| `chain_number` | HUGEINT | 100.0% | 0 | 565 | moy 156.21 · méd 152.0 · min/max 0.0/564.0 · p10/p90 31.0/285.0 | Numéro séquentiel de la chaîne de possession dans le match. |
| `chain_team_id` | BIGINT | 99.8% | 58414 | 175 |  | Id canonique de l'équipe EN POSSESSION durant la chaîne (possesseur résolu). ≠ team_id : team_id est l'équipe de l'action précise (ex. un dégagement adverse peut appartenir à la chaîne de l'équipe qui attaque).  |
| `team_id` | BIGINT | 100.0% | 0 | 175 |  | Id canonique de l'équipe de l'action (converti depuis l'id WhoScored via int_whoscored_match_index). |
| `player_id` | INTEGER | 100.0% | 0 | 8589 |  | Identifiant WhoScored du joueur auteur de l'action. NULL possible sur certains événements. |
| `event_id` | INTEGER | 100.0% | 0 | 23448 |  | Identifiant WhoScored de l'événement. ATTENTION : scopé par (match, équipe), PAS unique par match — ne pas joindre dessus sans contraindre l'équipe.  |
| `row_num` | INTEGER | 100.0% | 0 | 1925 | moy 775.167 · méd 769.0 · min/max 0.0/1924.0 · p10/p90 154.0/1399.0 | Index de l'événement dans le flux du match (ordre chronologique). |
| `expanded_minute` | INTEGER | 100.0% | 0 | 131 | moy 46.907 · méd 47.0 · min/max 0.0/32772.0 · p10/p90 8.0/87.0 | Minute cumulée, temps additionnel inclus (ex. 45+2 → 47). |
| `second` | INTEGER | 99.8% | 61553 | 61 | moy 29.356 · méd 29.0 · min/max -2.0/59.0 · p10/p90 5.0/53.0 | Seconde dans la minute. |
| `period` | INTEGER | 100.0% | 0 | 2 | moy 1.498 · méd 1.0 · min/max 1.0/2.0 · p10/p90 1.0/2.0 | Période : 1 = 1re mi-temps, 2 = 2e mi-temps, 16 = pré-match (FormationSet), 14 = fin de match (End). |
| `type_id` | INTEGER | 100.0% | 0 | 32 |  | Type d'événement WhoScored (cf. docs/Event_type_definition.txt ; ex. 1 Pass, 3 TakeOn, 4 Foul, 13-16 tirs). |
| `type_name` | VARCHAR | 100.0% | 0 | 32 |  | Libellé du type d'événement (Pass, TakeOn, Foul, Goal…). |
| `outcome_id` | INTEGER | 100.0% | 0 | 2 |  | Résultat de l'action : 1 = réussi, 0 = raté. |
| `is_shot` | BOOLEAN | 100.0% | 0 | 2 | top: False (24834618), True (421094) | Booléen : l'événement est un tir (flag WhoScored isShot). |
| `x` | DOUBLE | 100.0% | 0 | 1001 | moy 46.247 · méd 44.6 · min/max 0.0/100.0 · p10/p90 12.1/79.5 | Position X de l'action sur le terrain (0-100, croissant vers le but adverse ; repère relatif à l'équipe). |
| `y` | DOUBLE | 100.0% | 0 | 1001 | moy 50.163 · méd 50.1 · min/max 0.0/100.0 · p10/p90 8.8/91.2 | Position Y de l'action (0-100, largeur, 50 = axe central). |
| `is_rupture` | INTEGER | 100.0% | 0 | 2 | moy 0.179 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | Flag (0/1) : l'événement clôt la chaîne (le possesseur change juste après). |
| `chain_trigger` | VARCHAR | 100.0% | 0 | 6 | top: open_play (9410188), recovery (7732055), throw_in (3702196) | Déclencheur de la chaîne : recovery / corner / goal_kick / throw_in / free_kick / open_play. |
| `certain_possessor` | BIGINT | 82.7% | 4361230 | 175 | moy 202606100213.34 · méd 202606100219.0 · min/max 202606100001.0/202606100426.0 · p10/p90 202606100042.0/202606100392.0 | Id de l'équipe si la possession est certaine à cet événement ; NULL si possession indéterminée (duel / ballon libre). |
| `scraped_at` | VARCHAR | 100.0% | 0 | 16574 |  | Horodatage ISO du scrape du match. |