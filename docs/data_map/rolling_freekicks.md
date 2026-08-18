---
schema: gold
rows: 46887
---
# rolling_freekicks

#gold

Rolling windows W=3/5/10 sur les coups francs, depuis intermediate.freekick_profiles. 1 ligne par équipe par match — "for" (coups francs obtenus) et "against" (concédés), jointes via intermediate.backbone (team_id / opponent_id). Filtré sur fk_zone_type IN (crossed, direct_shot) : les coups francs too_far/own_box sont exclus, ils n'ont pas de danger réel et diluent le signal. Action dangereuse = coup franc ayant produit un tir. Ratio de sommes sur la fenêtre, pas moyenne de taux par match. Enrichi (Vague 5.4) : intensité de danger continue, profil aérien, bataille du dégagement, répartition tactique par zone (direct/crossed/own_box/too_far, non filtrée) et qualité de position (distance/angle) des coups francs directs.


## Intégrité
**Clé déclarée :** (match_id, team_id) — ✅ aucun doublon

## Lineage
**Sources :** [[backbone]], [[freekick_profiles]], [[player_possession_chains]]
**Alimente :** —

## Features & profiling  (46887 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 28499 |  | ID unique du match provenant du backbone |
| `team_id` | BIGINT | 100.0% | 0 | 153 |  | ID de l'équipe |
| `date` | DATE | 100.0% | 0 | 2417 |  | Date du match |
| `season` | VARCHAR | 100.0% | 0 | 9 | top: 2018-2019 (5623), 2017-2018 (5525), 2021-2022 (5379) | Saison |
| `league_source` | VARCHAR | 100.0% | 0 | 28 |  | Compétition source |
| `freekicks_for_3` | HUGEINT | 46.1% | 25263 | 17 | moy 3.928 · méd 4.0 · min/max 0.0/16.0 · p10/p90 1.0/7.0 | Nombre de coups francs crossés/directs obtenus sur les 3 derniers matchs |
| `freekick_danger_rate_for_3` | DOUBLE | 44.3% | 26122 | 49 | moy 0.299 · méd 0.25 · min/max 0.0/1.0 · p10/p90 0.0/0.667 | Taux de coups francs obtenus ayant produit une action dangereuse, sur 3 matchs |
| `freekick_conversion_rate_for_3` | DOUBLE | 30.8% | 32441 | 13 | moy 0.097 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.5 | Taux de conversion en but des actions dangereuses issues de coup franc, sur 3 matchs |
| `freekicks_against_3` | HUGEINT | 46.1% | 25263 | 17 | moy 3.93 · méd 4.0 · min/max 0.0/16.0 · p10/p90 1.0/7.0 | Nombre de coups francs crossés/directs concédés sur les 3 derniers matchs |
| `freekick_danger_rate_against_3` | DOUBLE | 44.5% | 26022 | 46 | moy 0.298 · méd 0.25 · min/max 0.0/1.0 · p10/p90 0.0/0.667 | Taux de coups francs concédés ayant produit une action dangereuse, sur 3 matchs |
| `freekick_conversion_rate_against_3` | DOUBLE | 31.0% | 32360 | 13 | moy 0.096 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.5 | Taux de conversion en but des coups francs concédés, sur 3 matchs |
| `freekick_danger_intensity_for_3` | DOUBLE | 44.3% | 26122 | 13203 | moy 0.283 · méd 0.241 · min/max 0.0/2.275 · p10/p90 0.0/0.629 | Danger continu généré par coup franc obtenu (somme chain_danger_total / nb coups francs), sur 3 matchs |
| `freekick_danger_intensity_against_3` | DOUBLE | 44.5% | 26022 | 13330 | moy 0.281 · méd 0.241 · min/max 0.0/2.275 · p10/p90 0.0/0.628 | Danger continu concédé par coup franc concédé, sur 3 matchs |
| `freekick_header_share_for_3` | DOUBLE | 30.8% | 32441 | 17 | moy 0.552 · méd 0.5 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | Part des tirs issus de coups francs obtenus qui sont des têtes, sur 3 matchs |
| `freekick_header_share_against_3` | DOUBLE | 31.0% | 32360 | 18 | moy 0.551 · méd 0.5 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | Part des tirs issus de coups francs concédés qui sont des têtes, sur 3 matchs |
| `freekick_header_goal_share_for_3` | DOUBLE | 4.8% | 44639 | 5 | moy 0.528 · méd 1.0 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | Part des buts sur coup franc obtenu marqués de la tête, sur 3 matchs |
| `freekick_header_goal_share_against_3` | DOUBLE | 4.8% | 44626 | 5 | moy 0.527 · méd 1.0 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | Part des buts sur coup franc concédé marqués de la tête, sur 3 matchs |
| `freekick_forced_bad_clearance_rate_for_3` | DOUBLE | 39.3% | 28442 | 29 | moy 0.494 · méd 0.5 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | Part des dégagements adverses mal exécutés face à nos coups francs centrés, sur 3 matchs |
| `freekick_clearance_fail_rate_against_3` | DOUBLE | 39.6% | 28312 | 26 | moy 0.49 · méd 0.5 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | Part de nos propres dégagements mal exécutés face aux coups francs centrés concédés, sur 3 matchs |
| `freekick_headed_clearance_rate_against_3` | DOUBLE | 36.8% | 29613 | 11 | moy 0.985 · méd 1.0 · min/max 0.0/1.0 · p10/p90 1.0/1.0 | Part de nos dégagements défensifs de la tête sur coup franc centré concédé, sur 3 matchs |
| `freekick_direct_distance_avg_for_3` | DOUBLE | 1.6% | 46155 | 267 | moy 26.41 · méd 27.345 · min/max 8.069/34.513 · p10/p90 20.297/31.398 | Distance moyenne (m) des coups francs directs obtenus, sur 3 matchs |
| `freekick_direct_distance_avg_against_3` | DOUBLE | 1.6% | 46146 | 262 | moy 26.432 · méd 27.367 · min/max 8.069/34.513 · p10/p90 20.274/31.398 | Distance moyenne (m) des coups francs directs concédés, sur 3 matchs — discipline défensive |
| `freekick_direct_angle_avg_for_3` | DOUBLE | 1.6% | 46155 | 267 | moy 0.27 · méd 0.252 · min/max 0.062/0.833 · p10/p90 0.221/0.351 | Angle moyen (rad) vers le but des coups francs directs obtenus, sur 3 matchs |
| `freekick_direct_angle_avg_against_3` | DOUBLE | 1.6% | 46146 | 262 | moy 0.269 · méd 0.251 · min/max 0.062/0.833 · p10/p90 0.221/0.351 | Angle moyen (rad) vers le but des coups francs directs concédés, sur 3 matchs |
| `freekick_zone_direct_rate_for_3` | DOUBLE | 46.1% | 25263 | 58 | moy 0.001 · méd 0.0 · min/max 0.0/0.071 · p10/p90 0.0/0.0 | Part des coups francs obtenus joués en tir direct, sur 3 matchs |
| `freekick_zone_direct_rate_against_3` | DOUBLE | 46.1% | 25263 | 56 | moy 0.001 · méd 0.0 · min/max 0.0/0.071 · p10/p90 0.0/0.0 | Part des coups francs concédés joués en tir direct par l'adversaire, sur 3 matchs |
| `freekick_zone_crossed_rate_for_3` | DOUBLE | 46.1% | 25263 | 384 | moy 0.107 · méd 0.102 · min/max 0.0/0.6 · p10/p90 0.032/0.188 | Part des coups francs obtenus centrés dans la surface, sur 3 matchs |
| `freekick_zone_crossed_rate_against_3` | DOUBLE | 46.1% | 25263 | 364 | moy 0.107 · méd 0.103 · min/max 0.0/0.6 · p10/p90 0.037/0.184 | Part des coups francs concédés centrés dans la surface par l'adversaire, sur 3 matchs |
| `freekicks_for_5` | HUGEINT | 46.4% | 25111 | 25 | moy 6.326 · méd 6.0 · min/max 0.0/25.0 · p10/p90 2.0/11.0 | Nombre de coups francs crossés/directs obtenus sur les 5 derniers matchs |
| `freekick_danger_rate_for_5` | DOUBLE | 45.6% | 25492 | 89 | moy 0.298 · méd 0.286 · min/max 0.0/1.0 · p10/p90 0.0/0.556 | Taux de coups francs obtenus ayant produit une action dangereuse, sur 5 matchs |
| `freekick_conversion_rate_for_5` | DOUBLE | 37.8% | 29179 | 18 | moy 0.096 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.4 | Taux de conversion en but des actions dangereuses issues de coup franc, sur 5 matchs |
| `freekicks_against_5` | HUGEINT | 46.4% | 25111 | 22 | moy 6.329 · méd 6.0 · min/max 0.0/22.0 · p10/p90 3.0/10.0 | Nombre de coups francs crossés/directs concédés sur les 5 derniers matchs |
| `freekick_danger_rate_against_5` | DOUBLE | 45.7% | 25472 | 77 | moy 0.298 · méd 0.286 · min/max 0.0/1.0 · p10/p90 0.0/0.556 | Taux de coups francs concédés ayant produit une action dangereuse, sur 5 matchs |
| `freekick_conversion_rate_against_5` | DOUBLE | 38.1% | 29020 | 19 | moy 0.095 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.4 | Taux de conversion en but des coups francs concédés, sur 5 matchs |
| `freekick_danger_intensity_for_5` | DOUBLE | 45.6% | 25492 | 15333 | moy 0.282 · méd 0.257 · min/max 0.0/2.275 · p10/p90 0.0/0.556 | Danger continu généré par coup franc obtenu, sur 5 matchs |
| `freekick_danger_intensity_against_5` | DOUBLE | 45.7% | 25472 | 15464 | moy 0.281 · méd 0.259 · min/max 0.0/2.275 · p10/p90 0.0/0.549 | Danger continu concédé par coup franc concédé, sur 5 matchs |
| `freekick_header_share_for_5` | DOUBLE | 37.8% | 29179 | 26 | moy 0.551 · méd 0.5 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | Part des tirs issus de coups francs obtenus qui sont des têtes, sur 5 matchs |
| `freekick_header_share_against_5` | DOUBLE | 38.1% | 29020 | 25 | moy 0.551 · méd 0.5 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | Part des tirs issus de coups francs concédés qui sont des têtes, sur 5 matchs |
| `freekick_header_goal_share_for_5` | DOUBLE | 7.5% | 43384 | 6 | moy 0.527 · méd 1.0 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | Part des buts sur coup franc obtenu marqués de la tête, sur 5 matchs |
| `freekick_header_goal_share_against_5` | DOUBLE | 7.5% | 43385 | 6 | moy 0.525 · méd 1.0 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | Part des buts sur coup franc concédé marqués de la tête, sur 5 matchs |
| `freekick_forced_bad_clearance_rate_for_5` | DOUBLE | 43.4% | 26516 | 45 | moy 0.495 · méd 0.5 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | Part des dégagements adverses mal exécutés face à nos coups francs centrés, sur 5 matchs |
| `freekick_clearance_fail_rate_against_5` | DOUBLE | 43.6% | 26439 | 43 | moy 0.489 · méd 0.5 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | Part de nos propres dégagements mal exécutés face aux coups francs centrés concédés, sur 5 matchs |
| `freekick_headed_clearance_rate_against_5` | DOUBLE | 42.1% | 27169 | 16 | moy 0.985 · méd 1.0 · min/max 0.0/1.0 · p10/p90 1.0/1.0 | Part de nos dégagements défensifs de la tête sur coup franc centré concédé, sur 5 matchs |
| `freekick_direct_distance_avg_for_5` | DOUBLE | 2.5% | 45719 | 276 | moy 26.413 · méd 27.367 · min/max 8.069/34.513 · p10/p90 20.274/31.398 | Distance moyenne (m) des coups francs directs obtenus, sur 5 matchs |
| `freekick_direct_distance_avg_against_5` | DOUBLE | 2.6% | 45690 | 264 | moy 26.457 · méd 27.433 · min/max 8.069/34.513 · p10/p90 20.501/31.398 | Distance moyenne (m) des coups francs directs concédés, sur 5 matchs |
| `freekick_direct_angle_avg_for_5` | DOUBLE | 2.5% | 45719 | 276 | moy 0.27 · méd 0.252 · min/max 0.062/0.833 · p10/p90 0.221/0.351 | Angle moyen (rad) vers le but des coups francs directs obtenus, sur 5 matchs |
| `freekick_direct_angle_avg_against_5` | DOUBLE | 2.6% | 45690 | 264 | moy 0.269 · méd 0.251 · min/max 0.062/0.833 · p10/p90 0.221/0.351 | Angle moyen (rad) vers le but des coups francs directs concédés, sur 5 matchs |
| `freekick_zone_direct_rate_for_5` | DOUBLE | 46.4% | 25111 | 101 | moy 0.001 · méd 0.0 · min/max 0.0/0.071 · p10/p90 0.0/0.0 | Part des coups francs obtenus joués en tir direct, sur 5 matchs |
| `freekick_zone_direct_rate_against_5` | DOUBLE | 46.4% | 25111 | 101 | moy 0.001 · méd 0.0 · min/max 0.0/0.071 · p10/p90 0.0/0.0 | Part des coups francs concédés joués en tir direct par l'adversaire, sur 5 matchs |
| `freekick_zone_crossed_rate_for_5` | DOUBLE | 46.4% | 25111 | 742 | moy 0.107 · méd 0.103 · min/max 0.0/0.6 · p10/p90 0.045/0.174 | Part des coups francs obtenus centrés dans la surface, sur 5 matchs |
| `freekick_zone_crossed_rate_against_5` | DOUBLE | 46.4% | 25111 | 678 | moy 0.107 · méd 0.103 · min/max 0.0/0.6 · p10/p90 0.05/0.169 | Part des coups francs concédés centrés dans la surface par l'adversaire, sur 5 matchs |
| `freekicks_for_10` | HUGEINT | 46.8% | 24923 | 37 | moy 11.653 · méd 12.0 · min/max 0.0/36.0 · p10/p90 4.0/19.0 | Nombre de coups francs crossés/directs obtenus sur les 10 derniers matchs |
| `freekick_danger_rate_for_10` | DOUBLE | 46.2% | 25217 | 184 | moy 0.298 · méd 0.286 · min/max 0.0/1.0 · p10/p90 0.1/0.5 | Taux de coups francs obtenus ayant produit une action dangereuse, sur 10 matchs |
| `freekick_conversion_rate_for_10` | DOUBLE | 42.9% | 26792 | 35 | moy 0.095 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.333 | Taux de conversion en but des actions dangereuses issues de coup franc, sur 10 matchs |
| `freekicks_against_10` | HUGEINT | 46.8% | 24923 | 34 | moy 11.658 · méd 12.0 · min/max 0.0/33.0 · p10/p90 4.0/18.0 | Nombre de coups francs crossés/directs concédés sur les 10 derniers matchs |
| `freekick_danger_rate_against_10` | DOUBLE | 46.2% | 25206 | 148 | moy 0.297 · méd 0.286 · min/max 0.0/1.0 · p10/p90 0.1/0.5 | Taux de coups francs concédés ayant produit une action dangereuse, sur 10 matchs |
| `freekick_conversion_rate_against_10` | DOUBLE | 43.0% | 26703 | 32 | moy 0.094 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.333 | Taux de conversion en but des coups francs concédés, sur 10 matchs |
| `freekick_danger_intensity_for_10` | DOUBLE | 46.2% | 25217 | 16449 | moy 0.282 · méd 0.267 · min/max 0.0/2.275 · p10/p90 0.083/0.489 | Danger continu généré par coup franc obtenu, sur 10 matchs |
| `freekick_danger_intensity_against_10` | DOUBLE | 46.2% | 25206 | 16609 | moy 0.281 · méd 0.269 · min/max 0.0/2.275 · p10/p90 0.081/0.479 | Danger continu concédé par coup franc concédé, sur 10 matchs |
| `freekick_header_share_for_10` | DOUBLE | 42.9% | 26792 | 49 | moy 0.551 · méd 0.5 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | Part des tirs issus de coups francs obtenus qui sont des têtes, sur 10 matchs |
| `freekick_header_share_against_10` | DOUBLE | 43.0% | 26703 | 46 | moy 0.55 · méd 0.5 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | Part des tirs issus de coups francs concédés qui sont des têtes, sur 10 matchs |
| `freekick_header_goal_share_for_10` | DOUBLE | 12.7% | 40941 | 9 | moy 0.525 · méd 0.5 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | Part des buts sur coup franc obtenu marqués de la tête, sur 10 matchs |
| `freekick_header_goal_share_against_10` | DOUBLE | 12.6% | 40963 | 8 | moy 0.526 · méd 0.5 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | Part des buts sur coup franc concédé marqués de la tête, sur 10 matchs |
| `freekick_forced_bad_clearance_rate_for_10` | DOUBLE | 45.3% | 25652 | 97 | moy 0.493 · méd 0.5 · min/max 0.0/1.0 · p10/p90 0.2/0.8 | Part des dégagements adverses mal exécutés face à nos coups francs centrés, sur 10 matchs |
| `freekick_clearance_fail_rate_against_10` | DOUBLE | 45.3% | 25664 | 87 | moy 0.486 · méd 0.5 · min/max 0.0/1.0 · p10/p90 0.182/0.778 | Part de nos propres dégagements mal exécutés face aux coups francs centrés concédés, sur 10 matchs |
| `freekick_headed_clearance_rate_against_10` | DOUBLE | 44.7% | 25945 | 24 | moy 0.986 · méd 1.0 · min/max 0.0/1.0 · p10/p90 1.0/1.0 | Part de nos dégagements défensifs de la tête sur coup franc centré concédé, sur 10 matchs |
| `freekick_direct_distance_avg_for_10` | DOUBLE | 4.4% | 44825 | 298 | moy 26.41 · méd 27.244 · min/max 8.069/34.513 · p10/p90 20.274/31.398 | Distance moyenne (m) des coups francs directs obtenus, sur 10 matchs |
| `freekick_direct_distance_avg_against_10` | DOUBLE | 4.6% | 44712 | 278 | moy 26.453 · méd 27.367 · min/max 8.069/34.513 · p10/p90 20.616/31.312 | Distance moyenne (m) des coups francs directs concédés, sur 10 matchs |
| `freekick_direct_angle_avg_for_10` | DOUBLE | 4.4% | 44825 | 298 | moy 0.27 · méd 0.253 · min/max 0.062/0.833 · p10/p90 0.221/0.351 | Angle moyen (rad) vers le but des coups francs directs obtenus, sur 10 matchs |
| `freekick_direct_angle_avg_against_10` | DOUBLE | 4.6% | 44712 | 278 | moy 0.27 · méd 0.251 · min/max 0.062/0.833 · p10/p90 0.221/0.351 | Angle moyen (rad) vers le but des coups francs directs concédés, sur 10 matchs |
| `freekick_zone_direct_rate_for_10` | DOUBLE | 46.8% | 24923 | 221 | moy 0.001 · méd 0.0 · min/max 0.0/0.071 · p10/p90 0.0/0.0 | Part des coups francs obtenus joués en tir direct, sur 10 matchs |
| `freekick_zone_direct_rate_against_10` | DOUBLE | 46.8% | 24923 | 197 | moy 0.001 · méd 0.0 · min/max 0.0/0.071 · p10/p90 0.0/0.0 | Part des coups francs concédés joués en tir direct par l'adversaire, sur 10 matchs |
| `freekick_zone_crossed_rate_for_10` | DOUBLE | 46.8% | 24923 | 1844 | moy 0.107 · méd 0.104 · min/max 0.0/0.6 · p10/p90 0.055/0.162 | Part des coups francs obtenus centrés dans la surface, sur 10 matchs |
| `freekick_zone_crossed_rate_against_10` | DOUBLE | 46.8% | 24923 | 1642 | moy 0.107 · méd 0.104 · min/max 0.0/0.6 · p10/p90 0.061/0.156 | Part des coups francs concédés centrés dans la surface par l'adversaire, sur 10 matchs |