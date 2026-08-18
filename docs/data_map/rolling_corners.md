---
schema: gold
rows: 46887
---
# rolling_corners

#gold

Rolling windows W=3/5/10 sur les corners, depuis intermediate.corner_profiles. 1 ligne par équipe par match — "for" (corners obtenus) et "against" (corners concédés), jointes via intermediate.backbone (team_id / opponent_id). Action dangereuse = corner ayant produit un tir (outcome IN goal/shot_saved/ shot_off_target). Ratio de sommes sur la fenêtre, pas moyenne de taux par match. Enrichi (Vague 5.4) : intensite de danger continue (xG/corner), profil aerien (tetes sur tirs/buts), tendance corner court, et bataille du degagement defensif.


## Intégrité
**Clé déclarée :** (match_id, team_id) — ✅ aucun doublon

## Lineage
**Sources :** [[backbone]], [[corner_profiles]], [[player_possession_chains]]
**Alimente :** —

## Features & profiling  (46887 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 28499 |  | ID unique du match provenant du backbone |
| `team_id` | BIGINT | 100.0% | 0 | 153 |  | ID de l'équipe |
| `date` | DATE | 100.0% | 0 | 2417 |  | Date du match |
| `season` | VARCHAR | 100.0% | 0 | 9 | top: 2018-2019 (5623), 2017-2018 (5525), 2021-2022 (5379) | Saison |
| `league_source` | VARCHAR | 100.0% | 0 | 28 |  | Compétition source |
| `corners_for_3` | HUGEINT | 46.1% | 25263 | 41 | moy 14.17 · méd 14.0 · min/max 0.0/41.0 · p10/p90 7.0/21.0 | Nombre de corners obtenus sur les 3 derniers matchs |
| `corner_danger_rate_for_3` | DOUBLE | 46.1% | 25284 | 235 | moy 0.345 · méd 0.333 · min/max 0.0/1.0 · p10/p90 0.167/0.533 | Taux de corners obtenus ayant produit une action dangereuse, sur 3 matchs |
| `corner_conversion_rate_for_3` | DOUBLE | 45.1% | 25731 | 46 | moy 0.098 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.333 | Taux de conversion en but des actions dangereuses issues de corner, sur 3 matchs |
| `corners_against_3` | HUGEINT | 46.1% | 25263 | 40 | moy 14.169 · méd 14.0 · min/max 0.0/39.0 · p10/p90 7.0/21.0 | Nombre de corners concédés sur les 3 derniers matchs |
| `corner_danger_rate_against_3` | DOUBLE | 46.1% | 25284 | 231 | moy 0.344 · méd 0.333 · min/max 0.0/1.0 · p10/p90 0.167/0.529 | Taux de corners concédés ayant produit une action dangereuse, sur 3 matchs |
| `corner_conversion_rate_against_3` | DOUBLE | 45.1% | 25731 | 43 | moy 0.098 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.333 | Taux de conversion en but des corners concédés, sur 3 matchs |
| `corner_danger_intensity_for_3` | DOUBLE | 46.1% | 25284 | 21118 | moy 0.351 · méd 0.34 · min/max 0.0/1.589 · p10/p90 0.154/0.56 | Danger continu généré par corner obtenu (somme chain_danger_total / nb corners), sur 3 matchs — équivalent xG par corner |
| `corner_danger_intensity_against_3` | DOUBLE | 46.1% | 25284 | 21086 | moy 0.349 · méd 0.339 · min/max 0.0/1.589 · p10/p90 0.154/0.555 | Danger continu concédé par corner concédé, sur 3 matchs |
| `corner_header_share_for_3` | DOUBLE | 45.1% | 25731 | 77 | moy 0.501 · méd 0.5 · min/max 0.0/1.0 · p10/p90 0.125/0.875 | Part des tirs issus de corners obtenus qui sont des têtes, sur 3 matchs |
| `corner_header_share_against_3` | DOUBLE | 45.1% | 25731 | 80 | moy 0.504 · méd 0.5 · min/max 0.0/1.0 · p10/p90 0.125/1.0 | Part des tirs issus de corners concédés qui sont des têtes, sur 3 matchs |
| `corner_header_goal_share_for_3` | DOUBLE | 17.0% | 38916 | 9 | moy 0.516 · méd 0.5 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | Part des buts sur corner obtenu marqués de la tête, sur 3 matchs |
| `corner_header_goal_share_against_3` | DOUBLE | 16.9% | 38974 | 8 | moy 0.517 · méd 0.5 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | Part des buts sur corner concédé marqués de la tête, sur 3 matchs |
| `corner_short_rate_for_3` | DOUBLE | 46.1% | 25284 | 134 | moy 0.045 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.133 | Part des corners obtenus joués courts plutôt que centrés, sur 3 matchs |
| `corner_short_rate_against_3` | DOUBLE | 46.1% | 25284 | 122 | moy 0.044 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.133 | Part des corners concédés joués courts par l'adversaire, sur 3 matchs |
| `corner_forced_bad_clearance_rate_for_3` | DOUBLE | 45.8% | 25392 | 123 | moy 0.478 · méd 0.5 · min/max 0.0/1.0 · p10/p90 0.2/0.75 | Part des dégagements adverses mal exécutés (poor/failed) face à nos corners, sur 3 matchs — capacité à créer du chaos en seconde balle |
| `corner_clearance_fail_rate_against_3` | DOUBLE | 45.8% | 25412 | 127 | moy 0.476 · méd 0.5 · min/max 0.0/1.0 · p10/p90 0.2/0.75 | Part de nos propres dégagements mal exécutés (poor/failed) face aux corners concédés, sur 3 matchs — solidité défensive |
| `corner_headed_clearance_rate_against_3` | DOUBLE | 45.5% | 25558 | 40 | moy 0.982 · méd 1.0 · min/max 0.0/1.0 · p10/p90 0.938/1.0 | Part de nos dégagements défensifs faits de la tête, sur 3 matchs |
| `corners_for_5` | HUGEINT | 46.4% | 25111 | 60 | moy 22.832 · méd 23.0 · min/max 0.0/64.0 · p10/p90 13.0/33.0 | Nombre de corners obtenus sur les 5 derniers matchs |
| `corner_danger_rate_for_5` | DOUBLE | 46.4% | 25131 | 415 | moy 0.345 · méd 0.342 · min/max 0.0/1.0 · p10/p90 0.2/0.5 | Taux de corners obtenus ayant produit une action dangereuse, sur 5 matchs |
| `corner_conversion_rate_for_5` | DOUBLE | 46.0% | 25319 | 73 | moy 0.096 · méd 0.071 · min/max 0.0/1.0 · p10/p90 0.0/0.25 | Taux de conversion en but des actions dangereuses issues de corner, sur 5 matchs |
| `corners_against_5` | HUGEINT | 46.4% | 25111 | 58 | moy 22.829 · méd 23.0 · min/max 0.0/60.0 · p10/p90 12.0/33.0 | Nombre de corners concédés sur les 5 derniers matchs |
| `corner_danger_rate_against_5` | DOUBLE | 46.4% | 25132 | 412 | moy 0.344 · méd 0.333 · min/max 0.0/1.0 · p10/p90 0.2/0.5 | Taux de corners concédés ayant produit une action dangereuse, sur 5 matchs |
| `corner_conversion_rate_against_5` | DOUBLE | 46.0% | 25324 | 77 | moy 0.097 · méd 0.067 · min/max 0.0/1.0 · p10/p90 0.0/0.25 | Taux de conversion en but des corners concédés, sur 5 matchs |
| `corner_danger_intensity_for_5` | DOUBLE | 46.4% | 25131 | 21413 | moy 0.351 · méd 0.344 · min/max 0.0/1.589 · p10/p90 0.191/0.514 | Danger continu généré par corner obtenu, sur 5 matchs — équivalent xG par corner |
| `corner_danger_intensity_against_5` | DOUBLE | 46.4% | 25132 | 21431 | moy 0.35 · méd 0.342 · min/max 0.0/1.589 · p10/p90 0.192/0.514 | Danger continu concédé par corner concédé, sur 5 matchs |
| `corner_header_share_for_5` | DOUBLE | 46.0% | 25319 | 133 | moy 0.502 · méd 0.5 · min/max 0.0/1.0 · p10/p90 0.25/0.75 | Part des tirs issus de corners obtenus qui sont des têtes, sur 5 matchs |
| `corner_header_share_against_5` | DOUBLE | 46.0% | 25324 | 145 | moy 0.505 · méd 0.5 · min/max 0.0/1.0 · p10/p90 0.25/0.778 | Part des tirs issus de corners concédés qui sont des têtes, sur 5 matchs |
| `corner_header_goal_share_for_5` | DOUBLE | 24.0% | 35643 | 12 | moy 0.516 · méd 0.5 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | Part des buts sur corner obtenu marqués de la tête, sur 5 matchs |
| `corner_header_goal_share_against_5` | DOUBLE | 23.8% | 35706 | 11 | moy 0.518 · méd 0.5 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | Part des buts sur corner concédé marqués de la tête, sur 5 matchs |
| `corner_short_rate_for_5` | DOUBLE | 46.4% | 25131 | 223 | moy 0.045 · méd 0.036 · min/max 0.0/1.0 · p10/p90 0.0/0.115 | Part des corners obtenus joués courts plutôt que centrés, sur 5 matchs |
| `corner_short_rate_against_5` | DOUBLE | 46.4% | 25132 | 198 | moy 0.045 · méd 0.036 · min/max 0.0/1.0 · p10/p90 0.0/0.111 | Part des corners concédés joués courts par l'adversaire, sur 5 matchs |
| `corner_forced_bad_clearance_rate_for_5` | DOUBLE | 46.2% | 25206 | 206 | moy 0.48 · méd 0.5 · min/max 0.0/1.0 · p10/p90 0.273/0.688 | Part des dégagements adverses mal exécutés face à nos corners, sur 5 matchs |
| `corner_clearance_fail_rate_against_5` | DOUBLE | 46.2% | 25215 | 215 | moy 0.477 · méd 0.5 · min/max 0.0/1.0 · p10/p90 0.273/0.667 | Part de nos propres dégagements mal exécutés face aux corners concédés, sur 5 matchs |
| `corner_headed_clearance_rate_against_5` | DOUBLE | 46.1% | 25270 | 55 | moy 0.981 · méd 1.0 · min/max 0.0/1.0 · p10/p90 0.917/1.0 | Part de nos dégagements défensifs faits de la tête, sur 5 matchs |
| `corners_for_10` | HUGEINT | 46.8% | 24923 | 102 | moy 42.117 · méd 44.0 · min/max 0.0/106.0 · p10/p90 18.0/61.0 | Nombre de corners obtenus sur les 10 derniers matchs |
| `corner_danger_rate_for_10` | DOUBLE | 46.8% | 24941 | 909 | moy 0.345 · méd 0.342 · min/max 0.0/1.0 · p10/p90 0.231/0.458 | Taux de corners obtenus ayant produit une action dangereuse, sur 10 matchs |
| `corner_conversion_rate_for_10` | DOUBLE | 46.5% | 25094 | 146 | moy 0.096 · méd 0.08 · min/max 0.0/1.0 · p10/p90 0.0/0.2 | Taux de conversion en but des actions dangereuses issues de corner, sur 10 matchs |
| `corners_against_10` | HUGEINT | 46.8% | 24923 | 96 | moy 42.113 · méd 44.0 · min/max 0.0/102.0 · p10/p90 18.0/62.0 | Nombre de corners concédés sur les 10 derniers matchs |
| `corner_danger_rate_against_10` | DOUBLE | 46.8% | 24940 | 848 | moy 0.344 · méd 0.34 · min/max 0.0/1.0 · p10/p90 0.231/0.458 | Taux de corners concédés ayant produit une action dangereuse, sur 10 matchs |
| `corner_conversion_rate_against_10` | DOUBLE | 46.5% | 25098 | 144 | moy 0.096 · méd 0.083 · min/max 0.0/1.0 · p10/p90 0.0/0.2 | Taux de conversion en but des corners concédés, sur 10 matchs |
| `corner_danger_intensity_for_10` | DOUBLE | 46.8% | 24941 | 21553 | moy 0.351 · méd 0.348 · min/max 0.0/1.589 · p10/p90 0.224/0.478 | Danger continu généré par corner obtenu, sur 10 matchs — équivalent xG par corner |
| `corner_danger_intensity_against_10` | DOUBLE | 46.8% | 24940 | 21562 | moy 0.35 · méd 0.345 · min/max 0.0/1.589 · p10/p90 0.228/0.474 | Danger continu concédé par corner concédé, sur 10 matchs |
| `corner_header_share_for_10` | DOUBLE | 46.5% | 25094 | 282 | moy 0.502 · méd 0.5 · min/max 0.0/1.0 · p10/p90 0.3/0.7 | Part des tirs issus de corners obtenus qui sont des têtes, sur 10 matchs |
| `corner_header_share_against_10` | DOUBLE | 46.5% | 25098 | 283 | moy 0.506 · méd 0.5 · min/max 0.0/1.0 · p10/p90 0.308/0.706 | Part des tirs issus de corners concédés qui sont des têtes, sur 10 matchs |
| `corner_header_goal_share_for_10` | DOUBLE | 33.3% | 31267 | 18 | moy 0.515 · méd 0.5 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | Part des buts sur corner obtenu marqués de la tête, sur 10 matchs |
| `corner_header_goal_share_against_10` | DOUBLE | 33.4% | 31213 | 23 | moy 0.515 · méd 0.5 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | Part des buts sur corner concédé marqués de la tête, sur 10 matchs |
| `corner_short_rate_for_10` | DOUBLE | 46.8% | 24941 | 488 | moy 0.045 · méd 0.036 · min/max 0.0/1.0 · p10/p90 0.0/0.102 | Part des corners obtenus joués courts plutôt que centrés, sur 10 matchs |
| `corner_short_rate_against_10` | DOUBLE | 46.8% | 24940 | 420 | moy 0.045 · méd 0.037 · min/max 0.0/1.0 · p10/p90 0.0/0.1 | Part des corners concédés joués courts par l'adversaire, sur 10 matchs |
| `corner_forced_bad_clearance_rate_for_10` | DOUBLE | 46.7% | 25010 | 448 | moy 0.48 · méd 0.483 · min/max 0.0/1.0 · p10/p90 0.321/0.636 | Part des dégagements adverses mal exécutés face à nos corners, sur 10 matchs |
| `corner_clearance_fail_rate_against_10` | DOUBLE | 46.6% | 25015 | 455 | moy 0.478 · méd 0.483 · min/max 0.0/1.0 · p10/p90 0.321/0.632 | Part de nos propres dégagements mal exécutés face aux corners concédés, sur 10 matchs |
| `corner_headed_clearance_rate_against_10` | DOUBLE | 46.5% | 25062 | 109 | moy 0.981 · méd 1.0 · min/max 0.0/1.0 · p10/p90 0.933/1.0 | Part de nos dégagements défensifs faits de la tête, sur 10 matchs |