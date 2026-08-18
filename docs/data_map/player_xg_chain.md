---
schema: intermediate
rows: 1083935
---
# player_xg_chain

#intermediate

Attribue une valeur xG (proxy via chance_creation du tir terminal) aux joueurs impliqués dans une chaîne se terminant par un tir — crédit offensif réparti sur la chaîne. Source : player_possession_chains. Incrémentale.


## Intégrité
**Clé déclarée :** (match_id, chain_id, player_id) — ⚠️ **17527 doublons**

## Lineage
**Sources :** [[event_values]], [[events_qual]], [[player_possession_chains]]
**Alimente :** [[features_players]], [[joueur_saison]], [[threat_conceded]]

## Features & profiling  (1083935 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 10968 |  | Identifiant unifié du match (SHA1). |
| `chain_id` | VARCHAR | 100.0% | 0 | 255349 |  | Identifiant de la chaîne de possession = match_id // '_' // numéro de chaîne (propagé de player_possession_chains). Regroupe les actions d'une même possession. |
| `chain_number` | HUGEINT | 100.0% | 0 | 469 | moy 164.592 · méd 164.0 · min/max 0.0/504.0 · p10/p90 39.0/289.0 | Numéro séquentiel de la chaîne de possession dans le match (propagé de player_possession_chains). |
| `chain_team_id` | BIGINT | 100.0% | 0 | 120 |  | Id canonique de l'équipe EN POSSESSION durant la chaîne (propagé de player_possession_chains). C'est l'équipe attaquante ; team_id peut différer (action d'un autre joueur). |
| `player_id` | INTEGER | 100.0% | 0 | 5486 |  | Identifiant du joueur de l'équipe en possession présent dans la chaîne (participant à la construction). |
| `season` | VARCHAR | 100.0% | 0 | 8 | top: 2018-2019 (145305), 2023-2024 (143336), 2021-2022 (138391) | Saison du match (ex. 2023-2024). |
| `league_source` | VARCHAR | 100.0% | 0 | 4 | top: Serie A (295400), Premier League (292123), Ligue 1 (262625) | Championnat / source des données (ex. Bundesliga). |
| `shot_event_id` | INTEGER | 100.0% | 0 | 1306 |  | event_id WhoScored du tir terminal (dernier tir) de la chaîne. |
| `shot_type_id` | INTEGER | 100.0% | 0 | 4 |  | type_id du tir terminal (13 MissedShots, 14 ShotOnPost, 15 SavedShot, 16 Goal). |
| `shot_minute` | INTEGER | 100.0% | 0 | 125 | moy 50.002 · méd 51.0 · min/max 0.0/32772.0 · p10/p90 11.0/88.0 | Minute (expanded_minute) du tir terminal de la chaîne. |
| `xg_proxy` | DECIMAL(3,2) | 100.0% | 0 | 7 | moy 0.594 · méd 0.8 · min/max 0.1/1.0 · p10/p90 0.1/1.0 | Proxy xG du tir terminal de la chaîne = chance_creation de event_values pour ce tir (0-1). |
| `is_counter_attack` | BOOLEAN | 100.0% | 0 | 2 | top: False (1044792), True (39143) | Booléen : la chaîne contient un tir en contre-attaque (qualifier 23 FastBreak) — propriété de la chaîne entière, pas du seul tir. |
| `position_in_chain` | BIGINT | 100.0% | 0 | 95 | moy 6.519 · méd 5.0 · min/max 1.0/109.0 · p10/p90 1.0/14.0 | Rang chronologique du joueur dans la chaîne (1 = premier), déduplication sur sa DERNIÈRE apparition (la plus proche du tir). |
| `chain_length` | BIGINT | 100.0% | 0 | 69 | moy 9.782 · méd 8.0 · min/max 1.0/109.0 · p10/p90 3.0/18.0 | Nombre total de participations (touches) de l'équipe en possession dans la chaîne, avant déduplication par joueur. |
| `position_weight` | DOUBLE | 100.0% | 0 | 906 | moy 0.676 · méd 0.714 · min/max 0.02/1.0 · p10/p90 0.25/1.0 | Poids de proximité au tir = position_in_chain / chain_length (0-1) : plus le joueur agit tard (près du tir), plus le poids est élevé. |
| `is_shooter` | BOOLEAN | 100.0% | 0 | 2 | top: False (825053), True (258882) | Booléen : le joueur est le tireur terminal de la chaîne. |
| `is_assister` | BOOLEAN | 100.0% | 0 | 2 | top: False (893925), True (190010) | Booléen : le joueur est le passeur décisif du tir (résolu via RelatedEventId, qualifier 55). |
| `xgchain` | DECIMAL(3,2) | 100.0% | 0 | 7 | moy 0.594 · méd 0.8 · min/max 0.1/1.0 · p10/p90 0.1/1.0 | xGChain : crédit uniforme = xg_proxy du tir terminal, attribué à chaque joueur de la chaîne. |
| `xgchain_weighted` | DOUBLE | 100.0% | 0 | 2018 | moy 0.397 · méd 0.364 · min/max 0.002/1.0 · p10/p90 0.073/0.8 | xGChain pondéré = xg_proxy × position_weight (crédit croissant vers le tir). |
| `xgbuildup` | DECIMAL(3,2) | 58.6% | 448281 | 7 | moy 0.599 · méd 0.8 · min/max 0.1/1.0 · p10/p90 0.1/1.0 | xGBuildup : = xgchain pour les joueurs de construction ; NULL pour le tireur et le passeur décisif (isole l'apport hors finition). |
| `xgbuildup_weighted` | DOUBLE | 58.6% | 448281 | 1966 | moy 0.325 · méd 0.3 · min/max 0.002/1.0 · p10/p90 0.057/0.667 | xGBuildup pondéré : = xgchain_weighted pour les joueurs de construction ; NULL pour le tireur et le passeur décisif. |