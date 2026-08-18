---
schema: intermediate
rows: 8616679
---
# player_passes_raw

#intermediate

Passes atomiques passeur → receveur reconstruites depuis les chaînes de possession (LEAD sur les événements de l'équipe en possession, sans sauter les événements adverses). Porte les flags is_progressive/is_creative/ is_buildup et le chain_trigger. Brique de base du réseau de passes. Grain : une passe réussie (type 1, outcome 1, avec receveur).


## Intégrité
**Clé déclarée :** (match_id, row_num) — ✅ aucun doublon

## Lineage
**Sources :** [[int_event_enriched]], [[player_possession_chains]]
**Alimente :** [[int_xt_actions]], [[player_network_passes]], [[player_zone_transitions]], [[team_network_features]]

## Features & profiling  (8616679 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 10969 |  | Identifiant unifié du match (SHA1), résolu via int_whoscored_match_index. |
| `chain_id` | VARCHAR | 100.0% | 0 | 1987903 |  | Identifiant de la chaîne de possession (repris de player_possession_chains). |
| `chain_trigger` | VARCHAR | 100.0% | 0 | 6 | top: open_play (3629157), recovery (2174845), throw_in (1425921) | Déclencheur de la chaîne : recovery / corner / goal_kick / throw_in / free_kick / open_play (repris de player_possession_chains). |
| `team_id` | BIGINT | 100.0% | 0 | 120 |  | Id canonique de l'équipe qui fait la passe (= chain_team_id, l'équipe en possession). |
| `passer_id` | INTEGER | 100.0% | 0 | 5784 |  | Joueur qui effectue la passe (= player_id de l'événement Pass). |
| `receiver_id` | INTEGER | 100.0% | 0 | 5799 |  | Joueur qui reçoit = prochain joueur de la MÊME équipe dans la chaîne (LEAD sur les événements de l'équipe en possession). |
| `row_num` | INTEGER | 100.0% | 0 | 1920 | moy 765.159 · méd 751.0 · min/max 0.0/1922.0 · p10/p90 150.0/1395.0 | Index de l'événement (la passe) dans le flux du match (ordre chronologique). |
| `expanded_minute` | INTEGER | 100.0% | 0 | 130 | moy 45.718 · méd 45.0 · min/max 0.0/129.0 · p10/p90 8.0/85.0 | Minute cumulée, temps additionnel inclus (ex. 45+2 → 47). |
| `second` | INTEGER | 100.0% | 0 | 61 | moy 29.313 · méd 29.0 · min/max -2.0/59.0 · p10/p90 5.0/53.0 | Seconde dans la minute. |
| `x` | DOUBLE | 100.0% | 0 | 1001 | moy 46.726 · méd 45.7 · min/max 0.0/100.0 · p10/p90 19.2/73.8 | Position X de départ de la passe (0-100, croissant vers le but adverse ; repère relatif à l'équipe). |
| `y` | DOUBLE | 100.0% | 0 | 1001 | moy 50.451 · méd 50.4 · min/max 0.0/100.0 · p10/p90 8.8/91.4 | Position Y de départ de la passe (0-100, largeur, 50 = axe central). |
| `end_x` | DOUBLE | 100.0% | 0 | 1001 | moy 49.421 · méd 47.8 · min/max 0.0/100.0 · p10/p90 21.9/77.8 | Position X de fin de la passe (repris de int_event_enriched). |
| `end_y` | DOUBLE | 100.0% | 0 | 1001 | moy 50.516 · méd 50.9 · min/max 0.0/100.0 · p10/p90 8.6/91.8 | Position Y de fin de la passe (repris de int_event_enriched). |
| `is_key_pass` | INTEGER | 100.0% | 0 | 2 | moy 0.023 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.0 | Flag (0/1) : passe clé — mène à un tir (qualifier 11113 KeyPass, repris de int_event_enriched). |
| `is_shot_assist` | INTEGER | 100.0% | 0 | 2 | moy 0.023 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.0 | Flag (0/1) : passe précédant directement un tir (qualifier 210 ShotAssist, repris de int_event_enriched). |
| `season` | VARCHAR | 100.0% | 0 | 8 | top: 2020-2021 (1146222), 2021-2022 (1116513), 2018-2019 (1114123) | Saison du match (ex. 2023-2024). |
| `league_source` | VARCHAR | 100.0% | 0 | 4 | top: Premier League (2325654), Serie A (2250488), Ligue 1 (2201897) | Championnat / source des données (ex. Bundesliga). |
| `is_progressive` | BOOLEAN | 100.0% | 0 | 2 | top: False (6663251), True (1953428) | Booléen : la passe avance le ballon d'au moins 10 unités (≈10 m) vers le but adverse (end_x > x + 10). |
| `is_creative` | BOOLEAN | 100.0% | 0 | 2 | top: False (8420345), True (196334) | Booléen : la passe mène directement à un tir (is_key_pass OU is_shot_assist). |
| `is_buildup` | BOOLEAN | 100.0% | 0 | 2 | top: True (4811706), False (3804973) | Booléen : la passe est jouée dans la moitié défensive (x < 50). |