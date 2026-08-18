---
schema: intermediate
rows: 5195
---
# int_penalties

#intermediate

Penaltys au grain « un penalty tiré ». Pivot = la frappe (types 13/14/15/16 portant qual 9) : tireur, résultat (marqué/arrêté/poteau/hors-cadre) et placement (goal_mouth_*). Rattachés en LEFT JOIN : PenaltyFaced (type 58, même instant) → équipe qui défend + gardien ; le foul en double (type 4 + qual 9) → joueur qui obtient / concède, dans une fenêtre de 300 s avant la frappe. Sources : int_event_enriched, int_whoscored_events, events_qual.


## Intégrité
**Clé déclarée :** (match_id, row_num) — ✅ aucun doublon

## Lineage
**Sources :** [[events_qual]], [[int_event_enriched]], [[int_whoscored_events]]
**Alimente :** [[joueur_saison]]

## Features & profiling  (5195 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 4432 |  | Identifiant unifié du match (SHA1) |
| `row_num` | INTEGER | 100.0% | 0 | 1576 | moy 860.275 · méd 886.0 · min/max 8.0/1851.0 · p10/p90 246.8/1430.0 | row_num du tir du penalty. Avec match_id, clé unique. |
| `season` | VARCHAR | 100.0% | 0 | 8 | top: 2020-2021 (773), 2017-2018 (718), 2018-2019 (662) | Saison du match (ex. 2023-2024). |
| `league_source` | VARCHAR | 100.0% | 0 | 7 | top: Serie A (1013), Ligue 1 (1001), La Liga (998) | Championnat / source des données (ex. Bundesliga). |
| `scraped_at` | VARCHAR | 100.0% | 0 | 4432 |  | Horodatage ISO du scrape. |
| `expanded_minute` | INTEGER | 100.0% | 0 | 110 | moy 54.213 · méd 56.0 · min/max 0.0/109.0 · p10/p90 15.0/91.0 | Minute (expanded_minute) du penalty. |
| `attacking_team_id` | BIGINT | 100.0% | 0 | 175 |  | Id canonique de l'équipe qui tire le penalty |
| `taker_player_id` | INTEGER | 100.0% | 0 | 1138 |  | Joueur qui tire le penalty |
| `defending_team_id` | BIGINT | 100.0% | 2 | 175 |  | Équipe qui défend (via type 58). NULL si pas de PenaltyFaced apparié (~0,1 %). |
| `gk_player_id` | INTEGER | 100.0% | 2 | 486 |  | Gardien face au penalty (via type 58) |
| `player_drew` | INTEGER | 76.0% | 1247 | 1898 | moy 221654.247 · méd 236531.5 · min/max 2943.0/541704.0 · p10/p90 41412.5/398301.0 | Joueur ayant obtenu le penalty. Souvent NULL (limite données WhoScored, ~23 %). |
| `player_conceded` | INTEGER | 100.0% | 0 | 2718 | moy 211827.634 · méd 143695.0 · min/max 661.0/554938.0 · p10/p90 34176.0/398725.0 | Joueur ayant concédé la faute (via qual 233). Rattaché à 100 %. |
| `is_goal` | BOOLEAN | 100.0% | 0 | 2 | top: True (4080), False (1115) | Booléen : penalty marqué. |
| `is_saved` | BOOLEAN | 100.0% | 0 | 2 | top: False (4350), True (845) | Booléen : penalty arrêté par le gardien. |
| `is_post` | BOOLEAN | 100.0% | 0 | 2 | top: False (5061), True (134) | Booléen : penalty sur le poteau/la barre. |
| `is_off_target` | BOOLEAN | 100.0% | 0 | 2 | top: False (5059), True (136) | Booléen : penalty non cadré. |
| `result_label` | VARCHAR | 100.0% | 0 | 4 | top: scored (4080), saved (845), off_target (136) | scored / saved / post / off_target |
| `goal_mouth_y` | DOUBLE | 98.0% | 105 | 147 | moy 50.266 · méd 50.7 · min/max 38.7/61.3 · p10/p90 46.1/54.0 | Position horizontale du penalty dans le cadre du but (qualifier 102). |
| `goal_mouth_z` | DOUBLE | 98.0% | 105 | 113 | moy 11.04 · méd 6.3 · min/max 0.6/98.6 · p10/p90 1.3/25.9 | Hauteur du penalty dans le cadre (qualifier 103). |