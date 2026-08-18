---
schema: intermediate
rows: 256649
---
# int_defensive_blocks

#intermediate

Contres défensifs au grain « un contre », avec le défenseur qui bloque. Basé sur l'event type 74 BlockedPass (bloqueur toujours nommé), rattaché à la passe bloquée via le qual 233 (99,4 %). Ce sont des contres de PASSE ; les contres de tir n'ont pas de bloqueur nommé chez Opta et sont hors périmètre. Le join qual 233 contraint l'équipe adverse (event_id scopé par équipe). Source : int_whoscored_events, events_qual.


## Intégrité
**Clé déclarée :** (match_id, row_num) — ✅ aucun doublon

## Lineage
**Sources :** [[events_qual]], [[int_whoscored_events]]
**Alimente :** —

## Features & profiling  (256649 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 16572 |  | Identifiant unifié du match (SHA1) |
| `row_num` | INTEGER | 100.0% | 0 | 1834 | moy 759.052 · méd 752.0 · min/max 3.0/1887.0 · p10/p90 136.0/1390.0 | row_num du contre. Avec match_id, clé unique. |
| `event_id` | INTEGER | 100.0% | 0 | 1267 |  | event_id WhoScored (scopé match+équipe). |
| `season` | VARCHAR | 100.0% | 0 | 8 | top: 2017-2018 (41907), 2018-2019 (33327), 2023-2024 (32453) | Saison du match (ex. 2023-2024). |
| `league_source` | VARCHAR | 100.0% | 0 | 7 | top: Premier League (46212), La Liga (43456), Bundesliga (43061) | Championnat / source des données (ex. Bundesliga). |
| `scraped_at` | VARCHAR | 100.0% | 0 | 16570 |  | Horodatage ISO du scrape. |
| `expanded_minute` | INTEGER | 100.0% | 0 | 119 | moy 45.835 · méd 45.0 · min/max 0.0/124.0 · p10/p90 7.0/86.0 | Minute (expanded_minute) du contre. |
| `blocking_team_id` | BIGINT | 100.0% | 0 | 175 |  | Équipe qui bloque (défend) |
| `blocker_player_id` | INTEGER | 100.0% | 0 | 7050 |  | Défenseur qui bloque (type 74). Toujours renseigné. |
| `blocked_team_id` | BIGINT | 100.0% | 124 | 175 |  | Équipe dont la passe est bloquée (adverse) |
| `blocked_player_id` | INTEGER | 100.0% | 124 | 7155 |  | Joueur dont la passe/action est bloquée (via qual 233). NULL <1 %. |
| `blocked_type` | VARCHAR | 100.0% | 124 | 2 | top: Pass (253288), Clearance (3237) | Type d'action bloquée (surtout Pass) |
| `block_x` | DOUBLE | 100.0% | 0 | 999 | moy 46.673 · méd 44.2 · min/max 0.0/99.9 · p10/p90 12.8/80.7 | Position X du contre (0-100). |
| `block_y` | DOUBLE | 100.0% | 0 | 1000 | moy 50.514 · méd 51.0 · min/max 0.0/99.9 · p10/p90 4.9/95.4 | Position Y du contre (0-100). |