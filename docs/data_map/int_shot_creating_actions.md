---
schema: intermediate
rows: 426125
---
# int_shot_creating_actions

#intermediate

Actions créatrices de tir (SCA) et de but (GCA), standard FBref/StatsBomb. Une ligne par crédit = (tir, sca_order 1|2) : les 2 actions offensives de l'équipe qui tire, dans la même chaîne de possession, juste avant le tir (Pass, TakeOn, faute obtenue, tir/rebond). is_gca vrai si le tir est un but. Source : player_possession_chains.


## Intégrité
**Clé déclarée :** (match_id, shot_row_num, sca_order) — ✅ aucun doublon

## Lineage
**Sources :** [[player_possession_chains]]
**Alimente :** —

## Features & profiling  (426125 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 10968 |  | Identifiant unifié du match (SHA1) |
| `shot_row_num` | INTEGER | 100.0% | 0 | 1845 | moy 838.669 · méd 860.0 · min/max 3.0/1921.0 · p10/p90 210.0/1435.0 | row_num du tir crédité. |
| `sca_order` | BIGINT | 100.0% | 0 | 2 | moy 1.427 · méd 1.0 · min/max 1.0/2.0 · p10/p90 1.0/2.0 | 1 = action juste avant le tir, 2 = l'action précédente |
| `shot_event_id` | INTEGER | 100.0% | 0 | 1290 |  | event_id du tir. |
| `chain_id` | VARCHAR | 100.0% | 0 | 226251 |  | Identifiant de la chaîne de possession (propagé de player_possession_chains). |
| `season` | VARCHAR | 100.0% | 0 | 8 | top: 2018-2019 (57793), 2023-2024 (55796), 2017-2018 (55753) | Saison du match (ex. 2023-2024). |
| `league_source` | VARCHAR | 100.0% | 0 | 4 | top: Serie A (115292), Premier League (115078), Ligue 1 (101807) | Championnat / source des données (ex. Bundesliga). |
| `scraped_at` | VARCHAR | 100.0% | 0 | 10968 |  | Horodatage ISO du scrape. |
| `attacking_team_id` | BIGINT | 100.0% | 0 | 120 |  | Équipe qui tire |
| `shot_taker_player_id` | INTEGER | 100.0% | 0 | 4682 |  | Joueur qui tire. |
| `creator_player_id` | INTEGER | 100.0% | 0 | 5059 |  | Joueur crédité de l'action créatrice |
| `action_type` | VARCHAR | 100.0% | 0 | 5 | top: Pass (378824), TakeOn (29144), SavedShot (14302) | Type de l'action créatrice (Pass, TakeOn, tir…) |
| `action_row_num` | INTEGER | 100.0% | 0 | 1865 | moy 836.665 · méd 858.0 · min/max 2.0/1919.0 · p10/p90 207.0/1433.0 | row_num de l'action créatrice. |
| `is_gca` | BOOLEAN | 100.0% | 0 | 2 | top: False (381068), True (45057) | Vrai si le tir est un but → ce crédit compte aussi en GCA |