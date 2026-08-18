---
schema: intermediate
rows: 159338
---
# int_whoscored_formations

#intermediate

Timeline tactique par équipe par match, identités normalisées (team_id canonique + match_id unifié via int_whoscored_match_index). formation_id reste la clé vers silver.stg_whoscored_formations_ref. Source : silver.stg_whoscored_formations.


## Intégrité
**Clé déclarée :** (match_id, team_id, formation_seq) — ⚠️ **2510 doublons**

## Lineage
**Sources :** [[int_whoscored_match_index]], [[silver.stg_whoscored_formations]]
**Alimente :** —

## Features & profiling  (159338 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 98.2% | 2807 | 16586 |  | Identifiant unifié du match (SHA1) |
| `team_id` | BIGINT | 99.2% | 1239 | 175 |  | Id canonique de l'équipe (converti depuis l'id WhoScored) |
| `end_minute` | INTEGER | 100.0% | 0 | 114 | moy 76.759 · méd 79.0 · min/max 0.0/118.0 · p10/p90 56.0/94.0 | Minute étendue de fin de la formation |
| `formation_seq` | INTEGER | 100.0% | 0 | 11 | moy 2.0 · méd 2.0 · min/max 0.0/10.0 · p10/p90 0.0/4.0 | Index de la formation dans la liste de l'équipe (0, 1, 2...) |
| `formation_id` | INTEGER | 100.0% | 0 | 23 |  | Identifiant de la formation (FK vers stg_whoscored_formations_ref) |
| `period` | INTEGER | 100.0% | 0 | 4 | moy 10.716 · méd 16.0 · min/max 1.0/16.0 · p10/p90 2.0/16.0 | Période du match où s'applique la formation : 1 = 1re mi-temps, 2 = 2e mi-temps, 16 = pré-match (FormationSet), 14 = fin de match (End). |
| `start_minute` | INTEGER | 100.0% | 0 | 113 | moy 56.781 · méd 69.0 · min/max 0.0/112.0 · p10/p90 0.0/88.0 | Minute étendue de début de la formation |
| `start_minute_reg` | INTEGER | 0.0% | 159338 | 0 | moy None · méd None · min/max None/None · p10/p90 None/None |  |
| `end_minute_reg` | INTEGER | 0.0% | 159338 | 0 | moy None · méd None · min/max None/None · p10/p90 None/None |  |
| `captain_player_id` | INTEGER | 100.0% | 19 | 2081 |  | Identifiant du capitaine sur cette période |
| `player_ids` | VARCHAR | 100.0% | 0 | 158577 |  | JSON — ordre des joueurs sur la grille tactique |
| `formation_slots` | VARCHAR | 100.0% | 0 | 177 |  |  |
| `formation_positions` | VARCHAR | 100.0% | 0 | 23 |  | JSON — coordonnées vertical/horizontal de chaque poste |