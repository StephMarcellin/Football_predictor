---
schema: intermediate
rows: 31258
---
# int_whoscored_players

#intermediate

Dimension joueur par saison et par équipe — grain (player_id, season, team_id canonique). Un transfert en cours de saison = plusieurs lignes. Profil courant height/weight (âge exclu car WhoScored renvoie l'âge du scrape, pas du match). Sources : int_whoscored_player_match, int_whoscored_match_index, silver.stg_whoscored_players_ref.


## Intégrité
**Clé déclarée :** (player_id, season, team_id) — ✅ aucun doublon

## Lineage
**Sources :** [[int_whoscored_match_index]], [[int_whoscored_player_match]], [[silver.stg_whoscored_players_ref]]
**Alimente :** —

## Features & profiling  (31258 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `player_id` | INTEGER | 100.0% | 0 | 10792 |  | Identifiant WhoScored du joueur |
| `season` | VARCHAR | 100.0% | 0 | 8 | top: 2017-2018 (4479), 2021-2022 (4194), 2023-2024 (4051) | Saison (rattachée via le match) |
| `team_id` | BIGINT | 100.0% | 0 | 174 |  | Id canonique de l'équipe |
| `player_name` | VARCHAR | 100.0% | 0 | 10704 |  | Nom du joueur (depuis la table de référence) |
| `height` | INTEGER | 100.0% | 0 | 48 | moy 179.094 · méd 183.0 · min/max 0.0/206.0 · p10/p90 173.0/191.0 | Taille du joueur (cm, depuis la table de référence) |
| `weight` | INTEGER | 100.0% | 0 | 55 | moy 73.389 · méd 76.0 · min/max 0.0/105.0 · p10/p90 66.0/86.0 | Poids du joueur (kg, depuis la table de référence) |
| `n_matchs` | BIGINT | 100.0% | 0 | 44 | moy 20.712 · méd 23.0 · min/max 1.0/44.0 · p10/p90 2.0/36.0 | Nombre de matchs joués pour cette équipe cette saison |