---
schema: intermediate
rows: 16870
---
# int_whoscored_match_index

#intermediate

Pont d'identité entre l'univers WhoScored (ws_match_id, ws_home/away_team_id) et les clés unifiées (match_id, team_id normalisé). Table de résolution d'identité centrale pour toute la chaîne événementielle WhoScored. Grain : une ligne par match.


## Intégrité
**Clé déclarée :** — (pas de test d'unicité)

## Lineage
**Sources :** [[intermediate.match_registry]], [[silver.stg_whoscored_match_index]], [[team_mapping]]
**Alimente :** [[events_qual]], [[features_draw]], [[features_whoscored]], [[int_event_enriched]], [[int_keeper_shots]], [[int_whoscored_events]], [[int_whoscored_formations]], [[int_whoscored_lineup]], [[int_whoscored_player_match]], [[int_whoscored_players]], [[int_whoscored_team_match]], [[player_match_stats]]

## Features & profiling  (16870 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 98.3% | 284 | 16586 |  | Identifiant unifié du match (SHA1), résolu via match_registry. NULL si le match n'est pas résolu au registre. |
| `team_id` | BIGINT | 99.3% | 123 | 175 |  | Id canonique de l'équipe À DOMICILE (résolu via team_mapping sur home_team_name). |
| `opponent_id` | BIGINT | 99.3% | 125 | 175 |  | Id canonique de l'équipe À L'EXTÉRIEUR (résolu via team_mapping sur away_team_name). |
| `ws_match_id` | VARCHAR | 100.0% | 0 | 16870 |  | Identifiant WhoScored du match (brut, depuis silver.stg_whoscored_match_index). |
| `match_date` | DATE | 100.0% | 0 | 1521 |  | Date du match. |
| `league_source` | VARCHAR | 100.0% | 0 | 7 | top: La Liga (2982), Premier League (2971), Serie A (2930) | Championnat / source des données (ex. Bundesliga). |
| `season` | VARCHAR | 100.0% | 0 | 8 | top: 2017-2018 (2676), 2020-2021 (2132), 2018-2019 (2103) | Saison du match (ex. 2023-2024). |
| `scraped_at` | VARCHAR | 100.0% | 0 | 16576 |  | Horodatage ISO du scrape du match. |
| `comp_category` | VARCHAR | 100.0% | 0 | 2 | top: Big5 (14112), D2 (2758) | Catégorie de compétition. Valeur actuelle : Big5 (les 5 grands championnats européens). |
| `ws_home_team_id` | INTEGER | 100.0% | 0 | 180 |  | Id WhoScored (brut) de l'équipe à domicile — sert de clé pour mapper vers team_id. |
| `ws_away_team_id` | INTEGER | 100.0% | 0 | 180 |  | Id WhoScored (brut) de l'équipe à l'extérieur — sert de clé pour mapper vers opponent_id. |