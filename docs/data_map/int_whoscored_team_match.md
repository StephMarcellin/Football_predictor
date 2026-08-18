---
schema: intermediate
rows: 32948
---
# int_whoscored_team_match

#intermediate

Stats d'équipe officielles WhoScored par match, identités normalisées. Conserve stats_json brut (35 métriques par minute) ; extraction des totaux plus tard. Source : silver.stg_whoscored_team_match.


## Intégrité
**Clé déclarée :** (match_id, team_id) — ⚠️ **519 doublons**

## Lineage
**Sources :** [[int_whoscored_match_index]], [[silver.stg_whoscored_team_match]]
**Alimente :** —

## Features & profiling  (32948 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 98.3% | 570 | 16188 |  | Identifiant unifié du match (SHA1) |
| `team_id` | BIGINT | 99.2% | 248 | 174 |  | Id canonique de l'équipe (converti depuis l'id WhoScored) |
| `field` | VARCHAR | 100.0% | 0 | 2 | top: home (16474), away (16474) | Côté : home ou away |
| `manager_name` | VARCHAR | 100.0% | 0 | 579 |  | Nom de l'entraîneur |
| `country_name` | VARCHAR | 100.0% | 0 | 10 | top: Germany (9284), England (6914), Spain (5958) | Nom du pays de l'équipe (ex. France, England, Italy, Germany, Wales). |
| `average_age` | DOUBLE | 100.0% | 0 | 158 | moy 31.268 · méd 31.4 · min/max 21.9/38.7 · p10/p90 27.6/34.8 | Âge moyen de l'équipe |
| `stats_json` | VARCHAR | 100.0% | 0 | 32944 |  | Stats équipe par minute (JSON brut) — totaux calculés plus tard |