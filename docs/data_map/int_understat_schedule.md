---
schema: intermediate
rows: 16213
---
# int_understat_schedule

#intermediate

Calendrier Understat rematché sur le match_id unifié. Conserve us_match_id (id Understat d'origine) et produit la correspondance us_match_id ↔ match_id réutilisée par int_understat_stats.


## Intégrité
**Clé déclarée :** — (pas de test d'unicité)

## Lineage
**Sources :** [[intermediate.match_registry]], [[silver.understat_schedule]], [[team_mapping]]
**Alimente :** [[backbone]], [[int_understat_stats]]

## Features & profiling  (16213 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `league_source` | VARCHAR | 100.0% | 0 | 5 | top: Serie A (3421), Premier League (3420), La Liga (3420) | Nom de la compétition source |
| `home_goals` | BIGINT | 100.0% | 0 | 10 | moy 1.495 · méd 1.0 · min/max 0.0/9.0 · p10/p90 0.0/3.0 | Buts marqués par l'équipe domicile |
| `away_goals` | BIGINT | 100.0% | 0 | 10 | moy 1.219 · méd 1.0 · min/max 0.0/9.0 · p10/p90 0.0/3.0 | Buts marqués par l'équipe extérieur |
| `home_xg` | DOUBLE | 97.3% | 431 | 15510 | moy 1.589 · méd 1.433 · min/max 0.0/6.882 · p10/p90 0.532/2.825 | Expected goals équipe domicile |
| `away_xg` | DOUBLE | 97.3% | 431 | 15495 | moy 1.273 · méd 1.12 · min/max 0.0/7.11 · p10/p90 0.355/2.388 | Expected goals équipe extérieur |
| `is_result` | BOOLEAN | 100.0% | 0 | 2 | top: True (15782), False (431) | Indique si le match a un résultat définitif (1) ou est à venir (0) |
| `match_url` | VARCHAR | 100.0% | 0 | 16212 |  | URL Understat du match |
| `season` | VARCHAR | 100.0% | 0 | 9 | top: 2022-2023 (1827), 2019-2020 (1826), 2017-2018 (1826) | Saison au format YYYY-YYYY |
| `source` | VARCHAR | 100.0% | 0 | 1 | top: understat (16213) | Source des données (Understat). |
| `scraped_at` | VARCHAR | 100.0% | 0 | 39 |  | Horodatage ISO du scrape. |
| `comp_category` | VARCHAR | 100.0% | 0 | 1 | top: Big5 (16213) | Catégorie de compétition : Big5, Cup, Europe |
| `us_match_id` | BIGINT | 100.0% | 0 | 16212 |  | Identifiant du match côté Understat (source), conservé avant résolution en match_id canonique. |
| `match_id` | VARCHAR | 100.0% | 0 | 16213 |  | Identifiant unifié du match (SHA1), rattaché via match_registry. |
| `team_id` | BIGINT | 100.0% | 0 | 153 |  | Id canonique de l'équipe à domicile (via team_mapping sur home_team). |
| `opponent_id` | BIGINT | 100.0% | 0 | 153 |  | Id canonique de l'équipe à l'extérieur (via team_mapping sur away_team). |