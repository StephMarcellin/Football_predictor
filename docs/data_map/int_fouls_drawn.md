---
schema: intermediate
rows: 409703
---
# int_fouls_drawn

#intermediate

Fautes subies (coups francs obtenus) au grain « une faute subie » : côté outcome_id=1 des events Foul (type 4). Rattache le fautif via le qual 233 (OppositeRelatedEvent, déterministe 100 %), la localisation (x/y, zone), le tiers offensif (x > 66.7) et si la faute donne un penalty. Le joueur qui subit est NULL ~5 % (limite source). Source : int_whoscored_events, events_qual.


## Intégrité
**Clé déclarée :** (match_id, row_num) — ✅ aucun doublon

## Lineage
**Sources :** [[events_qual]], [[int_whoscored_events]]
**Alimente :** —

## Features & profiling  (409703 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 16573 |  | Identifiant unifié du match (SHA1) |
| `row_num` | INTEGER | 100.0% | 0 | 1850 | moy 808.277 · méd 809.0 · min/max 3.0/1901.0 · p10/p90 184.0/1417.0 | row_num de la faute subie. Avec match_id, clé unique. |
| `event_id` | INTEGER | 100.0% | 0 | 1284 |  | event_id WhoScored (scopé match+équipe). |
| `season` | VARCHAR | 100.0% | 0 | 8 | top: 2017-2018 (67545), 2018-2019 (53284), 2020-2021 (52180) | Saison du match (ex. 2023-2024). |
| `league_source` | VARCHAR | 100.0% | 0 | 7 | top: La Liga (78887), Serie A (76245), Ligue 1 (70205) | Championnat / source des données (ex. Bundesliga). |
| `scraped_at` | VARCHAR | 100.0% | 0 | 16571 |  | Horodatage ISO du scrape. |
| `expanded_minute` | INTEGER | 100.0% | 0 | 126 | moy 49.26 · méd 50.0 · min/max 0.0/129.0 · p10/p90 10.0/88.0 | Minute (expanded_minute) de la faute. |
| `drawing_team_id` | BIGINT | 100.0% | 0 | 175 |  | Équipe qui subit la faute / obtient le coup franc |
| `drawer_player_id` | INTEGER | 95.0% | 20287 | 7436 |  | Joueur qui subit la faute. NULL ~5 % (non renseigné à la source). |
| `committed_by_player_id` | INTEGER | 100.0% | 9 | 7528 |  | Fautif, rattaché via qual 233. NULL <1 %. |
| `x` | DOUBLE | 100.0% | 0 | 997 | moy 46.114 · méd 44.8 · min/max 0.1/99.9 · p10/p90 15.5/73.2 | Position X de la faute (0-100). |
| `y` | DOUBLE | 100.0% | 0 | 1001 | moy 50.545 · méd 50.8 · min/max 0.0/100.0 · p10/p90 8.6/91.6 | Position Y de la faute (0-100). |
| `foul_zone` | VARCHAR | 100.0% | 12 | 4 | top: Back (231207), Center (86144), Left (47150) | Zone de la faute (qualifiers Opta) : Left / Center / Right = couloir latéral (gauche / axe / droite) ; Back = zone défensive/arrière. NULL si indéterminé. |
| `is_attacking_third` | BOOLEAN | 100.0% | 0 | 2 | top: False (333055), True (76648) | Faute subie dans le tiers offensif (x > 66.7) — coup franc dangereux. NULL si x absent. |
| `leads_to_penalty` | BOOLEAN | 100.0% | 0 | 2 | top: False (404506), True (5197) | Vrai si la faute donne un penalty (qual 9 présent) |