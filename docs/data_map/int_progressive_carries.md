---
schema: intermediate
rows: 166193
---
# int_progressive_carries

#intermediate

Conduites (dribbles) au grain « un dribble réussi » : TakeOn (type 3) réussis côté offensif (outcome 1 + qual 286). WhoScored n'a pas d'event carry ; le TakeOn est la seule conduite balle au pied enregistrée. Progression mesurée du dribble jusqu'à la touche suivante du joueur dans la même chaîne, en mètres (réduction de distance au but). is_progressive = gain ≥ 5 m. Source : player_possession_chains, events_qual.


## Intégrité
**Clé déclarée :** (match_id, row_num) — ✅ aucun doublon

## Lineage
**Sources :** [[events_qual]], [[player_possession_chains]]
**Alimente :** —

## Features & profiling  (166193 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 10965 |  | Identifiant unifié du match (SHA1) |
| `row_num` | INTEGER | 100.0% | 0 | 1836 | moy 825.653 · méd 840.0 · min/max 3.0/1907.0 · p10/p90 186.0/1437.0 | row_num du dribble (TakeOn). Avec match_id, clé unique. |
| `event_id` | INTEGER | 100.0% | 0 | 1515 |  | event_id WhoScored du dribble (scopé match+équipe). |
| `season` | VARCHAR | 100.0% | 0 | 8 | top: 2017-2018 (24119), 2020-2021 (23009), 2021-2022 (21894) | Saison du match (ex. 2023-2024). |
| `league_source` | VARCHAR | 100.0% | 0 | 4 | top: Ligue 1 (45612), Premier League (45498), Serie A (40580) | Championnat / source des données (ex. Bundesliga). |
| `scraped_at` | VARCHAR | 100.0% | 0 | 10965 |  | Horodatage ISO du scrape. |
| `team_id` | BIGINT | 100.0% | 0 | 120 |  | Équipe du dribbleur |
| `player_id` | INTEGER | 100.0% | 0 | 4459 |  | Joueur qui réussit le dribble |
| `expanded_minute` | INTEGER | 100.0% | 0 | 119 | moy 49.329 · méd 50.0 · min/max 0.0/125.0 · p10/p90 10.0/88.0 | Minute (expanded_minute) du dribble. |
| `dribble_x` | DOUBLE | 100.0% | 0 | 683 | moy 65.373 · méd 67.0 · min/max 0.0/99.9 · p10/p90 40.3/88.0 | Position X du dribble (0-100). |
| `dribble_y` | DOUBLE | 100.0% | 0 | 1000 | moy 51.05 · méd 52.7 · min/max 0.0/99.9 · p10/p90 9.2/91.3 | Position Y du dribble (0-100). |
| `next_x` | DOUBLE | 96.4% | 5952 | 775 | moy 69.734 · méd 70.9 · min/max 0.0/100.0 · p10/p90 44.0/92.8 | Position X de la touche suivante du joueur (fin de la conduite). |
| `next_y` | DOUBLE | 96.4% | 5952 | 1000 | moy 50.873 · méd 52.2 · min/max 0.0/99.9 · p10/p90 12.0/88.6 | Position Y de la touche suivante. |
| `progress_m` | DOUBLE | 96.4% | 5952 | 159942 | moy 4.303 · méd 3.03 · min/max -98.871/97.173 · p10/p90 -2.251/12.829 | Gain de distance vers le but adverse (m). NULL si pas de touche suivante en chaîne. |
| `is_progressive` | BOOLEAN | 100.0% | 0 | 2 | top: False (107724), True (58469) | Vrai si la conduite gagne ≥ 5 m vers le but |