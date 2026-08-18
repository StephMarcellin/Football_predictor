---
schema: intermediate
rows: 123935
---
# int_substitutions

#intermediate

Changements au grain « un remplacement » : joueur qui sort (SubstitutionOff, type 18) apparié à celui qui entre (SubstitutionOn, type 19) par rang au sein du groupe (match, équipe, minute, seconde) — gère les changements simultanés. Croisé avec related_player_id (présent 27 %) : concordance 99,8 %. sub_number = k-ième changement de l'équipe. Source : int_whoscored_events.


## Intégrité
**Clé déclarée :** (match_id, off_row_num) — ✅ aucun doublon

## Lineage
**Sources :** [[int_whoscored_events]]
**Alimente :** —

## Features & profiling  (123935 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 16570 |  | Identifiant unifié du match (SHA1) |
| `off_row_num` | INTEGER | 100.0% | 0 | 1742 | moy 1164.312 · méd 1194.0 · min/max 14.0/1878.0 · p10/p90 825.0/1447.0 | row_num de l'événement de sortie (SubstitutionOff). Clé avec match_id. |
| `season` | VARCHAR | 100.0% | 0 | 8 | top: 2023-2024 (17894), 2024-2025 (17826), 2021-2022 (17078) | Saison du match (ex. 2023-2024). |
| `league_source` | VARCHAR | 100.0% | 0 | 7 | top: La Liga (23717), Serie A (23303), Ligue 1 (20709) | Championnat / source des données (ex. Bundesliga). |
| `scraped_at` | VARCHAR | 100.0% | 0 | 16569 |  | Horodatage ISO du scrape. |
| `team_id` | BIGINT | 100.0% | 0 | 175 |  | Équipe qui effectue le changement |
| `expanded_minute` | INTEGER | 100.0% | 0 | 117 | moy 72.415 · méd 75.0 · min/max 2.0/121.0 · p10/p90 51.0/89.0 | Minute du changement (temps additionnel inclus) |
| `player_off` | INTEGER | 100.0% | 0 | 6697 | moy 229282.222 · méd 254262.0 · min/max 0.0/561413.0 · p10/p90 41330.0/403646.0 | Joueur qui sort |
| `player_on` | INTEGER | 99.9% | 123 | 7925 | moy 247112.975 · méd 294163.0 · min/max 0.0/561795.0 · p10/p90 44425.0/422662.0 | Joueur qui entre. NULL si sortie sans remplaçant (ex. carton rouge). |
| `sub_number` | BIGINT | 100.0% | 0 | 10 | moy 2.514 · méd 2.0 · min/max 1.0/10.0 · p10/p90 1.0/4.0 | k-ième changement de l'équipe dans le match |