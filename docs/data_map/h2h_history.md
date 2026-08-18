---
schema: intermediate
rows: 46887
---
# h2h_history

#intermediate

Registre brut des confrontations directes entre équipes. Une ligne par match.

## Intégrité
**Clé déclarée :** (match_id, team_id) — ✅ aucun doublon

## Lineage
**Sources :** [[backbone]]
**Alimente :** [[equipe_adversaire_match]], [[features_final]]

## Features & profiling  (46887 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 28499 |  |  |
| `team_id` | BIGINT | 100.0% | 0 | 153 |  | Id canonique de l'équipe (perspective de la ligne). |
| `opponent_id` | BIGINT | 97.7% | 1067 | 426 |  | Id canonique de l'adversaire. |
| `date` | DATE | 100.0% | 0 | 2417 |  |  |
| `season` | VARCHAR | 100.0% | 0 | 9 | top: 2018-2019 (5623), 2017-2018 (5525), 2021-2022 (5379) | Saison du match (ex. 2023-2024). |
| `league_source` | VARCHAR | 100.0% | 0 | 28 |  | Championnat / source des données. |
| `venue` | VARCHAR | 100.0% | 0 | 3 | top: Away (23660), Home (22995), Neutral (232) | Lieu du match pour l'équipe : Home / Away / Neutral. |
| `gf` | INTEGER | 100.0% | 0 | 14 | moy 1.451 · méd 1.0 · min/max 0.0/14.0 · p10/p90 0.0/3.0 | Buts marqués par l'équipe dans ce match (goals for). |
| `ga` | INTEGER | 100.0% | 0 | 10 | moy 1.297 · méd 1.0 · min/max 0.0/9.0 · p10/p90 0.0/3.0 | Buts encaissés par l'équipe (goals against). |
| `result_1n2` | VARCHAR | 98.2% | 841 | 3 | top: H (20128), A (14654), D (11264) | Résultat brut du match : H (victoire domicile) / D (nul) / A (victoire extérieur). NULL si indéterminé. |
| `np_xg` | DOUBLE | 67.3% | 15329 | 30539 | moy 1.308 · méd 1.148 · min/max 0.0/7.11 · p10/p90 0.392/2.436 | xG hors penalty de l'équipe dans ce match. |
| `np_xg_conceded` | DOUBLE | 67.3% | 15329 | 30539 | moy 1.308 · méd 1.148 · min/max 0.0/7.11 · p10/p90 0.393/2.436 | xG hors penalty concédé par l'équipe. |
| `h2h_played` | HUGEINT | 85.2% | 6942 | 106 | moy 6.372 · méd 5.0 · min/max 1.0/106.0 · p10/p90 1.0/14.0 | Nombre de confrontations directes antérieures entre les deux équipes (cumul avant ce match). |
| `h2h_wins` | HUGEINT | 85.2% | 6942 | 60 | moy 2.489 · méd 2.0 · min/max 0.0/59.0 · p10/p90 0.0/6.0 | Victoires de l'équipe dans l'historique des confrontations directes. |
| `h2h_draws` | HUGEINT | 85.2% | 6942 | 28 | moy 1.578 · méd 1.0 · min/max 0.0/27.0 · p10/p90 0.0/4.0 | Nuls dans l'historique des confrontations directes. |
| `h2h_losses` | HUGEINT | 85.2% | 6942 | 18 | moy 2.258 · méd 1.0 · min/max 0.0/17.0 · p10/p90 0.0/6.0 | Défaites dans l'historique des confrontations directes. |
| `h2h_home_played` | HUGEINT | 85.2% | 6942 | 53 | moy 3.125 · méd 2.0 · min/max 0.0/52.0 · p10/p90 1.0/7.0 | Confrontations directes jouées à domicile (historique). |
| `h2h_home_wins` | HUGEINT | 85.2% | 6942 | 31 | moy 1.425 · méd 1.0 · min/max 0.0/30.0 · p10/p90 0.0/3.0 | Victoires à domicile en confrontation directe. |
| `h2h_home_draws` | HUGEINT | 85.2% | 6942 | 14 | moy 0.782 · méd 0.0 · min/max 0.0/13.0 · p10/p90 0.0/2.0 | Nuls à domicile en confrontation directe. |
| `h2h_home_losses` | HUGEINT | 85.2% | 6942 | 11 | moy 0.914 · méd 0.0 · min/max 0.0/10.0 · p10/p90 0.0/3.0 | Défaites à domicile en confrontation directe. |
| `h2h_away_played` | HUGEINT | 85.2% | 6942 | 55 | moy 3.205 · méd 2.0 · min/max 0.0/54.0 · p10/p90 1.0/7.0 | Confrontations directes jouées à l'extérieur (historique). |
| `h2h_away_wins` | HUGEINT | 85.2% | 6942 | 30 | moy 1.064 · méd 1.0 · min/max 0.0/29.0 · p10/p90 0.0/3.0 | Victoires à l'extérieur en confrontation directe. |
| `h2h_away_draws` | HUGEINT | 85.2% | 6942 | 15 | moy 0.789 · méd 0.0 · min/max 0.0/14.0 · p10/p90 0.0/2.0 | Nuls à l'extérieur en confrontation directe. |
| `h2h_away_losses` | HUGEINT | 85.2% | 6942 | 12 | moy 1.344 · méd 1.0 · min/max 0.0/11.0 · p10/p90 0.0/3.0 | Défaites à l'extérieur en confrontation directe. |
| `h2h_played_10` | HUGEINT | 85.2% | 6942 | 10 | moy 5.435 · méd 5.0 · min/max 1.0/10.0 · p10/p90 1.0/10.0 | Confrontations directes sur les 10 dernières. |
| `h2h_wins_10` | HUGEINT | 85.2% | 6942 | 11 | moy 2.109 · méd 2.0 · min/max 0.0/10.0 · p10/p90 0.0/5.0 | Victoires sur les 10 dernières confrontations directes. |
| `h2h_draws_10` | HUGEINT | 85.2% | 6942 | 8 | moy 1.351 · méd 1.0 · min/max 0.0/7.0 · p10/p90 0.0/3.0 | Nuls sur les 10 dernières confrontations directes. |
| `h2h_losses_10` | HUGEINT | 85.2% | 6942 | 11 | moy 1.945 · méd 1.0 · min/max 0.0/10.0 · p10/p90 0.0/5.0 | Défaites sur les 10 dernières confrontations directes. |
| `h2h_avg_gf_10` | DOUBLE | 85.2% | 6942 | 159 | moy 1.433 · méd 1.333 · min/max 0.0/9.0 · p10/p90 0.5/2.5 | Moyenne de buts marqués sur les 10 dernières confrontations directes. |
| `h2h_avg_ga_10` | DOUBLE | 85.2% | 6942 | 141 | moy 1.329 · méd 1.2 · min/max 0.0/8.0 · p10/p90 0.429/2.286 | Moyenne de buts encaissés sur les 10 dernières confrontations directes. |
| `h2h_avg_xg_diff_10` | DOUBLE | 85.2% | 6942 | 31088 | moy 0.104 · méd 0.032 · min/max -8.0/9.0 · p10/p90 -1.068/1.315 | Différentiel xG moyen sur les 10 dernières confrontations directes. |