---
schema: gold
rows: 46887
---
# equipe_adversaire_match

#gold #famille1

Grain (match_id, team_id, opponent_id). Famille CDC 1 (confrontation directe H2H), classe H2H-CUTOFF (cumul avant match, vérifié anti-leakage). Taux NULL si pas d'historique, compteurs à 0.

## Intégrité
**Clé déclarée :** (match_id, team_id) — ✅ aucun doublon

## Lineage
**Sources :** [[h2h_history]]
**Alimente :** —

## Features & profiling  (46887 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 28499 |  | Identifiant unifié du match (SHA1). |
| `team_id` | BIGINT | 100.0% | 0 | 153 |  | Id canonique de l'équipe (perspective de la ligne). |
| `opponent_id` | BIGINT | 97.7% | 1067 | 426 |  | Id canonique de l'adversaire. |
| `date` | DATE | 100.0% | 0 | 2417 |  | Date du match. |
| `season` | VARCHAR | 100.0% | 0 | 9 | top: 2018-2019 (5623), 2017-2018 (5525), 2021-2022 (5379) | Saison (ex. 2023-2024). |
| `league_source` | VARCHAR | 100.0% | 0 | 28 |  | Championnat / compétition source. |
| `h2h_played` | HUGEINT | 100.0% | 0 | 107 | moy 5.429 · méd 4.0 · min/max 0.0/106.0 · p10/p90 0.0/13.0 | [Famille 1, H2H-CUTOFF] Nombre de confrontations directes antérieures (0 si aucune). |
| `h2h_home_played` | HUGEINT | 100.0% | 0 | 53 | moy 2.663 · méd 2.0 · min/max 0.0/52.0 · p10/p90 0.0/6.0 | [Famille 1, H2H-CUTOFF] Confrontations directes à domicile antérieures (0 si aucune). |
| `h2h_win_rate_cutoff` | DOUBLE | 85.2% | 6942 | 237 | moy 0.394 · méd 0.333 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | [Famille 1] Taux de victoire en H2H cumulé avant le match. NULL si aucune confrontation antérieure (inconnu ≠ 0). |
| `h2h_draw_rate_cutoff` | DOUBLE | 85.2% | 6942 | 182 | moy 0.249 · méd 0.2 · min/max 0.0/1.0 · p10/p90 0.0/0.545 | [Famille 1] Taux de nul en H2H cumulé avant le match. NULL si aucune confrontation. |
| `h2h_loss_rate_cutoff` | DOUBLE | 85.2% | 6942 | 236 | moy 0.352 · méd 0.333 · min/max 0.0/1.0 · p10/p90 0.0/0.818 | [Famille 1] Taux de défaite en H2H cumulé avant le match. NULL si aucune confrontation. |
| `h2h_home_win_rate_cutoff` | DOUBLE | 76.9% | 10822 | 83 | moy 0.459 · méd 0.5 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | [Famille 1] Taux de victoire à domicile en H2H cumulé avant le match. NULL si aucune confrontation à domicile. |
| `h2h_avg_gf_10` | DOUBLE | 85.2% | 6942 | 159 | moy 1.433 · méd 1.333 · min/max 0.0/9.0 · p10/p90 0.5/2.5 | [Famille 1] Moyenne de buts marqués sur les 10 dernières confrontations directes. |
| `h2h_avg_ga_10` | DOUBLE | 85.2% | 6942 | 141 | moy 1.329 · méd 1.2 · min/max 0.0/8.0 · p10/p90 0.429/2.286 | [Famille 1] Moyenne de buts encaissés sur les 10 dernières confrontations directes. |
| `h2h_avg_xg_diff_10` | DOUBLE | 85.2% | 6942 | 31088 | moy 0.104 · méd 0.032 · min/max -8.0/9.0 · p10/p90 -1.068/1.315 | [Famille 1] Différentiel xG moyen sur les 10 dernières confrontations directes. |