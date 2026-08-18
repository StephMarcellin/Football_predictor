---
schema: gold
rows: 322476
---
# features_players

#gold

Étape 6.1 — Profil de forme par joueur AVANT chaque match : moyenne des métriques sur les 5 apparitions précédentes de la saison (match courant exclu, anti-leakage strict). 1 ligne par (match_id, team_id, player_id). Peuplé uniquement pour les 4 championnats couverts par WhoScored.


## Intégrité
**Clé déclarée :** (match_id, team_id, player_id) — ✅ aucun doublon

## Lineage
**Sources :** [[player_match_stats]], [[player_network_centrality]], [[player_xg_chain]]
**Alimente :** —

## Features & profiling  (322476 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 10969 |  | ID du match |
| `team_id` | BIGINT | 100.0% | 0 | 120 |  | ID de l'équipe du joueur pour ce match |
| `player_id` | INTEGER | 100.0% | 0 | 5865 |  | ID du joueur |
| `date` | DATE | 100.0% | 0 | 1350 |  | Date du match cible |
| `season` | VARCHAR | 100.0% | 0 | 8 | top: 2022-2023 (43340), 2021-2022 (42709), 2024-2025 (42390) | Saison — clé de partition de la fenêtre rolling |
| `league_source` | VARCHAR | 100.0% | 0 | 4 | top: Serie A (86636), Premier League (83845), Ligue 1 (81620) | Championnat source (WhoScored) |
| `avg_xg_chain_5` | DOUBLE | 94.8% | 16859 | 1070 | moy 2.009 · méd 1.82 · min/max 0.0/13.1 · p10/p90 0.6/3.667 | xG-chain moyen (somme du crédit des chaînes où le joueur est impliqué), 5 matchs précédents |
| `avg_xg_contribution_5` | DOUBLE | 94.8% | 16859 | 196789 | moy 0.055 · méd 0.037 · min/max 0.0/0.832 · p10/p90 0.0/0.133 | Proxy xG issu des tirs, moyenne 5 matchs précédents |
| `avg_progressive_passes_5` | DOUBLE | 94.8% | 16859 | 326 | moy 6.104 · méd 5.0 · min/max 0.0/48.0 · p10/p90 0.8/13.4 | Passes progressives moyennes, 5 matchs précédents |
| `avg_key_passes_5` | DOUBLE | 94.8% | 16859 | 62 | moy 0.649 · méd 0.4 · min/max 0.0/8.0 · p10/p90 0.0/1.6 | Passes clés moyennes, 5 matchs précédents |
| `avg_shots_5` | DOUBLE | 94.8% | 16859 | 79 | moy 0.877 · méd 0.6 · min/max 0.0/10.0 · p10/p90 0.0/2.0 | Tirs moyens, 5 matchs précédents |
| `avg_pass_share_5` | DOUBLE | 94.6% | 17470 | 29796 | moy 0.069 · méd 0.067 · min/max 0.001/0.233 · p10/p90 0.021/0.119 | Part moyenne des passes de l'équipe portées par le joueur, 5 matchs précédents |
| `avg_betweenness_5` | DOUBLE | 94.6% | 17470 | 75620 | moy 0.339 · méd 0.336 · min/max 0.0/1.0 · p10/p90 0.13/0.55 | Proxy de centralité de passage moyen, 5 matchs précédents |
| `avg_creative_rate_5` | DOUBLE | 94.6% | 17470 | 19585 | moy 0.032 · méd 0.018 · min/max 0.0/1.0 · p10/p90 0.0/0.082 | Taux moyen de passes créatives, 5 matchs précédents |
| `avg_aerial_win_rate_5` | DOUBLE | 88.7% | 36600 | 12710 | moy 0.483 · méd 0.5 · min/max 0.0/1.0 · p10/p90 0.042/0.875 | Taux moyen de duels aériens gagnés, 5 matchs précédents |
| `avg_tackle_win_rate_5` | DOUBLE | 81.7% | 58892 | 2009 | moy 0.606 · méd 0.625 · min/max 0.0/1.0 · p10/p90 0.222/1.0 | Taux moyen de tacles réussis, 5 matchs précédents |
| `avg_defensive_actions_5` | DOUBLE | 94.8% | 16859 | 226 | moy 6.324 · méd 5.8 · min/max 0.0/29.0 · p10/p90 1.5/12.0 | Actions défensives moyennes (tacles gagnés + interceptions + dégagements + récupérations), 5 matchs précédents |
| `avg_clearances_5` | DOUBLE | 94.8% | 16859 | 125 | moy 1.411 · méd 0.8 · min/max 0.0/17.0 · p10/p90 0.0/3.8 | Dégagements moyens, 5 matchs précédents |
| `avg_zone_x_5` | DOUBLE | 94.8% | 16859 | 292953 | moy 38.335 · méd 40.036 · min/max 0.0/75.5 · p10/p90 24.534/51.667 | Position longueur moyenne approximée depuis les 25 zones, 5 matchs précédents |
| `avg_zone_y_5` | DOUBLE | 94.8% | 16859 | 294004 | moy 39.085 · méd 39.194 · min/max 0.0/90.0 · p10/p90 19.659/58.561 | Position largeur moyenne approximée depuis les 25 zones, 5 matchs précédents |
| `avg_touches_5` | DOUBLE | 94.8% | 16859 | 1186 | moy 20.414 · méd 9.0 · min/max 0.0/161.0 · p10/p90 2.6/58.2 | Touches moyennes, 5 matchs précédents |
| `n_matchs_window` | BIGINT | 100.0% | 0 | 6 | moy 4.268 · méd 5.0 · min/max 0.0/5.0 · p10/p90 1.0/5.0 | Nombre de matchs réellement utilisés dans la fenêtre (0 à 5) ; 0 = première apparition de la saison, toutes les moyennes sont NULL |