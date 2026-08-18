---
schema: intermediate
rows: 316196
---
# player_network_centrality

#intermediate

Centralité de chaque joueur dans le réseau de passes : degrés entrant et sortant (bruts et pondérés par le volume), passes créatives/progressives émises et reçues. Source : player_network_passes.


## Intégrité
**Clé déclarée :** (match_id, team_id, player_id) — ✅ aucun doublon

## Lineage
**Sources :** [[player_network_passes]]
**Alimente :** [[features_players]], [[team_network_features]]

## Features & profiling  (316196 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 10969 |  | Identifiant unifié du match (SHA1). |
| `team_id` | BIGINT | 100.0% | 0 | 120 |  | Id canonique de l'équipe. |
| `player_id` | INTEGER | 100.0% | 0 | 5784 |  | Identifiant WhoScored du joueur (nœud du réseau de passes de l'équipe). |
| `season` | VARCHAR | 100.0% | 0 | 8 | top: 2022-2023 (42399), 2021-2022 (41863), 2024-2025 (41452) | Saison du match (ex. 2023-2024). |
| `league_source` | VARCHAR | 100.0% | 0 | 4 | top: Serie A (85196), Premier League (82208), Ligue 1 (80051) | Championnat / source des données (ex. Bundesliga). |
| `degree_out` | BIGINT | 100.0% | 0 | 16 | moy 7.725 · méd 8.0 · min/max 1.0/16.0 · p10/p90 3.0/11.0 | Degré sortant : nombre de receveurs DISTINCTS du joueur dans le match. |
| `degree_in` | BIGINT | 100.0% | 0 | 16 | moy 7.707 · méd 8.0 · min/max 0.0/15.0 · p10/p90 4.0/11.0 | Degré entrant : nombre de passeurs DISTINCTS vers le joueur. |
| `weighted_degree_out` | HUGEINT | 100.0% | 0 | 170 | moy 27.251 · méd 23.0 · min/max 1.0/195.0 · p10/p90 5.0/55.0 | Volume total de passes émises par le joueur (somme des n_passes sortantes). |
| `weighted_degree_in` | HUGEINT | 100.0% | 0 | 166 | moy 27.231 · méd 24.0 · min/max 0.0/190.0 · p10/p90 6.0/53.0 | Volume total de passes reçues par le joueur (somme des n_passes entrantes). |
| `n_creative_out` | HUGEINT | 100.0% | 0 | 13 | moy 0.621 · méd 0.0 · min/max 0.0/13.0 · p10/p90 0.0/2.0 | Nombre de passes créatives émises par le joueur (somme n_creative sortantes). |
| `n_progressive_out` | HUGEINT | 100.0% | 0 | 57 | moy 6.178 · méd 4.0 · min/max 0.0/68.0 · p10/p90 0.0/15.0 | Nombre de passes progressives émises par le joueur (somme n_progressive sortantes). |
| `pass_share` | DOUBLE | 100.0% | 0 | 2141 | moy 0.069 · méd 0.066 · min/max 0.001/1.0 · p10/p90 0.013/0.13 | Part des passes de l'équipe portées par le joueur = weighted_degree_out / total passes équipe (arrondi 4 déc.). |
| `creative_rate` | DOUBLE | 100.0% | 0 | 560 | moy 0.032 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.097 | Taux de passes créatives parmi ses passes émises = n_creative_out / weighted_degree_out (arrondi 4 déc.). |
| `betweenness_proxy` | DOUBLE | 100.0% | 1 | 417 | moy 0.341 · méd 0.333 · min/max 0.0/1.0 · p10/p90 0.066/0.619 | Proxy de centralité d'intermédiarité = (degree_out × degree_in) / (n_players × (n_players − 1)) : diversité entrante × sortante normalisée par les paires possibles (arrondi 4 déc.). |