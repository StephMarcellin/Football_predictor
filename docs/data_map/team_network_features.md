---
schema: intermediate
rows: 21938
---
# team_network_features

#intermediate

Agrège les métriques réseau joueur → features d'équipe par match : nombre de joueurs et d'arêtes actifs, dépendance au top créateur (top_creator_share), betweenness moyenne, entropie de Shannon du réseau (network_entropy). Source : player_network_centrality.


## Intégrité
**Clé déclarée :** (match_id, team_id) — ✅ aucun doublon

## Lineage
**Sources :** [[player_network_centrality]], [[player_passes_raw]]
**Alimente :** —

## Features & profiling  (21938 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 10969 |  | Identifiant unifié du match (SHA1). |
| `team_id` | BIGINT | 100.0% | 0 | 120 |  | Id canonique de l'équipe. |
| `season` | VARCHAR | 100.0% | 0 | 8 | top: 2018-2019 (2890), 2017-2018 (2886), 2021-2022 (2864) | Saison du match (ex. 2023-2024). |
| `league_source` | VARCHAR | 100.0% | 0 | 4 | top: Premier League (5884), Serie A (5784), Ligue 1 (5544) | Championnat / source des données (ex. Bundesliga). |
| `n_players` | BIGINT | 100.0% | 0 | 10 | moy 14.413 · méd 14.0 · min/max 1.0/20.0 · p10/p90 13.0/16.0 | Nombre de joueurs (nœuds) du réseau de passes de l'équipe (depuis player_network_centrality). |
| `n_edges` | HUGEINT | 100.0% | 0 | 112 | moy 111.34 · méd 111.0 · min/max 1.0/170.0 · p10/p90 92.0/131.0 | Nombre d'arêtes dirigées du réseau = somme des degree_out des joueurs. |
| `network_density` | DOUBLE | 100.0% | 1 | 400 | moy 0.58 · méd 0.577 · min/max 0.276/1.0 · p10/p90 0.479/0.686 | Densité du réseau = n_edges / (n_players × (n_players − 1)) : proportion des connexions possibles réalisées (arrondi 4 déc.). |
| `top_creator_share` | DOUBLE | 100.0% | 0 | 1103 | moy 0.149 · méd 0.147 · min/max 0.092/1.0 · p10/p90 0.126/0.174 | Part de passes du plus gros contributeur = MAX(pass_share) parmi les joueurs (arrondi 4 déc.). |
| `avg_betweenness` | DOUBLE | 100.0% | 1 | 3072 | moy 0.345 · méd 0.339 · min/max 0.083/0.76 · p10/p90 0.241/0.457 | Moyenne du betweenness_proxy des joueurs de l'équipe (arrondi 4 déc.). |
| `network_entropy` | DOUBLE | 100.0% | 1 | 1305 | moy 0.919 · méd 0.92 · min/max 0.795/1.0 · p10/p90 0.89/0.947 | Entropie de Shannon normalisée de la distribution des passes : −Σ(pass_share·ln(pass_share)) / ln(n_players). 0 = jeu concentré sur un joueur, proche de 1 = passes réparties uniformément (arrondi 4 déc.). |
| `centroid_x` | DOUBLE | 100.0% | 0 | 2297 | moy 46.196 · méd 46.26 · min/max 29.79/99.5 · p10/p90 40.807/51.5 | Centroïde X des passes (moyenne des x de départ des passes réussies) : proche de 100 = équipe qui joue haut, proche de 0 = joue bas (arrondi 2 déc.). |
| `centroid_y` | DOUBLE | 100.0% | 0 | 1988 | moy 50.484 · méd 50.46 · min/max 34.44/99.5 · p10/p90 45.98/55.06 | Centroïde Y des passes (moyenne des y de départ) : ~50 = jeu axial, décalé = jeu sur un côté (arrondi 2 déc.). |
| `centroid_x_progressive` | DOUBLE | 100.0% | 1 | 2387 | moy 38.757 · méd 38.76 · min/max 21.0/57.13 · p10/p90 33.11/44.33 | Centroïde X des seules passes progressives (arrondi 2 déc.). |
| `centroid_y_progressive` | DOUBLE | 100.0% | 1 | 2337 | moy 50.028 · méd 50.03 · min/max 32.87/67.77 · p10/p90 44.48/55.59 | Centroïde Y des seules passes progressives (arrondi 2 déc.). |