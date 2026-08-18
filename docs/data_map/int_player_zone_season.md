---
schema: intermediate
rows: 639525
---
# int_player_zone_season

#intermediate

Agrégat zonal 5×5 offensif par (joueur, cellule, saison jouée) : part moyenne des touches (grille pct_zX_cY remise en tall) et tirs pris dans la cellule (int_shot_placement binné, CSC exclus). Base de gold.joueur_zone_saison. Scindé + matérialisé pour contourner un bug du planner DuckDB 1.5.1.


## Intégrité
**Clé déclarée :** (player_id, zone_5x5, season) — ✅ aucun doublon

## Lineage
**Sources :** [[int_shot_placement]], [[player_match_stats]], [[player_network_duels]]
**Alimente :** [[joueur_zone_saison]]

## Features & profiling  (639525 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `player_id` | INTEGER | 100.0% | 0 | 8638 |  | Identifiant du joueur. |
| `zone_5x5` | VARCHAR | 100.0% | 0 | 25 |  | Cellule 5×5 (zX_cY) : z = profondeur (x), c = couloir (y), bornes tous les 20. |
| `season` | VARCHAR | 100.0% | 0 | 8 | top: 2017-2018 (91425), 2021-2022 (82200), 2023-2024 (79375) | Saison jouée (ex. 2023-2024). |
| `avg_touch_share` | DOUBLE | 100.0% | 0 | 447386 | moy 0.03 · méd 0.016 · min/max 0.0/0.857 · p10/p90 0.0/0.074 | Part moyenne des touches du joueur dans la cellule sur la saison. |
| `n_matches` | BIGINT | 100.0% | 0 | 44 | moy 19.123 · méd 20.0 · min/max 1.0/44.0 · p10/p90 2.0/33.0 | Nombre de matchs du joueur dans la saison (compteur d'observations). |
| `total_shots` | HUGEINT | 100.0% | 0 | 96 | moy 0.657 · méd 0.0 · min/max 0.0/113.0 · p10/p90 0.0/1.0 | Tirs pris par le joueur dans la cellule sur la saison (CSC exclus). |
| `n_duels` | BIGINT | 100.0% | 0 | 82 | moy 2.75 · méd 1.0 · min/max 0.0/110.0 · p10/p90 0.0/8.0 | Duels défensifs disputés par le joueur dans la cellule (repère retourné vers sa cage). |
| `duels_won` | HUGEINT | 100.0% | 0 | 72 | moy 1.331 · méd 0.0 · min/max 0.0/105.0 · p10/p90 0.0/4.0 | Duels défensifs gagnés dans la cellule (outcome_b = 1). |