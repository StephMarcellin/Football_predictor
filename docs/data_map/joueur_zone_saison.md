---
schema: gold
rows: 639525
---
# joueur_zone_saison

#gold #famille5

Grain (player_id, zone_5x5, season) — format tall 5×5. Familles CDC 5 (profils offensifs zonaux) et 6 (défensifs), classe SEASON-LAG. Passe 1 offensive (touch share, volume de tirs par cellule) depuis int_player_zone_season ; passe 2 (danger, duels défensifs, menace concédée) à venir. Chaque ligne porte le profil de la saison N-1 (repli saison précédente, par cellule l'échantillon d'une saison est mince). Anti-leakage par construction.

## Intégrité
**Clé déclarée :** (player_id, zone_5x5, season) — ✅ aucun doublon

## Lineage
**Sources :** [[int_player_zone_season]]
**Alimente :** [[joueur_match]], [[team_corridor_profile]]

## Features & profiling  (639525 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `player_id` | INTEGER | 100.0% | 0 | 8638 |  | Identifiant du joueur. |
| `zone_5x5` | VARCHAR | 100.0% | 0 | 25 |  | Cellule 5×5 (zX_cY) : z = profondeur/tiers (x), c = couloir (y), bornes tous les 20. |
| `season` | VARCHAR | 100.0% | 0 | 8 | top: 2017-2018 (91425), 2021-2022 (82200), 2023-2024 (79375) | Saison du match à venir ; le profil porté est celui de N-1. |
| `off_touch_share_by_zone_lag` | DOUBLE | 60.1% | 255100 | 301643 | moy 0.031 · méd 0.017 · min/max 0.0/0.8 · p10/p90 0.0/0.074 | [Famille 5, feature 34] Part moyenne des touches du joueur dans la cellule, saison précédente. |
| `off_shot_volume_by_zone_lag` | DOUBLE | 60.1% | 255100 | 866 | moy 0.032 · méd 0.0 · min/max 0.0/3.414 · p10/p90 0.0/0.083 | [Famille 5, feature 36] Tirs pris dans la cellule par match, saison précédente (total tirs zone / matchs joués). |
| `def_duel_win_rate_by_zone_lag` | DOUBLE | 38.5% | 393473 | 583 | moy 0.481 · méd 0.5 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | [Famille 6, feature 45] Taux de duels défensifs gagnés dans la cellule, saison précédente (coords retournées vers la cage du défenseur). NULL si aucun duel dans la cellule. |
| `n_duels_prev` | BIGINT | 60.1% | 255100 | 75 | moy 3.317 · méd 1.0 · min/max 0.0/85.0 · p10/p90 0.0/9.0 | Duels défensifs disputés la saison précédente dans la cellule (compteur de confiance). |
| `n_matches_prev` | BIGINT | 60.1% | 255100 | 44 | moy 21.979 · méd 24.0 · min/max 1.0/44.0 · p10/p90 6.0/34.0 | Nombre de matchs du joueur la saison précédente (compteur d'observations, base imputation famille 11). NULL = pas de saison N-1 (recrue). |
| `profile_confidence_flag` | VARCHAR | 100.0% | 0 | 4 | top: high (326375), none (255100), medium (39325) | [Famille 11, feature 79] Fiabilité du profil zonal : high (n_matches_prev≥10) / medium (≥3) / low (≥1) / none (pas de saison N-1). Seuils par défaut, à calibrer. 'none' = cible du KNN. |