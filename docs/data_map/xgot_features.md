---
schema: machine_learning
rows: 137523
---
# xgot_features

#machine_learning

Socle de features xGOT — logique de calcul PARTAGÉE entre l'entraînement (xgot_training) et le scoring (pipelines/xgot_score.py). Un tir cadré éligible par ligne. Périmètre : tirs cadrés, placement présent, hors CSC, hors penalty. Définir features + périmètre à un seul endroit interdit structurellement le train/serve skew. Vue → toujours à jour avec int_shot_placement. Schéma : machine_learning.


## Intégrité
**Clé déclarée :** (match_id, row_num) — ✅ aucun doublon

## Lineage
**Sources :** [[events_qual]], [[int_shot_placement]]
**Alimente :** [[xgot_training]]

## Features & profiling  (137523 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 16577 |  | Identifiant unifié du match (SHA1). |
| `row_num` | INTEGER | 100.0% | 0 | 1818 | moy 832.901 · méd 855.0 · min/max 2.0/1905.0 · p10/p90 205.0/1426.8 | Index de l'événement dans le match. Avec match_id, clé unique du tir. |
| `season` | VARCHAR | 100.0% | 0 | 8 | top: 2017-2018 (21340), 2018-2019 (17578), 2023-2024 (17447) | Saison du match (ex. 2023-2024). |
| `is_goal` | BOOLEAN | 100.0% | 0 | 2 | top: False (96479), True (41044) | Booléen : le tir cadré a fini au fond (base du label côté training, du comptage buts côté PSxG). |
| `offset_center` | DOUBLE | 100.0% | 0 | 49 | moy 2.177 · méd 2.2 · min/max 0.0/4.8 · p10/p90 0.3/4.0 | Écart horizontal au centre du but (m dans le cadre) : ABS(goal_mouth_y - 50). |
| `height` | DOUBLE | 100.0% | 0 | 83 | moy 12.125 · méd 10.1 · min/max 0.6/38.0 · p10/p90 1.9/26.6 | Hauteur du tir dans le but (= goal_mouth_z). |
| `corner_dist` | DOUBLE | 100.0% | 0 | 3000 | moy 0.755 · méd 0.801 · min/max 0.036/1.104 · p10/p90 0.43/1.002 | Distance normalisée à la lucarne la plus proche (0 = pleine lucarne). |
| `placement_col` | VARCHAR | 100.0% | 0 | 3 | top: center (53503), right (43186), left (40834) | Colonne du placement : left / center / right. |
| `placement_row` | VARCHAR | 100.0% | 0 | 3 | top: low (77518), mid (44030), high (15975) | Rangée du placement : low / mid / high. |
| `placement_zone` | VARCHAR | 100.0% | 0 | 9 | top: low_center (28840), low_right (25445), low_left (23233) | Zone 3×3 du cadre (ex: high_left = lucarne gauche). |
| `x` | DOUBLE | 100.0% | 0 | 568 | moy 86.601 · méd 88.2 · min/max 22.6/99.9 · p10/p90 75.4/95.4 | Position X de la frappe (0-100, croissant vers le but adverse). |
| `y` | DOUBLE | 100.0% | 0 | 889 | moy 50.578 · méd 50.5 · min/max 0.3/99.8 · p10/p90 33.7/67.3 | Position Y de la frappe (0-100, 50 = axe central). |
| `shot_distance_m` | DOUBLE | 100.0% | 0 | 63297 | moy 16.37 · méd 15.074 · min/max 0.322/82.839 · p10/p90 7.091/27.739 | Distance de la frappe au centre du but adverse, en mètres. |
| `shot_angle_rad` | DOUBLE | 100.0% | 0 | 63505 | moy 0.494 · méd 0.384 · min/max 0.002/3.073 · p10/p90 0.228/0.88 | Angle de tir (ouverture du but vu depuis la frappe), en radians. Loi des cosinus, but 7.32 m. |
| `pre_shot_xg_proxy` | DECIMAL(3,2) | 100.0% | 0 | 5 | moy 0.707 · méd 0.8 · min/max 0.0/1.0 · p10/p90 0.4/1.0 | Proxy de xG pré-tir (chance_creation, non calibré) — repère/entrée additionnelle. |