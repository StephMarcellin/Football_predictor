---
schema: intermediate
rows: 421342
---
# int_shot_placement

#intermediate

Placement de tir au grain « un tir » — base du Post-Shot xG (xGOT). Tirs (types 13/14/15/16) issus de int_event_enriched, avec la géométrie du placement dans le cadre du but (goal_mouth_y/z) dérivée pour les tirs cadrés : zone 3×3, écart au centre, hauteur, distance normalisée à la lucarne. Le xGOT entraîné reste en aval ; ce modèle expose les entrées + un xG pré-tir proxy. Sources : int_event_enriched, event_values.


## Intégrité
**Clé déclarée :** (match_id, row_num) — ✅ aucun doublon

## Lineage
**Sources :** [[event_values]], [[events_qual]], [[int_event_enriched]], [[int_whoscored_events]]
**Alimente :** [[int_keeper_shots]], [[int_player_zone_season]], [[int_xt_actions]], [[xgot_features]]

## Features & profiling  (421342 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 16584 |  | Identifiant unifié du match (SHA1). Clé de match commune à tout le projet. |
| `team_id` | BIGINT | 100.0% | 31 | 175 |  | Id canonique de l'équipe qui tire (converti depuis l'id WhoScored). CAS CSC : pour un but contre son camp, team_id est l'équipe QUI CONCÈDE (celle du joueur marquant c.s.c.), pas celle qui profite du but.  |
| `player_id` | INTEGER | 100.0% | 0 | 7061 |  | Identifiant WhoScored du joueur qui tire. |
| `event_id` | INTEGER | 100.0% | 0 | 1328 |  | Identifiant WhoScored de l'événement. ATTENTION : scopé par (match, équipe), PAS unique par match — ne pas joindre dessus sans contraindre l'équipe.  |
| `row_num` | INTEGER | 100.0% | 0 | 1857 | moy 829.026 · méd 848.0 · min/max 2.0/1921.0 · p10/p90 204.0/1422.0 | Index de l'événement dans le flux du match (ordre chronologique). Unique par match avec match_id. |
| `expanded_minute` | INTEGER | 100.0% | 0 | 126 | moy 50.448 · méd 52.0 · min/max 0.0/32772.0 · p10/p90 11.0/88.0 | Minute de jeu du tir, temps additionnel inclus (ex. 45+2 → 47). |
| `season` | VARCHAR | 100.0% | 0 | 8 | top: 2017-2018 (66037), 2018-2019 (54622), 2021-2022 (53370) | Saison du match (ex. 2023-2024). |
| `league_source` | VARCHAR | 100.0% | 0 | 7 | top: Serie A (76346), Premier League (75846), La Liga (70702) | Championnat / source des données (ex. Bundesliga). |
| `scraped_at` | VARCHAR | 100.0% | 0 | 16293 |  | Horodatage ISO du scrape de ce match (sert au filtre incrémental). |
| `type_id` | INTEGER | 100.0% | 0 | 4 |  | Type d'événement WhoScored : 13 MissedShots, 14 ShotOnPost, 15 SavedShot, 16 Goal. |
| `type_name` | VARCHAR | 100.0% | 0 | 4 | top: SavedShot (207039), MissedShots (159700), Goal (46489) | Libellé du type d'événement (MissedShots / ShotOnPost / SavedShot / Goal). |
| `x` | DOUBLE | 100.0% | 0 | 849 | moy 84.895 · méd 86.7 · min/max 0.3/99.9 · p10/p90 74.2/94.2 | Position X de la frappe sur le terrain, échelle 0-100 (x croissant vers le but adverse). |
| `y` | DOUBLE | 100.0% | 0 | 948 | moy 50.458 · méd 50.1 · min/max 0.3/99.8 · p10/p90 33.4/67.5 | Position Y de la frappe sur le terrain, échelle 0-100 (largeur, 50 = axe central). |
| `goal_mouth_y` | DOUBLE | 90.3% | 40910 | 692 | moy 50.152 · méd 50.1 · min/max 0.2/100.0 · p10/p90 41.0/59.3 | Position horizontale (largeur) où le tir franchit ou atteint la ligne de but (qualifier 102 GoalMouthY, définition Opta). Cadre : poteaux à Y≈45.2 et 54.8, centre 50. NULL si non renseigné à la source.  |
| `goal_mouth_z` | DOUBLE | 90.3% | 40910 | 136 | moy 24.262 · méd 19.0 · min/max 0.0/100.0 · p10/p90 2.8/68.1 | Hauteur à laquelle le tir franchit/atteint la ligne de but (qualifier 103 GoalMouthZ). Barre à Z≈38. NULL si non renseigné à la source.  |
| `blocked_x` | DOUBLE | 55.2% | 188681 | 336 | moy 92.763 · méd 94.7 · min/max 48.4/100.5 · p10/p90 82.7/99.0 | Coordonnée X (repère de l'équipe qui tire, 0-100) du point où le tir est STOPPÉ (qualifier 146 BlockedX). Pour un tir arrêté, c'est la position de l'arrêt du gardien (vérifié sur exemple : blocked_x ≈ position du Save adverse converti). Peut aussi marquer un contre défenseur si le qualifier 82 Blocked est présent. Renseigné seulement pour une partie des tirs.  |
| `blocked_y` | DOUBLE | 55.2% | 188681 | 700 | moy 50.184 · méd 50.0 · min/max 0.0/100.0 · p10/p90 40.1/60.8 | Coordonnée Y (repère de l'équipe qui tire, 0-100) du point où le tir est stoppé (qualifier 147 BlockedY). Même sémantique que blocked_x.  |
| `is_own_goal` | BOOLEAN | 100.0% | 0 | 2 | top: False (419977), True (1365) | Booléen : le but est un but contre son camp (CSC). TRUE pour les 306 CSC (type 16), sinon FALSE/NULL. Combiné à is_goal, permet d'isoler les vrais buts de l'équipe qui tire. Propagé depuis int_event_enriched.  |
| `is_goal` | BOOLEAN | 100.0% | 0 | 2 | top: False (374853), True (46489) | Booléen : événement de type but (type_id = 16). ⚠️ Inclut 306 buts contre son camp (CSC) rattachés à l'équipe qui CONCÈDE, pas à celle qui marque. Pour « but marqué par l'équipe qui tire », exclure les CSC via is_own_goal. À filtrer aussi pour le xGOT (un CSC n'est pas un tir cadré normal).  |
| `is_blocked` | BOOLEAN | 100.0% | 0 | 2 | top: False (311654), True (109688) |  |
| `is_on_target` | BOOLEAN | 100.0% | 0 | 2 | top: False (277502), True (143840) | Booléen : tir cadré (SavedShot ou Goal, type 15/16). Seuls les tirs cadrés portent les dérivés de placement. |
| `pre_shot_xg_proxy` | DECIMAL(3,2) | 100.0% | 0 | 8 | moy 0.558 · méd 0.4 · min/max 0.0/1.0 · p10/p90 0.1/1.0 | Proxy de xG pré-tir = chance_creation issu de event_values (baseline du xGOT). Non calibré : sert d'entrée/repère, pas de probabilité exacte.  |
| `offset_center` | DOUBLE | 34.1% | 277529 | 49 | moy 2.205 · méd 2.2 · min/max 0.0/4.8 · p10/p90 0.4/4.0 | Écart horizontal au centre du but : ABS(goal_mouth_y - 50). Tirs cadrés uniquement, NULL sinon. |
| `height` | DOUBLE | 34.1% | 277529 | 83 | moy 12.04 · méd 9.5 · min/max 0.6/38.0 · p10/p90 1.9/26.6 | Hauteur du tir dans le but (= goal_mouth_z). Tirs cadrés uniquement, NULL sinon. |
| `placement_col` | VARCHAR | 34.1% | 277502 | 3 | top: center (54627), right (45955), left (43258) | Colonne de la grille 3×3 du cadre : left / center / right (selon goal_mouth_y). Tirs cadrés uniquement. |
| `placement_row` | VARCHAR | 34.1% | 277502 | 3 | top: low (81540), mid (45790), high (16510) | Rangée de la grille 3×3 : low / mid / high (selon goal_mouth_z). Tirs cadrés uniquement. |
| `placement_zone` | VARCHAR | 34.1% | 277502 | 9 | top: low_center (29407), low_right (27270), low_left (24863) | Zone 3×3 du cadre (rangée_colonne, ex: high_left = lucarne gauche). Tirs cadrés uniquement, NULL sinon. |
| `corner_dist` | DOUBLE | 34.1% | 277529 | 3002 | moy 0.756 · méd 0.802 · min/max 0.036/1.104 · p10/p90 0.43/1.001 | Distance normalisée à la lucarne la plus proche (0 = pleine lucarne, plus grand = plus central/bas). Tirs cadrés uniquement. |