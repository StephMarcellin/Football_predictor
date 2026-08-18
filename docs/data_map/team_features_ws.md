---
schema: intermediate
rows: 33146
---
# team_features_ws

#intermediate

Features tactiques WhoScored par équipe par match. Calculées depuis silver.stg_whoscored_events et intermediate.events_qual. 32 métriques couvrant le pressing, les zones d'attaque, la structure défensive.


## Intégrité
**Clé déclarée :** (match_id, team_id) — ✅ aucun doublon

## Lineage
**Sources :** [[events_qual]], [[int_whoscored_events]]
**Alimente :** [[equipe_match]], [[features_draw]], [[features_whoscored]]

## Features & profiling  (33146 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 16573 |  | Identifiant unifié du match (SHA1). |
| `team_id` | BIGINT | 100.0% | 0 | 175 |  | Identifiant WhoScored de l'équipe |
| `ws_field_tilt_actions` | DOUBLE | 100.0% | 0 | 21965 | moy 0.24 · méd 0.237 · min/max 0.0/0.654 · p10/p90 0.166/0.319 | Tilt de terrain — proportion de touches en zone offensive |
| `ws_high_turnover_rate` | DOUBLE | 100.0% | 0 | 11436 | moy 0.069 · méd 0.067 · min/max 0.0/0.6 · p10/p90 0.043/0.099 | Taux de pertes de balle en zone haute = turnovers_high_zone / total_passes. |
| `ws_deep_completion_rt` | DOUBLE | 100.0% | 0 | 10535 | moy 0.053 · méd 0.051 · min/max 0.0/0.2 · p10/p90 0.03/0.078 | Taux de passes complétées en zone profonde (deep completions) = deep_completions / total_passes. |
| `ws_momentum_delta` | DOUBLE | 73.6% | 8742 | 17376 | moy 1.158 · méd 1.027 · min/max 0.0/36.5 · p10/p90 0.657/1.588 | Delta de momentum après avoir encaissé un but (actions post / actions pre) |
| `ws_counter_shot_rate` | DOUBLE | 100.0% | 11 | 86 | moy 0.038 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.125 | Part des tirs issus de contre-attaque = shots_counter_attack / total_shots. |
| `ws_set_piece_pressure` | DOUBLE | 100.0% | 1 | 2637 | moy 0.08 · méd 0.076 · min/max 0.0/0.333 · p10/p90 0.043/0.122 | Part des actions offensives issues de coups de pied arrêtés = set_pieces_offensive / offensive_actions. |
| `ws_attack_left_pct` | DOUBLE | 100.0% | 0 | 17193 | moy 0.365 · méd 0.364 · min/max 0.0/0.667 · p10/p90 0.293/0.438 | Part des touches offensives sur le flanc gauche = att_touches_left / att_touches_total. |
| `ws_attack_center_pct` | DOUBLE | 100.0% | 0 | 15331 | moy 0.261 · méd 0.259 · min/max 0.0/0.484 · p10/p90 0.205/0.32 | Part des touches offensives dans l'axe = att_touches_center / att_touches_total. |
| `ws_attack_right_pct` | DOUBLE | 100.0% | 0 | 17277 | moy 0.374 · méd 0.372 · min/max 0.0/1.0 · p10/p90 0.302/0.448 | Part des touches offensives sur le flanc droit = att_touches_right / att_touches_total. |
| `ws_zone_def_pct` | DOUBLE | 100.0% | 0 | 23235 | moy 0.321 · méd 0.318 · min/max 0.0/0.684 · p10/p90 0.218/0.428 | Part des touches de l'équipe en zone défensive = zone_def_touches / total_touches. |
| `ws_zone_mid_pct` | DOUBLE | 100.0% | 0 | 21077 | moy 0.446 · méd 0.446 · min/max 0.223/0.708 · p10/p90 0.372/0.52 | Part des touches en zone médiane = zone_mid_touches / total_touches. |
| `ws_zone_att_pct` | DOUBLE | 100.0% | 0 | 21691 | moy 0.233 · méd 0.229 · min/max 0.0/0.643 · p10/p90 0.16/0.31 | Pourcentage de touches en zone offensive (x > 66.6) |
| `ws_shot_six_yard_pct` | DOUBLE | 100.0% | 11 | 140 | moy 0.078 · méd 0.067 · min/max 0.0/1.0 · p10/p90 0.0/0.2 | Part des tirs pris dans les 6 mètres = shots_six_yard / total_shots. |
| `ws_shot_penalty_pct` | DOUBLE | 100.0% | 11 | 247 | moy 0.546 · méd 0.55 · min/max 0.0/1.0 · p10/p90 0.333/0.75 | Part des tirs pris dans la surface = shots_penalty_area / total_shots. |
| `ws_shot_oob_pct` | DOUBLE | 100.0% | 11 | 250 | moy 0.376 · méd 0.368 · min/max 0.0/1.0 · p10/p90 0.176/0.583 | Part des tirs pris hors de la surface = shots_out_of_box / total_shots. |
| `ws_shot_open_play_pct` | DOUBLE | 100.0% | 11 | 235 | moy 0.672 · méd 0.679 · min/max 0.0/1.0 · p10/p90 0.455/0.875 | Part des tirs en jeu ouvert = shots_open_play / total_shots. |
| `ws_shot_set_piece_pct` | DOUBLE | 100.0% | 11 | 112 | moy 0.043 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.143 | Part des tirs sur coup de pied arrêté = shots_set_piece / total_shots. |
| `ws_shot_penalty_att_pct` | DOUBLE | 100.0% | 11 | 56 | moy 0.013 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.067 | Part des tirs qui sont des penalties = shots_penalty / total_shots. |
| `ws_conversion_rate` | DOUBLE | 100.0% | 11 | 148 | moy 0.117 · méd 0.1 · min/max 0.0/1.0 · p10/p90 0.0/0.25 | Taux de conversion = buts marqués / total_shots. |
| `ws_cross_rate` | DOUBLE | 100.0% | 0 | 9206 | moy 0.038 · méd 0.036 · min/max 0.0/0.2 · p10/p90 0.019/0.059 | Part des passes qui sont des centres = passes_cross / total_passes. |
| `ws_through_ball_rate` | DOUBLE | 100.0% | 0 | 13273 | moy 0.11 · méd 0.107 · min/max 0.0/0.4 · p10/p90 0.072/0.151 | Part des passes en profondeur (through balls) = passes_through_ball / total_passes. |
| `ws_long_ball_rate` | DOUBLE | 100.0% | 0 | 16485 | moy 0.156 · méd 0.15 · min/max 0.024/0.478 · p10/p90 0.088/0.233 | Part des longs ballons = passes_long_ball / total_passes. |
| `ws_short_pass_rate` | DOUBLE | 100.0% | 0 | 21280 | moy 0.696 · méd 0.703 · min/max 0.2/0.941 · p10/p90 0.578/0.805 | Part des passes courtes = 1 − (cross + through_ball + long_ball) / total_passes. |
| `ws_def_exposed_left_pct` | DOUBLE | 100.0% | 0 | 17193 | moy 0.365 · méd 0.364 · min/max 0.0/0.667 · p10/p90 0.293/0.438 | Part des attaques adverses subies sur le flanc gauche = opp_att_left / opp_att_total. |
| `ws_def_exposed_center_pct` | DOUBLE | 100.0% | 0 | 15331 | moy 0.261 · méd 0.259 · min/max 0.0/0.484 · p10/p90 0.205/0.32 | Part des attaques adverses subies dans l'axe = opp_att_center / opp_att_total. |
| `ws_def_exposed_right_pct` | DOUBLE | 100.0% | 0 | 17277 | moy 0.374 · méd 0.372 · min/max 0.0/1.0 · p10/p90 0.302/0.448 | Part des attaques adverses subies sur le flanc droit = opp_att_right / opp_att_total. |
| `ws_counter_attack_dna` | DOUBLE | 100.0% | 11 | 216 | moy 0.24 · méd 0.222 · min/max 0.0/1.0 · p10/p90 0.063/0.429 | ADN contre-attaque — proportion de tirs issus d'une récupération < 15 secondes |
| `ws_midfield_control_idx` | DOUBLE | 100.0% | 0 | 22815 | moy 0.5 · méd 0.5 · min/max 0.06/0.94 · p10/p90 0.297/0.703 | Contrôle du milieu — part des actions réussies en zone centrale |
| `ws_defensive_line_height` | DOUBLE | 100.0% | 2 | 32101 | moy 26.776 · méd 26.224 · min/max 11.575/55.95 · p10/p90 20.207/34.095 | Hauteur moyenne de la ligne défensive (position X) |
| `ws_flank_exposure_asymm` | DOUBLE | 100.0% | 9 | 3741 | moy -0.01 · méd -0.006 · min/max -1.0/1.0 · p10/p90 -0.231/0.208 | Asymétrie d'exposition sur les flancs — différentiel gauche/droite |