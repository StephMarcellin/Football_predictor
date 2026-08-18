---
schema: gold
rows: 46887
---
# features_whoscored

#gold

Features WhoScored par équipe par match — anti-leakage LAG(1). Jointure de intermediate.team_features_ws × intermediate.backbone via referentiel.team_mapping. Inclut les squad features rolling 5 matchs.


## Intégrité
**Clé déclarée :** (date, team, league_source) — ?

## Lineage
**Sources :** [[backbone]], [[int_whoscored_match_index]], [[player_match_stats]], [[team_features_ws]]
**Alimente :** [[features_final]]

## Features & profiling  (46887 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `date` | DATE | 100.0% | 0 | 2417 |  | Date du match |
| `team_id` | BIGINT | 100.0% | 0 | 153 |  |  |
| `league_source` | VARCHAR | 100.0% | 0 | 28 |  | Compétition source |
| `match_id` | VARCHAR | 100.0% | 0 | 28499 |  |  |
| `ws_field_tilt_actions` | DOUBLE | 46.8% | 24955 | 16336 | moy 0.239 · méd 0.235 · min/max 0.0/0.654 · p10/p90 0.164/0.318 | Tilt de terrain — proportion de touches en zone offensive (dernier match connu) |
| `ws_high_turnover_rate` | DOUBLE | 46.8% | 24955 | 9483 | moy 0.067 · méd 0.066 · min/max 0.0/0.6 · p10/p90 0.042/0.095 |  |
| `ws_deep_completion_rt` | DOUBLE | 46.8% | 24955 | 8865 | moy 0.052 · méd 0.05 · min/max 0.0/0.2 · p10/p90 0.029/0.077 |  |
| `ws_momentum_delta` | DOUBLE | 34.6% | 30669 | 12369 | moy 1.164 · méd 1.03 · min/max 0.0/33.333 · p10/p90 0.654/1.603 | Delta de momentum après avoir encaissé un but |
| `ws_counter_shot_rate` | DOUBLE | 46.8% | 24964 | 81 | moy 0.038 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.118 |  |
| `ws_set_piece_pressure` | DOUBLE | 46.8% | 24956 | 2396 | moy 0.078 · méd 0.074 · min/max 0.0/0.333 · p10/p90 0.042/0.12 |  |
| `ws_attack_left_pct` | DOUBLE | 46.8% | 24955 | 13549 | moy 0.364 · méd 0.363 · min/max 0.0/0.667 · p10/p90 0.291/0.437 |  |
| `ws_attack_center_pct` | DOUBLE | 46.8% | 24955 | 12372 | moy 0.263 · méd 0.261 · min/max 0.0/0.484 · p10/p90 0.206/0.323 |  |
| `ws_attack_right_pct` | DOUBLE | 46.8% | 24955 | 13626 | moy 0.373 · méd 0.371 · min/max 0.0/1.0 · p10/p90 0.301/0.447 |  |
| `ws_zone_def_pct` | DOUBLE | 46.8% | 24955 | 17136 | moy 0.319 · méd 0.315 · min/max 0.0/0.684 · p10/p90 0.213/0.43 |  |
| `ws_zone_mid_pct` | DOUBLE | 46.8% | 24955 | 15920 | moy 0.449 · méd 0.449 · min/max 0.223/0.677 · p10/p90 0.373/0.526 |  |
| `ws_zone_att_pct` | DOUBLE | 46.8% | 24955 | 16220 | moy 0.231 · méd 0.227 · min/max 0.0/0.643 · p10/p90 0.158/0.309 | Pourcentage de touches en zone offensive |
| `ws_shot_six_yard_pct` | DOUBLE | 46.8% | 24964 | 137 | moy 0.078 · méd 0.067 · min/max 0.0/1.0 · p10/p90 0.0/0.2 |  |
| `ws_shot_penalty_pct` | DOUBLE | 46.8% | 24964 | 236 | moy 0.549 · méd 0.556 · min/max 0.0/1.0 · p10/p90 0.333/0.75 |  |
| `ws_shot_oob_pct` | DOUBLE | 46.8% | 24964 | 237 | moy 0.373 · méd 0.364 · min/max 0.0/1.0 · p10/p90 0.167/0.583 |  |
| `ws_shot_open_play_pct` | DOUBLE | 46.8% | 24964 | 223 | moy 0.675 · méd 0.688 · min/max 0.0/1.0 · p10/p90 0.462/0.875 |  |
| `ws_shot_set_piece_pct` | DOUBLE | 46.8% | 24964 | 106 | moy 0.045 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.143 |  |
| `ws_shot_penalty_att_pct` | DOUBLE | 46.8% | 24964 | 54 | moy 0.013 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/0.063 |  |
| `ws_conversion_rate` | DOUBLE | 46.8% | 24964 | 140 | moy 0.118 · méd 0.1 · min/max 0.0/1.0 · p10/p90 0.0/0.25 |  |
| `ws_cross_rate` | DOUBLE | 46.8% | 24955 | 7871 | moy 0.037 · méd 0.035 · min/max 0.0/0.2 · p10/p90 0.019/0.057 |  |
| `ws_through_ball_rate` | DOUBLE | 46.8% | 24955 | 10914 | moy 0.107 · méd 0.104 · min/max 0.0/0.4 · p10/p90 0.07/0.147 |  |
| `ws_long_ball_rate` | DOUBLE | 46.8% | 24955 | 12831 | moy 0.147 · méd 0.141 · min/max 0.025/0.406 · p10/p90 0.084/0.218 |  |
| `ws_short_pass_rate` | DOUBLE | 46.8% | 24955 | 15713 | moy 0.709 · méd 0.715 · min/max 0.2/0.926 · p10/p90 0.6/0.812 |  |
| `ws_def_exposed_left_pct` | DOUBLE | 46.8% | 24955 | 13549 | moy 0.364 · méd 0.363 · min/max 0.0/0.667 · p10/p90 0.291/0.437 |  |
| `ws_def_exposed_center_pct` | DOUBLE | 46.8% | 24955 | 12372 | moy 0.263 · méd 0.261 · min/max 0.0/0.484 · p10/p90 0.206/0.323 |  |
| `ws_def_exposed_right_pct` | DOUBLE | 46.8% | 24955 | 13626 | moy 0.373 · méd 0.371 · min/max 0.0/1.0 · p10/p90 0.301/0.447 |  |
| `ws_counter_attack_dna` | DOUBLE | 46.8% | 24964 | 203 | moy 0.244 · méd 0.231 · min/max 0.0/1.0 · p10/p90 0.067/0.444 | ADN contre-attaque — proportion de tirs issus d'une récupération rapide |
| `ws_midfield_control_idx` | DOUBLE | 46.8% | 24955 | 16903 | moy 0.5 · méd 0.5 · min/max 0.081/0.919 · p10/p90 0.292/0.708 | Contrôle du milieu de terrain |
| `ws_defensive_line_height` | DOUBLE | 46.8% | 24957 | 21444 | moy 27.04 · méd 26.491 · min/max 11.575/55.95 · p10/p90 20.312/34.488 |  |
| `ws_flank_exposure_asymm` | DOUBLE | 46.8% | 24964 | 3144 | moy -0.01 · méd -0.006 · min/max -1.0/1.0 · p10/p90 -0.232/0.214 |  |
| `has_ws_events` | INTEGER | 100.0% | 0 | 2 | moy 0.468 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | 1 si des données WhoScored sont disponibles pour ce match, 0 sinon |
| `squad_avg_form_5` | DOUBLE | 23.2% | 35991 | 10885 | moy 53.029 · méd 52.325 · min/max 16.2/106.778 · p10/p90 44.314/62.573 | Forme moyenne des joueurs du squad sur les 5 derniers matchs |
| `squad_xg_quality_5` | DOUBLE | 23.2% | 35991 | 10896 | moy 0.054 · méd 0.053 · min/max 0.007/0.141 · p10/p90 0.038/0.073 | Qualité xG moyenne du squad sur les 5 derniers matchs |
| `squad_regularity` | DOUBLE | 23.2% | 35991 | 41 | moy 0.802 · méd 0.867 · min/max 0.0/1.0 · p10/p90 0.643/1.0 | Stabilité du squad — proportion de joueurs présents au match précédent |
| `squad_top3_share` | DOUBLE | 23.2% | 35991 | 8372 | moy 0.344 · méd 0.343 · min/max 0.26/0.778 · p10/p90 0.313/0.378 | Part des actions portées par les 3 meilleurs joueurs (concentration) |