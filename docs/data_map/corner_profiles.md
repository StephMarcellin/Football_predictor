---
schema: intermediate
rows: 107426
---
# corner_profiles

#intermediate

Profils de corners. Pour chaque chaîne déclenchée par un corner, caractérise la livraison (end_x/end_y, joueur, via qualifier CornerTaken=6) et l'issue de la séquence. Source : player_possession_chains. Incrémentale.


## Intégrité
**Clé déclarée :** (match_id, team_id, corner_row_num) — ✅ aucun doublon

## Lineage
**Sources :** [[event_values]], [[events_qual]], [[player_possession_chains]]
**Alimente :** [[rolling_corners]]

## Features & profiling  (107426 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 10969 |  | Identifiant unifié du match (SHA1). |
| `team_id` | BIGINT | 100.0% | 0 | 120 |  | Id de l'équipe qui tire le corner. |
| `corner_taker_id` | INTEGER | 100.0% | 0 | 2160 |  | Identifiant du joueur qui tire le corner. |
| `corner_row_num` | INTEGER | 100.0% | 0 | 1812 | moy 818.503 · méd 833.0 · min/max 4.0/1922.0 · p10/p90 188.0/1426.0 | row_num de l'événement du corner dans le match. |
| `expanded_minute` | INTEGER | 100.0% | 0 | 120 | moy 49.594 · méd 51.0 · min/max 0.0/124.0 · p10/p90 10.0/88.0 | Minute (expanded_minute) du corner. |
| `corner_side` | VARCHAR | 100.0% | 0 | 2 | top: right (55220), left (52206) | Côté du corner déduit du y de départ : 'right' si corner_start_y < 50, sinon 'left'. |
| `landing_zone` | VARCHAR | 100.0% | 0 | 4 | top: near_post (47575), center (39912), far_post (15003) | Zone d'atterrissage relative au côté : 'short' (livraison courte, end_x<83), 'center' (end_y 45-55), 'near_post' ou 'far_post' selon le côté. |
| `n_aerial_duels` | BIGINT | 100.0% | 0 | 11 | moy 0.655 · méd 0.0 · min/max 0.0/10.0 · p10/p90 0.0/2.0 | Nombre de duels aériens disputés sur la chaîne issue du corner. |
| `shot_body_part` | VARCHAR | 34.6% | 70297 | 4 | top: head (18566), right_foot (11208), left_foot (6927) | Partie du corps du tir issu du corner : right_foot, left_foot, head ou 'other' (NULL si pas de tir). |
| `clearance_player_id` | INTEGER | 55.5% | 47786 | 4112 |  | Identifiant du défenseur qui dégage (NULL si pas de dégagement). |
| `clearance_quality` | VARCHAR | 55.4% | 47951 | 4 | top: good (19916), failed (17191), poor (11633) | Qualité du dégagement défensif : perfect / good / poor / failed (NULL si pas de dégagement). |
| `is_headed_clearance` | BOOLEAN | 43.5% | 60705 | 2 | top: True (45897), False (824) | Booléen : le dégagement est effectué de la tête. |
| `outcome` | VARCHAR | 100.0% | 0 | 5 | top: clearance (44353), retained (25944), shot_off_target (19628) | Issue de la chaîne du corner : 'goal' (tir 16), 'shot_saved' (15), 'shot_off_target' (13/14), 'clearance' (dégagement), 'retained' (possession conservée sinon). |
| `chain_danger_total` | DOUBLE | 100.0% | 0 | 24009 | moy 0.352 · méd 0.0 · min/max 0.0/4.037 · p10/p90 0.0/1.158 | Somme du danger (action_value de event_values) généré par l'équipe attaquante sur toute la chaîne issue du corner. |
| `chain_danger_momentum` | DOUBLE | 34.6% | 70297 | 16098 | moy 0.42 · méd 0.489 · min/max 0.0/0.94 · p10/p90 0.0/0.682 | Part du danger accumulée AVANT le dernier tir / danger total de la chaîne (danger_before_shot / chain_danger_total). NULL si aucun tir. |