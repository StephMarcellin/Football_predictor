---
schema: intermediate
rows: 276654
---
# freekick_profiles

#intermediate

Profils de coups francs par chaîne. Caractérise chaque coup franc par son type (direct, indirect, short_pass) et son issue (goal, shot_saved, shot_off_target, offside, foul_won, corner_won, open_play, turnover). Source : player_possession_chains. Incrémentale.


## Intégrité
**Clé déclarée :** (match_id, row_num) — ✅ aucun doublon

## Lineage
**Sources :** [[event_values]], [[events_qual]], [[player_possession_chains]]
**Alimente :** [[joueur_saison]], [[rolling_freekicks]]

## Features & profiling  (276654 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 10968 |  |  |
| `chain_id` | VARCHAR | 100.0% | 0 | 276387 |  | Identifiant de la chaîne de possession = match_id // '_' // numéro de chaîne (propagé de player_possession_chains). |
| `chain_number` | HUGEINT | 100.0% | 0 | 463 | moy 162.713 · méd 160.0 · min/max 2.0/499.0 · p10/p90 38.0/289.0 | Numéro séquentiel de la chaîne de possession dans le match (propagé de player_possession_chains). |
| `chain_team_id` | BIGINT | 100.0% | 0 | 120 |  | Id de l'équipe en possession (attaquante) sur le coup franc (propagé de player_possession_chains). |
| `freekick_taker_id` | INTEGER | 100.0% | 0 | 4487 |  | Identifiant du joueur qui tire le coup franc. |
| `row_num` | INTEGER | 100.0% | 0 | 1848 | moy 814.022 · méd 816.0 · min/max 3.0/1903.0 · p10/p90 183.0/1430.0 |  |
| `event_id` | INTEGER | 100.0% | 0 | 1366 |  | event_id WhoScored de l'événement d'ancrage (scopé match+équipe). |
| `expanded_minute` | INTEGER | 100.0% | 0 | 126 | moy 49.517 · méd 50.0 · min/max 0.0/129.0 · p10/p90 10.0/89.0 | Minute (expanded_minute) du coup franc. |
| `second` | INTEGER | 100.0% | 0 | 60 | moy 28.905 · méd 29.0 · min/max 0.0/59.0 · p10/p90 5.0/53.0 | Seconde dans la minute. |
| `type_id` | INTEGER | 100.0% | 0 | 6 |  | type_id de l'événement d'ancrage (13-16 pour un tir direct ; 1/2 pour une passe / OffsidePass). |
| `outcome_id` | INTEGER | 100.0% | 0 | 2 |  | Résultat de l'événement d'ancrage (1 = réussi, 0 = raté). |
| `x` | DOUBLE | 100.0% | 0 | 995 | moy 42.386 · méd 41.1 · min/max 0.1/99.9 · p10/p90 13.8/70.1 | Position X du coup franc (0-100, repère équipe). |
| `y` | DOUBLE | 100.0% | 0 | 999 | moy 50.35 · méd 50.2 · min/max 0.1/99.9 · p10/p90 9.4/90.9 | Position Y du coup franc (0-100, 50 = axe central). |
| `fk_type` | VARCHAR | 100.0% | 0 | 3 | top: short_pass (246456), indirect (29931), direct (267) |  |
| `is_offside` | BOOLEAN | 100.0% | 0 | 2 | top: False (275654), True (1000) | Booléen : la passe de coup franc a été signalée hors-jeu (OffsidePass, type 2). FALSE pour les coups francs directs. |
| `fk_zone_type` | VARCHAR | 100.0% | 0 | 4 | top: too_far (209233), own_box (37718), crossed (29436) | Nature de la livraison : 'direct_shot' (frappe directe), 'own_box' (jouée dans sa propre surface, x<17), 'crossed' (centrée) ou 'too_far' (sinon). |
| `outcome` | VARCHAR | 100.0% | 0 | 8 | top: turnover (223191), shot_off_target (12076), shot_saved (11634) |  |
| `chain_danger_total` | DOUBLE | 99.6% | 1000 | 37832 | moy 0.118 · méd 0.0 · min/max 0.0/4.534 · p10/p90 0.0/0.452 | Somme du danger (action_value de event_values) généré par l'équipe sur la chaîne issue du coup franc. |
| `chain_danger_momentum` | DOUBLE | 9.5% | 250278 | 21701 | moy 0.439 · méd 0.469 · min/max 0.0/0.942 · p10/p90 0.0/0.705 | Part du danger accumulée AVANT le dernier tir / danger total de la chaîne. NULL si aucun tir. |
| `shot_body_part` | VARCHAR | 3.1% | 268042 | 4 | top: head (4905), right_foot (2254), left_foot (1311) | Partie du corps du tir résultant (head / right_foot / left_foot / other, via qualifiers 15/20/72/21) — renseigné pour les coups francs centrés ('crossed'). |
| `clearance_player_id` | INTEGER | 5.8% | 260633 | 3055 |  | Identifiant du défenseur qui dégage (type 12), pour les coups francs centrés (NULL sinon). |
| `clearance_quality` | VARCHAR | 5.8% | 260708 | 4 | top: failed (5352), good (4822), perfect (3301) | Qualité du dégagement : 'perfect' (l'équipe qui dégage récupère), 'failed' (récupération adverse haut, recovery_x>=83), 'poor' (>=75), 'good' (sinon). NULL si pas de dégagement. |
| `is_headed_clearance` | BOOLEAN | 4.7% | 263601 | 2 | top: True (12852), False (201) | Booléen : dégagement de la tête (qual 15) plutôt qu'autre (qual 21). |
| `x_m` | DOUBLE | 100.0% | 0 | 995 | moy 44.505 · méd 43.155 · min/max 0.105/104.895 · p10/p90 14.49/73.605 | Position X convertie en mètres = x * 1.05 (longueur terrain 105 m). |
| `y_m` | DOUBLE | 100.0% | 0 | 999 | moy 34.238 · méd 34.136 · min/max 0.068/67.932 · p10/p90 6.392/61.812 | Position Y convertie en mètres = y * 0.68 (largeur terrain 68 m). |
| `distance_to_goal` | DOUBLE | 100.0% | 0 | 180726 | moy 64.491 · méd 65.328 · min/max 6.102/109.585 · p10/p90 37.94/91.751 | Distance en mètres du point du coup franc au centre du but adverse (105, 34). |
| `angle` | DOUBLE | 100.0% | 0 | 190457 | moy 0.112 · méd 0.105 · min/max 0.001/1.065 · p10/p90 0.078/0.157 | Angle d'ouverture du but vu depuis le point du coup franc, en radians (loi des cosinus avec les poteaux à y=30.34 et 37.66 m). |