---
schema: intermediate
rows: 104173
---
# int_whoscored_lineup_agg

#intermediate

Features positionnelles au grain joueur×match, agrégées depuis int_whoscored_lineup : position moyenne pondérée par les minutes, slot principal, capitaine, nombre de périodes tactiques. Couverture liée à formation_slots (se remplit avec le load des archives). Source : int_whoscored_lineup.


## Intégrité
**Clé déclarée :** (match_id, team_id, player_id) — ✅ aucun doublon

## Lineage
**Sources :** [[int_whoscored_lineup]]
**Alimente :** —

## Features & profiling  (104173 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 3760 |  | Identifiant unifié du match (SHA1) |
| `team_id` | BIGINT | 99.9% | 112 | 105 |  | Id canonique de l'équipe. |
| `player_id` | BIGINT | 100.0% | 0 | 3458 |  | Identifiant WhoScored du joueur |
| `avg_vertical` | DOUBLE | 99.8% | 237 | 6914 | moy 4.974 · méd 5.132 · min/max 0.0/9.0 · p10/p90 2.17/9.0 | Position verticale moyenne (0 = ligne de but propre, ~10 = attaque) |
| `avg_horizontal` | DOUBLE | 99.8% | 237 | 7453 | moy 4.987 · méd 5.0 · min/max 1.0/9.0 · p10/p90 1.335/8.526 | Position horizontale moyenne (5 = axe central) |
| `primary_slot` | INTEGER | 100.0% | 0 | 11 | moy 6.379 · méd 7.0 · min/max 1.0/11.0 · p10/p90 2.0/11.0 | Slot de formation dominant (1..11, 1 = gardien) |
| `is_captain` | BOOLEAN | 100.0% | 44 | 2 | top: False (95347), True (8782) | Booléen : le joueur a été capitaine sur au moins une période. |
| `n_formation_periods` | BIGINT | 100.0% | 0 | 10 | moy 3.273 · méd 4.0 · min/max 1.0/10.0 · p10/p90 1.0/5.0 | Nombre de périodes tactiques distinctes du joueur dans le match (proxy de repositionnement). |