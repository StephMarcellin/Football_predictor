---
schema: gold
rows: 87756
---
# team_corridor_profile

#gold #famille7

Grain (match_id, team_id, corridor). Brique de la famille 7 : agrège les profils zonaux N-1 des titulaires (XI de départ) par couloir (gauche/axe/droit via grid_horizontal). off_strength = Σ volume de tirs dans les cellules attaquantes du couloir ; def_solidity = taux de duels gagnés pondéré dans les cellules défensives.

## Intégrité
**Clé déclarée :** (match_id, team_id, corridor) — ✅ aucun doublon

## Lineage
**Sources :** [[backbone]], [[int_whoscored_lineup]], [[joueur_zone_saison]]
**Alimente :** [[joueur_match]], [[zone_confrontation_match]]

## Features & profiling  (87756 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 15040 |  | Identifiant unifié du match. |
| `team_id` | BIGINT | 100.0% | 0 | 150 |  | Équipe. |
| `opponent_id` | BIGINT | 100.0% | 0 | 174 |  | Adversaire (pour le croisement famille 7). |
| `corridor` | VARCHAR | 100.0% | 0 | 3 | top: droit (29252), gauche (29252), axe (29252) | Couloir occupé : gauche / axe / droit (via grid_horizontal du XI). |
| `off_strength` | DOUBLE | 100.0% | 0 | 30169 | moy 0.84 · méd 0.745 · min/max 0.0/6.917 · p10/p90 0.0/1.849 | Force offensive du couloir = Σ volume de tirs des titulaires dans les cellules attaquantes (z4,z5) du couloir. |
| `def_solidity` | DOUBLE | 80.7% | 16911 | 5088 | moy 0.506 · méd 0.418 · min/max 0.0/1.0 · p10/p90 0.176/0.853 | Solidité défensive du couloir = taux de duels gagnés pondéré par volume, cellules défensives (z1,z2). NULL si aucun duel. |