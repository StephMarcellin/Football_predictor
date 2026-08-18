---
schema: gold
rows: 85272
---
# zone_confrontation_match

#gold #famille7

Grain (match_id, attacking_team_id, attack_corridor) — famille CDC 7. Croise la menace offensive de A et la vulnérabilité défensive de B PAR COULOIR, en MIROIR (A-gauche ↔ B-droite, car les équipes se font face). 6 lignes par match (2 sens × 3 couloirs). Calculable une fois les compos connues.

## Intégrité
**Clé déclarée :** (match_id, attacking_team_id, attack_corridor) — ✅ aucun doublon

## Lineage
**Sources :** [[team_corridor_profile]]
**Alimente :** —

## Features & profiling  (85272 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 14212 |  | Identifiant unifié du match. |
| `attacking_team_id` | BIGINT | 100.0% | 0 | 150 |  | Équipe en position d'attaque (sens A→B). |
| `defending_team_id` | BIGINT | 100.0% | 0 | 150 |  | Équipe en position de défense (adversaire). |
| `attack_corridor` | VARCHAR | 100.0% | 0 | 3 | top: droit (28424), gauche (28424), axe (28424) | Couloir d'attaque de A : gauche / axe / droit. |
| `off_strength` | DOUBLE | 100.0% | 0 | 29690 | moy 0.845 · méd 0.751 · min/max 0.0/6.917 · p10/p90 0.0/1.854 | Force offensive de A dans le couloir. |
| `opp_def_solidity` | DOUBLE | 81.3% | 15977 | 5072 | moy 0.504 · méd 0.417 · min/max 0.0/1.0 · p10/p90 0.174/0.853 | Solidité défensive de B dans le couloir miroir. |
| `matchup_danger_by_corridor` | DOUBLE | 81.3% | 15977 | 56714 | moy 0.476 · méd 0.336 · min/max 0.0/4.902 · p10/p90 0.0/1.1 | [Famille 7, feature 52] Danger du couloir = off_strength(A) × (1 − def_solidity(B miroir)). Produit menace × vulnérabilité. NULL si solidité adverse inconnue → imputation famille 11. |