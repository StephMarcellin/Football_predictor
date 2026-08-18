---
schema: gold
rows: 321784
---
# joueur_match

#gold

Grain (match_id, team_id, player_id) — 7e table de grain, assemblage du modèle Buteurs (une ligne = un titulaire dans un match). Réunit les features scorer SEASON-LAG (joueur_saison) et la feature propre 75 (scorer_context_vs_opponent_style : croisement tirs du joueur × vulnérabilité adverse par couloir miroir). Candidats = XI de départ.

## Intégrité
**Clé déclarée :** (match_id, team_id, player_id) — ✅ aucun doublon

## Lineage
**Sources :** [[backbone]], [[int_whoscored_lineup]], [[joueur_saison]], [[joueur_zone_saison]], [[team_corridor_profile]]
**Alimente :** —

## Features & profiling  (321784 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 15040 |  | Identifiant unifié du match. |
| `team_id` | BIGINT | 100.0% | 0 | 150 |  | Équipe du joueur. |
| `opponent_id` | BIGINT | 100.0% | 0 | 174 |  | Adversaire. |
| `player_id` | BIGINT | 100.0% | 0 | 6309 |  | Joueur (titulaire). |
| `is_home` | INTEGER | 100.0% | 0 | 2 | moy 0.5 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | 1 si le joueur joue à domicile. |
| `scorer_xg_per90_lag` | DOUBLE | 99.0% | 3373 | 282471 | moy 0.069 · méd 0.049 · min/max 0.0/3.73 · p10/p90 0.0/0.164 | [Famille 10] xG/90 du joueur (saison N-1) — repris de joueur_saison. |
| `scorer_shots_per90_lag` | DOUBLE | 99.0% | 3373 | 98833 | moy 1.114 · méd 0.805 · min/max 0.0/45.0 · p10/p90 0.025/2.521 | [Famille 10] Tirs/90 du joueur. |
| `scorer_team_shot_share_lag` | DOUBLE | 99.0% | 3345 | 31804 | moy 0.068 · méd 0.053 · min/max 0.0/0.75 · p10/p90 0.002/0.151 | [Famille 10] Part des tirs de l'équipe prise par le joueur. |
| `scorer_penalty_taker_lag` | HUGEINT | 99.0% | 3344 | 18 | moy 0.374 · méd 0.0 · min/max 0.0/17.0 · p10/p90 0.0/1.0 | [Famille 10] Penaltys tirés sur la fenêtre (rôle de tireur). |
| `scorer_freekick_taker_lag` | HUGEINT | 99.0% | 3344 | 225 | moy 22.612 · méd 10.0 · min/max 0.0/228.0 · p10/p90 0.0/65.0 | [Famille 10] Coups francs tirés sur la fenêtre. |
| `off_xg_per_shot_lag` | DOUBLE | 89.4% | 34141 | 204586 | moy 0.062 · méd 0.058 · min/max 0.01/0.35 · p10/p90 0.04/0.089 | [Famille 5/10] Qualité de tir (xG/tir). |
| `scorer_context_vs_opponent_style` | DOUBLE | 68.5% | 101435 | 174117 | moy 0.396 · méd 0.238 · min/max 0.0/4.86 · p10/p90 0.0/1.01 | [Famille 10, feature 75] Σ sur les couloirs [ tirs du joueur × (1 − solidité défensive de l'adversaire, couloir miroir) ]. Danger attendu du joueur face à CET adversaire. NULL/partiel si solidité adverse inconnue → imputation famille 11. |