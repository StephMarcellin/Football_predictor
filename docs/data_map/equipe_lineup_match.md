---
schema: gold
rows: 33172
---
# equipe_lineup_match

#gold #famille4

Grain (match_id, team_id). Famille CDC 4 : qualité brute du XI de départ (indépendante des matchups), agrégée depuis les profils SEASON-LAG de joueur_saison. Table séparée d'equipe_match car dépendante de la compo (~1h avant le coup d'envoi). Reporté : age (source salie), rating (season-lag à construire), n_key_starters_missing (seuil à calibrer).

## Intégrité
**Clé déclarée :** (match_id, team_id) — ✅ aucun doublon

## Lineage
**Sources :** [[int_whoscored_lineup]], [[joueur_saison]]
**Alimente :** —

## Features & profiling  (33172 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 16586 |  | Identifiant unifié du match. |
| `team_id` | BIGINT | 100.0% | 0 | 175 |  | Équipe. |
| `lineup_sum_xg_per90_lag` | DOUBLE | 99.6% | 140 | 33031 | moy 0.753 · méd 0.736 · min/max 0.0/4.144 · p10/p90 0.556/0.973 | [Famille 4, feature 30] Somme des xG/90 (saison N-1) des 11 titulaires — menace offensive cumulée de la compo. |
| `lineup_avg_shots_per90_lag` | DOUBLE | 99.6% | 140 | 33031 | moy 1.119 · méd 1.097 · min/max 0.0/5.541 · p10/p90 0.853/1.415 | [Famille 4] Tirs/90 moyens des titulaires. |
| `lineup_avg_def_actions_per90_lag` | DOUBLE | 99.6% | 140 | 33032 | moy 2.392 · méd 2.362 · min/max 0.0/10.505 · p10/p90 1.965/2.846 | [Famille 4] Actions défensives/90 moyennes des titulaires (solidité brute). |
| `lineup_avg_aerial_win_rate_lag` | DOUBLE | 99.6% | 140 | 33011 | moy 0.508 · méd 0.51 · min/max 0.0/1.0 · p10/p90 0.454/0.558 | [Famille 4] Taux de duels aériens gagnés moyen des titulaires. |
| `n_starters_profiled` | BIGINT | 100.0% | 0 | 14 | moy 10.874 · méd 11.0 · min/max 0.0/13.0 · p10/p90 11.0/11.0 | Nombre de titulaires (sur 11) ayant un profil joueur_saison (confiance). |