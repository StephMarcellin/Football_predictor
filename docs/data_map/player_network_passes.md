---
schema: intermediate
rows: 2442572
---
# player_network_passes

#intermediate

Arêtes du graphe de passes agrégées par (match, équipe, passeur, receveur) : volume total et ventilé par contexte (contre-attaque, open play, recovery, autre), passes progressives/créatives/buildup. Source : player_passes_raw.


## Intégrité
**Clé déclarée :** (match_id, team_id, passer_id, receiver_id) — ✅ aucun doublon

## Lineage
**Sources :** [[player_passes_raw]]
**Alimente :** [[player_network_centrality]]

## Features & profiling  (2442572 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 10969 |  | Identifiant unifié du match (SHA1). |
| `team_id` | BIGINT | 100.0% | 0 | 120 |  | Id canonique de l'équipe. |
| `passer_id` | INTEGER | 100.0% | 0 | 5784 |  | Identifiant WhoScored du joueur passeur (nœud source de l'arête du réseau de passes). |
| `receiver_id` | INTEGER | 100.0% | 0 | 5799 |  | Identifiant WhoScored du joueur receveur (nœud cible de l'arête). |
| `season` | VARCHAR | 100.0% | 0 | 8 | top: 2022-2023 (323273), 2021-2022 (321669), 2020-2021 (318644) | Saison du match (ex. 2023-2024). |
| `league_source` | VARCHAR | 100.0% | 0 | 4 | top: Serie A (659313), Premier League (632062), Ligue 1 (619053) | Championnat / source des données (ex. Bundesliga). |
| `n_passes` | BIGINT | 100.0% | 0 | 65 | moy 3.528 · méd 2.0 · min/max 1.0/75.0 · p10/p90 1.0/8.0 | Nombre total de passes réussies passeur -> receveur dans le match (poids de l'arête). |
| `n_passes_counter_attack` | BIGINT | 100.0% | 0 | 1 | moy 0.0 · méd 0.0 · min/max 0.0/0.0 · p10/p90 0.0/0.0 | Parmi n_passes, celles dont la chaîne est une contre-attaque (chain_trigger = 'counter_attack'). |
| `n_passes_open_play` | BIGINT | 100.0% | 0 | 30 | moy 1.486 · méd 1.0 · min/max 0.0/29.0 · p10/p90 0.0/4.0 | Parmi n_passes, celles en jeu ouvert (chain_trigger = 'open_play'). |
| `n_passes_recovery` | BIGINT | 100.0% | 0 | 28 | moy 0.89 · méd 1.0 · min/max 0.0/37.0 · p10/p90 0.0/2.0 | Parmi n_passes, celles issues d'une récupération (chain_trigger = 'recovery'). |
| `n_passes_other` | BIGINT | 100.0% | 0 | 35 | moy 1.152 · méd 1.0 · min/max 0.0/35.0 · p10/p90 0.0/3.0 | Parmi n_passes, celles sur coup de pied arrêté (chain_trigger dans corner / free_kick / throw_in / goal_kick). |
| `n_progressive` | BIGINT | 100.0% | 0 | 31 | moy 0.8 · méd 0.0 · min/max 0.0/33.0 · p10/p90 0.0/2.0 | Nombre de passes progressives (flag is_progressive de player_passes_raw). |
| `n_creative` | BIGINT | 100.0% | 0 | 7 | moy 0.08 · méd 0.0 · min/max 0.0/6.0 · p10/p90 0.0/0.0 | Nombre de passes créatives (flag is_creative de player_passes_raw). |
| `n_buildup` | BIGINT | 100.0% | 0 | 56 | moy 1.97 · méd 1.0 · min/max 0.0/60.0 · p10/p90 0.0/5.0 | Nombre de passes de construction (flag is_buildup de player_passes_raw). |