---
schema: intermediate
rows: 272793
---
# threat_conceded

#intermediate

Symétrique défensif de player_xg_chain : attribue la menace concédée sur les chaînes adverses terminées par un tir (penalty exclu), pour mesurer la responsabilité défensive. Source : player_xg_chain. Incrémentale.


## Intégrité
**Clé déclarée :** (match_id, chain_id, player_id) — ✅ aucun doublon

## Lineage
**Sources :** [[events_qual]], [[player_possession_chains]], [[player_xg_chain]]
**Alimente :** —

## Features & profiling  (272793 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 10968 |  | Identifiant unifié du match (SHA1). |
| `chain_id` | VARCHAR | 100.0% | 0 | 176892 |  | Identifiant de la chaîne de possession = match_id // '_' // numéro de chaîne (propagé de player_possession_chains). Regroupe les actions d'une même possession. |
| `chain_number` | HUGEINT | 100.0% | 0 | 465 | moy 167.641 · méd 167.0 · min/max 0.0/504.0 · p10/p90 40.0/294.0 | Numéro séquentiel de la chaîne de possession dans le match (propagé de player_possession_chains). |
| `chain_team_id` | BIGINT | 100.0% | 0 | 120 |  | Id canonique de l'équipe EN POSSESSION durant la chaîne (propagé de player_possession_chains). C'est l'équipe attaquante ; team_id peut différer (action d'un autre joueur). |
| `player_id` | INTEGER | 100.0% | 0 | 5097 |  | Identifiant du joueur DÉFENSEUR adverse présent dans la chaîne (team_id != chain_team_id) — celui qui concède la menace. |
| `team_id` | BIGINT | 100.0% | 0 | 120 |  | Id de l'équipe du défenseur (équipe qui concède, adverse à chain_team_id). |
| `season` | VARCHAR | 100.0% | 0 | 8 | top: 2018-2019 (37131), 2017-2018 (35739), 2023-2024 (35406) | Saison du match (ex. 2023-2024). |
| `league_source` | VARCHAR | 100.0% | 0 | 4 | top: Premier League (74302), Serie A (72393), Ligue 1 (66608) | Championnat / source des données (ex. Bundesliga). |
| `xg_proxy` | DECIMAL(3,2) | 100.0% | 0 | 7 | moy 0.591 · méd 0.8 · min/max 0.1/1.0 · p10/p90 0.4/0.8 | Proxy xG du tir terminal de la chaîne (propagé de player_xg_chain) = chance_creation de event_values pour ce tir (0-1). |
| `is_penalty` | BOOLEAN | 100.0% | 0 | 2 | top: False (265624), True (7169) | Booléen : le tir terminal de la chaîne est un penalty (qualifier 9). Si TRUE, le gardien n'est pas sanctionné. |
| `position_in_chain` | BIGINT | 100.0% | 0 | 11 | moy 1.765 · méd 1.0 · min/max 1.0/11.0 · p10/p90 1.0/3.0 | Rang chronologique du défenseur dans la chaîne (1 = première intervention défensive), par expanded_minute/second/row_num. |
| `chain_length` | BIGINT | 100.0% | 0 | 11 | moy 2.313 · méd 2.0 · min/max 1.0/11.0 · p10/p90 1.0/4.0 | Nombre total d'interventions défensives adverses dans la chaîne. |
| `position_weight` | DOUBLE | 100.0% | 0 | 35 | moy 0.826 · méd 1.0 · min/max 0.1/1.0 · p10/p90 0.5/1.0 | Poids de responsabilité = position_in_chain / chain_length (0-1) : plus le défenseur intervient tard (proche du tir), plus le poids est élevé. |
| `is_keeper` | BOOLEAN | 100.0% | 0 | 2 | top: False (138842), True (133951) | Booléen : le défenseur est le gardien (a réalisé Save/Claim/Punch/KeeperPickup — type 10/11/41/52 — dans la chaîne). |
| `has_error_leading_to_goal` | BOOLEAN | 100.0% | 0 | 2 | top: False (271743), True (1050) | Booléen : le gardien a commis une Error menant au but (type 51 + qualifier 170) dans la chaîne. |
| `threat_conceded` | DECIMAL(3,2) | 100.0% | 0 | 7 | moy 0.591 · méd 0.8 · min/max 0.1/1.0 · p10/p90 0.4/0.8 | Menace concédée brute = xg_proxy du tir de la chaîne (non pondérée). |
| `threat_conceded_weighted` | DOUBLE | 100.0% | 0 | 95 | moy 0.232 · méd 0.05 · min/max 0.0/1.0 · p10/p90 0.0/0.8 | Menace concédée pondérée + règles gardien : penalty -> gardien = 0 ; erreur du gardien -> sanction pleine (xg_proxy × position_weight) ; défense trouée (gardien sans erreur) -> 0 ; défenseur non-gardien -> xg_proxy × position_weight. |