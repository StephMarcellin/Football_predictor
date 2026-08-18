---
schema: intermediate
rows: 6252820
---
# player_zone_transitions

#intermediate

Compte les transitions de zone (zone_from → zone_to) par joueur, ventilées par période, état au score (score_state) et formation. Décrit les patterns de circulation du ballon. Source : int_event_enriched (via player_passes_raw pour les passes + TakeOns réussis). Incrémentale. Grain : (match, équipe, joueur, zone_from, zone_to, période, score_state, formation). Les « déplacements » = passes réussies + dribbles (TakeOn) réussis.


## Intégrité
**Clé déclarée :** (match_id, team_id, player_id, zone_from, zone_to, period, score_state, formation) — ✅ aucun doublon

## Lineage
**Sources :** [[events_qual]], [[int_event_enriched]], [[player_passes_raw]]
**Alimente :** —

## Features & profiling  (6252820 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 10969 |  | Identifiant unifié du match (SHA1), résolu via int_whoscored_match_index. |
| `team_id` | BIGINT | 100.0% | 0 | 120 |  | Id canonique de l'équipe du joueur (converti depuis l'id WhoScored via int_whoscored_match_index). |
| `player_id` | INTEGER | 100.0% | 0 | 5784 |  | Identifiant WhoScored du joueur auteur des déplacements. |
| `season` | VARCHAR | 100.0% | 0 | 8 | top: 2020-2021 (825106), 2021-2022 (813016), 2017-2018 (808403) | Saison du match (ex. 2023-2024). |
| `league_source` | VARCHAR | 100.0% | 0 | 4 | top: Premier League (1679323), Serie A (1643692), Ligue 1 (1593771) | Championnat / source des données (ex. Bundesliga). |
| `period` | INTEGER | 100.0% | 0 | 2 | moy 1.494 · méd 1.0 · min/max 1.0/2.0 · p10/p90 1.0/2.0 | Période : 1 = 1re mi-temps, 2 = 2e mi-temps, 16 = pré-match, 14 = fin de match. |
| `score_state` | VARCHAR | 100.0% | 0 | 3 | top: drawing (2888211), losing (1766789), winning (1597820) | État au score au moment des déplacements : winning / losing / drawing (buts cumulés équipe vs adverse). |
| `formation` | VARCHAR | 100.0% | 13 | 23 |  | Code de la formation active de l'équipe à cet instant (via FormationSet/FormationChange, qualifier 130). |
| `zone_from` | VARCHAR | 100.0% | 0 | 15 | top: B4 (663867), C3 (656832), B3 (642364) | Cellule de DÉPART (grille 3×5). Bande latérale sur y : A (y<33.3), B (33.3–66.6), C (≥66.6) ; bande longitudinale sur x : 1 (x≥80, près du but adverse), 2 (≥60), 3 (≥40), 4 (≥20), 5 (<20, près de son but). Ex. B1 = axe central, tiers offensif.  |
| `zone_to` | VARCHAR | 100.0% | 0 | 15 | top: C3 (643713), A3 (616687), B4 (601509) | Cellule d'ARRIVÉE du déplacement, même encodage 3×5, calculée depuis end_x / end_y. |
| `n_transitions` | BIGINT | 100.0% | 0 | 29 | moy 1.391 · méd 1.0 · min/max 1.0/30.0 · p10/p90 1.0/2.0 | Nombre de déplacements (passes + dribbles réussis) dans le groupe zone_from → zone_to. |
| `pct_transitions` | DOUBLE | 100.0% | 0 | 836 | moy 0.139 · méd 0.091 · min/max 0.009/1.0 · p10/p90 0.038/0.273 | Part de ces déplacements sur l'ensemble des déplacements du joueur dans le contexte (match, période, score_state, formation). |
| `progressive_rate` | DOUBLE | 100.0% | 0 | 97 | moy 0.24 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | Part des déplacements progressifs (end_x > x + 10, ≈10 m vers le but adverse) dans le groupe. |