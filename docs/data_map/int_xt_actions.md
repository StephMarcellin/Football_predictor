---
schema: intermediate
rows: 12120881
---
# int_xt_actions

#intermediate

Actions atomiques pour l'expected threat (xT), affectées à la grille fine 16×12. Grain : une action de possession (déplacement, tir OU perte de balle). L'affectation (x,y)→case est définie UNIQUEMENT ici (col/row) et relue telle quelle par xt_grid.py et int_xt_contributions. Déplacements = passes réussies (player_passes_raw) + dribbles TakeOn réussis. Tirs = int_shot_placement, hors CSC. Pertes = fin de possession (passe/dribble ratés, contrôle manqué, Dispossessed, OffsidePass) depuis int_event_enriched.


## Intégrité
**Clé déclarée :** (match_id, row_num) — ✅ aucun doublon

## Lineage
**Sources :** [[int_event_enriched]], [[int_shot_placement]], [[player_passes_raw]]
**Alimente :** [[int_xt_contributions]]

## Features & profiling  (12120881 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 11030 |  | Identifiant unifié du match (SHA1). |
| `row_num` | INTEGER | 100.0% | 0 | 1923 | moy 773.707 · méd 764.0 · min/max 0.0/1923.0 · p10/p90 151.0/1404.0 | Index de l'événement dans le match (ordre chronologique). |
| `season` | VARCHAR | 100.0% | 0 | 8 | top: 2020-2021 (1605358), 2017-2018 (1590965), 2018-2019 (1585764) | Saison du match (ex. 2023-2024). |
| `league_source` | VARCHAR | 100.0% | 0 | 4 | top: Premier League (3274730), Serie A (3133373), Ligue 1 (3075371) | Championnat / source des données (ex. Bundesliga). |
| `team_id` | BIGINT | 100.0% | 345 | 120 |  | Id canonique de l'équipe de l'action. |
| `player_id` | INTEGER | 100.0% | 0 | 5828 |  | Identifiant WhoScored du joueur (passeur, dribbleur ou tireur). |
| `action_kind` | VARCHAR | 100.0% | 0 | 3 | top: move (8810422), turnover (3029131), shot (281328) | Type d'action : 'move' (passe/dribble réussi, avec destination), 'shot' (tir), ou 'turnover' (perte de balle, sans destination). |
| `is_shot` | BOOLEAN | 100.0% | 0 | 2 | top: False (11839553), True (281328) | Booléen : l'action est un tir. |
| `is_goal` | BOOLEAN | 2.3% | 11839553 | 2 | top: False (250747), True (30581) | Booléen : le tir est un but (renseigné pour les tirs ; NULL pour déplacements et pertes). |
| `col_from` | INTEGER | 100.0% | 0 | 16 | moy 7.907 · méd 8.0 · min/max 0.0/15.0 · p10/p90 3.0/13.0 | Colonne (0-15) de la case de DÉPART sur la grille 16×12 (0 = son but, 15 = but adverse). = GREATEST(0, LEAST(15, INT(x/100·16))). |
| `row_from` | INTEGER | 100.0% | 0 | 12 | moy 5.981 · méd 6.0 · min/max 0.0/11.0 · p10/p90 1.0/11.0 | Ligne (0-11) de la case de départ (largeur, ~6 = axe central). = GREATEST(0, LEAST(11, INT(y/100·12))). |
| `col_to` | INTEGER | 72.7% | 3310995 | 16 | moy 7.955 · méd 8.0 · min/max 0.0/15.0 · p10/p90 4.0/13.0 | Colonne (0-15) de la case d'ARRIVÉE (déplacements). NULL pour les tirs et les dribbles sans touche suivante. |
| `row_to` | INTEGER | 72.7% | 3310995 | 12 | moy 6.026 · méd 6.0 · min/max 0.0/11.0 · p10/p90 1.0/11.0 | Ligne (0-11) de la case d'arrivée (déplacements). NULL pour les tirs et les dribbles sans touche suivante. |