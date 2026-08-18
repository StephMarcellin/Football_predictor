---
schema: intermediate
rows: 1747026
---
# int_whoscored_lineup

#intermediate

Composition positionnelle par période de formation — grain (match × équipe × formation_seq × joueur), une ligne par titulaire (slot 1..11). La position est récupérée via le slot du joueur (formation_positions[slot]), pas par alignement d'index : robuste aux trous de slot (carton rouge). Identités normalisées (team_id canonique + match_id unifié via int_whoscored_match_index). Source : silver.stg_whoscored_formations (prérequis : formation_slots rempli).


## Intégrité
**Clé déclarée :** (match_id, team_id, formation_seq, player_id) — ⚠️ **22023 doublons**

## Lineage
**Sources :** [[int_whoscored_match_index]], [[silver.stg_whoscored_formations]]
**Alimente :** [[equipe_lineup_match]], [[int_keeper_shots]], [[int_player_minutes]], [[int_whoscored_lineup_agg]], [[joueur_match]], [[team_corridor_profile]]

## Features & profiling  (1747026 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 98.2% | 30781 | 16586 |  | Identifiant unifié du match (SHA1) |
| `team_id` | BIGINT | 99.2% | 13567 | 175 |  | Id canonique de l'équipe (converti depuis l'id WhoScored) |
| `formation_seq` | INTEGER | 100.0% | 0 | 11 | moy 1.996 · méd 2.0 · min/max 0.0/10.0 · p10/p90 0.0/4.0 | Numéro de séquence de la formation dans le match (chaque changement de formation incrémente la séquence). |
| `formation_id` | INTEGER | 100.0% | 0 | 23 |  | Identifiant WhoScored du schéma tactique (formation, ex. 442, 433). |
| `period` | INTEGER | 100.0% | 0 | 4 | moy 10.745 · méd 16.0 · min/max 1.0/16.0 · p10/p90 2.0/16.0 | Période du match de la séquence : 1 = 1re mi-temps, 2 = 2e mi-temps, 16 = pré-match, 14 = fin de match. |
| `start_minute` | INTEGER | 100.0% | 0 | 113 | moy 56.733 · méd 69.0 · min/max 0.0/112.0 · p10/p90 0.0/88.0 | Minute de début de la séquence de formation. |
| `end_minute` | INTEGER | 100.0% | 0 | 114 | moy 76.739 · méd 79.0 · min/max 0.0/118.0 · p10/p90 56.0/94.0 | Minute de fin de la séquence de formation. |
| `player_id` | BIGINT | 100.0% | 0 | 8718 |  | Identifiant WhoScored du joueur titulaire |
| `slot` | INTEGER | 100.0% | 0 | 11 | moy 5.991 · méd 6.0 · min/max 1.0/11.0 · p10/p90 2.0/10.0 | Slot de formation 1..11 (1 = gardien) |
| `grid_vertical` | DOUBLE | 100.0% | 0 | 13 | moy 4.637 · méd 5.0 · min/max 0.0/9.0 · p10/p90 2.0/9.0 | Coordonnée verticale sur la grille tactique (0 = ligne de but propre) |
| `grid_horizontal` | DOUBLE | 100.0% | 0 | 15 | moy 5.0 · méd 5.0 · min/max 1.0/9.0 · p10/p90 1.0/9.0 | Coordonnée horizontale sur la grille tactique (5 = axe central) |
| `is_captain` | BOOLEAN | 100.0% | 209 | 2 | top: False (1594782), True (152035) | Vrai si ce joueur est le capitaine sur cette période |