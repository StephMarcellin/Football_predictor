---
schema: intermediate
rows: 658510
---
# int_whoscored_player_match

#intermediate

Stats + note WhoScored par joueur par match, identités normalisées. Traduit l'id d'équipe WhoScored vers le team_id canonique et rattache le match_id unifié via int_whoscored_match_index (même logique que int_whoscored_events). player_id reste l'id WhoScored (clé vers silver.stg_whoscored_players_ref). Source : silver.stg_whoscored_player_match.


## Intégrité
**Clé déclarée :** — (pas de test d'unicité)

## Lineage
**Sources :** [[int_whoscored_match_index]], [[silver.stg_whoscored_player_match]]
**Alimente :** [[int_whoscored_players]]

## Features & profiling  (658510 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 98.3% | 11100 | 16188 |  | Identifiant unifié du match (SHA1) |
| `team_id` | BIGINT | 99.3% | 4850 | 174 |  | Id canonique de l'équipe (converti depuis l'id WhoScored) |
| `player_id` | INTEGER | 100.0% | 0 | 10916 |  | Identifiant WhoScored du joueur (FK vers stg_whoscored_players_ref) |
| `shirt_no` | INTEGER | 100.0% | 0 | 99 | moy 18.819 · méd 17.0 · min/max 1.0/99.0 · p10/p90 4.0/34.0 | Numéro de maillot du joueur. |
| `position` | VARCHAR | 100.0% | 0 | 17 |  | Position WhoScored du joueur (ex. GK, DC, DR/DL, DMC/DML, MC/ML, AMR/AML, FW/FWL/FWR, Sub). |
| `is_first_eleven` | BOOLEAN | 55.0% | 296087 | 1 | top: True (362423) | Booléen : le joueur est titulaire. |
| `is_man_of_the_match` | BOOLEAN | 100.0% | 0 | 2 | top: False (642037), True (16473) | Booléen : le joueur est l'homme du match. |
| `height` | INTEGER | 100.0% | 0 | 48 | moy 182.377 · méd 183.0 · min/max 0.0/206.0 · p10/p90 174.0/191.0 | Taille du joueur (cm). |
| `weight` | INTEGER | 100.0% | 0 | 55 | moy 76.235 · méd 77.0 · min/max 0.0/105.0 · p10/p90 68.0/86.0 | Poids du joueur (kg). |
| `age` | INTEGER | 100.0% | 0 | 34 | moy 31.177 · méd 31.0 · min/max 0.0/49.0 · p10/p90 25.0/38.0 | Âge du joueur (années). |
| `rating` | DOUBLE | 73.7% | 173475 | 624 | moy 6.668 · méd 6.55 · min/max 3.29/10.0 · p10/p90 5.97/7.58 | Note WhoScored finale du joueur sur le match |
| `stats_json` | VARCHAR | 100.0% | 0 | 483057 |  | Stats du joueur par minute (JSON brut) — totaux calculés plus tard |
| `touches` | DOUBLE | 100.0% | 0 | 190 | moy 31.817 · méd 29.0 · min/max 0.0/206.0 · p10/p90 0.0/72.0 | Nombre de touches de balle. |
| `possession` | DOUBLE | 100.0% | 0 | 180 | moy 23.292 · méd 19.0 · min/max 0.0/202.0 · p10/p90 0.0/56.0 | Nombre de possessions de balle du joueur (comptage WhoScored, pas un pourcentage). |
| `passes_total` | DOUBLE | 100.0% | 0 | 180 | moy 22.162 · méd 18.0 · min/max 0.0/202.0 · p10/p90 0.0/54.0 | Passes tentées. |
| `passes_accurate` | DOUBLE | 100.0% | 0 | 172 | moy 17.796 · méd 12.0 · min/max 0.0/195.0 · p10/p90 0.0/45.0 | Passes réussies. |
| `passes_key` | DOUBLE | 100.0% | 0 | 15 | moy 0.469 · méd 0.0 · min/max 0.0/14.0 · p10/p90 0.0/2.0 | Passes clés (menant à un tir). |
| `shots_total` | DOUBLE | 100.0% | 0 | 15 | moy 0.634 · méd 0.0 · min/max 0.0/14.0 · p10/p90 0.0/2.0 | Tirs tentés. |
| `shots_on_target` | DOUBLE | 100.0% | 0 | 10 | moy 0.219 · méd 0.0 · min/max 0.0/10.0 · p10/p90 0.0/1.0 | Tirs cadrés. |
| `shots_off_target` | DOUBLE | 100.0% | 0 | 9 | moy 0.254 · méd 0.0 · min/max 0.0/8.0 · p10/p90 0.0/1.0 | Tirs non cadrés. |
| `shots_blocked` | DOUBLE | 100.0% | 0 | 7 | moy 0.162 · méd 0.0 · min/max 0.0/6.0 · p10/p90 0.0/1.0 | Tirs contrés. |
| `shots_on_post` | DOUBLE | 100.0% | 0 | 4 | moy 0.012 · méd 0.0 · min/max 0.0/3.0 · p10/p90 0.0/0.0 | Tirs sur le poteau/la barre. |
| `dribbles_attempted` | DOUBLE | 100.0% | 0 | 24 | moy 0.829 · méd 0.0 · min/max 0.0/23.0 · p10/p90 0.0/3.0 | Dribbles tentés. |
| `dribbles_won` | DOUBLE | 100.0% | 0 | 17 | moy 0.439 · méd 0.0 · min/max 0.0/16.0 · p10/p90 0.0/2.0 | Dribbles réussis. |
| `dribbles_lost` | DOUBLE | 100.0% | 0 | 15 | moy 0.39 · méd 0.0 · min/max 0.0/15.0 · p10/p90 0.0/1.0 | Dribbles ratés. |
| `dribbled_past` | DOUBLE | 100.0% | 0 | 12 | moy 0.439 · méd 0.0 · min/max 0.0/12.0 · p10/p90 0.0/2.0 | Nombre de fois où le joueur s'est fait éliminer par un dribble adverse. |
| `dispossessed` | DOUBLE | 100.0% | 0 | 13 | moy 0.462 · méd 0.0 · min/max 0.0/13.0 · p10/p90 0.0/2.0 | Ballons perdus (dépossessions). |
| `tackles_total` | DOUBLE | 100.0% | 0 | 18 | moy 1.261 · méd 0.0 · min/max 0.0/17.0 · p10/p90 0.0/4.0 | Tacles tentés. |
| `tackle_successful` | DOUBLE | 100.0% | 0 | 15 | moy 0.822 · méd 0.0 · min/max 0.0/14.0 · p10/p90 0.0/3.0 | Tacles réussis. |
| `tackle_unsuccesful` | DOUBLE | 100.0% | 0 | 12 | moy 0.439 · méd 0.0 · min/max 0.0/12.0 · p10/p90 0.0/2.0 | Tacles ratés (orthographe de colonne conservée : 'unsuccesful'). |
| `interceptions` | DOUBLE | 100.0% | 0 | 13 | moy 0.509 · méd 0.0 · min/max 0.0/13.0 · p10/p90 0.0/2.0 | Interceptions. |
| `clearances` | DOUBLE | 100.0% | 0 | 25 | moy 0.991 · méd 0.0 · min/max 0.0/24.0 · p10/p90 0.0/3.0 | Dégagements. |
| `aerials_total` | DOUBLE | 100.0% | 0 | 37 | moy 1.668 · méd 1.0 · min/max 0.0/37.0 · p10/p90 0.0/5.0 | Duels aériens disputés. |
| `aerials_won` | DOUBLE | 100.0% | 0 | 25 | moy 0.834 · méd 0.0 · min/max 0.0/24.0 · p10/p90 0.0/3.0 | Duels aériens gagnés. |
| `offensive_aerials` | DOUBLE | 100.0% | 0 | 34 | moy 0.834 · méd 0.0 · min/max 0.0/35.0 · p10/p90 0.0/2.0 | Duels aériens offensifs. |
| `defensive_aerials` | DOUBLE | 100.0% | 0 | 24 | moy 0.834 · méd 0.0 · min/max 0.0/25.0 · p10/p90 0.0/3.0 | Duels aériens défensifs. |
| `fouls_commited` | DOUBLE | 100.0% | 0 | 10 | moy 0.619 · méd 0.0 · min/max 0.0/9.0 · p10/p90 0.0/2.0 | Fautes commises (orthographe de colonne conservée : 'commited'). |
| `offsides_caught` | DOUBLE | 100.0% | 0 | 9 | moy 0.093 · méd 0.0 · min/max 0.0/8.0 · p10/p90 0.0/0.0 | Nombre de fois où le joueur a été pris hors-jeu. |
| `errors` | DOUBLE | 100.0% | 0 | 4 | moy 0.019 · méd 0.0 · min/max 0.0/3.0 · p10/p90 0.0/0.0 | Erreurs menant à un tir/but. |
| `corners_total` | DOUBLE | 100.0% | 0 | 9 | moy 0.248 · méd 0.0 · min/max 0.0/8.0 · p10/p90 0.0/1.0 | Corners tirés. |
| `corners_accurate` | DOUBLE | 100.0% | 0 | 12 | moy 0.109 · méd 0.0 · min/max 0.0/11.0 · p10/p90 0.0/0.0 | Corners réussis (trouvant un partenaire). |
| `throw_ins_total` | DOUBLE | 100.0% | 0 | 31 | moy 1.045 · méd 0.0 · min/max 0.0/30.0 · p10/p90 0.0/4.0 | Touches (remises en jeu) effectuées. |
| `throw_ins_accurate` | DOUBLE | 100.0% | 0 | 28 | moy 0.873 · méd 0.0 · min/max 0.0/28.0 · p10/p90 0.0/3.0 | Touches réussies. |
| `total_saves` | DOUBLE | 100.0% | 0 | 18 | moy 0.149 · méd 0.0 · min/max 0.0/19.0 · p10/p90 0.0/0.0 | Arrêts (gardien). |
| `parried_safe` | DOUBLE | 100.0% | 0 | 9 | moy 0.057 · méd 0.0 · min/max 0.0/8.0 · p10/p90 0.0/0.0 | Ballons repoussés en zone sûre (gardien). |
| `parried_danger` | DOUBLE | 100.0% | 0 | 9 | moy 0.031 · méd 0.0 · min/max 0.0/8.0 · p10/p90 0.0/0.0 | Ballons repoussés en zone dangereuse (gardien). |
| `claims_high` | DOUBLE | 100.0% | 0 | 10 | moy 0.032 · méd 0.0 · min/max 0.0/9.0 · p10/p90 0.0/0.0 | Sorties aériennes captées (gardien). |
| `collected` | DOUBLE | 100.0% | 0 | 9 | moy 0.048 · méd 0.0 · min/max 0.0/8.0 · p10/p90 0.0/0.0 | Ballons captés/récupérés (gardien). |