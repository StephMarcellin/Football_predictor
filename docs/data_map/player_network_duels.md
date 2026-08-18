---
schema: intermediate
rows: 1758901
---
# player_network_duels

#intermediate

Apparie les duels adverses via le qualifier 233 (miroir d'événement) pour relier le joueur A et son vis-à-vis B sur le même affrontement. Matière première des duels individuels. Sources : events_qual, int_event_enriched.


## Intégrité
**Clé déclarée :** (match_id, event_id_a) — ⚠️ **48692 doublons**

## Lineage
**Sources :** [[events_qual]], [[int_event_enriched]]
**Alimente :** [[int_player_zone_season]]

## Features & profiling  (1758901 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 16572 |  | Identifiant unifié du match (SHA1). |
| `season` | VARCHAR | 100.0% | 0 | 8 | top: 2017-2018 (305609), 2018-2019 (234990), 2020-2021 (216652) | Saison du match (ex. 2023-2024). |
| `league_source` | VARCHAR | 100.0% | 0 | 7 | top: La Liga (315641), Premier League (309466), Ligue 1 (296603) | Championnat / source des données (ex. Bundesliga). |
| `expanded_minute` | INTEGER | 100.0% | 0 | 128 | moy 48.898 · méd 49.0 · min/max 0.0/129.0 · p10/p90 9.0/89.0 | Minute cumulée du duel (temps additionnel inclus). |
| `second` | INTEGER | 100.0% | 0 | 60 | moy 29.423 · méd 29.0 · min/max 0.0/59.0 · p10/p90 6.0/53.0 | Seconde dans la minute. |
| `duel_type_id` | INTEGER | 100.0% | 0 | 6 |  | type_id de l'événement de duel (côté A) — ex. Aerial, Tackle, Foul, Dispossessed. |
| `duel_type` | VARCHAR | 100.0% | 0 | 6 | top: Aerial (535166), Foul (389858), TakeOn (315756) | Libellé du type de duel (type_name côté A). |
| `x` | DOUBLE | 100.0% | 0 | 1001 | moy 52.098 · méd 54.0 · min/max 0.0/100.0 · p10/p90 19.8/86.0 | Position X du duel sur le terrain (côté A, repère de l'équipe A ; 0-100). |
| `y` | DOUBLE | 100.0% | 0 | 1001 | moy 50.23 · méd 50.3 · min/max 0.0/100.0 · p10/p90 8.9/91.3 | Position Y du duel (côté A ; 0-100, 50 = axe central). |
| `event_id_a` | INTEGER | 100.0% | 0 | 1542 | moy 467.51 · méd 441.0 · min/max 3.0/1000729.0 · p10/p90 94.0/795.0 | event_id WhoScored du côté A du duel. |
| `team_id_a` | BIGINT | 100.0% | 0 | 175 | moy 202606100213.711 · méd 202606100213.0 · min/max 202606100001.0/202606100426.0 · p10/p90 202606100043.0/202606100393.0 | Id de l'équipe du côté A. |
| `player_id_a` | INTEGER | 100.0% | 0 | 8174 | moy 215871.917 · méd 227655.0 · min/max 0.0/561794.0 · p10/p90 37099.0/398374.0 | Identifiant du joueur du côté A. |
| `outcome_a` | INTEGER | 100.0% | 0 | 2 | moy 0.57 · méd 1.0 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | Résultat du côté A (1 = réussi, 0 = raté). |
| `event_id_b` | INTEGER | 100.0% | 0 | 1894 | moy 492.033 · méd 458.0 · min/max 2.0/1000735.0 · p10/p90 99.0/828.0 | event_id du côté B, adverse, apparié via OppositeRelatedEvent (qualifier 233). |
| `team_id_b` | BIGINT | 100.0% | 0 | 175 | moy 202606100213.889 · méd 202606100216.0 · min/max 202606100001.0/202606100426.0 · p10/p90 202606100042.0/202606100392.0 | Id de l'équipe du côté B (adverse à A). |
| `player_id_b` | INTEGER | 100.0% | 0 | 8146 | moy 213211.169 · méd 146634.0 · min/max 0.0/561794.0 · p10/p90 35549.0/398301.0 | Identifiant du joueur du côté B. |
| `outcome_b` | INTEGER | 100.0% | 0 | 2 | moy 0.484 · méd 0.0 · min/max 0.0/1.0 · p10/p90 0.0/1.0 | Résultat du côté B (1 = réussi, 0 = raté). |