---
schema: intermediate
rows: 487368
---
# int_player_minutes

#intermediate

Minutes jouées par joueur et par match, dérivées de la timeline de formation WhoScored (int_whoscored_lineup) — Σ des durées de segments, minutes expanded (absolues). Base du per-90 en couche Gold. Garde-fous : match_id non NULL (exclut les orphelins D2), segments bornés 0..130 (exclut la sentinelle 32767), GREATEST(end-start,0).


## Intégrité
**Clé déclarée :** (match_id, team_id, player_id) — ✅ aucun doublon

## Lineage
**Sources :** [[int_whoscored_lineup]]
**Alimente :** [[player_match_stats]]

## Features & profiling  (487368 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `match_id` | VARCHAR | 100.0% | 0 | 16586 |  | Identifiant unifié du match (SHA1). |
| `team_id` | BIGINT | 100.0% | 0 | 175 |  | Id canonique de l'équipe du joueur pour ce match. |
| `player_id` | BIGINT | 100.0% | 0 | 8601 |  | Identifiant WhoScored du joueur. |
| `minutes_played` | HUGEINT | 100.0% | 0 | 116 | moy 70.508 · méd 90.0 · min/max 0.0/128.0 · p10/p90 16.0/96.0 | Minutes jouées dans le match (0..~128, temps additionnel inclus). 0 = présent mais non entré. Aucun plafond appliqué : le test ci-dessous ALERTE si la source redevient aberrante (sentinelle 32767), au lieu de la masquer. |