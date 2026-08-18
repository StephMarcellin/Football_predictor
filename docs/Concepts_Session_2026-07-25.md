# Concepts — Session du 25 juillet 2026

> Thème : extension des features **intermediate** à partir des events WhoScored
> (partie B du plan). 6 nouveaux modèles + correction d'un bug de parsing amont.

---

## 1. Le qualifier 233 « OppositeRelatedEvent » — appariement déterministe

Un même fait de jeu laisse **plusieurs events** dans WhoScored, un par acteur/équipe.
Le qualifier **233** porte l'`event_id` de l'event **miroir de l'autre équipe**. Il
permet d'apparier ces events de façon **déterministe** (pas d'heuristique de temps) :

- **Penalty** : `PenaltyFaced` (type 58) → frappe via son qual 233 ; les deux côtés
  de la faute (obtient/concède) se pointent mutuellement.
- **Faute** : la faute subie (outcome 1) et commise (outcome 0) sont liées par 233.
- **Contre** : `BlockedPass` (type 74) → la passe bloquée via son qual 233.

Couverture mesurée : ~99–100 % là où le mécanisme s'applique.

## 2. PIÈGE MAJEUR — `event_id` scopé par (match, équipe)

`event_id` **n'est PAS unique par match** : **66,8 %** des `event_id` existent pour
les **deux** équipes (chaque équipe numérote ses events). Conséquence :

> **Tout join sur `event_id` (via qual 233) DOIT contraindre l'équipe adverse**,
> sinon il attrape le mauvais côté.

Bug réel trouvé : `int_fouls_drawn` attribuait le fautif à la mauvaise équipe dans
**0,6 %** des cas. Corrigé par `... AND committed_team <> drawing_team`.
`int_penalties` a été **durci** par la même garde (il était à 0 faux car les
penaltys sont rares, mais on protège les données futures).

Leçon : un contrôle « même équipe » qui compare `team_id` à un `player_id` est
**toujours faux** → il ne prouve rien. Toujours comparer deux `team_id`.

## 3. Appariement par RANG (`pen_seq`) — sans fenêtre de temps

Quand aucun lien direct n'existe entre deux grappes d'events (ex. faute → frappe du
penalty, ou sortie ↔ entrée d'un changement), on **n'utilise pas de fenêtre de
temps** (fragile aux événements successifs). On apparie par **rang** :

- Les faits d'une équipe arrivent dans l'ordre du `row_num`, et `#A == #B` par équipe
  (vérifié) → le **k-ième A ↔ le k-ième B**.
- `int_penalties` : k-ième tir ↔ k-ième faute de l'équipe.
- `int_substitutions` : k-ième sortie ↔ k-ième entrée dans le groupe (minute, sec).

Robuste aux penaltys / changements simultanés.

## 4. Bug de parsing `goal_mouth` / `blocked` (amont, corrigé)

`scrape_whoscored_details.py` lisait `ev.get("goalMouthY")` — mais `goalMouthY/Z`
(102/103) et `blockedX/Y` (146/147) **ne sont pas des champs d'event, ce sont des
qualifiers**. Résultat : colonne remplie à **26 %** alors que le qualifier est là à
**91 %**. Correction : helper `_qualifier_value()` + **COALESCE(qualifier,
top-level)** (les deux sources coexistent selon les saisons/ligues).

- `related_event_id` / `related_player_id`, eux, SONT des champs d'event → OK.
- Fix appliqué dans `parse_events` (utilisé par le scrape ET le re-parse d'archives).
- Récupération des matchs déjà en base : **re-scrape** (l'archi fetch/parse le permet).
  Décision : pas de backfill SQL, on laisse le re-scrape couvrir. Voir **ADR-009**.

Dénormalisation : ces colonnes pré-parsées sont une commodité ; la **source de
vérité** reste `qualifiers_json` (et sa version éclatée `events_qual`).

## 5. Pas d'event « carry » — conduites reconstruites via `TakeOn`

WhoScored/Opta **ne loggue aucun event de conduite balle au pied**. Le seul
événement de dribble est le **`TakeOn` (type 3)**. `int_progressive_carries` pivote
donc sur les `TakeOn` **réussis offensifs** (outcome 1 + qual 286). Le TakeOn étant
ponctuel (pas de `end_x`, pas de Length/Angle), la **progression** = position du
dribble → **prochaine touche du même joueur** dans la même chaîne, bornée à **5 s**
(anti-bruit). Progressive = gain ≥ **5 m** vers le but (distance en mètres :
x·1,05 / y·0,68, but adverse au centre (105, 34)).

## 6. Les 6 nouveaux modèles intermediate

| Modèle | Grain | Source clé | Idée |
|---|---|---|---|
| `int_penalties` | un penalty tiré | events_qual (9, 233), int_event_enriched | tireur, résultat, placement, gardien, obtenteur, fautif |
| `int_fouls_drawn` | une faute subie | int_whoscored_events, events_qual (233) | qui obtient / concède, tiers offensif, → penalty |
| `int_defensive_blocks` | un contre (passe) | type 74 BlockedPass, events_qual (233) | bloqueur nommé + passe bloquée |
| `int_shot_creating_actions` | un crédit SCA (tir×rang) | player_possession_chains | 2 actions avant le tir (SCA/GCA) |
| `int_progressive_carries` | un dribble réussi | player_possession_chains, events_qual (286) | conduite via TakeOn, progression en m |
| `int_substitutions` | un remplacement | int_whoscored_events (18/19) | sortie ↔ entrée par rang, timing |

`int_shot_placement` a été converti en **incrémental** au passage.

## 7. Restant (session dédiée modélisation)

- `int_keeper_psxg` — bloqué : nécessite un modèle **xGOT** (post-shot xG) entraîné.
  Gardien à identifier via `int_whoscored_lineup` (slot 1), pas via les events Save.
- `int_xt_grid` — **expected threat** (grille Karun Singh) : estimation à part.
