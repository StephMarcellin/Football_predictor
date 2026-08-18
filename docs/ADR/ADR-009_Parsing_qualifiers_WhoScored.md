# ADR-009 — Parsing des qualifiers WhoScored : source de vérité et point de correction

## Statut

Adopté (25 juillet 2026)

## Contexte

Les events WhoScored portent deux types d'information :

- des **champs de haut niveau** de l'objet event (`teamId`, `x`, `y`, `relatedEventId`,
  `relatedPlayerId`, `isShot`…) ;
- un tableau de **qualifiers** (`qualifiers`), chacun étant `{type: {value, displayName}, value}`.

La session du 22 juillet avait ajouté 9 colonnes dénormalisées à
`silver.stg_whoscored_events` (`goal_mouth_y/z`, `blocked_x/y`, `card_type`,
`related_*`, `is_goal`, `is_own_goal`) pour éviter de désempaqueter le JSON en aval.

Un bug a été découvert : `goal_mouth_y/z` et `blocked_x/y` n'étaient remplis qu'à
**26 %** des tirs, alors que le qualifier correspondant (102/103/146/147) est présent
à **91 %**. Cause : `parse_events` lisait `ev.get("goalMouthY")` — or **ce ne sont
pas des champs d'event mais des qualifiers**. La donnée était bien là (dans
`qualifiers_json`), mais l'extraction la ratait. WhoScored n'est pas en cause.

Nuance : selon les saisons/ligues, la valeur existe tantôt au niveau event, tantôt
seulement dans les qualifiers → il faut **les deux sources**.

### Options considérées

| Option | Profil |
|---|---|
| **Corriger le parser Python + re-parser les archives** | Racine du problème ; l'archi fetch/parse (archivage gzip) permet de re-parser sans re-scraper |
| Dériver en dbt dans `int_event_enriched` (depuis `events_qual`) | Corrige les consommateurs sans toucher silver, mais laisse la colonne canonique fausse |
| Pivoter tous les qualifiers en colonnes dans `int_whoscored_events` | Table très large et creuse (50+ colonnes NULL) ; dépendance circulaire avec `events_qual` ; rejeté |
| Backfill SQL sur silver depuis `qualifiers_json` | Corrige tout de suite tous les matchs, mais écriture manuelle sur la donnée canonique |

## Décision

1. **Source de vérité = `qualifiers_json`** (et sa version éclatée `events_qual`).
   Les colonnes dénormalisées restent une commodité, pas la référence.

2. **Correction à la racine, dans `parse_events`** (`scrape_whoscored_details.py`) :
   helper `_qualifier_value(qualifiers, type_id)` + **`COALESCE(qualifier, champ
   top-level)`** pour `goal_mouth_y/z` (102/103) et `blocked_x/y` (146/147).
   Cette fonction étant partagée, le fix couvre le scrape live ET le re-parse
   d'archives (`load_whoscored_archive.py`).

3. **Pas de backfill SQL sur silver.** Les matchs déjà en base sans archive seront
   corrigés au fil du **re-scrape** (le backfill nocturne finit par archiver tous
   les matchs). Choix motivé par la volonté de ne pas écrire manuellement sur la
   donnée canonique et par le fait que l'architecture d'archivage est faite pour ça.

4. **Règle de join `event_id`** (corollaire, voir Concepts_Session_25juillet) :
   `event_id` est scopé par (match, équipe) — tout join via qual 233 **doit
   contraindre l'équipe adverse**.

## Conséquences

- **Positif** : la couche silver redevient correcte à mesure du re-scrape ; aucun
  modèle dbt aval à modifier ; la règle « qualifiers_json = vérité » clarifie où
  chercher une info manquante.
- **Négatif / à surveiller** : tant que le re-scrape n'a pas couvert 100 % des
  matchs, `goal_mouth`/`blocked` restent NULL pour les matchs non ré-archivés →
  `int_shot_placement` et `int_penalties` ont un placement partiel sur ces matchs.
- **Contrôle** : requête de couverture `goal_mouth` (colonne remplie vs qualifier
  présent) à relancer après chaque vague de re-scrape.
