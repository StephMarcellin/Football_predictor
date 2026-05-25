# ADR-008 — Great Expectations pour la validation des données

## Statut
Accepté — 2026-05-18

## Contexte

Le pipeline Silver → Gold transforme des données scrapées en features ML.
Sans validation formelle, une corruption silencieuse (NULLs massifs, valeurs
inattendues, perte de volume) peut se propager jusqu'au modèle sans déclencher
aucune erreur Python. Le problème ne serait détecté qu'en analysant les
métriques MLflow, potentiellement plusieurs runs plus tard.

dbt dispose de tests natifs (`not_null`, `unique`, `accepted_values`) mais
ils s'exécutent après `dbt run` — trop tard pour bloquer la transformation
si les sources Silver sont déjà corrompues.

## Décision

Intégrer **Great Expectations** comme couche de validation des data contracts,
avec deux points de contrôle dans la chaîne Prefect :

1. **Après `process`, avant `dbt_run`** — valide la couche Silver
2. **Après `dbt_test`, avant `train`** — valide la couche Gold

Les deux étapes sont `critical=True` dans Prefect : une violation arrête
immédiatement le pipeline.

## Mode de fonctionnement choisi : `ephemeral`

GE propose trois modes :
- `file` — persiste la configuration dans un dossier `gx/` (JSON, YAML)
- `cloud` — configuration hébergée sur GE Cloud
- `ephemeral` — tout en mémoire, aucun fichier écrit sur le disque

**Choix : `ephemeral`**

Les règles de validation sont définies directement en Python et versionnées
dans Git avec le reste du code. Le mode `file` génère une arborescence de
dizaines de fichiers JSON qui seraient du bruit dans le repo pour un pipeline
solo. Le mode `ephemeral` est plus simple, plus lisible, et suffisant pour
ce cas d'usage.

Si le projet évoluait vers une équipe avec des data analysts non-développeurs
devant modifier les règles, la migration vers `file` ou `cloud` serait
justifiée.

## Séparation des responsabilités GE / dbt

| Outil | Périmètre | Moment |
|---|---|---|
| **Great Expectations** | Sources Silver (données scrapées) | Avant `dbt run` |
| **dbt tests** | Modèles Gold (données transformées) | Après `dbt run` |

dbt *peut* tester ses sources via le bloc `sources:` en YAML, mais ses tests
s'exécutent dans `dbt test` — après la transformation. GE intercepte *avant*,
ce qui permet un fail-fast plus tôt dans la chaîne.

Les deux outils sont complémentaires et ne se chevauchent pas.

## Règles implémentées

### Silver — `understat_schedule`
- `home_xg` et `away_xg` non-nulles à 80% minimum (matchs à venir exclus)
- `home_xg` et `away_xg` ≥ 0 (valeur physiquement impossible sinon)
- Volume > 5 000 lignes (détecte une perte massive de scraping)
- `season` dans l'ensemble des saisons connues

### Silver — `fbref_schedule`
- `result_1n2` contient uniquement `H`, `D`, `A`
- `date` et `team` jamais NULL

### Gold — `features_final`
- Volume > 30 000 lignes
- `result_1n2` contient uniquement `H`, `D`, `A`
- `match_id` jamais NULL
- Les 5 grands championnats toujours présents dans `league_source`
- `np_xg_roll_3` et `ppda_roll_3` non-nulles à 60% minimum (NULLs
  structurels en début de saison tolérés)

## Conséquences

**Positives**
- Toute corruption silencieuse des données est détectée avant d'atteindre
  le modèle
- Les règles sont versionnées dans Git, lisibles, modifiables sans toucher
  à l'orchestration
- Fail-fast garanti via l'intégration Prefect `critical=True`

**Négatives / Points de vigilance**
- Les seuils (`mostly=0.80`, `min_value=30000`) doivent être recalibrés
  si le volume de données change significativement (nouvelles saisons,
  nouveaux championnats)
- La saison `2025-2026` a été découverte lors de la calibration initiale —
  le `value_set` des saisons devra être mis à jour chaque année

## Alternatives considérées

**dbt tests sur les sources uniquement** — possible mais les tests s'exécutent
trop tard dans la chaîne pour un fail-fast efficace sur Silver.

**Pandera** — bibliothèque Python de validation de DataFrames, plus légère
que GE. Moins expressive pour les règles de volume et de distribution.
Envisageable si GE s'avérait trop lourd à maintenir.