---
name: data_test_generator
description: >
  Génère la suite de tests complète (dbt structure + Great Expectations signal + pytest logique)
  pour n'importe quelle table du warehouse DuckDB du Projet 3-Étoiles. Respecte strictement la
  répartition dbt=structure / GE=signal / pytest=code, la contrainte spatiotemporelle WhoScored/
  Understat/Odds (Big5 domestique uniquement) et l'exclusion de la saison en cours dans les
  tests de dérive volumétrique.

  Déclencher quand l'utilisateur demande de "générer les tests", "sécuriser la table X", "ajouter
  les checks DQ", "couvrir X en tests", "appliquer le générateur de tests", ou toute variante.
---

# data_test_generator — v2

Skill autoritaire pour produire la couverture Data Quality **complète, non-redondante,
reproductible** sur n'importe quelle table du warehouse (silver, intermediate, gold, marts, ml)
et sur les seeds. Réécrit après les apprentissages issus de team_mapping, int_fbref_keeper et
scraping_targets_int_fbref_keeper.

## 1. Charte (loi du skill — ne jamais transgresser)

| Outil | Domaine exclusif | Signature dbt/YAML |
|---|---|---|
| **dbt tests** | STRUCTURE | `not_null`, `unique`, `dbt_utils.unique_combination_of_columns`, `relationships`, `accepted_values`, `is_positive`, `matches_regex`, `one_value_per_key` (custom), `no_suspicious_similar_names` (custom), **`dbt_utils.expression_is_true` pour contraintes physiques métier** (ex: `ga_keeper <= sota`). S'applique aussi aux SEEDS. |
| **Great Expectations** | SIGNAL statistique | Taux max de NULL (`mostly`), taux max de zéros (`zero_ratio`), bornes stat (`mean_to_be_between`), bornes de valeurs (`values_to_be_between`), dérive volumétrique (`partition_volume_stability`), cohérence croisée (`row_condition_to_hold`). |
| **pytest** | LOGIQUE Python | Uniquement sur code Python complexe (`02_process.py`, `pipelines/03_knn_impute.py`, `pipelines/xgot_score.py`, PyTorch/NumPy). **Bridge pytest → GE** TOUJOURS créé, même sans logique métier — rend la suite visible dans la CI. |

**Aucun chevauchement, avec UNE exception documentée** : les contraintes physiques métier critiques
(`ga_keeper <= sota`, `saves + goals ≤ shots`, etc.) sont testées **aux DEUX endroits** —
en dbt (bloque la matérialisation) ET en GE (rapporte à chaque run indépendamment de dbt).
Justification : dbt garantit à la matérialisation, GE garantit à l'exécution post-run.

## 2. Contraintes de gouvernance (règles métier immuables)

### 2.1 Contrainte spatiotemporelle Big5

Les colonnes issues de **Understat**, **WhoScored**, **Pinnacle Odds** ne sont peuplées que pour
`comp_category = 'Big5'` (5 grands championnats domestiques). Pour les autres compétitions
(Cup, D2, Europe, Other) → **100 % NULL par design, pas un bug**.

Conséquence : deux stratégies possibles pour les tests GE, à choisir selon la table :
- **Suite unique** si toutes les colonnes sont universelles (ex: `int_fbref_keeper` — FBref couvre toutes les compétitions)
- **Suite segmentée** en 2 fichiers :
  - `<table>_all.yml` : colonnes universelles, testée sur toute la table
  - `<table>_big5.yml` : colonnes Big5-only, testée avec `WHERE comp_category = 'Big5'` dans la query source

**Détection automatique des colonnes Big5-only** :
- Nom contient `xg`, `ppda`, `deep`, `xpts`, `ws_`, `rating`, `att_rating`, `def_rating`, `dribbles_pg`, `fouled_pg`, `shots_ot_pg`, `xt_`, `zone_5x5`, `corridor`, `matchup`
- OU provient d'un modèle qui `ref('*understat*')` / `ref('*whoscored*')` / `source('silver', '*understat*')` / `source('silver', 'stg_whoscored*')` / `source('silver', 'odds')`

### 2.2 Scraping en cours

La saison **la plus récente** est en cours de scraping (2025-2026 à la date d'écriture). Le test
`expect_partition_volume_stability` **doit exclure cette saison** via
`exclude_partitions: ['<saison courante>']` — sinon faux positifs systématiques.

Détection : sélectionner la saison la plus élevée dans `SELECT DISTINCT season FROM <table>`
et l'exclure automatiquement.

### 2.3 Rescrap ciblé et modèles d'audit

Quand une contrainte physique métier révèle des données incohérentes en **silver** (ex:
`ga_keeper > sota` dans `silver.fbref_keeper`), créer un **modèle d'audit**
`models/audits/scraping_targets_<table>.sql` :
- Source : **`silver.<table>`** (JAMAIS l'intermediate)
- Colonnes : **`team, season`** seulement (grain que le scraper attend)
- Matérialisation : `view` (se met à jour à chaque `dbt run`, suit la dette en temps réel)

## 3. Procédure — 6 étapes strictes, dans cet ordre

### Étape 1 — Découverte documentaire (obligatoire, non skippable)

Lire le bloc `- name: <table>` correspondant :
- Modèles : `dbt_project/models/<layer>/schema.yml`
- Sources silver : `dbt_project/models/sources.yml`
- Seeds : `dbt_project/seeds/schema.yml`

Extraire :
- Description métier (`description:`)
- Grain candidat (`- name:` avec description "Identifiant unifié" ou similaire)
- Sources upstream (via description : "reprise de X", "jointure de X + Y")
- Tests déjà présents (niveau table et niveau colonne)
- Contraintes NULL documentées ("NULL toléré si...", "NULL si match hors Understat")

**Produire un mini-rapport de découverte AVANT l'audit empirique.**

### Étape 2 — Audit empirique DuckDB

```python
import duckdb
con = duckdb.connect("db/football.duckdb", read_only=True)

# Colonnes réelles
cols = con.execute(f"PRAGMA table_info('{schema}.{table}')").fetchall()

# Row count et saisons
n = con.execute(f"SELECT COUNT(*) FROM {schema}.{table}").fetchone()[0]
if any(c[1] == 'season' for c in cols):
    seasons = con.execute(f"SELECT DISTINCT season FROM {schema}.{table} ORDER BY season").fetchall()

# NULL rate par colonne + min/max/avg/stddev pour les numériques
# → SEGMENTATION par comp_category si détecté

# Grain candidat : test unicité
# → si duplication, remonter à silver pour trouver la racine

# Valeurs distinctes des catégorielles
```

**Focaliser sur** :
- Colonnes présentes en base mais **absentes du schema.yml** → à documenter
- Colonnes absentes en base mais présentes dans schema.yml → doc obsolète
- Duplications sur le grain candidat
- Anomalies grossières (négatifs sur métrique positive, valeurs impossibles)
- Taux NULL par famille (avec segmentation Big5 si applicable)

### Étape 3 — Différentiel et classification

| Type détecté | Critère | Tests dbt automatiques | Tests GE automatiques |
|---|---|---|---|
| **Clé primaire** | 100 % unique, 0 % NULL | `not_null`, `unique` | — |
| **Grain composite** | Combo unique déclaré ou détecté | `dbt_utils.unique_combination_of_columns` | — |
| **Clé étrangère** | Nom `*_id` avec table cible (`match_id`, `team_id`, `player_id`) | `relationships` | — |
| **Catégorielle bornée** | ≤ 20 valeurs distinctes, string | `accepted_values` | — |
| **Temporelle** | DATE, TIMESTAMP, ou nom `season` | `not_null` sur les critiques + `matches_regex` sur season | dérive volumétrique (excluant saison courante) |
| **Numérique positive** | INTEGER/DOUBLE hors clés | `is_positive` | taux max NULL, taux max zéros, bornes stats |
| **Numérique signée** | Peut être négatif (ex: diff xG) | — | taux max NULL, bornes stats symétriques |
| **Booléenne** | BOOLEAN ou 0/1 | `accepted_values: [0, 1]` | taux max NULL |
| **Contrainte physique** | Règle métier vérifiable en SQL (`a <= b + c`) | `dbt_utils.expression_is_true` | `expect_row_condition_to_hold` (doublon volontaire) |
| **Texte libre** | VARCHAR non catégoriel (nom, url) | `not_null` si structurellement requis | — |

### Étape 4 — Écriture des tests dbt (STRUCTURE)

Localisation : `dbt_project/models/<layer>/schema.yml` — **éditer** le bloc existant, ne
jamais dupliquer.

```yaml
- name: <table_name>
  description: '<garder la description existante>'
  tests:
    - dbt_utils.unique_combination_of_columns:
        combination_of_columns: [<pk_col_1>, <pk_col_2>]
        config: { severity: warn }   # warn tant que la seed team_mapping n'est pas nettoyée
    - dbt_utils.expression_is_true:
        expression: "<contrainte physique>"
        config: { severity: error }
  columns:
    - name: <pk_col>
      tests:
        - not_null
        - relationships:
            to: source('intermediate', 'match_registry')
            field: match_id
    - name: <fk_team>
      tests:
        - not_null
        - relationships:
            to: ref('team_mapping')
            field: team_id
    - name: <categorical>
      tests:
        - accepted_values:
            arguments:
              values: [<...>]
    - name: <numeric_positive>
      tests:
        - not_null
        - is_positive
    - name: season
      tests:
        - not_null
        - matches_regex:
            arguments:
              pattern: '^\d{4}-\d{4}$'
```

**Format `arguments:`** obligatoire pour les tests avec paramètres (dbt 1.9+ style projet).

### Étape 5 — Écriture de la suite GE (SIGNAL)

Localisation : `tests/great_expectations/<schema>/<table>[_all|_big5].yml`.

Format YAML déclaratif, lu par `tests/great_expectations/runner.py` (pandas-natif,
Windows-friendly, sans dépendance à `great_expectations`).

**Structure canonique** :

```yaml
suite_name: <schema>.<table>
description: >
  <une phrase>

source:
  duckdb_path: db/football.duckdb
  query: |
    SELECT <colonnes utiles>
    FROM <schema>.<table>
    -- WHERE comp_category = 'Big5'   -- si suite _big5.yml

expectations:

  # 1. Volumétrie
  - type: expect_table_row_count_to_be_between
    kwargs: { min_value: <empirique × 0.7>, max_value: <empirique × 1.5> }
    severity: error

  # 2. Complétion NULL (empirique + 5 pts)
  - type: expect_column_values_to_not_be_null
    for_each_column: [<colonnes 0% NULL empirique>]
    kwargs: { mostly: 1.0 }
    severity: error
  - type: expect_column_values_to_not_be_null
    for_each_column: [<colonnes avec NULL empirique X%>]
    kwargs: { mostly: <1 - (X + 0.05)> }
    severity: error   # ou warn si NULL est légitime (ex: save_pct quand sota=0)

  # 3. Bornes de valeurs (attrape les anomalies)
  - type: expect_column_values_to_be_between
    for_each_column: [<colonne borne stricte>]
    kwargs: { min_value: <min_physique>, max_value: <max_physique>, mostly: 1.0 }
    severity: error

  # 4. Bornes statistiques (mean empirique ± 25%)
  - type: expect_column_mean_to_be_between
    for_each_column: [<colonne>]
    kwargs: { min_value: <mean_emp × 0.75>, max_value: <mean_emp × 1.25> }
    severity: error

  # 5. Cohérence croisée (contraintes métier)
  - type: expect_row_condition_to_hold
    kwargs: { condition: "<expression pandas>" }
    severity: error

  # 6. Dérive volumétrique — EXCLURE la saison courante
  - type: expect_partition_volume_stability
    kwargs:
      partition_col: season
      max_deviation_warn: 0.20
      max_deviation_error: 0.40
      min_seasons_history: 2
      exclude_partitions: ['<saison courante>']
    severity: warn
```

**Seuils par défaut à défendre** (à ajuster empiriquement) :
- Taux NULL critique : **empirique + 5 pts** (mostly = 1 − (empirique + 0.05))
- Bornes mean : **empirique × [0.75, 1.25]** — laisse assez de latitude pour l'évolution
- Dérive volumétrique : **±20 % warn**, **±40 % error** — hors saison courante

### Étape 6 — Bridge pytest et modèle d'audit

**Bridge pytest** (toujours créé) — `tests/unit/test_<table>_ge.py` :

```python
from pathlib import Path
import pytest
from tests.great_expectations.runner import run_suite

SUITE_PATH = Path(__file__).resolve().parents[1] / "great_expectations" / "<layer>" / "<table>.yml"

def test_ge_suite_<table>():
    if not SUITE_PATH.exists():
        pytest.skip(f"Suite YAML absente : {SUITE_PATH}")
    result = run_suite(SUITE_PATH)
    assert result.success, "\n" + result.report()
```

**Modèle d'audit** (uniquement si contrainte physique violée détectée) —
`dbt_project/models/audits/scraping_targets_<table>.sql` :

```sql
{{ config(materialized='view', schema='audits') }}

-- Cible : (team, season) à rescraper — silver.<table> lignes en violation physique.

SELECT DISTINCT team, season
FROM {{ source('silver', '<table>') }}
WHERE <contrainte violée>
ORDER BY season DESC, team
```

Documenter dans `dbt_project/models/audits/schema.yml` (ajouter un bloc `- name:`).

## 4. Rapport de livraison

Après chaque table, produire dans le journal `AUTONOMY_RUN.md` :

```
## <schema>.<table> — <date>

Découvertes doc :
  - <N> colonnes documentées / <M> présentes en base — <écart> non documentées : [liste]
  - Grain candidat : <combo> — <unique | X doublons>
  - Segmentation Big5 : <oui/non — justification>

Découvertes empiriques :
  - Row count : <N>
  - NULL rate max : <X> % sur <colonne>
  - Anomalies : [liste avec quantification]

Livrables :
  - dbt tests structure : <N> ajoutés
  - Suite GE : 1 fichier | 2 fichiers segmentés
  - Bridge pytest : ✓
  - Modèle d'audit : oui/non (justification)

Contrainte spatiotemporelle appliquée : oui/non (justification)

Points en attente d'arbitrage :
  - <question stratégique 1>
  - <question stratégique 2>
```

## 5. Anti-patterns à éviter (retours d'expérience)

1. **Ne PAS partir de DuckDB pour la découverte** — lire le schema.yml en premier. La sémantique
   métier n'est pas dans les types SQL.
2. **Ne PAS générer une contrainte physique sans la vérifier empiriquement** — la première
   version (`sota >= saves + ga_keeper`) était trop stricte, 12 % de faux positifs.
3. **Ne PAS utiliser le nouveau format `{% test %}` pour les macros custom** — utiliser
   `{% macro test_<name> %}` (ancien style) pour cohérence avec le projet et compatibilité dbt.
4. **Ne PAS oublier `arguments:`** pour les tests custom dans le YAML (dbt ≥ 1.9).
5. **Ne PAS créer de modèle d'audit à partir d'intermediate** — toujours partir de **silver**
   (couche brute alimentée par le scraper).
6. **Ne PAS oublier la saison courante** dans les tests de dérive → `exclude_partitions:` obligatoire.
7. **Ne PAS écrire le mot "leakage" sans le mesurer** — les colonnes de saison agrégée sont
   suspectes, les colonnes par match ne le sont pas si utilisées en rolling/lag.
8. **Ne PAS supprimer un test existant** sans avoir consigné la raison dans `AUTONOMY_RUN.md`.

## 6. Convention de nommage

- Suite GE : `tests/great_expectations/<layer>/<table>.yml` (ou `_all.yml` + `_big5.yml`)
- Bridge pytest : `tests/unit/test_<table>_ge.py`
- Modèle d'audit : `models/audits/scraping_targets_<table>.sql`
- Le layer utilisé dans le chemin est le schéma DuckDB (`silver`, `intermediate`, `gold`,
  `marts`, `machine_learning`).

## 7. Autorisation d'action

Le skill autorise uniquement à :
- Éditer les blocs `- name: <table>` dans les `schema.yml` (dbt structure)
- Créer les YAML de suite GE
- Créer les bridges pytest
- Créer les modèles d'audit `scraping_targets_*`

Le skill **N'AUTORISE PAS** à :
- Modifier `silver.*` ou les scripts d'ingest
- Modifier `process_common.py` ou tout code Python de pipeline
- Corriger les seeds directement (créer un test qui détecte, laisser la correction humaine)
- Rebuild `--full-refresh` du warehouse
- Supprimer des tests existants sans consignation dans `AUTONOMY_RUN.md`
