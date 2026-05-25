# ADR-002 — dbt pour les transformations Gold

## Statut

Adopté

## Contexte

La couche Gold du projet matérialise les features ML finales à partir des tables Silver. Initialement, tout le feature engineering était réalisé dans `03_features.py` : un script Python/Polars monolithique qui chargeait les tables Silver, calculait les rolling windows, les features WhoScored, les features draw, et écrivait `gold.features_final` dans DuckDB.

Ce script fonctionnait, mais présentait plusieurs frictions au fil des itérations :

- **Aucune traçabilité des dépendances** : il était difficile de savoir quelle table Silver alimentait quelle feature Gold sans lire le code entier
- **Pas de tests de données automatiques** : une valeur aberrante dans Silver se propageait silencieusement jusqu'au modèle
- **Pas de documentation générée** : les transformations n'étaient documentées que dans les commentaires de code
- **`os.chdir()` au niveau module** : un side-effect critique qui rendait l'orchestration complexe (voir ADR-004 Prefect)

Le projet est aussi un **projet d'apprentissage** visant à acquérir des compétences data engineering en conditions réelles. L'intégration de dbt répond à cet objectif pédagogique autant qu'aux besoins fonctionnels.

### Options considérées

|Option|Profil|
|---|---|
|**dbt Core + dbt-duckdb**|Transformations SQL déclaratives, tests de données, documentation auto, lineage|
|**SQLAlchemy + scripts SQL**|SQL dans Python, pas de framework, pas de tests natifs|
|**Conserver 03_features.py**|Python/Polars, connu, fonctionnel, mais sans les apports ci-dessus|
|**Great Expectations seul**|Tests de données uniquement, pas de transformations|
|**Spark SQL**|Overkill pour ce volume|

---

## Décision

**dbt Core avec l'adaptateur `dbt-duckdb`** est adopté pour toutes les transformations **SQL** de la couche Gold.

Le périmètre de dbt est strictement délimité :

- **Dans le périmètre dbt** : déclaration des sources Silver, modèles SQL Gold (backbone, rolling features, WhoScored features, draw features, features_final), tests de schema et de données, seeds (référentiels statiques)
- **Hors périmètre dbt** : les scripts Python/Polars Silver (`02_process.py`, `01b_odds.py`) — dbt ne peut pas remplacer de la logique Python

---

## Justification

### Transformations déclaratives et lineage automatique

Avec dbt, chaque transformation est un fichier `.sql` qui déclare ses dépendances via `{{ ref('model_name') }}` ou `{{ source('schema', 'table') }}`. dbt construit automatiquement le DAG d'exécution et peut l'exécuter dans le bon ordre, en parallèle si les dépendances le permettent.

```sql
-- models/gold/features_rolling.sql
SELECT
    b.*,
    AVG(b.xg) OVER (
        PARTITION BY b.team
        ORDER BY b.date
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS xg_roll_5
FROM {{ ref('stg_backbone') }} b
```

Le lineage est visualisable dans la documentation dbt auto-générée : on peut voir exactement quel modèle dépend de quelle source.

### Tests de données natifs

dbt propose deux niveaux de tests :

**Tests génériques** (dans `schema.yml`) :

```yaml
models:
  - name: features_final
    columns:
      - name: result_1n2
        tests:
          - not_null
          - accepted_values:
              values: ['H', 'D', 'A']
      - name: final_match_id
        tests:
          - unique
          - not_null
```

**Tests singuliers** (fichiers SQL dans `tests/`) : requêtes SQL qui doivent retourner 0 ligne pour passer. Ex. "aucun match avec xg_roll_5 > 20" ou "pas de doublons home/away pour le même match_id".

Dans `run_pipeline.py`, `dbt test` est exécuté **après** `dbt run` et **avant** `train` — c'est un gate de qualité : si les 235 tests échouent, le modèle ML n'est pas ré-entraîné sur des données corrompues.

> [!info] Pourquoi 235 tests ? Le nombre de tests dbt dans ce projet est le résultat de l'accumulation : chaque nouvelle feature ajoutée à `features_final` apporte ses propres tests `not_null`, ses `accepted_values`, et parfois des tests d'intervalle. C'est un indicateur de maturité du projet — plus il y a de tests, plus la couche Gold est fiable.

### Séparation des responsabilités

dbt ne remplace pas Python — il le complète. La règle adoptée dans ce projet est claire :

|Responsabilité|Outil|
|---|---|
|Normalisation des noms, slugification, parsing de saison|Python/Polars (`02_process.py`)|
|Chargement des CSV bruts, encodage, types|Python/Polars (`01_ingest.py`, `01b_odds.py`)|
|Transformations SQL (joins, agrégations, window functions)|dbt|
|Tests de données|dbt|
|Entraînement, inférence, backtest|Python/scikit-learn/LightGBM|

> [!warning] Contrainte architecturale critique dbt ne peut pas remplacer `02_process.py`. Les opérations de normalisation des noms d'équipes (team_mapping, slugification), de parsing des saisons, et d'encodage des résultats sont de la logique Python qui n'a pas d'équivalent SQL propre. Tenter de les implémenter en SQL dans dbt produirait du code fragile et illisible. dbt prend le relais à partir du moment où les tables Silver sont propres et structurées.

### Documentation auto-générée

`dbt docs generate` produit un site statique avec :

- Le catalogue de toutes les tables et leurs colonnes avec descriptions
- Le DAG de lineage interactif (visualisation des dépendances)
- Les résultats des derniers tests

C'est particulièrement utile pour l'onboarding d'un collaborateur ou pour reprendre le projet après une longue pause.

### Seeds : référentiels statiques versionnables

Les seeds dbt sont des fichiers CSV dans `seeds/` qui sont chargés comme tables dans DuckDB. Dans ce projet : `team_mapping.csv` et `transfermarkt_clubs.csv`. L'avantage par rapport à `config.yaml` : ces référentiels sont des tables SQL requêtables dans les modèles dbt, avec les mêmes tests de données que n'importe quelle autre table.

---

## Architecture dbt du projet

```
dbt_project/
├── models/
│   ├── staging/
│   │   └── stg_backbone.sql          ← pivot central : 1 ligne par match
│   └── gold/
│       ├── features_rolling.sql      ← moyennes mobiles 3/5/10 matchs
│       ├── features_whoscored.sql    ← features WhoScored (pressing, xG)
│       ├── features_draw.sql         ← features spécifiques nuls
│       └── features_final.sql        ← join final → gold.features_final
├── seeds/
│   ├── team_mapping.csv
│   └── transfermarkt_clubs.csv
├── tests/
│   └── *.sql                         ← tests singuliers
├── schema.yml                        ← tests génériques + descriptions
└── dbt_project.yml                   ← configuration projet
```

### Séquence d'exécution dans le pipeline

```
dbt seed    → charge team_mapping + transfermarkt_clubs dans DuckDB
dbt run     → exécute backbone → rolling → whoscored → draw → features_final
dbt test    → valide 235 tests sur gold.features_final
```

---

## Conséquences

### Positives

- Transformations Gold lisibles par quiconque connaît SQL, sans lire du Python
- 235 tests automatiques exécutés à chaque run — détection précoce des corruptions
- Lineage documenté et visualisable
- Séparation nette Python (ingestion/normalisation) / SQL (transformation/agrégation)
- Compatible avec les outils de l'écosystème dbt (dbt Cloud, Elementary, re_data)

### Négatives et limites

- **Courbe d'apprentissage** : dbt introduit ses propres conventions (ref, source, schema.yml, profiles.yml)
- **Deux langages pour la couche Gold** : certaines features complexes (sterility_index, press_resistance) resteraient plus lisibles en Python. Le SQL pour des features composites peut devenir verbeux.
- **Profiles.yml hors repo** : le fichier `~/.dbt/profiles.yml` n'est pas versionné (il contient le chemin DuckDB local). Chaque développeur doit le configurer manuellement.
- **Pas de hot-reload** : modifier un modèle SQL requiert de relancer `dbt run --select model_name`. Pas de rechargement automatique.

---

## Questions d'entretien anticipées

**"Qu'est-ce que dbt et pourquoi l'utilise-t-on ?"**

dbt (data build tool) est un framework de transformation de données qui permet d'écrire des transformations SQL sous forme de modèles versionnable, avec gestion des dépendances, tests automatiques, et documentation générée. Son principe : "write SQL selects, dbt handles the rest" — on écrit uniquement la logique de transformation, dbt gère la matérialisation (CREATE TABLE AS SELECT), l'ordre d'exécution, et le testing. Il est devenu un standard dans les équipes data engineering modernes.

**"Quelle est la différence entre un modèle dbt, un seed et un test ?"**

Un **modèle** est un fichier `.sql` qui contient un SELECT — dbt le matérialise en table ou vue dans la base de données. Un **seed** est un fichier CSV dans `seeds/` que dbt charge comme table — utile pour les référentiels statiques (mapping d'équipes, listes de ligues). Un **test** est soit une contrainte générique déclarée dans `schema.yml` (not_null, unique, accepted_values), soit une requête SQL dans `tests/` qui doit retourner 0 ligne pour passer.

**"Qu'est-ce que le lineage dans dbt et pourquoi est-ce important ?"**

Le lineage est la cartographie des dépendances entre tables : "cette table Gold dépend de cette table Silver qui dépend de cette source Bronze". dbt le construit automatiquement à partir des `{{ ref() }}` dans les modèles. C'est important pour trois raisons : (1) comprendre l'impact d'une modification upstream (si je change `stg_backbone`, quels modèles Gold sont affectés ?), (2) déboguer une anomalie (remonter la chaîne pour trouver où la valeur incorrecte est introduite), (3) onboarder rapidement un nouveau membre sur le projet.

**"Comment dbt s'intègre-t-il dans un pipeline ML ?"**

dbt s'intègre en amont du modèle ML, dans la phase de feature engineering. Il prend en charge toutes les transformations SQL (joins, agrégations, window functions) et produit une table Gold propre et testée. Le modèle ML consomme cette table comme input. L'avantage : le code de transformation est séparé du code ML, versionné, testable, et réutilisable. Dans ce projet, `dbt test` est un gate de qualité obligatoire — si des tests échouent, le modèle n'est pas réentraîné, évitant d'apprendre sur des données corrompues.

**"Qu'est-ce que `{{ ref() }}` dans dbt ?"**

`{{ ref('model_name') }}` est la macro dbt pour référencer un autre modèle. dbt la résout au moment de la compilation vers le nom complet de la table (ex. `gold.features_rolling`). L'avantage : dbt peut ainsi construire le DAG de dépendances et s'assurer d'exécuter les modèles dans le bon ordre. Si on écrivait le nom de table en dur, dbt ne pourrait pas détecter la dépendance.