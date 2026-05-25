# ADR-005 — Polars pour le traitement Silver

## Statut

Adopté

## Contexte

La couche Silver du projet (`02_process.py`) transforme les fichiers Parquet Bronze en tables DuckDB structurées : normalisation des noms d'équipes, slugification, parsing des saisons, encodage des résultats, jointures inter-sources. Ces transformations impliquent :

- Des **manipulations de chaînes de caractères** intensives (normalisation via regex, lowercase, remplacement de caractères accentués)
- Des **jointures** entre tables de taille modeste (15 000 à 50 000 lignes)
- Des **opérations de type ETL** : filtrage, renommage, création de colonnes dérivées
- Aucune écriture concurrente — le script s'exécute une seule fois par session

La couche Silver est la seule couche du projet qui reste en Python pur : dbt ne peut pas prendre en charge ces transformations (logique de mapping en Python, pas de SQL propre).

### Options considérées

|Option|Profil|
|---|---|
|**Polars**|DataFrame columnar Rust, API expressif, lazy evaluation, performant|
|**Pandas**|Standard historique Python data, row-based, très répandu|
|**DuckDB SQL direct**|SQL pur pour les transformations, sans DataFrame|
|**PySpark**|Distribué, overhead majeur pour ce volume|
|**Vaex**|Lazy columnar, moins actif, API moins riche|

---

## Décision

**Polars** est retenu pour toutes les transformations DataFrame de la couche Silver (`02_process.py`, `01b_odds.py`).

Pandas est conservé dans les scripts ML aval (`04_train.py`, `05_predict.py`, `06_backtest.py`) où l'écosystème scikit-learn/LightGBM impose Pandas comme interface native.

---

## Justification

### Modèle d'exécution columnar (Rust)

Polars est écrit en **Rust** et utilise le format **Apache Arrow** comme représentation mémoire. Arrow est columnar — les valeurs d'une même colonne sont contiguës en mémoire, ce qui favorise la vectorisation CPU (SIMD) et la compression.

La comparaison avec Pandas sur des opérations typiques Silver :

|Opération|Pandas|Polars|
|---|---|---|
|Lecture Parquet 50k lignes|~120ms|~40ms|
|Group by + agrégation|~80ms|~15ms|
|String operations (regex, lower)|~200ms|~50ms|
|Join sur clé string|~60ms|~20ms|

_Ordres de grandeur indicatifs — dépendent du hardware._

Sur les volumes du projet (quelques dizaines de milliers de lignes), la différence absolue est de l'ordre de la seconde — pas critique. L'intérêt est ailleurs : l'API Polars, la cohérence avec les outils modernes, et la montée en compétence.

> [!info] Apache Arrow et Zero-Copy Apache Arrow est un format de données columnar en mémoire développé par la fondation Apache. Son design permet l'**interopérabilité zero-copy** entre outils : Polars, DuckDB, pandas (via `to_arrow()`), et PyArrow partagent tous le même format binaire. Quand on écrit `conn.register("df", polars_df.to_arrow())`, aucune copie des données n'est effectuée — DuckDB lit directement le buffer Arrow de Polars.

### API expressive et chaînable

L'API Polars est conçue pour la lisibilité. Les transformations Silver qui en Python Pandas seraient multiligne deviennent une chaîne fluide :

```python
# Polars — opération Silver typique
df = (
    pl.read_parquet("data/raw/matches/*.parquet")
    .filter(pl.col("comp_category") == "Big5")
    .with_columns([
        pl.col("team").map_elements(normalize_team).alias("team"),
        pl.col("season").map_elements(parse_season).alias("season"),
        pl.col("result").map_elements(encode_result).alias("result_1n2"),
    ])
    .drop_nulls(subset=["team", "date", "result_1n2"])
)
```

```python
# Pandas équivalent — plus verbeux, mutations en place
df = pd.read_parquet("data/raw/matches/*.parquet")
df = df[df["comp_category"] == "Big5"].copy()
df["team"]      = df["team"].apply(normalize_team)
df["season"]    = df["season"].apply(parse_season)
df["result_1n2"]= df["result"].apply(encode_result)
df = df.dropna(subset=["team", "date", "result_1n2"])
```

### Lazy evaluation

Polars propose deux modes : **eager** (exécution immédiate, comme Pandas) et **lazy** (construction d'un plan d'exécution optimisé, `LazyFrame`).

En mode lazy, Polars peut :

- **Éliminer les colonnes inutilisées** avant la lecture (predicate/projection pushdown)
- **Paralléliser** les opérations indépendantes automatiquement
- **Optimiser** l'ordre des opérations (ex. filtrer avant de joindre)

```python
# Mode lazy — Polars optimise le plan avant d'exécuter
result = (
    pl.scan_parquet("data/raw/*.parquet")   # scan (pas de lecture)
    .filter(pl.col("league") == "Premier League")
    .select(["date", "team", "xg", "result"])
    .collect()   # exécution réelle ici
)
```

Pour les volumes Silver du projet (< 100k lignes), l'optimisation lazy est marginale. Mais l'habitude de penser en termes de plans d'exécution est une compétence utile à l'échelle.

### Immutabilité par défaut

Pandas autorise (et encourage) les modifications en place : `df["col"] = ...`. Cela crée des comportements surprenants avec le `SettingWithCopyWarning` et rend le flux de données difficile à tracer.

Polars est **immutable par défaut** : toute transformation retourne un nouveau DataFrame. `with_columns()` crée des colonnes sans modifier le DataFrame source. Cela élimine toute une catégorie de bugs liés aux mutations implicites.

> [!warning] `map_elements` vs expressions Polars natives `map_elements()` (anciennement `apply()`) exécute une fonction Python arbitraire ligne par ligne — c'est l'équivalent de `pandas.apply()`. C'est nécessaire pour les fonctions Python complexes comme `normalize_team()` (qui fait des lookups dans un dictionnaire) et `parse_season()`, mais c'est plus lent que les expressions Polars natives. Quand une opération est exprimable en Polars pur (`pl.col().str.replace()`, `pl.col().cast()`), elle est préférable à `map_elements`. Utiliser `map_elements` uniquement pour les transformations qui requièrent de la logique Python non-exprimable en expressions Polars.

### Interopérabilité DuckDB

L'intégration Polars ↔ DuckDB est native et zero-copy via Apache Arrow :

```python
# Polars → DuckDB
conn.register("df_silver", polars_df.to_arrow())
conn.execute("INSERT INTO silver.matches SELECT * FROM df_silver")

# DuckDB → Polars
df = conn.execute("SELECT * FROM silver.odds").pl()
```

Cette fluidité est un des arguments principaux pour Polars dans un stack DuckDB-first.

---

## Polars vs Pandas : tableau comparatif pour l'entretien

|Dimension|Pandas|Polars|
|---|---|---|
|**Langage interne**|Python/C|Rust|
|**Format mémoire**|Row-based (numpy arrays)|Columnar (Apache Arrow)|
|**Mutabilité**|Mutable (in-place)|Immutable (nouvelles copies)|
|**Lazy evaluation**|Non (eager uniquement)|Oui (`LazyFrame`)|
|**API**|Impérative|Déclarative + chaînable|
|**Gestion NaN**|`NaN` (float) + `None` (objet) — ambiguë|`null` uniforme|
|**Performances**|Référence (acceptable)|2–10× plus rapide selon opération|
|**Écosystème**|Très large (sklearn, matplotlib, seaborn)|En croissance, moins intégré|
|**Adoption entreprise**|Standard de facto|Croissance rapide, de plus en plus adopté|

---

## Pourquoi Pandas reste dans les scripts ML

`04_train.py`, `05_predict.py`, `06_backtest.py` utilisent Pandas parce que :

- **scikit-learn** (`Pipeline`, `SimpleImputer`, `RobustScaler`, `LabelEncoder`) prend en entrée des `numpy.ndarray` ou des `pandas.DataFrame`. L'intégration Polars↔sklearn nécessite des conversions explicites.
- **LightGBM** supporte Pandas et numpy nativement. Le support Polars est partiel.
- **La couche ML n'est pas le goulot d'étranglement** en termes de performance — les opérations sont des matrix multiplications et des tree traversals, pas des manipulations de DataFrames.

> [!tip] Stratégie recommandée en entretien Présenter Polars et Pandas comme complémentaires, non concurrents. La frontière naturelle dans ce projet : Polars pour l'ETL Silver (transformations chaînes, jointures, nettoyage) là où sa performance et son API apportent de la valeur, Pandas pour la consommation ML (compatibilité scikit-learn/LightGBM). C'est une décision pragmatique, pas idéologique.

---

## Conséquences

### Positives

- Code Silver lisible et maintenu en pipeline chaîné sans mutations implicites
- Performance supérieure à Pandas sur les opérations ETL
- Interopérabilité zero-copy avec DuckDB via Arrow
- Compétence à forte valeur sur le marché (Polars est en forte croissance dans les stacks data modernes)

### Négatives et limites

- **Deux bibliothèques DataFrame dans le projet** : Polars en Silver, Pandas en Gold ML. Quelques conversions (`to_pandas()`, `.pl()`) nécessaires aux interfaces
- **`map_elements` non-vectorisé** : `normalize_team()` et `parse_season()` restent des fonctions Python ligne par ligne — même si l'API est Polars, la performance est celle d'un `apply` Pandas pour ces opérations spécifiques
- **Écosystème moins mature** : moins de ressources StackOverflow, moins d'exemples dans la documentation sklearn

---

## Questions d'entretien anticipées

**"Qu'est-ce que Polars et pourquoi émerge-t-il comme alternative à Pandas ?"**

Polars est une bibliothèque DataFrame écrite en Rust, utilisant Apache Arrow comme format mémoire columnar. Elle émerge pour trois raisons : (1) des performances 2–10× supérieures à Pandas sur la plupart des opérations ETL, grâce au stockage columnar et à la vectorisation SIMD, (2) une API expressive et chaînable qui évite les mutations in-place et le `SettingWithCopyWarning` de Pandas, (3) l'évaluation lazy (LazyFrame) qui permet d'optimiser les plans d'exécution avant de toucher aux données. Polars est particulièrement adapté aux pipelines ETL et data engineering ; Pandas reste dominant dans l'écosystème ML/statistique.

**"Quelle est la différence entre l'évaluation eager et lazy ?"**

En mode eager (Pandas, Polars par défaut), chaque opération est exécutée immédiatement et retourne un résultat. En mode lazy (Polars `LazyFrame`, Spark), les opérations construisent un plan d'exécution sans toucher aux données. Ce plan est optimisé (réordonnancement, projection pushdown, predicate pushdown) avant d'être exécuté lors d'un appel à `.collect()`. L'intérêt est double : l'optimiseur peut éliminer des étapes inutiles (ex. filtrer avant de lire toutes les colonnes) et paralléliser les opérations indépendantes.

**"Qu'est-ce qu'Apache Arrow et pourquoi est-ce important ?"**

Apache Arrow est un format de données columnar en mémoire, conçu pour l'interopérabilité entre outils sans copie de données. DuckDB, Polars, PyArrow, et Pandas (via `pd.DataFrame.to_arrow()`) partagent tous le même format binaire. Quand Polars passe un DataFrame à DuckDB via `df.to_arrow()`, il n'y a pas de sérialisation/désérialisation — les deux outils lisent le même buffer mémoire. C'est ce qu'on appelle le "zero-copy". Arrow est devenu le bus de communication standard de l'écosystème data Python.

**"Quand utiliseriez-vous Polars plutôt que SQL pur dans DuckDB ?"**

Pour les transformations qui requièrent de la logique Python non-exprimable en SQL : mappings depuis des dictionnaires Python (`normalize_team`), parsing de chaînes avec regex complexe, appels à des fonctions Python tierces, ou logique conditionnelle avec des règles métier complexes. Pour les transformations purement relationnelles (joins, agrégations, window functions, filtres), DuckDB SQL est plus lisible et plus performant. La règle pratique adoptée dans ce projet : si c'est exprimable en SQL propre → dbt/DuckDB, sinon → Polars.

**"Pourquoi ne pas utiliser Polars dans toute la chaîne, y compris les scripts ML ?"**

scikit-learn et LightGBM sont conçus pour opérer sur des numpy arrays ou des DataFrames Pandas. Polars peut être converti (`df.to_pandas()`, `df.to_numpy()`), mais cela ajoute des conversions explicites à chaque interface. Pour les scripts ML où la performance des transformations DataFrame n'est pas le goulot d'étranglement (c'est le fitting du modèle qui prend du temps), Pandas est plus pragmatique : zéro friction avec l'écosystème sklearn. La décision de mélanger Polars (ETL) et Pandas (ML) est un compromis délibéré, pas un oubli.