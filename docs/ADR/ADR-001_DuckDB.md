# ADR-001 — DuckDB comme base de données analytique

## Statut

Adopté

## Contexte

Le projet nécessite un stockage structuré pour les trois couches de données (Bronze → Silver → Gold) et pour les résultats intermédiaires (features, cotes, prédictions). Les données sont exclusivement analytiques : pas d'écritures concurrentes, pas de transactions multiples, des lectures massives en colonnes (agrégations, joins multi-tables, fenêtres temporelles).

Le volume de données est modeste : environ 30 000 à 60 000 lignes dans `gold.features_final` (5 ligues × 8 saisons × ~380 matchs × 2 lignes par match), des tables Silver d'ordre similaire.

### Options considérées

|Option|Profil|
|---|---|
|**DuckDB**|Base OLAP embarquée, fichier unique, SQL complet, natif Parquet/Pandas/Polars|
|**SQLite**|Base OLTP embarquée, fichier unique, SQL standard, pas de colonnar|
|**PostgreSQL**|Base client-serveur, OLTP + extensions analytiques, infrastructure lourde|
|**Pandas + Parquet**|Pas de SQL, jointures manuelles, formats fichiers|
|**MotherDuck**|DuckDB hébergé dans le cloud, SaaS|

---

## Décision

**DuckDB** est retenu comme unique store de données du projet.

Toutes les tables (bronze, silver, gold) sont des tables DuckDB dans un fichier unique `db/football.duckdb`. Le SQL est le langage de requête pour toutes les transformations Gold et toutes les lectures dans les scripts ML.

---

## Justification

### Modèle d'exécution vectorisé (OLAP)

DuckDB est un moteur **OLAP** (Online Analytical Processing) à exécution columnar. Contrairement à SQLite (OLTP row-store), il stocke les données colonne par colonne et exécute les requêtes en mode vectorisé — il traite des batches de valeurs par opération plutôt qu'une ligne à la fois.

Pour des opérations analytiques typiques du projet (moyennes mobiles sur une fenêtre temporelle, agrégations par équipe/saison, jointures multi-tables), l'exécution columnar est structurellement plus rapide car elle exploite le cache CPU et les instructions SIMD.

> [!info] OLAP vs OLTP 
> **OLTP** (Online Transaction Processing) : optimisé pour de nombreuses petites transactions (INSERT/UPDATE/DELETE) — ex. base d'une application web. Stockage en lignes. 
> **OLAP** (Online Analytical Processing) : optimisé pour des lectures massives sur peu de colonnes — ex. agrégations, analytics. Stockage en colonnes. Les données football de ce projet ne changent qu'une fois par semaine (nouvelles données du week-end) et sont lues des dizaines de fois. C'est un profil OLAP pur.

### Zéro infrastructure

DuckDB est embarqué : un simple fichier `.duckdb` sur le disque. Pas de serveur à démarrer, pas de port à ouvrir, pas de configuration réseau. L'ouverture d'une connexion est instantanée :

```python
conn = duckdb.connect("db/football.duckdb")
```

Cela simplifie radicalement le setup du projet (pas de Docker, pas de `systemctl start postgresql`) et la portabilité (le fichier `.duckdb` peut être copié ou versionné).

### Intégration native avec l'écosystème Python

DuckDB lit et écrit nativement des DataFrames Pandas et Polars **sans copie inutile** (zero-copy sur les Arrow tables) :

```python
# DuckDB → Polars
df = conn.execute("SELECT * FROM silver.odds").pl()

# Polars → DuckDB (register + INSERT)
conn.register("df_odds", polars_df.to_arrow())
conn.execute("INSERT INTO silver.odds SELECT * FROM df_odds")
```

Il peut aussi lire directement des fichiers Parquet, CSV ou JSON depuis le SQL :

```sql
SELECT * FROM read_parquet('data/raw/matches/*.parquet')
```

### SQL complet avec fenêtres analytiques

DuckDB supporte l'intégralité du SQL analytique moderne : window functions, CTEs, QUALIFY, LATERAL joins, LIST aggregates, UNNEST. Les transformations Gold (rolling windows, head-to-head, ratios par saison) sont expressibles directement en SQL sans passer par Pandas :

```sql
AVG(xg) OVER (
    PARTITION BY team
    ORDER BY date
    ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
) AS xg_roll_5
```

### Schémas (schemas DuckDB)

DuckDB supporte les schémas SQL (`bronze`, `silver`, `gold`) comme espaces de noms. Cela structure logiquement les tables sans multiplier les fichiers et correspond exactement à l'architecture Medallion du projet.

---

## Conséquences

### Positives

- Setup en une ligne (`pip install duckdb`) — aucune infrastructure externe
- Performances analytiques comparables à des systèmes distribués pour ces volumes
- Compatibilité directe avec dbt (dbt-duckdb)
- Lisibilité : toutes les transformations sont en SQL standard

### Négatives et limites

- **Un seul writer à la fois** : DuckDB ne supporte pas les écritures concurrentes. Si deux processus tentent d'écrire simultanément (ex. `run_pipeline.py` et un script ad hoc), l'un échoue avec une erreur de lock. Dans ce projet à pipeline séquentiel, ce n'est pas un problème pratique.
- **Pas de streaming** : DuckDB charge les données en mémoire pour les traiter. Pour des datasets > RAM, il faudrait migrer vers Spark ou un système distribué. Au-dessous de ~10 GB, DuckDB reste compétitif.
- **Fichier binaire** : `football.duckdb` n'est pas versionnable avec Git (binaire volumineux). Il est dans `.gitignore` et doit être reconstruit depuis les scripts si nécessaire.
- **Pas de persistance web** : pour exposer les données via une API REST, il faudrait une couche supplémentaire (FastAPI + duckdb en lecture seule, ou migration vers PostgreSQL).

---

## Alternatives rejetées et pourquoi

**SQLite** : row-store, pas de window functions avancées, pas de schémas multi-namespace. Acceptable pour du stockage clé-valeur, sous-optimal pour de l'analytique.

**PostgreSQL** : surpuissant pour ce volume et cette infrastructure. Nécessite un serveur, une gestion des connexions, des migrations. La friction opérationnelle dépasse largement la valeur ajoutée à ce stade.

**Pandas + Parquet** : pas de SQL, jointures complexes nécessitent du code Python verbeux, pas de schéma de données formalisé. Difficulté à interroger ad hoc.

---

## Questions d'entretien anticipées

**"Qu'est-ce que DuckDB et pourquoi l'utiliseriez-vous plutôt que SQLite ?"**

DuckDB est un moteur OLAP embarqué columnar, optimisé pour les requêtes analytiques (agrégations, window functions, joins massifs). SQLite est OLTP row-store, optimisé pour les transactions et les lectures par clé primaire. Sur des requêtes analytiques type "moyenne mobile sur les 5 derniers matchs par équipe", DuckDB est structurellement plus rapide car il n'a pas besoin de lire les colonnes non utilisées. De plus, DuckDB supporte les window functions SQL complètes et l'intégration native avec Pandas/Polars/Parquet.

**"Quand est-ce que vous migreriez de DuckDB vers quelque chose d'autre ?"**

Pour trois cas principaux : (1) si les données dépassent la RAM disponible (DuckDB traite en mémoire, Spark ou BigQuery prendraient le relais), (2) si plusieurs utilisateurs écrivent simultanément (PostgreSQL ou un système transactionnel), (3) si on a besoin d'exposer les données via une API avec des accès concurrents élevés. Pour un projet analytique personnel sous 10 GB, DuckDB est un excellent choix qui ne nécessite aucune infrastructure.

**"Qu'est-ce que l'architecture Medallion (Bronze/Silver/Gold) ?"**

C'est un pattern d'organisation des données en trois couches de qualité croissante. Bronze : données brutes telles qu'ingérées (pas de transformation, fidélité maximale). Silver : données nettoyées et normalisées (noms canoniques, types corrects, doublons supprimés). Gold : données métier agrondies (features engineerées, agrégations, joins entre sources). Chaque couche est lisible indépendamment, ce qui facilite le debug (on peut inspecter exactement à quelle étape une anomalie est introduite).

**"Quelle est la différence entre OLAP et OLTP ?"**

OLTP (Online Transaction Processing) est optimisé pour de nombreuses petites transactions — insertion, mise à jour, lecture par clé primaire. Le stockage est en lignes (toutes les colonnes d'un enregistrement sont contiguës). OLAP (Online Analytical Processing) est optimisé pour des lectures massives sur peu de colonnes — comptages, moyennes, agrégations sur des millions de lignes. Le stockage est en colonnes (toutes les valeurs d'une colonne sont contiguës), ce qui permet de ne lire que les colonnes nécessaires et d'exploiter les instructions vectorielles du CPU. Un pipeline ML analytique est un cas d'usage OLAP par nature.