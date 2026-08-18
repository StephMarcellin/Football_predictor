---
name: data-map
description: >
  Génère ou rafraîchit une carte Obsidian du projet dbt : une note markdown par
  table, avec son rôle, son lineage en [[wikilinks]] (pour la vue graphe Obsidian)
  et le profiling de chaque feature (complétion %, nulls, distinct, distribution).
  Utilise ce skill dès que l'utilisateur veut documenter ou cartographier ses tables,
  visualiser le lineage silver→intermediate→gold, comprendre ce que fait une table
  ou une feature, rafraîchir la « data map », ou analyser la complétion / qualité
  des colonnes — même s'il ne dit pas explicitement « Obsidian » ou « skill ».
---

# data-map — carte Obsidian du projet dbt

## Ce que fait ce skill

Il **écrit ou rafraîchit** un ensemble de notes markdown (une par table) dans un
dossier de sortie (`docs/data_map/` par défaut). L'opération est **idempotente** :
chaque exécution réécrit les notes en place à partir de l'état courant du projet et
de la base — il n'y a pas de « nouveau vault » créé à chaque appel, on met à jour
le même dossier, que l'utilisateur ouvre une fois pour toutes comme vault Obsidian.

L'intérêt par rapport à `dbt docs` : dans la **même vue**, on a le lineage ET
l'analyse de chaque feature (profiling + contrôle d'intégrité). La vue graphe
d'Obsidian, alimentée par les `[[wikilinks]]`, donne la carte cliquable ; chaque
note donne le rôle de la table, un contrôle de doublons, et le profiling colonne
par colonne. Le script émet aussi un `_data_map.json` (mêmes données en format
machine) pour qu'un futur agent d'analyse puisse consommer le profiling sans parser
le markdown.

## Comment l'exécuter

Le travail est fait par un script — ne pas régénérer les notes à la main.

```bash
python skills/data-map/scripts/gen_data_map.py
```

Options utiles :
- `--models a,b,c` : ne régénérer que ces tables (sinon toutes).
- `--out <dossier>` : dossier de sortie (défaut : `docs/data_map/`).
- `--basic` : profiling basique (sans quantiles ni top-valeurs).

Le script lit les `.sql` (pour le lineage via `ref()`/`source()`), les `schema.yml`
(pour les descriptions), et interroge la base DuckDB (`db/football.duckdb`) pour le
profiling. Il écrit une note par table + une note d'index `_Data Map.md`.

## Règles de format (le contrat)

Chaque note de table suit **exactement** cette structure — c'est ce qui rend le
vault cohérent et régénérable :

1. **Frontmatter** : `schema` (gold/intermediate/…) et `rows` (nb de lignes).
2. **Titre** = nom de la table, puis **tags** : `#<schema>` et `#famille<N>` où N
   est tiré de la *description de la table* uniquement (pas des colonnes, sinon on
   récupère de faux tags comme « imputation famille 11 » mentionnée en passant).
3. **Description** de la table (depuis `schema.yml`).
4. **Lineage** : `Sources :` (les `ref()`/`source()` en `[[wikilinks]]`) et
   `Alimente :` (les tables enfants). C'est ce qui construit le graphe.
5. **Features & profiling** : un tableau, une ligne par colonne, avec :
   `Feature | Type | Complétion % | Null | Distinct | Stats | Description`.

Profiling par colonne :
- Toujours : complétion %, nb de nulls, nb de valeurs distinctes.
- Colonnes numériques : moyenne, médiane, min/max, et (mode étendu) p10/p90.
- Colonnes catégorielles (peu de distinct) : (mode étendu) top-3 des valeurs.
- **Ne PAS** calculer moyenne/médiane/quantiles sur les colonnes d'identifiant
  (`*_id`, `match_id`…) : c'est du bruit sans intérêt.

La note d'index `_Data Map.md` liste les tables groupées par schéma, chacune en
`[[wikilink]]`, pour servir de point d'entrée du vault.

## Quand régénérer

À chaque évolution des modèles (nouvelle table, nouvelles colonnes, rechargement
des données). Le profiling reflète l'état réel de la base au moment du run, donc
c'est aussi un contrôle qualité : une complétion qui chute ou une distribution qui
dérive se voit immédiatement.
