{{
    config(
        materialized='table',
        schema='intermediate',
        alias='intermediate_possession_chains'
    )
}}

{#
  Chaînes de possession — CALCULÉES PAR SPARK, relues ici.

  La logique (12 CTE, fenêtres LAG/LEAD, auto-jointures des duels aériens) vit
  désormais dans pipelines/spark/spark_possession_chains.py. En dbt/DuckDB le
  modèle dépassait 2 h 30 par run et saturait le spill (37 Go) ; le port Spark a
  été validé bloc par bloc contre ce SQL, puis ligne à ligne contre la table
  dbt de dev (2022-2023 et 2023-2024) : 0 différence sur les 21 colonnes.

  Ancienne version SQL : historique git de ce fichier.

  Contrat de sortie INCHANGÉ (mêmes 23 colonnes, mêmes types) : aucun modèle aval
  n'est à modifier. Le Parquet est déjà écrit sous les noms refondus
  (str_, int_, dec_, dt_, bool_ ; partition str_season) : aucun renommage ici.

  Prérequis : l'étape `spark_possession_chains` de l'orchestrateur doit avoir
  tourné AVANT ce modèle (elle suit spark_events).

  Résolution du dossier : --vars (injecté par dbt_helpers depuis ROOT_DIR)
  → variable d'environnement SPARK_OUT_DIR → sentinelle (voir
  intermediate_whoscored_events_enriched pour le raisonnement).
#}

{% set spark_out = var('spark_out_dir',
                       env_var('SPARK_OUT_DIR', 'SPARK_OUT_DIR_NON_DEFINI')) %}

SELECT * FROM read_parquet(
    '{{ spark_out }}/int_possession_chains/**/*.parquet',
    hive_partitioning = 1
)
