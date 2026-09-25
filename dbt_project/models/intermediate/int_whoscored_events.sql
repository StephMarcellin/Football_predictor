{{
    config(
        materialized='table',
        schema='intermediate',
        alias='int_whoscored_events'
    )
}}

{#
  Chaîne de résolution : --vars (injecté par dbt_helpers depuis ROOT_DIR)
  → variable d'environnement SPARK_OUT_DIR → sentinelle.

  Pas de raise_compiler_error : il casserait la compilation de l'extension
  VS Code, qui ne passe ni par l'orchestrateur ni par .env. La sentinelle
  compile sans broncher — ce n'est qu'une chaîne — mais DuckDB refusera de
  créer la vue et l'erreur nommera la variable manquante.
#}
-- Refonte nommage : le job Spark (pipelines/spark/spark_events.py) écrit déjà les
-- colonnes sous la convention du projet (str_, int_, dec_, dt_, bool_ ; partition
-- str_season), d'après docs/proposition_nommage_v2.csv. Aucun renommage ici.

{% set spark_out = var('spark_out_dir',
                       env_var('SPARK_OUT_DIR', 'SPARK_OUT_DIR_NON_DEFINI')) %}

SELECT * FROM read_parquet(
    '{{ spark_out }}/int_whoscored_events/**/*.parquet',
    hive_partitioning = 1
)