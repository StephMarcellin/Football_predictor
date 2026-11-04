{% macro test_is_positive(model, column_name) %}
-- Retourne les lignes où la valeur est strictement négative
-- TRY_CAST : colonnes stockées en VARCHAR (silver / refonte) ; une valeur non numérique est ignorée.
-- Une métrique négative (buts, tirs, arrêts) est une corruption de données

SELECT {{ column_name }}
FROM {{ model }}
WHERE TRY_CAST({{ column_name }} AS DOUBLE) IS NOT NULL
  AND TRY_CAST({{ column_name }} AS DOUBLE) < 0

{% endmacro %}