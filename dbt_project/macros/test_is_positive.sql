{% macro test_is_positive(model, column_name) %}
-- Retourne les lignes où la valeur est strictement négative
-- Une métrique négative (buts, tirs, arrêts) est une corruption de données

SELECT {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
  AND {{ column_name }} < 0

{% endmacro %}