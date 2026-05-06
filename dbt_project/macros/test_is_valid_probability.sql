{% macro test_is_valid_probability(model, column_name) %}
-- Une probabilité doit être comprise entre 0 et 1
-- En dehors de cette plage, le calcul d'edge est faux

SELECT {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
  AND ({{ column_name }} < 0 OR {{ column_name }} > 1)

{% endmacro %}