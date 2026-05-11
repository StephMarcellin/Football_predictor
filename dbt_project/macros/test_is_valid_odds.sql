{% macro test_is_valid_odds(model, column_name) %}
-- Une cote valide est toujours > 1.0
-- Une cote <= 1.0 est mathématiquement impossible

SELECT {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
  AND {{ column_name }} <= 1.0

{% endmacro %}