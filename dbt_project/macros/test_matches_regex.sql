{% macro test_matches_regex(model, column_name, pattern) %}
-- Retourne les lignes où column_name ne matche pas le pattern regex fourni.
-- Test passe si le SELECT est vide.
--
-- Générique et réutilisable : validation de formats de saison (YYYY-YYYY),
-- d'URL, d'identifiants formatés, etc.
--
-- Utilise REGEXP_MATCHES de DuckDB (renvoie BOOLEAN).

SELECT {{ column_name }} AS value
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
  AND NOT REGEXP_MATCHES({{ column_name }}, '{{ pattern }}')

{% endmacro %}
