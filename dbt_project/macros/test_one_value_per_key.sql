{% test one_value_per_key(model, key_column, value_column) %}
-- Retourne les valeurs de key_column pour lesquelles plusieurs value_column distincts
-- existent. Test passe si le SELECT est vide.
--
-- Ex : dans team_mapping, un team_id ne doit avoir qu'un seul club_name canonique.
-- Ex : dans competition_mapping, un competition_name ne doit avoir qu'une seule category.
-- Macro générique, réutilisable partout où l'on veut vérifier une dépendance fonctionnelle.

SELECT
    {{ key_column }} AS key_value,
    COUNT(DISTINCT {{ value_column }}) AS n_distinct,
    LIST(DISTINCT {{ value_column }}) AS distinct_values
FROM {{ model }}
WHERE {{ key_column }} IS NOT NULL
  AND {{ value_column }} IS NOT NULL
GROUP BY {{ key_column }}
HAVING COUNT(DISTINCT {{ value_column }}) > 1

{% endtest %}
