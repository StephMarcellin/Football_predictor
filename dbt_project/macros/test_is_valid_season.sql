-- dbt_project/macros/test_is_valid_season.sql
{% test is_valid_season(model, column_name) %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} is not null
  and not regexp_matches({{ column_name }}::varchar, '^\d{4}-\d{4}$')

{% endtest %}