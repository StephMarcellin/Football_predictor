{% macro spatial_zones(filter_condition='e.is_touch = TRUE', prefix='') %}
{% set x_bounds = [0, 20, 40, 60, 80, 100] %}
{% set y_bounds = [0, 20, 40, 60, 80, 100] %}

{% for i in range(x_bounds|length - 1) %}
{% for j in range(y_bounds|length - 1) %}
    AVG(CASE
        WHEN {{ filter_condition }}
         AND e.x >= {{ x_bounds[i] }} AND e.x < {{ x_bounds[i+1] }}
         AND e.y >= {{ y_bounds[j] }} AND e.y < {{ y_bounds[j+1] }}
        THEN 1.0 ELSE 0.0
    END) AS {{ prefix }}pct_z{{ i+1 }}_c{{ j+1 }}
    {%- if not (loop.last and i == x_bounds|length - 2) %},{% endif %}
{% endfor %}
{% endfor %}
{% endmacro %}