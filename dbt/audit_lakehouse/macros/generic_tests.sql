{#- Small generic tests so the project needs no external packages (no dbt deps). -#}

{% test non_negative(model, column_name) %}
select {{ column_name }}
from {{ model }}
where {{ column_name }} < 0
{% endtest %}

{% test unique_combination(model, combination_of_columns) %}
select {{ combination_of_columns | join(', ') }}, count(*) as n
from {{ model }}
group by {{ combination_of_columns | join(', ') }}
having count(*) > 1
{% endtest %}
