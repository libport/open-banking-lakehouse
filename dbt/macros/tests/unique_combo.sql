{% test unique_combo(model, combination_of_columns) %}
  select
    {% for col in combination_of_columns -%}
      {{ col }}{% if not loop.last %}, {% endif %}
    {%- endfor %},
    count(*) as n_records
  from {{ model }}
  group by
    {% for col in combination_of_columns -%}
      {{ col }}{% if not loop.last %}, {% endif %}
    {%- endfor %}
  having count(*) > 1
{% endtest %}
