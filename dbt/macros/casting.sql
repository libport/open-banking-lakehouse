{% macro to_numeric(expr) -%}
(
  CASE
    WHEN NULLIF({{ expr }}, '') IS NULL THEN NULL
    WHEN {{ expr }} ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ({{ expr }})::numeric
    ELSE NULL
  END
)
{%- endmacro %}

{% macro to_timestamptz(expr) -%}
(
  CASE
    WHEN NULLIF({{ expr }}, '') IS NULL THEN NULL
    ELSE ({{ expr }})::timestamptz
  END
)
{%- endmacro %}
