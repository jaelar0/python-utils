{% macro seconds_to_hhmmss(seconds_expr) -%}
printf(
    '%02d:%02d:%02d',
    cast(round({{ seconds_expr }}) as integer) / 3600,
    (cast(round({{ seconds_expr }}) as integer) % 3600) / 60,
    cast(round({{ seconds_expr }}) as integer) % 60
)
{%- endmacro %}
