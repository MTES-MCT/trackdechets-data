{% macro get_non_final_status_bordereaux() -%}
    (
        'CANCELED',
        'SEALED',
        'DRAFT',
        'INITIAL',
        'SIGNED_BY_PRODUCER'
    )
{%- endmacro %}
