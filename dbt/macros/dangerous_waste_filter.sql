{% macro dangerous_waste_filter(
        model_name
    ) -%}

    {% if model_name == "bsdd" %}
            (match(waste_details_code,'(?i).*\*$')
            OR waste_details_pop
            OR waste_details_is_dangerous )
    {% elif model_name in ["bordereaux_enriched","registry"] %}
            (match(waste_code,'(?i).*\*$')
            OR coalesce(waste_pop,false)
            OR coalesce(waste_is_dangerous,false))
    {% elif model_name == "bsda" %}
        (match(waste_code,'(?i).*\*$')
        OR waste_pop)
    {% else %}
        (match(waste_code,'(?i).*\*$'))
    {% endif %}
    
{%- endmacro %}
